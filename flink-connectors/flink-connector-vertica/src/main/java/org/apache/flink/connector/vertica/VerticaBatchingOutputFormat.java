/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.vertica;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.connector.vertica.VerticaOptions.CONNECTION_CHECK_TIMEOUT_SECONDS;

/**
 * A Vertica outputFormat that supports batching records before writing records to database.
 */
@Internal
public class VerticaBatchingOutputFormat<In, JdbcIn, VerticaExec extends VerticaBufferedBatchStatementExecutor<JdbcIn>> extends AbstractVerticaOutputFormat<In> {

	/**
	 * An interface to extract a value from given argument.
	 * @param <F> The type of given argument
	 * @param <T> The type of the return value
	 */
	private static final long serialVersionUID = 1L;
	public static final int DEFAULT_FLUSH_MAX_SIZE = 5000;
	public static final long DEFAULT_FLUSH_INTERVAL_MILLS = 0L;

	private static final Logger LOG = LoggerFactory.getLogger(VerticaBatchingOutputFormat.class);
	public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
		static <T> RecordExtractor<T, T> identity() {
			return x -> x;
		}
	}


	public interface StatementExecutorFactory<T extends VerticaBufferedBatchStatementExecutor<?>> extends Function<RuntimeContext, T>, Serializable {
	}

	private final VerticaExecutionOptions executionOptions;
	private final StatementExecutorFactory<VerticaExec> statementExecutorFactory;
	private final RecordExtractor<In, JdbcIn> jdbcRecordExtractor;

	private transient VerticaExec jdbcStatementExecutor;
	private transient volatile boolean closed = false;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;

	public VerticaBatchingOutputFormat(
		@Nonnull VerticaConnectionProvider connectionProvider,
		@Nonnull VerticaExecutionOptions executionOptions,
		@Nonnull StatementExecutorFactory<VerticaExec> statementExecutorFactory,
		@Nonnull RecordExtractor<In, JdbcIn> recordExtractor) {
		super(connectionProvider);
		this.executionOptions = checkNotNull(executionOptions);
		this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
		this.jdbcRecordExtractor = checkNotNull(recordExtractor);
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 */
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
		if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
			this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("vertica-upsert-output-format"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (VerticaBatchingOutputFormat.this) {
					if (!closed) {
						try {
							flush();
						} catch (Exception e) {
							flushException = e;
						}
					}
				}
			}, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
		}
	}

	private VerticaExec createAndOpenStatementExecutor(StatementExecutorFactory<VerticaExec> statementExecutorFactory) throws IOException {
		VerticaExec exec = statementExecutorFactory.apply(getRuntimeContext());
		try {
			exec.prepareStatements(connection);
		} catch (SQLException e) {
			throw new IOException("unable to open JDBC writer", e);
		}
		return exec;
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public final synchronized void writeRecord(In record) throws IOException {
		checkFlushException();

		try {
			addToBatch(record, jdbcRecordExtractor.apply(record));
			if (executionOptions.getBatchSize() > 0 && jdbcStatementExecutor.getBufferSize() >= executionOptions.getBatchSize()) {
				flush();
			}
		} catch (Exception e) {
			throw new IOException("Writing records to JDBC failed.", e);
		}
	}

	protected void addToBatch(In original, JdbcIn extracted) throws SQLException {
		jdbcStatementExecutor.addToBatch(extracted);
	}

	@Override
	public synchronized void flush() throws IOException {
		checkFlushException();

		for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
			try {
				attemptFlush();
				break;
			} catch (SQLException e) {
				LOG.error("JDBC executeBatch error, retry times = {}", i, e);
				if (i >= executionOptions.getMaxRetries()) {
					throw new IOException(e);
				}
				try {
					if (!connection.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS)) {
						connection = connectionProvider.reestablishConnection();
						jdbcStatementExecutor.closeStatements();
						jdbcStatementExecutor.prepareStatements(connection);
					}
				} catch (Exception excpetion) {
					LOG.error("JDBC connection is not valid, and reestablish connection failed.", excpetion);
					throw new IOException("Reestablish JDBC connection failed", excpetion);
				}
				try {
					Thread.sleep(1000 * i);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IOException("unable to flush; interrupted while doing another attempt", e);
				}
			}
		}
	}

	protected void attemptFlush() throws SQLException {
		jdbcStatementExecutor.executeBatch();
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 */
	@Override
	public synchronized void close() {
		if (!closed) {
			closed = true;

			if (this.scheduledFuture != null) {
				scheduledFuture.cancel(false);
				this.scheduler.shutdown();
			}

			if (jdbcStatementExecutor.getBufferSize() > 0) {
				try {
					flush();
				} catch (Exception e) {
					LOG.warn("Writing records to JDBC failed.", e);
					throw new RuntimeException("Writing records to JDBC failed.", e);
				}
			}

			try {
				if (jdbcStatementExecutor != null) {
					jdbcStatementExecutor.closeStatements();
				}
			} catch (SQLException e) {
				LOG.warn("Close JDBC writer failed.", e);
			}
		}
		super.close();
		checkFlushException();
	}

	public static Builder builder() {
		return new Builder();
	}


	public static class Builder {
		private VerticaOptions options;
		private String[] fieldNames;
		private String[] keyFields;
		private int[] fieldTypes;
		private VerticaExecutionOptions.Builder executionOptionsBuilder = VerticaExecutionOptions.builder();

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(VerticaOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * required, field names of this jdbc sink.
		 */
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, upsert unique keys.
		 */
		public Builder setKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		/**
		 * required, field types of this jdbc sink.
		 */
		public Builder setFieldTypes(int[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setFlushMaxSize(int flushMaxSize) {
			executionOptionsBuilder.withBatchSize(flushMaxSize);
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setFlushIntervalMills(long flushIntervalMills) {
			executionOptionsBuilder.withBatchIntervalMs(flushIntervalMills);
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			executionOptionsBuilder.withMaxRetries(maxRetryTimes);
			return this;
		}
	}

}
