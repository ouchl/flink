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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

@Internal
public class VerticaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "vertica";
	public static final ConfigOption<String> URL = ConfigOptions
		.key("url")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc database url.");
	public static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc table name.");
	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc user name.");
	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("the jdbc password.");
	private static final ConfigOption<String> DRIVER = ConfigOptions
		.key("driver")
		.stringType()
		.defaultValue("com.vertica.jdbc.Driver")
		.withDescription("the class name of the JDBC driver to use to connect to this URL. " +
			"If not set, it will automatically be derived from the URL.");
	// read config options
	private static final ConfigOption<String> SCAN_PARTITION_COLUMN = ConfigOptions
		.key("scan.partition.column")
		.stringType()
		.noDefaultValue()
		.withDescription("the column name used for partitioning the input.");
	private static final ConfigOption<Integer> SCAN_PARTITION_NUM = ConfigOptions
		.key("scan.partition.num")
		.intType()
		.noDefaultValue()
		.withDescription("the number of partitions.");
	private static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND = ConfigOptions
		.key("scan.partition.lower-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the smallest value of the first partition.");
	private static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND = ConfigOptions
		.key("scan.partition.upper-bound")
		.longType()
		.noDefaultValue()
		.withDescription("the largest value of the last partition.");
	private static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions
		.key("scan.fetch-size")
		.intType()
		.defaultValue(0)
		.withDescription("gives the reader a hint as to the number of rows that should be fetched, from" +
			" the database when reading per round trip. If the value specified is zero, then the hint is ignored. The" +
			" default value is zero.");
	private static final ConfigOption<Boolean> SCAN_AUTO_COMMIT = ConfigOptions
		.key("scan.auto-commit")
		.booleanType()
		.defaultValue(true)
		.withDescription("sets whether the driver is in auto-commit mode. The default value is true, per" +
			" the JDBC spec.");
	// write config options
	private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(100)
		.withDescription("the flush max size (includes all append, upsert and delete records), over this number" +
			" of records, will flush data. The default value is 100.");
	private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
		.key("sink.buffer-flush.interval")
		.durationType()
		.defaultValue(Duration.ofSeconds(1))
		.withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The " +
			"default value is 1s.");
	private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
		.key("sink.max-retries")
		.intType()
		.defaultValue(3)
		.withDescription("the max retry times if writing records to database failed.");
	private static final ConfigOption<Boolean> SINK_IGNORE_DELETE = ConfigOptions
		.key("sink.ignore.delete")
		.booleanType()
		.defaultValue(false)
		.withDescription("if ignore delete operation.");
	private static final ConfigOption<String> SINK_EXECUTOR = ConfigOptions
		.key("sink.executor")
		.stringType()
		.defaultValue("merge")
		.withDescription("statement executor.");
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		validateConfigOptions(config);
		VerticaOptions verticaOptions = getVerticaOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		return new VerticaDynamicTableSink(
			verticaOptions,
			getVerticaExecutionOptions(config),
			getVerticaDmlOptions(verticaOptions, physicalSchema),
			physicalSchema);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		validateConfigOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		return new VerticaDynamicTableSource(
			getVerticaOptions(helper.getOptions()),
			getVerticaReadOptions(helper.getOptions()),
			physicalSchema);
	}

	private VerticaOptions getVerticaOptions(ReadableConfig readableConfig) {
		final String url = readableConfig.get(URL);
		final VerticaOptions.Builder builder = VerticaOptions.builder()
			.setDBUrl(url)
			.setTableName(readableConfig.get(TABLE_NAME))
			.setDriverName(readableConfig.get(DRIVER));

		readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
		readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
		return builder.build();
	}

	private VerticaReadOptions getVerticaReadOptions(ReadableConfig readableConfig) {
		final Optional<String> partitionColumnName = readableConfig.getOptional(SCAN_PARTITION_COLUMN);
		final VerticaReadOptions.Builder builder = VerticaReadOptions.builder();
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
			builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
			builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
		}
		readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
		builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
		return builder.build();
	}
	private VerticaExecutionOptions getVerticaExecutionOptions(ReadableConfig config) {
		final VerticaExecutionOptions.Builder builder = new VerticaExecutionOptions.Builder();
		builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
		builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
		builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
		builder.withIgnoreDelete(config.get(SINK_IGNORE_DELETE));
		builder.withExecutor(config.get(SINK_EXECUTOR));
		return builder.build();
	}

	private VerticaDmlOptions getVerticaDmlOptions(VerticaOptions jdbcOptions, TableSchema schema) {
		String[] keyFields = schema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);

		return VerticaDmlOptions.builder()
			.withTableName(jdbcOptions.getTableName())
			.withFieldNames(schema.getFieldNames())
			.withKeyFields(keyFields)
			.build();
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(URL);
		requiredOptions.add(TABLE_NAME);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(DRIVER);
		optionalOptions.add(USERNAME);
		optionalOptions.add(PASSWORD);
		optionalOptions.add(SCAN_PARTITION_COLUMN);
		optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
		optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
		optionalOptions.add(SCAN_PARTITION_NUM);
		optionalOptions.add(SCAN_FETCH_SIZE);
		optionalOptions.add(SCAN_AUTO_COMMIT);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(SINK_IGNORE_DELETE);
		optionalOptions.add(SINK_EXECUTOR);
		return optionalOptions;
	}

	private void validateConfigOptions(ReadableConfig config) {
		String jdbcUrl = config.get(URL);

		checkAllOrNone(config, new ConfigOption[]{
			USERNAME,
			PASSWORD
		});

		checkAllOrNone(config, new ConfigOption[]{
			SCAN_PARTITION_COLUMN,
			SCAN_PARTITION_NUM,
			SCAN_PARTITION_LOWER_BOUND,
			SCAN_PARTITION_UPPER_BOUND
		});

		if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent() &&
			config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
			long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
			long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
			if (lowerBound > upperBound) {
				throw new IllegalArgumentException(String.format(
					"'%s'='%s' must not be larger than '%s'='%s'.",
					SCAN_PARTITION_LOWER_BOUND.key(), lowerBound,
					SCAN_PARTITION_UPPER_BOUND.key(), upperBound));
			}
		}

		if (config.get(SINK_MAX_RETRIES) < 0) {
			throw new IllegalArgumentException(String.format(
				"The value of '%s' option shouldn't be negative, but is %s.",
				SINK_MAX_RETRIES.key(),
				config.get(SINK_MAX_RETRIES)));
		}
	}

	private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
		int presentCount = 0;
		for (ConfigOption configOption : configOptions) {
			if (config.getOptional(configOption).isPresent()) {
				presentCount++;
			}
		}
		String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
		Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
			"Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
	}
}
