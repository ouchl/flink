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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * A {@link DynamicTableSource} for Vertica.
 */
@Internal
public class VerticaDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown {

	private final VerticaOptions options;
	private final VerticaReadOptions readOptions;
	private TableSchema physicalSchema;

	public VerticaDynamicTableSource(
			VerticaOptions options,
			VerticaReadOptions readOptions,
			TableSchema physicalSchema) {
		this.options = options;
		this.readOptions = readOptions;
		this.physicalSchema = physicalSchema;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		final VerticaRowDataInputFormat.Builder builder = VerticaRowDataInputFormat.builder()
			.setDrivername(options.getDriverName())
			.setDBUrl(options.getDbURL())
			.setUsername(options.getUsername().orElse(null))
			.setPassword(options.getPassword().orElse(null))
			.setAutoCommit(readOptions.getAutoCommit());

		if (readOptions.getFetchSize() != 0) {
			builder.setFetchSize(readOptions.getFetchSize());
		}
		String query = VerticaStatements.getSelectFromStatement(
			options.getTableName(), physicalSchema.getFieldNames(), new String[0]);
		if (readOptions.getPartitionColumnName().isPresent()) {
			long lowerBound = readOptions.getPartitionLowerBound().get();
			long upperBound = readOptions.getPartitionUpperBound().get();
			int numPartitions = readOptions.getNumPartitions().get();
			builder.setParametersProvider(
				new VerticaNumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions));
			query += " WHERE " +
				VerticaStatements.quoteIdentifier(readOptions.getPartitionColumnName().get()) +
				" BETWEEN ? AND ?";
		}
		builder.setQuery(query);
		final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
		builder.setRowConverter(new VerticaRowConverter(rowType));
		builder.setRowDataTypeInfo(
				runtimeProviderContext.createTypeInformation(physicalSchema.toRowDataType()));

		return InputFormatProvider.of(builder.build());
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public boolean supportsNestedProjection() {
		// Vertica doesn't support nested projection
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
	}

	@Override
	public DynamicTableSource copy() {
		return new VerticaDynamicTableSource(options, readOptions, physicalSchema);
	}

	@Override
	public String asSummaryString() {
		return "Vertica";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof VerticaDynamicTableSource)) {
			return false;
		}
		VerticaDynamicTableSource that = (VerticaDynamicTableSource) o;
		return Objects.equals(options, that.options) &&
			Objects.equals(readOptions, that.readOptions) &&
			Objects.equals(physicalSchema, that.physicalSchema);
	}

	@Override
	public int hashCode() {
		return Objects.hash(options, readOptions, physicalSchema);
	}
}
