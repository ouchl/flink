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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


public class VerticaDynamicOutputFormatBuilder implements Serializable {

	private static final long serialVersionUID = 1L;

	private VerticaOptions verticaOptions;
	private VerticaExecutionOptions executionOptions;
	private VerticaDmlOptions dmlOptions;
	private TypeInformation<RowData> rowDataTypeInformation;
	private DataType[] fieldDataTypes;

	public VerticaDynamicOutputFormatBuilder() {
	}

	public VerticaDynamicOutputFormatBuilder setVerticaOptions(VerticaOptions verticaOptions) {
		this.verticaOptions = verticaOptions;
		return this;
	}

	public VerticaDynamicOutputFormatBuilder setVerticaExecutionOptions(VerticaExecutionOptions executionOptions) {
		this.executionOptions = executionOptions;
		return this;
	}

	public VerticaDynamicOutputFormatBuilder setVerticaDmlOptions(VerticaDmlOptions dmlOptions) {
		this.dmlOptions = dmlOptions;
		return this;
	}

	public VerticaDynamicOutputFormatBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
		this.rowDataTypeInformation = rowDataTypeInfo;
		return this;
	}

	public VerticaDynamicOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
		this.fieldDataTypes = fieldDataTypes;
		return this;
	}

	public VerticaBatchingOutputFormat<RowData, ?, ?> build() {
		checkNotNull(verticaOptions, "Vertica options can not be null");
		checkNotNull(dmlOptions, "Vertica dml options can not be null");
		checkNotNull(executionOptions, "Vertica execution options can not be null");

		final LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
			.map(DataType::getLogicalType)
			.toArray(LogicalType[]::new);
		if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
			//upsert query
			return new VerticaBatchingOutputFormat<>(
				new VerticaConnectionProvider(verticaOptions),
				executionOptions,
				ctx -> createBufferReduceExecutor(dmlOptions, executionOptions, ctx, rowDataTypeInformation, logicalTypes, executionOptions.getIgnoreDelete()),
				VerticaBatchingOutputFormat.RecordExtractor.identity());
		} else {
			// append only query
			final String sql = VerticaStatements.getInsertIntoStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
			return new VerticaBatchingOutputFormat<>(
				new VerticaConnectionProvider(verticaOptions),
				executionOptions,
				ctx -> createSimpleBufferedExecutor(
					ctx,
					dmlOptions.getFieldNames(),
					logicalTypes,
					sql,
					rowDataTypeInformation),
				VerticaBatchingOutputFormat.RecordExtractor.identity());
		}
	}

	private static VerticaBufferedBatchStatementExecutor<RowData> createBufferReduceExecutor(
			VerticaDmlOptions opt,
			VerticaExecutionOptions executionOptions,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] fieldTypes,
			boolean ignoreDelete) {
		checkArgument(opt.getKeyFields().isPresent());
		String tableName = opt.getTableName();
		String[] pkNames = opt.getKeyFields().get();
		int[] pkFields = Arrays.stream(pkNames).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		final Function<RowData, RowData> valueTransform = ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity();

		return new TableBufferReducedStatementExecutor(
			createMergeExecutor(opt, executionOptions, fieldTypes),
			createDeleteExecutor(opt, pkTypes),
			createRowKeyExtractor(fieldTypes, pkFields),
			valueTransform,
			ignoreDelete);
	}

	private static VerticaBufferedBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
			RuntimeContext ctx,
			String[] fieldNames,
			LogicalType[] fieldTypes,
			String sql,
			TypeInformation<RowData> rowDataTypeInfo) {
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		return new TableBufferedStatementExecutor(
			createSimpleRowExecutor(fieldNames, fieldTypes, sql),
			ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity()
		);
	}

	private static VerticaBatchStatementExecutor<RowData> createMergeExecutor(
		VerticaDmlOptions dmlOptions,
		VerticaExecutionOptions executionOptions,
		LogicalType[] fieldTypes) {
		final VerticaRowConverter rowConverter = new VerticaRowConverter(RowType.of(fieldTypes));
		VerticaBatchStatementExecutor<RowData> executor = null;
		if (executionOptions.getExecutor().equals("merge")){
			executor = new TableMergeExecutor(
				dmlOptions,
				rowConverter
			);
		}else if (executionOptions.getExecutor().equals("replicator")) {
			executor = new TableReplicationExecutor(
				dmlOptions,
				rowConverter
			);
		}
		return executor;
	}

	private static VerticaBatchStatementExecutor<RowData> createSimpleRowExecutor(
			String[] fieldNames,
			LogicalType[] fieldTypes,
			final String sql) {
		final VerticaRowConverter rowConverter = new VerticaRowConverter(RowType.of(fieldTypes));
		return new TableSimpleStatementExecutor(
			connection -> FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
			rowConverter
		);
	}
	private static VerticaBatchStatementExecutor<RowData> createDeleteExecutor(
		VerticaDmlOptions dmlOptions,
		LogicalType[] pkTypes) {
		final VerticaRowConverter rowConverter = new VerticaRowConverter(RowType.of(pkTypes));
		return new TableDeleteStatementExecutor(dmlOptions, rowConverter);
	}

	private static Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
		for (int i = 0; i < pkFields.length; i++) {
			fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
		}
		return row -> getPrimaryKey(row, fieldGetters);
	}

	private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
		GenericRowData pkRow = new GenericRowData(fieldGetters.length);
		for (int i = 0; i < fieldGetters.length; i++) {
			pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
		}
		return pkRow;
	}
}
