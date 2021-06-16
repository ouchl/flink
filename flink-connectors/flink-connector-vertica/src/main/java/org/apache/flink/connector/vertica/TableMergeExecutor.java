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

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.StatementFactory;
import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import org.apache.commons.lang3.StringUtils;


public final class TableMergeExecutor implements VerticaBatchStatementExecutor<RowData> {

	private final VerticaDmlOptions dmlOptions;
	private final VerticaRowConverter converter;
	private final String tableName;
	private final String tempTableName;
	private final String[] fieldNames;
	private final String[] pkNames;
	private transient Statement st;
	private transient FieldNamedPreparedStatement insertStatement;

	/**
	 * Keep in mind object reuse: if it's on then key extractor may be required to return new object.
	 */
	public TableMergeExecutor(VerticaDmlOptions dmlOptions, VerticaRowConverter converter) {
		this.dmlOptions = checkNotNull(dmlOptions);
		this.converter = checkNotNull(converter);
		this.tableName = checkNotNull(dmlOptions.getTableName());
		this.tempTableName = tableName + "_temp";
		this.fieldNames = checkNotNull(dmlOptions.getFieldNames());
		this.pkNames = dmlOptions.getKeyFields().get();
	}

	@Override
	public void prepareStatements(Connection connection) throws SQLException {

		st = connection.createStatement();
		st.execute(VerticaStatements.getCreateTempTableStatement2(tableName, tempTableName));
		insertStatement = FieldNamedPreparedStatement.prepareStatement(connection, VerticaStatements.getInsertIntoStatement(tempTableName, fieldNames), fieldNames);
	}

	@Override
	public void addToBatch(RowData record) throws SQLException {
		converter.toExternal(record, insertStatement);
		insertStatement.addBatch();
	}

	@Override
	public void executeBatch() throws SQLException {
		insertStatement.executeBatch();
		String mergeStatement = VerticaStatements.getMergeStatement(tableName, tempTableName, pkNames,
			fieldNames);
		st.executeUpdate(mergeStatement);
		st.execute(VerticaStatements.getTruncateStatement(tempTableName));
	}

	@Override
	public void closeStatements() throws SQLException {
		if (st != null) {
			st.close();
			st = null;
		}
		if (insertStatement != null) {
			insertStatement.close();
			insertStatement = null;
		}
	}
}
