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


public final class TableReplicationExecutor implements VerticaBatchStatementExecutor<RowData> {

	private final VerticaDmlOptions dmlOptions;
	private final VerticaRowConverter converter;
	private final String tableName;
	private final String tempTableName;
	private final String[] fieldNames;
	private final String[] pkNames;
	private transient Statement st;
	private transient FieldNamedPreparedStatement insertStatement;
	private final String tempView;
	private String[] allFieldNames;

	/**
	 * Keep in mind object reuse: if it's on then key extractor may be required to return new object.
	 */
	public TableReplicationExecutor(VerticaDmlOptions dmlOptions, VerticaRowConverter converter) {
		this.dmlOptions = checkNotNull(dmlOptions);
		this.converter = checkNotNull(converter);
		this.tableName = checkNotNull(dmlOptions.getTableName());
		this.tempTableName = tableName + "_temp";
		this.fieldNames = checkNotNull(dmlOptions.getFieldNames());
		this.pkNames = dmlOptions.getKeyFields().get();
		this.tempView = "vw_"+tempTableName;
	}

	public String[] getAllFieldNames(Connection connection, String schema) throws SQLException {
		List<String> fields = new ArrayList<>();
		DatabaseMetaData dm = connection.getMetaData();
		ResultSet rs = dm.getColumns(null, schema, tableName, null);
		while (rs.next()) {
			fields.add(rs.getString("COLUMN_NAME"));
		}
		return fields.toArray(new String[0]);
	}

	@Override
	public void prepareStatements(Connection connection) throws SQLException {

		st = connection.createStatement();
		DatabaseMetaData dm = connection.getMetaData();
		String url = dm.getURL();
		Pattern p = Pattern.compile("SearchPath=([^&]*)", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(url);
		String schema =  m.find() ? m.group(1) : null;
		allFieldNames = getAllFieldNames(connection, schema);
//		ResultSet rs2 = st.executeQuery("select export_tables('', '"+tableName+"')");
//		while (rs2.next()) {
//			String s =rs2.getString(1);
//			if (StringUtils.containsIgnoreCase(s, "SET USING")){
//				needRefresh = true;
//			};
//		}
//		st.execute("CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS "+tableName+"_temp AS SELECT * FROM "+tableName+" WHERE 1=0");
		st.execute(VerticaStatements.getCreateTempTableStatement1(tableName, tempTableName, fieldNames));
//		st.execute("DROP TABLE IF EXISTS "+tempTableName+" CASCADE");
		st.execute("TRUNCATE TABLE "+tempTableName);
		ResultSet rs2 = st.executeQuery("select 1 from views where table_name='"+tempView+"' and lower(table_schema)='"+schema.toLowerCase()+"'");
		if (!rs2.next()){
			st.execute("create view "+tempView+" as select * from "+tempTableName);
		}
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
		String mergeStatement = VerticaStatements.getMergeStatement(tableName, tempView, pkNames,
			allFieldNames);
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
