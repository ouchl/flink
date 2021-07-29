package org.apache.flink.connector.vertica;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class VerticaStatements {

	public static String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}

	public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
		String columns = Arrays.stream(fieldNames)
			.map(VerticaStatements::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames)
			.map(f -> ":" + f)
			.collect(Collectors.joining(", "));
		return "INSERT INTO " + quoteIdentifier(tableName) +
			"(" + columns + ")" + " VALUES (" + placeholders + ")";
	}

	public static String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		String selectExpressions = Arrays.stream(selectFields)
			.map(VerticaStatements::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String fieldExpressions = Arrays.stream(conditionFields)
			.map(f -> format("%s = :%s", quoteIdentifier(f), f))
			.collect(Collectors.joining(" AND "));
		return "SELECT " + selectExpressions + " FROM " +
			quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
	}

	public static String getDeleteStatement(String tableName, String[] conditionFields) {
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> format("%s = :%s", quoteIdentifier(f), f))
			.collect(Collectors.joining(" AND "));
		return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
	}

	public static String getMergeStatement(String tableName, String tempTable, String[] pkFields, String[] fieldNames) {
		String onClause = Arrays.stream(pkFields)
			.map(f -> format("tgt.%s = tmp.%s", quoteIdentifier(f), quoteIdentifier(f)))
			.collect(Collectors.joining(" AND "));
		String setClause = Arrays.stream(fieldNames)
			.map(f -> format("%s = tmp.%s", quoteIdentifier(f), quoteIdentifier(f)))
			.collect(Collectors.joining(", "));
		String insertColumns = Arrays.stream(fieldNames)
			.map(VerticaStatements::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String valuesClause = Arrays.stream(fieldNames)
			.map(f -> format("tmp.%s", quoteIdentifier(f)))
			.collect(Collectors.joining(", "));
		return "MERGE INTO "+quoteIdentifier(tableName)+ " tgt USING "+quoteIdentifier(tempTable)+" tmp "
			+ "ON ("+onClause+") WHEN MATCHED THEN UPDATE SET "+setClause+" WHEN NOT MATCHED THEN INSERT "
			+ "("+ insertColumns +") VALUES ("+valuesClause+")";
	}
	public static String getDeleteStatement(String tableName, String tempTable, String[] pkFields) {
		String whereClause = "WHERE "+Arrays.stream(pkFields).map(f -> quoteIdentifier(tableName)+"."+f+"="+"tmp."+f).collect(
			Collectors.joining(" AND "));
		return "DELETE FROM "+quoteIdentifier(tableName)+ " WHERE EXISTS (SELECT 1 FROM "+quoteIdentifier(tempTable)+" tmp "
			+ whereClause+ ")";
	}
	public static String getTruncateStatement(String tableName) {
		return "TRUNCATE TABLE "+quoteIdentifier(tableName);
	}

	public static String getCreateTempTableStatement1(String tableName, String tempTable, String[] fieldNames) {
//		return "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS "+quoteIdentifier(tempTable)+
//			" ON COMMIT PRESERVE ROWS AS SELECT * FROM "+quoteIdentifier(tableName)+" WHERE 1=0";
		String fieldClause = Arrays.stream(fieldNames).map(VerticaStatements::quoteIdentifier).collect(
			Collectors.joining(", "));
		return "CREATE TABLE IF NOT EXISTS "+quoteIdentifier(tempTable)+
			" AS SELECT "+fieldClause+" FROM "+quoteIdentifier(tableName)+" WHERE FALSE";
	}

	public static String getCreateTempTableStatement2(String tableName, String tempTable) {
		return "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS "+quoteIdentifier(tempTable)+
			" ON COMMIT PRESERVE ROWS AS SELECT * FROM "+quoteIdentifier(tableName)+" WHERE 1=0";
	}
}
