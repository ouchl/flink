package org.apache.flink.connector.vertica;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for Vertica.
 */
public class VerticaRowConverter extends AbstractJdbcRowConverter {

	private static final long serialVersionUID = 1L;

	@Override
	public String converterName() {
		return "Vertica";
	}

	public VerticaRowConverter(RowType rowType) {
		super(rowType);
	}
}
