/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.vertica;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Vertica sink DML options.
 */
public class VerticaDmlOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String[] fieldNames;
	@Nullable
	private final int[] fieldTypes;
	@Nullable
	private final String[] keyFields;
	private final String tableName;

	public static VerticaDmlOptionsBuilder builder() {
		return new VerticaDmlOptionsBuilder();
	}

	private VerticaDmlOptions(String tableName, String[] fieldNames, int[] fieldTypes, String[] keyFields) {
		this.fieldTypes = fieldTypes;
		this.tableName = Preconditions.checkNotNull(tableName, "table is empty");
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "field names is empty");
		this.keyFields = keyFields;
	}

	public String getTableName() {
		return tableName;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public Optional<String[]> getKeyFields() {
		return Optional.ofNullable(keyFields);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		VerticaDmlOptions that = (VerticaDmlOptions) o;
		return Arrays.equals(fieldNames, that.fieldNames) &&
			Arrays.equals(keyFields, that.keyFields) &&
			Objects.equals(tableName, that.tableName);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(tableName);
		result = 31 * result + Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(keyFields);
		return result;
	}

	/**
	 * Builder for {@link VerticaDmlOptions}.
	 */
	public static class VerticaDmlOptionsBuilder {
		private String tableName;
		private String[] fieldNames;
		private String[] keyFields;
		private int[] fieldTypes;

		protected VerticaDmlOptionsBuilder self() {
			return this;
		}

		public VerticaDmlOptionsBuilder withFieldNames(String field, String... fieldNames) {
			this.fieldNames = concat(field, fieldNames);
			return this;
		}

		public VerticaDmlOptionsBuilder withFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public VerticaDmlOptionsBuilder withKeyFields(String keyField, String... keyFields) {
			this.keyFields = concat(keyField, keyFields);
			return this;
		}

		public VerticaDmlOptionsBuilder withKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		public VerticaDmlOptionsBuilder withTableName(String tableName) {
			this.tableName = tableName;
			return self();
		}

		public VerticaDmlOptionsBuilder withFieldTypes(int[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return self();
		}

		public VerticaDmlOptions build() {
			return new VerticaDmlOptions(tableName, fieldNames, fieldTypes, keyFields);
		}

		static String[] concat(String first, String... next) {
			if (next == null || next.length == 0) {
				return new String[]{first};
			} else {
				return Stream.concat(Stream.of(new String[]{first}), Stream.of(next)).toArray(String[]::new);
			}
		}

	}
}
