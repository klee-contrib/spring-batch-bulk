// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package io.github.kleecontrib.spring.batch.bulk.mapping;

import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;

/**
 * Mapping to use upsert
 * 
 * @author gderuette
 * @param <E> mapped class
 */
public abstract class AbstractReplaceMapping<E> extends AbstractMapping<E> {

	protected AbstractReplaceMapping(String schemaName, String tableName) {
		this(new ValueHandlerProvider(), schemaName, tableName, false);
	}

	protected AbstractReplaceMapping(String schemaName, String tableName, boolean usePostgresQuoting) {
		this(new ValueHandlerProvider(), schemaName, tableName, usePostgresQuoting);
	}

	protected AbstractReplaceMapping(IValueHandlerProvider provider, String schemaName, String tableName,
			boolean usePostgresQuoting) {
		super(provider, schemaName, tableName, usePostgresQuoting);
	}

	/**
	 * @return target table name
	 */
	public String getTableName() {
		return this.table.getTableName();
	}
}
