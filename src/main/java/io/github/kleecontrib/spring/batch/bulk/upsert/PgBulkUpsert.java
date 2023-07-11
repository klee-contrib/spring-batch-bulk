// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package io.github.kleecontrib.spring.batch.bulk.upsert;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.postgresql.PGConnection;

import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.configuration.Configuration;
import de.bytefish.pgbulkinsert.configuration.IConfiguration;
import io.github.kleecontrib.spring.batch.bulk.mapping.AbstractUpsertMapping;

/**
 * Bulk upsert class, to insert or update rows if it already exist
 * 
 * @author gderuette
 * @param <T>
 */
public class PgBulkUpsert<T> implements IPgBulkInsert<T> {
	private final PgBulkInsert<T> pgBulkInsert;
	private final AbstractUpsertMapping<T> mapping;

	public PgBulkUpsert(AbstractUpsertMapping<T> mapping) {
		this(new Configuration(), mapping);
	}

	public PgBulkUpsert(IConfiguration configuration, AbstractUpsertMapping<T> mapping) {
		Objects.requireNonNull(configuration, "'configuration' has to be set");
		Objects.requireNonNull(mapping, "'mapping' has to be set");
		this.pgBulkInsert = new PgBulkInsert<>(configuration, mapping);
		this.mapping = mapping;
	}

	/**
	 * Bulk upsert entities : creates a temporary table, insert all values in it and
	 * then try to copy from the temporary table to the target.
	 */
	public void saveAll(PGConnection pgConnection, Stream<T> entities) throws SQLException {
		try (Statement statement = ((Connection) pgConnection).createStatement()) {
			String tempTableQuery = String.format("create table if not exists %s as select * from %s with no data;",
					this.mapping.getTempTableName(), this.mapping.getTableName());
			statement.execute(tempTableQuery);
			pgBulkInsert.saveAll(pgConnection, entities);
			StringBuilder insertQuery = new StringBuilder(String.format(
					"insert into %s select * from %s on conflict (%s) do update ", this.mapping.getTableName(),
					this.mapping.getTempTableName(), this.mapping.getPrimaryKey()));

			String setColumns = this.mapping.getColumns().stream()
					.map(column -> String.format(" %s = EXCLUDED.%s", column.getColumnName(), column.getColumnName()))
					.collect(Collectors.joining(","));

			insertQuery.append(" set ");
			insertQuery.append(setColumns);
			insertQuery.append(';');
			statement.execute(insertQuery.toString());
			statement.execute("drop table if exists " + this.mapping.getTempTableName());
		}
	}

	public void saveAll(PGConnection connection, Collection<T> entities) throws SQLException {
		saveAll(connection, entities.stream());
	}
}