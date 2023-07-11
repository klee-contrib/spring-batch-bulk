// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package io.github.kleecontrib.spring.batch.bulk.upsert;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import org.postgresql.PGConnection;

import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.configuration.Configuration;
import de.bytefish.pgbulkinsert.configuration.IConfiguration;
import io.github.kleecontrib.spring.batch.bulk.mapping.AbstractReplaceMapping;

/**
 * Bulk upsert class, to insert or update rows if it already exist
 * 
 * @author gderuette
 * @param <T>
 */
public class PgBulkReplace<T> implements IPgBulkInsert<T> {
	private final PgBulkInsert<T> pgBulkInsert;
	private final AbstractReplaceMapping<T> mapping;

	public PgBulkReplace(AbstractReplaceMapping<T> mapping) {
		this(new Configuration(), mapping);
	}

	public PgBulkReplace(IConfiguration configuration, AbstractReplaceMapping<T> mapping) {
		Objects.requireNonNull(configuration, "'configuration' has to be set");
		Objects.requireNonNull(mapping, "'mapping' has to be set");
		this.pgBulkInsert = new PgBulkInsert<>(configuration, mapping);
		this.mapping = mapping;
	}

	/**
	 * Bulk replace entities : truncate table and then insert all data in table
	 */
	public void saveAll(PGConnection pgConnection, Stream<T> entities) throws SQLException {
		try (Statement statement = ((Connection) pgConnection).createStatement()) {
			String truncateQuery = String.format("truncate table %s", mapping.getTableName());
			statement.execute(truncateQuery);
			pgBulkInsert.saveAll(pgConnection, entities);
		}
	}

	public void saveAll(PGConnection connection, Collection<T> entities) throws SQLException {
		saveAll(connection, entities.stream());
	}
}