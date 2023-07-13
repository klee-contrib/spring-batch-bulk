package io.github.kleecontrib.spring.batch.bulk.upsert;

import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import io.github.kleecontrib.spring.batch.bulk.mapping.AbstractUpsertMapping;

/**
 * 
 * Item writer based on bulk insert or bulk upsert
 * 
 * @param <T> Writted class object
 */
public class BulkItemWriter<T> implements ItemWriter<T> {

	private final DataSource dataSource;
	private final IPgBulkInsert<T> bulkInsert;

	/**
	 * @param dataSource data source
	 * @param mapping    column mapping
	 */
	public BulkItemWriter(DataSource dataSource, AbstractMapping<T> mapping) {
		Objects.requireNonNull(dataSource, "'dataSource' has to be set");
		Objects.requireNonNull(mapping, "'mapping' has to be set");
		this.dataSource = dataSource;
		if (mapping instanceof AbstractUpsertMapping<T> upsertMapping) {
			bulkInsert = new PgBulkUpsert<>(upsertMapping);
		} else {
			bulkInsert = new PgBulkInsert<>(mapping);
		}
	}

	@Override
	public void write(Chunk<? extends T> itemsChunk) throws Exception {
		@SuppressWarnings("unchecked")
		List<T> itemsList = (List<T>) itemsChunk.getItems();
		try (var connection = dataSource.getConnection()) {
			var pgConnection = PostgreSqlUtils.getPGConnection(connection);
			bulkInsert.saveAll(pgConnection, itemsList.stream());
		}
	}
}