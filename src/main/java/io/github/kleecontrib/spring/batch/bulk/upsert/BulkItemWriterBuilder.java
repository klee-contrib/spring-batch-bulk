package io.github.kleecontrib.spring.batch.bulk.upsert;

import javax.sql.DataSource;

import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import io.github.kleecontrib.spring.batch.bulk.mapping.AbstractUpsertMapping;

/**
 * @param <T>
 */
public class BulkItemWriterBuilder<T> {
	private DataSource dataSource;
	private AbstractMapping<T> abstractMapping;

	public BulkItemWriterBuilder<T> dataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		return this;
	}

	public BulkItemWriterBuilder<T> abstractMapping(AbstractUpsertMapping<T> abstractMapping) {
		this.abstractMapping = abstractMapping;
		return this;
	}

	public BulkItemWriterBuilder<T> abstractUpsertMapping(AbstractUpsertMapping<T> abstractMapping) {
		return this.abstractMapping(abstractMapping);
	}

	public BulkItemWriterBuilder<T> abstractReplaceMapping(AbstractUpsertMapping<T> abstractMapping) {
		return this.abstractMapping(abstractMapping);
	}

	public BulkItemWriter<T> build() {
		return new BulkItemWriter<>(dataSource, abstractMapping);
	}
}
