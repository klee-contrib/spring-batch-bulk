package io.github.kleecontrib.spring.batch.tasklet;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Class to create a query tasklet
 */
public class QueryTasklet implements Tasklet {

	private final DataSource dataSource;
	private final String query;

	/**
	 * @param dataSource the source to run the query
	 * @param tableName the table name 
	 */
	public QueryTasklet(DataSource dataSource, String query) {
		this.dataSource = dataSource;
		this.query = query;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
			statement.execute(query);
		}

		return RepeatStatus.FINISHED;
	}

}
