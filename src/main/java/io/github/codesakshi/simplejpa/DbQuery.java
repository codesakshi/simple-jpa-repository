
package io.github.codesakshi.simplejpa;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 
 * @author anilalps
 *
 */
public class DbQuery {

	@FunctionalInterface
	public static interface ThrowableFunction<T, R> extends Function<T,R> {

		@Override
		default R apply( final T elm ) {
			try {
				return applyThrows( elm );
			}catch( final Exception e ) {
				throw new RuntimeException( e);
			}
		}

		R applyThrows( T elm ) throws Exception;

	}

	@FunctionalInterface
	public static interface ThrowableConsumer<T> extends Consumer<T> {

		@Override
		default void accept( final T elm ) {
			try {
				acceptThrows( elm );
			}catch( final Exception e ) {
				throw new RuntimeException( e);
			}
		}

		void acceptThrows( T elm ) throws Exception;

	}

	/**
	 * 
	 * PreparedStatement configuration
	 *
	 */
	public class StatementConfiguration {

		private Integer fetchDirection;
		private Integer fetchSize;
		private Integer maxFieldSize;
		private Integer maxRows;
		private Integer queryTimeout;

		public StatementConfiguration() {

		}

		public StatementConfiguration(final Integer fetchDirection, final Integer fetchSize,
				final Integer maxFieldSize, final Integer maxRows,
				final Integer queryTimeout) {

			this.fetchDirection = fetchDirection;
			this.fetchSize = fetchSize;
			this.maxFieldSize = maxFieldSize;
			this.maxRows = maxRows;
			this.queryTimeout = queryTimeout;
		}

		public StatementConfiguration setFetchDirection(Integer fetchDirection) {
			this.fetchDirection = fetchDirection;

			return this;
		}

		public StatementConfiguration setFetchSize(Integer fetchSize) {
			this.fetchSize = fetchSize;
			return this;
		}

		public StatementConfiguration setMaxFieldSize(Integer maxFieldSize) {
			this.maxFieldSize = maxFieldSize;
			return this;
		}

		public StatementConfiguration setMaxRows(Integer maxRows) {
			this.maxRows = maxRows;
			return this;
		}

		public StatementConfiguration setQueryTimeout(Integer queryTimeout) {
			this.queryTimeout = queryTimeout;
			return this;
		}

		public Integer getFetchDirection() {
			return fetchDirection;
		}

		public Integer getFetchSize() {
			return fetchSize;
		}

		public Integer getMaxFieldSize() {
			return maxFieldSize;
		}

		public Integer getMaxRows() {
			return maxRows;
		}

		public Integer getQueryTimeout() {
			return queryTimeout;
		}
	}

	protected StatementConfiguration statementConfiguration = null;

	/**
	 * Constructor for DbQuery.
	 */
	public DbQuery() {

		statementConfiguration = null;
	}

	/**
	 * 
	 * @param statementConfiguration Configuration for PreparedStatement
	 */
	public DbQuery(StatementConfiguration statementConfiguration) {

		this.statementConfiguration = statementConfiguration;
	}

	/**
	 * 
	 * @param statementConfiguration PreparedStatement Configuration
	 */
	public void setStatementConfiguration( StatementConfiguration statementConfiguration ) {
		this.statementConfiguration = statementConfiguration;
	}

	private void configureStatement( Statement stmt ) throws SQLException {

		if( null != statementConfiguration ) {

			if( null != statementConfiguration.getFetchDirection() ) {
				stmt.setFetchDirection( statementConfiguration.getFetchDirection() );
			}

			if( null != statementConfiguration.getFetchSize() ) {
				stmt.setFetchSize( statementConfiguration.getFetchSize() );
			}

			if( null != statementConfiguration.getMaxFieldSize() ) {
				stmt.setMaxFieldSize( statementConfiguration.getMaxFieldSize() );
			}

			if( null != statementConfiguration.getMaxRows() ) {
				stmt.setMaxRows( statementConfiguration.getMaxRows() );
			}

			if( null != statementConfiguration.getQueryTimeout() ) {
				stmt.setQueryTimeout( statementConfiguration.getQueryTimeout() );
			}

		}

	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh) throws SQLException {
		return insert(conn, sql, rsh, (Object[]) null);
	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @param varMap Query Parameter Map
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return insert( conn, parsedQuery, rsh, params );
	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @param params Query Parameters
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		try( PreparedStatement stmt = conn.prepareStatement(sql)){

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					stmt.setObject( i+1, params[i] );

				}
			}

			stmt.executeUpdate();

			T generatedKey = null;

			// Get generatedKey

			if( null != rsh ) {

				try( ResultSet resultSet = stmt.getGeneratedKeys() ){
					generatedKey = rsh.apply(resultSet);
				}
			}

			return generatedKey;

		}

	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param autoGenColumnNames Autogenerated column Names
	 * @param rsh ResultSet Handler Function
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, final String[] autoGenColumnNames, 
			final ThrowableFunction<ResultSet,T> rsh) throws SQLException {
		return insert(conn, sql, autoGenColumnNames, rsh, (Object[]) null);
	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param autoGenColumnNames Autogenerated column Names
	 * @param rsh ResultSet Handler Function
	 * @param varMap Query Parameter Map
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, final String[] autoGenColumnNames, 
			final ThrowableFunction<ResultSet,T> rsh, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return insert( conn, parsedQuery, autoGenColumnNames, rsh, params );
	}

	/**
	 * Insert Single row to the database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param autoGenColumnNames Autogenerated column Names
	 * @param rsh ResultSet Handler Function
	 * @param params Query Parameters
	 * @return Mapped Generated Key
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T insert(final Connection conn, final String sql, final String[] autoGenColumnNames, 
			final ThrowableFunction<ResultSet,T> rsh, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		try( PreparedStatement stmt = conn.prepareStatement(sql, autoGenColumnNames)){

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					stmt.setObject( i+1, params[i] );

				}
			}

			stmt.executeUpdate();

			T generatedKey = null;

			// Get generatedKey

			if( null != rsh ) {

				try( ResultSet resultSet = stmt.getGeneratedKeys() ){
					generatedKey = rsh.apply(resultSet);
				}
			}

			return generatedKey;

		}

	}

	/**
	 *  Convert named parameter query to '=?' format 
	 *  
	 * @param sql SQL Query
	 * @param paramMap Query Parameter Map
	 * @param outList List to return the parameters
	 * @return Modified SQL Query
	 */
	public static String parseNamedParameterQuery( String sql, Map<String,Object> paramMap, List<Object> outList ) {

		int length = sql.length();

		StringBuilder parsedQuery = new StringBuilder();

		boolean inSingleQuote = false;

		boolean inDoubleQuote = false;

		for( int i = 0; i < length; i ++ ) {

			char chVal = sql.charAt(i);

			if( chVal == '\'') {

				inSingleQuote = ! inSingleQuote;

			}else if( chVal == '\"') {

				inDoubleQuote = ! inDoubleQuote;

			} else if( ! inSingleQuote && ! inDoubleQuote 
					&& chVal == ':' && i+1 < length
					&& Character.isJavaIdentifierStart(sql.charAt( i + 1 ))) {

				int j = i + 2;

				while( j < length && Character.isJavaIdentifierPart( sql.charAt( j ) ) ) {

					j++;
				}

				String param = sql.substring(i+1, j); // get the parameter name
				outList.add( paramMap.get(param) ); // add the parameter to outList

				chVal = '?'; // replace the parameter with '?'
				i += param.length(); // skip past the end of the parameter
			}

			parsedQuery.append(chVal);
		}

		return String.valueOf( parsedQuery );
	}

	/**
	 * Insert Multiple rows to Database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @return Mapped Generated Key List
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> List<T> insertBatch( final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,List<T>> rsh)throws SQLException {

		return insertBatch(conn, sql, rsh, (Object[][])null );
	}

	/**
	 * Insert Multiple rows to Database
	 * 
	 * @param <T> Type of Generated Key
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @param params Query Parameters
	 * @return Mapped Generated Key List
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> List<T> insertBatch( final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,List<T>> rsh, final Object[][] params)throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		try( PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS )){

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					for( int j = 0; j < params[i].length; j ++) {
						stmt.setObject(j+1, params[i][j] );

					}

					stmt.addBatch();

				}
			}

			stmt.executeBatch();

			List<T> genertedKeyList = null;

			if( null != rsh ) {

				try( ResultSet resultSet = stmt.getGeneratedKeys() ){
					genertedKeyList = rsh.apply(resultSet);
				}
			}

			return genertedKeyList;
		}

	}

	/**
	 * Execute Batch Query
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int[] batch( final Connection conn, final String sql) throws SQLException {

		return batch(conn, sql, (Object[][])null );
	}

	/**
	 * Execute Batch Query
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param params Query Parameters
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int[] batch(final Connection conn, final String sql, final Object[][] params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		try( PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS )){

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					for( int j = 0; j < params[i].length; j ++) {
						stmt.setObject(j+1, params[i][j] );

					}

					stmt.addBatch();

				}
			}

			int[] rows = stmt.executeBatch();

			return rows;
		}

	}

	/**
	 * Query the Database
	 * 
	 * @param <T> Type of Mapped Object  
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @return Java Object mapped using rsh
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T query(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh) throws SQLException {
		return query(conn, sql, rsh, (Object[]) null);
	}

	/**
	 * Query the Database
	 * 
	 * @param <T> Type of Mapped Object  
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @param varMap Query Parameter Map
	 * @return Java Object mapped using rsh
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T query(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return query( conn, parsedQuery, rsh, params );
	}

	/**
	 * Query the Database
	 * 
	 * @param <T> Type of Mapped Object  
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param rsh ResultSet Handler Function
	 * @param params Query Parameters
	 * @return Java Object mapped using rsh
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T query(final Connection conn, final String sql, 
			final ThrowableFunction<ResultSet,T> rsh, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		List<Object> paramList = null != params ? new ArrayList<Object>( Arrays.asList( params )) : new ArrayList<Object>();  

		String nullModifiedQuery = modifyNullParameterInQuery( sql, paramList);

		T result = null;

		try( PreparedStatement stmt = conn.prepareStatement( nullModifiedQuery )){

			configureStatement(stmt);

			for( int i = 0; i < paramList.size(); i ++) {

				stmt.setObject( i+1, paramList.get(i) );
			}

			try( ResultSet resultSet = stmt.executeQuery() ){

				if( null != rsh ) {
					result = rsh.apply(resultSet);
				}
			}

		}

		return result;

	}

	/**
	 * Change SQL query to 'IS NULL' if parameter is NULL
	 *  
	 * @param sql  SQL Query
	 * @param paramList Query Parameters
	 * @return Converted Query
	 */
	public static String modifyNullParameterInQuery( String sql, List<Object> paramList ) {

		int length = sql.length();

		StringBuilder parsedQuery = new StringBuilder();

		boolean inSingleQuote = false;

		boolean inDoubleQuote = false;

		int paramIndex = 0;

		for( int i = 0; i < length; i ++ ) {

			char chVal = sql.charAt(i);

			if( chVal == '\'') {

				inSingleQuote = ! inSingleQuote;
				
				parsedQuery.append(chVal);

			}else if( chVal == '\"') {

				inDoubleQuote = ! inDoubleQuote;
				
				parsedQuery.append(chVal);

			} else if( ! inSingleQuote && ! inDoubleQuote && chVal == '=' ) {

				int j = i + 1;

				while( j < length && Character.isWhitespace( sql.charAt( j ) ) ) {

					j++;
				}

				if( j < length ) {

					if( '?' == sql.charAt(j) ) {

						// we got ' =? '

						if( null == paramList.get( paramIndex ) ) {

							parsedQuery.append( " IS NULL ");

							paramList.remove(paramIndex);
						}else {

							parsedQuery.append( "=?");
							paramIndex++;						
						}

					}else {
						parsedQuery.append( "=");
						parsedQuery.append( sql.charAt(j) );
					}

				}else {
					parsedQuery.append( "=");
				}

				i = j;

			}else {

				parsedQuery.append(chVal);
			}
		}

		return String.valueOf( parsedQuery );
	}

	/**
	 * Update the Database
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int update(final Connection conn, final String sql) throws SQLException {
		return update(conn, sql, (Object[]) null);
	}

	/**
	 * Update the Database
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param varMap Query Parameter Map
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int update(final Connection conn, final String sql, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return update( conn, parsedQuery, params );
	}

	/**
	 * Update the Database
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Query
	 * @param params Query Parameters
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int update(final Connection conn, final String sql, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		try( PreparedStatement stmt = conn.prepareStatement( sql )){

			configureStatement(stmt);

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					stmt.setObject( i+1, params[i] );

				}
			}

			int rows = stmt.executeUpdate();

			return rows;
		}

	}

	/**
	 * Execute SQL Procedure 
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Procedure
	 * @param sth Statement Handler to get return values from Procedure
	 * @param varMap Query Parameter Map
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int executeProcedure(final Connection conn, final String sql, 
			final ThrowableConsumer<CallableStatement> sth, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return executeProcedure( conn, parsedQuery, sth, params );
	}

	/**
	 * Execute SQL Procedure 
	 * 
	 * @param conn SQL Connection
	 * @param sql SQL Procedure
	 * @param sth Statement Handler to get return values from Procedure
	 * @param params Query Parameters
	 * @return Number of rows modified
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public int executeProcedure(final Connection conn, final String sql,
			final ThrowableConsumer<CallableStatement> sth, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		int rows = 0;
		try( CallableStatement stmt = conn.prepareCall( sql )){

			configureStatement(stmt);

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					stmt.setObject( i+1, params[i] );

				}
			}

			stmt.execute();

			rows = stmt.getUpdateCount();

			if( null != sth ) {
				sth.accept(stmt);
			}

			return rows;
		}

	}

	/**
	 * Execute SQL Procedure and give ResultSet
	 * 
	 * @param <T> Type of Mapped Object
	 * @param conn SQL Connection
	 * @param sql SQL Procedure
	 * @param sth Statement Handler to get return values from Procedure
	 * @param rsh ResultSet Handler Function
	 * @param varMap Query Parameter Map
	 * @return Java Object mapped using rsh
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T executeProcedureWithResult(final Connection conn, final String sql, final ThrowableConsumer<CallableStatement> sth, 
			final ThrowableFunction<ResultSet,T> rsh, Map<String,Object> varMap) throws SQLException {

		List<Object> paramList = new ArrayList<Object>();

		String parsedQuery = parseNamedParameterQuery(sql, varMap, paramList);

		Object[] params = paramList.toArray(new Object[0]);

		return executeProcedureWithResult( conn, parsedQuery, sth, rsh, params );
	}

	/**
	 * Execute SQL Procedure and give ResultSet
	 * 
	 * @param <T> Type of Mapped Object
	 * @param conn SQL Connection
	 * @param sql SQL Procedure
	 * @param sth Statement Handler to get return values from Procedure
	 * @param rsh ResultSet Handler Function
	 * @param params Query Parameters
	 * @return Java Object mapped using rsh
	 * @throws SQLException if the query is malformed or cannot be executed
	 */
	public <T> T executeProcedureWithResult(final Connection conn, final String sql, final ThrowableConsumer<CallableStatement> sth,  
			final ThrowableFunction<ResultSet,T> rsh, final Object... params) throws SQLException {

		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		T result = null;

		try( CallableStatement stmt = conn.prepareCall( sql )){

			configureStatement(stmt);

			if( null != params) {

				for( int i = 0; i < params.length; i ++) {

					stmt.setObject( i+1, params[i] );

				}
			}

			try( ResultSet resultSet = stmt.executeQuery() ){

				if( null != rsh ) {
					result = rsh.apply(resultSet);
				}
			}

			if( null != sth ) {
				sth.accept(stmt);
			}

			return result;
		}

	}

	public static <T> ThrowableFunction<ResultSet,List<T>> mapRow( final ThrowableFunction<ResultSet,T> rowMapper ){

		return ( (ResultSet rs )-> {

			List<T> list = new ArrayList<T>();

			while( rs.next() ) {

				// ID will be first Entry
				T value = rowMapper.apply(rs);

				list.add( value );
			}

			return list;

		});
	}

	@SuppressWarnings("unchecked")
	public static <T> ThrowableFunction<ResultSet,T> toID(){

		return ( (ResultSet rs )-> {

			T value = null;

			if( rs.next() ) {

				// ID will be first Entry
				value = (T) rs.getObject( 1 );
			}

			return value;

		});
	}

	@SuppressWarnings("unchecked")
	public static <T> ThrowableFunction<ResultSet,List<T>> toIDList(){

		return ( (ResultSet rs )-> {

			List<T> idList = new ArrayList<T>();

			while( rs.next() ) {

				// ID will be first Entry
				T value = (T) rs.getObject( 1 );

				idList.add( value );
			}

			return idList;

		});
	}

	public static ThrowableFunction<ResultSet,Object[]> toArray(){

		return ( (ResultSet rs )-> {

			Object[] result = null;

			if( rs.next() ) {

				ResultSetMetaData meta = rs.getMetaData();
				int cols = meta.getColumnCount();

				result = new Object[cols];

				for( int i = 0; i < cols; i ++) {
					result[i] = rs.getObject( i + 1 );
				}
			}

			return result;

		});
	}

	public static ThrowableFunction<ResultSet,List<Object[]>> toListOfArray(){

		return ( (ResultSet rs )-> {

			List<Object[]> resultList = new LinkedList<Object[]>();

			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();

			while( rs.next() ) {

				Object[] row = new Object[cols];

				for( int i = 0; i < cols; i ++) {
					row[i] = rs.getObject( i + 1 );
				}

				resultList.add( row );
			}

			return resultList;

		});
	}

	public static ThrowableFunction<ResultSet,Map<String,Object>> toMap(){

		return ( (ResultSet rs )-> {

			Map<String,Object> result = null;

			if( rs.next() ) {

				result = new TreeMap<String,Object>(String.CASE_INSENSITIVE_ORDER);

				ResultSetMetaData meta = rs.getMetaData();
				int cols = meta.getColumnCount();

				for( int i = 1; i <= cols; i ++) {

					String columnName = meta.getColumnLabel(i);
					if( null == columnName || columnName.isEmpty() ) {
						columnName = meta.getColumnName(i);
					}

					result.put( columnName, rs.getObject(i));
				}
			}

			return result;

		});
	}

	public static ThrowableFunction<ResultSet,List<Map<String,Object>>> toListOfMap(){

		return ( (ResultSet rs )-> {

			List<Map<String,Object>> resultList = new LinkedList<Map<String,Object>>();

			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();

			while( rs.next() ) {

				Map<String,Object> row = new TreeMap<String,Object>(String.CASE_INSENSITIVE_ORDER);

				for( int i = 1; i <= cols; i ++) {

					String columnName = meta.getColumnLabel(i);
					if( null == columnName || columnName.isEmpty() ) {
						columnName = meta.getColumnName(i);
					}

					row.put( columnName, rs.getObject(i));
				}

				resultList.add( row );
			}

			return resultList;

		});
	}

}