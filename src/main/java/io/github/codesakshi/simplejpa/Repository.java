package io.github.codesakshi.simplejpa;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.persistence.AttributeConverter;
import javax.persistence.CascadeType;
import javax.persistence.Convert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.codesakshi.simplejpa.DataConverter.ConverterType;
import io.github.codesakshi.simplejpa.DbQuery.ThrowableFunction;
import io.github.codesakshi.simplejpa.EntityProcessor.AssociationMetaInfo;
import io.github.codesakshi.simplejpa.EntityProcessor.ColumnJoinInfo;
import io.github.codesakshi.simplejpa.EntityProcessor.ManyToManyMetaInfo;
import io.github.codesakshi.simplejpa.EntityProcessor.OneToManyMetaInfo;
import io.github.codesakshi.simplejpa.EntityProcessor.SingleTargetMetaInfo;
import io.github.codesakshi.simplejpa.EntityProcessor.TableMetaInfo;

/**
 * 
 * @author anilalps
 * 
 *  Select operation
 *  Save operation
 *  Delete operation
 *
 * @param <T> Entity Type
 * @param <ID> ID Type
 */
public class Repository<T,ID> {

	private static Logger logger  = LoggerFactory.getLogger(Repository.class);

	private static class ParentEntityData{

		private AssociationMetaInfo associationInfo;

		protected Object parentEntity;

		protected ParentEntityData(AssociationMetaInfo associationInfo, Object parentEntity) {
			super();
			this.associationInfo = associationInfo;
			this.parentEntity = parentEntity;
		}

		@Override
		public int hashCode() {
			return Objects.hash(associationInfo, parentEntity);
		}

		/**
		 * Should implement equals method.
		 * Else ResultJoinData -> parentEntitySet will have duplicate entries
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ParentEntityData other = (ParentEntityData) obj;
			return Objects.equals(associationInfo, other.associationInfo)
					&& Objects.equals(parentEntity, other.parentEntity);
		}

		protected AssociationMetaInfo getAssociationInfo() {
			return associationInfo;
		}

		protected Object getParentEntity() {
			return parentEntity;
		}
	}

	/** Result Entity Join Data */
	private static class ResultJoinData{

		protected Object entity;

		protected Set<ParentEntityData> parentEntitySet = new HashSet<ParentEntityData>();

		protected ResultJoinData(Object entity) {
			this.entity = entity;
		}

		protected Object getEntity() {
			return entity;
		}

		protected Set<ParentEntityData> getParentEntitySet() {
			return parentEntitySet;
		}
	}

	/** Member variables ***/
	protected EntityProcessor<T,ID> processor;

	protected DbQuery dbQuery = new DbQuery();

	@SuppressWarnings("unchecked")
	/**
	 * Default Constructor
	 */
	public Repository() {

		/** get Types from Template arguments */
		List<Class<?>> templateArgs = getTemplateArguments(Repository.class, getClass());

		if( templateArgs.size() < 2) {

			throw new RuntimeException(" Missing template Argument Classes");
		}

		initialize( (Class<T>) templateArgs.get( 0 ), (Class<ID>) templateArgs.get( 1 ));
	}

	/**
	 * Construct the entity with Class of the entity and Class of the Id variable
	 * 
	 * @param entityClazz Class of the entity
	 * @param idClazz Class of the @Id variable
	 */
	public Repository(Class<T> entityClazz, Class<ID> idClazz) {

		initialize( entityClazz, idClazz);
	}

	private void initialize( Class<T> entityClazz, Class<ID> idClazz) {

		this.processor = new EntityProcessor<T,ID>(entityClazz, idClazz );
	}

	/**
	 * Get the actual type arguments a child class has used to extend a generic base class.
	 *
	 * @param baseClass the base class
	 * @param childClass the child class
	 * @return a list of the raw classes for the actual type arguments.
	 */
	private static <T> List<Class<?>> getTemplateArguments( Class<T> baseClass, Class<? extends T> childClass) {

		Map<Type, Type> resolvedTypes = new HashMap<Type, Type>();

		Type type = childClass;

		// start walking up the inheritance hierarchy until we hit baseClass

		while (! getClass(type).equals(baseClass)) {

			if (type instanceof Class) {

				// there is no useful information for us in raw types, so just keep going.
				type = ((Class<?>) type).getGenericSuperclass();

			}else {

				ParameterizedType parameterizedType = (ParameterizedType) type;
				Class<?> rawType = (Class<?>) parameterizedType.getRawType();

				Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
				TypeVariable<?>[] typeParameters = rawType.getTypeParameters();

				for (int i = 0; i < actualTypeArguments.length; i++) {

					resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
				}

				if (!rawType.equals(baseClass)) {

					type = rawType.getGenericSuperclass();
				}
			}
		}

		// finally, for each actual type argument provided to baseClass, determine (if possible)
		// the raw class for that type argument.
		Type[] actualTypeArguments;

		if (type instanceof Class) {

			actualTypeArguments = ((Class<?>) type).getTypeParameters();

		}else {

			actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();

		}

		List<Class<?>> typeArgumentsAsClasses = new ArrayList<Class<?>>();

		// resolve types by chasing down type variables.
		for (Type baseType: actualTypeArguments) {

			while( resolvedTypes.containsKey(baseType) ) {

				baseType = resolvedTypes.get(baseType);
			}

			typeArgumentsAsClasses.add( getClass(baseType) );
		}

		return typeArgumentsAsClasses;
	}

	/**
	 * Get the underlying class for a type, or null if the type is a variable type.
	 * 
	 * @param type the type
	 * @return the underlying class
	 */
	private static Class<?> getClass(Type type) {

		if (type instanceof Class) {

			return (Class<?>) type;

		}else if (type instanceof ParameterizedType) {

			return getClass(((ParameterizedType) type).getRawType());

		}else if (type instanceof GenericArrayType) {

			Type componentType = ((GenericArrayType) type).getGenericComponentType();
			Class<?> componentClass = getClass(componentType);

			if (componentClass != null ) {

				return Array.newInstance(componentClass, 0).getClass();

			}else {

				return null;
			}

		}else {

			return null;
		}
	}

	/**
	 * Get an entity by id.
	 * 
	 * @param conn SQL Connection
	 * @param inId Id of the entity
	 * @return Single Entity Object if found. null otherwise
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public T findById(Connection conn, ID inId) throws SQLException {

		String whereClause =  processor.getTableName() + "." + processor.getIdColumnName() + " = ? ";

		return findSingleWithWhere( conn, whereClause, inId);
	}

	/**
	 *  Get an entity by WHERE Criteria
	 *  
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param params Query parameters for the WHERE Criteria
	 * @return Single Entity Object if found. null otherwise
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public T findSingleWithWhere(Connection conn, String whereClause, Object... params) throws SQLException {

		String sql = " select " + processor.getFullSelectSql() + " where " + whereClause;

		return findSingle( conn, sql, params );
	}

	/**
	 *  Get an entity by WHERE Criteria
	 *  
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param varMap Query Parameter Map
	 * @return Single Entity Object if found. null otherwise
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public T findSingleWithWhere(Connection conn, String whereClause, Map<String,Object> varMap) throws SQLException {

		String sql = " SELECT " + processor.getFullSelectSql() + " WHERE " + whereClause;

		return findSingle( conn, sql, varMap);
	}


	/**
	 *  Get an entity by SQL query
	 *  
	 * @param conn SQL Connection
	 * @param selectSql Query for Selecting the entity
	 * @param params Query Parameters
	 * @return Single Entity Object if found. null otherwise
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public T findSingle(Connection conn, String selectSql, Object... params) throws SQLException {

		logger.debug( "selectSql : " + selectSql );

		return dbQuery.query(conn, selectSql, (rs->{

			return toEntity(rs);

		}), params );
	}	

	/**
	 *  Get an entity by SQL query
	 *  
	 * @param conn SQL Connection
	 * @param selectSql Query for Selecting the entity
	 * @param varMap Query Parameter Map
	 * @return Single Entity Object if found. null otherwise
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public T findSingle(Connection conn, String selectSql, Map<String,Object> varMap) throws SQLException {

		logger.debug( "selectSql : " + selectSql );

		return dbQuery.query(conn, selectSql, (rs->{

			return toEntity(rs);

		}), varMap );
	}

	/**
	 * Get all records.
	 *
	 * @param conn SQL Connection
	 * @return A list of all records.
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findAll(Connection conn) throws SQLException {

		String whereClause = " 1 = 1 ";

		return findMultipleWithWhere( conn, whereClause, (Object[])null);
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param params Query Parameters
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findMultipleWithWhere(Connection conn, String whereClause, Object... params) throws SQLException {

		String sql = " SELECT " + processor.getFullSelectSql() + " WHERE " + whereClause;

		return findMultiple( conn, sql, params);
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param varMap Query Parameter Map
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findMultipleWithWhere(Connection conn, String whereClause, Map<String,Object> varMap) throws SQLException {

		String sql = " SELECT " + processor.getFullSelectSql() + " WHERE " + whereClause;

		return findMultiple( conn, sql, varMap );
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param params Query Parameters
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findDistinctWithWhere(Connection conn, String whereClause, Object... params) throws SQLException {

		String sql = " SELECT DISTICT " + processor.getFullSelectSql() + " WHERE " + whereClause;

		return findMultiple( conn, sql, params);
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param varMap Query Parameter Map
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findDistinctWithWhere(Connection conn, String whereClause, Map<String,Object> varMap) throws SQLException {

		String sql = " SELECT DISTICT " + processor.getFullSelectSql() + " WHERE " + whereClause;

		return findMultiple( conn, sql, varMap );
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param selectSql Query for Selecting the entity
	 * @param params Query Parameters
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public List<T> findMultiple(Connection conn, String selectSql, Object... params) throws SQLException {

		logger.debug( "selectSql : " + selectSql );

		return dbQuery.query(conn, selectSql, (rs->{

			return toList(rs);

		}), params );
	}

	/**
	 * Get a list of records.
	 * 
	 * @param conn SQL Connection
	 * @param selectSql Query for Selecting the entity
	 * @param varMap Query Parameter Map
	 * @return Multiple Records matching the query
	 * @throws SQLException If the query is malformed or cannot be executed 
	 */
	public List<T> findMultiple(Connection conn, String selectSql, Map<String,Object> varMap) throws SQLException {

		logger.debug( "selectSql : " + selectSql );

		return dbQuery.query(conn, selectSql, (rs->{

			return toList(rs);

		}), varMap );
	}

	/**
	 * Check whether ID is exists in Database
	 * 
	 * @param conn SQL Connection
	 * @param inId Id of the Entity
	 * @return true if id exists in database, false otherwise.
	 * @throws SQLException If the query is malformed or cannot be executed
	 */
	public boolean hasId( Connection conn, ID inId ) throws SQLException {

		return isIdPresentInDatabase(conn, processor.getTableMetaInfo(), inId);		
	}
	
	/**
	 * Convert ResultSet to Single Entity Object
	 * 
	 * @param rs ResultSet from database Query
	 * @return Single Entity Object
	 * @throws Exception IF the conversion fails
	 */
	private T toEntity( ResultSet rs ) throws Exception{

		/**
		 * First Key -> Table name
		 * Second key - > Current Entity Object ID
		 * Value - > ResultJoinData ( contains Parent Object Set and Current Entity  )
		 */

		/** Use LinkedHashMap. So the insertion order is preserved . Insertion order is required in processResult**/
		LinkedHashMap<String,Map<Object,ResultJoinData>> tableToEntityMap = new LinkedHashMap<String,Map<Object,ResultJoinData>>();

		// rootEntityMap is used to fetch root entity.
		// This redundant Object is used to improve performance
		Map<ID,T> rootEntityMap = new LinkedHashMap<ID,T>();

		//* Since it is join columns, there may be multiple rows coming from ResultSet
		while( rs.next() ) {

			mapRow( tableToEntityMap, rootEntityMap, rs );
		}

		T value = null;

		//We need data in root Set to process final result
		if( ! rootEntityMap.isEmpty() ) {

			// Re arrange entity values.
			processResult(tableToEntityMap);

			// Get first entry from result 
			value = rootEntityMap.values().iterator().next();
		}

		return value; 
	}

	/**
	 * Convert ResultSet to List of Entity Objects
	 * 
	 * @param rs ResultSet from database Query
	 * @return List of Entity Objects
	 * @throws Exception IF the conversion fails
	 */
	private List<T> toList( ResultSet rs ) throws Exception{

		/**
		 * First Key -> Table name
		 * Second key - > Current Entity Object ID
		 * Value - > ResultJoinData ( contains Parent Object Set and Current Entity  )
		 */

		/** Use LinkedHashMap. So the insertion order is preserved . Insertion order is required in processResult**/
		LinkedHashMap<String,Map<Object,ResultJoinData>> tableToEntityMap = new LinkedHashMap<String,Map<Object,ResultJoinData>>();

		// rootEntityMap is used to fetch root entity.
		// This redundant Object is used to improve performance
		Map<ID,T> rootEntityMap = new LinkedHashMap<ID,T>();

		while( rs.next() ) {

			mapRow( tableToEntityMap, rootEntityMap, rs );

		}

		List<T> list =  null;

		// We need data in root Set to process final result
		if( ! rootEntityMap.isEmpty() ) {

			// Re arrange entity values.
			processResult(tableToEntityMap);

			// Convert result entry to Collection 
			list =  new ArrayList<T>(rootEntityMap.values());

		}else {

			list = new ArrayList<T>();
		}

		return list;
	}

	private	void mapRow( Map<String,Map<Object,ResultJoinData>> tableToEntityMap, Map<ID,T> rootEntityMap, ResultSet rs ) throws Exception {

		/** store IDs of tables accessed in this result row */
		Map<String,Object> tableToObjectMap = new HashMap<String,Object>();

		String rootTableName = processor.getTableName(); 
		// Root TableMetaInfo
		TableMetaInfo tableMetaInfo = processor.getTableMetaInfo( );

		T rootEntity = populateRootEntity( tableMetaInfo, rootEntityMap, rs );

		tableToObjectMap.put( rootTableName, rootEntity);

		int startIndex = 1 + tableMetaInfo.getColumnCount();

		for( AssociationMetaInfo associationInfo : processor.getAssociationList() ) {

			String tableName = associationInfo.getChildTableName();

			tableMetaInfo = EntityProcessor.getTableMetaInfo( tableName );

			// Get parent Object for current row
			Object parentObject = tableToObjectMap.get( associationInfo.getParentTableName() );

			// check whether parent is available, 
			// If parent is not available, we wont be able to save this 
			if( null != parentObject ) {

				// Id Field
				Field idField = tableMetaInfo.getIdField();

				// get Id value of table being processed
				Object idValue = getValueFromResultSet( rs, startIndex, idField );

				if( null != idValue ) {

					// We got valid id from database
					// Check whether entity with this id is already populated or not
					Object entityObject =  null;

					ResultJoinData resultJoinData = getResultJoinData( tableToEntityMap, tableName, idValue);

					if( null == resultJoinData ) {

						// Create ResultJoinData
						resultJoinData = createResultJoinData( tableToEntityMap, tableName, tableMetaInfo, idValue );

						entityObject = resultJoinData.getEntity();

						// fill data.
						// Set id value
						setEntityFieldValue( tableMetaInfo, tableMetaInfo.getIdColumnName(), entityObject, idValue);

						// Fill subsequent fields for this object;
						fillRemainingEntityValues(tableMetaInfo, tableMetaInfo.getPrimitiveFieldMap(), entityObject, rs, startIndex);

					}else {

						entityObject = resultJoinData.getEntity();
					}

					// Put this entry to tableToIdMap. So subsequent columnInfo can use this.
					tableToObjectMap.put( tableName, entityObject);

					resultJoinData.getParentEntitySet().add( new ParentEntityData( associationInfo, parentObject) );

				}
			}

			//set start index for next table;
			startIndex += tableMetaInfo.getColumnCount();
		}

	}

	@SuppressWarnings("unchecked")
	private T populateRootEntity( TableMetaInfo tableMetaInfo, Map<ID,T> rootEntityMap, ResultSet rs  ) throws Exception {

		int startIndex = 1;

		// Id Field
		Field idField = tableMetaInfo.getIdField();

		// get Id value of table being processed
		ID idValue = (ID) getValueFromResultSet( rs, startIndex, idField );

		T rootEntity = rootEntityMap.get(idValue);
		if( null == rootEntity ) {

			rootEntity = (T) constructEntityObject(tableMetaInfo);

			rootEntityMap.put(idValue, rootEntity);

			// Set id value
			setEntityFieldValue( tableMetaInfo, tableMetaInfo.getIdColumnName(), rootEntity, idValue);			

			fillRemainingEntityValues( tableMetaInfo, tableMetaInfo.getPrimitiveFieldMap(), rootEntity, rs, startIndex );
		}

		return rootEntity;
	}

	/** Fill entries other than id ( starting from 1 ) **/
	private static void fillRemainingEntityValues( TableMetaInfo tableMetaInfo, Map<String, Field> fieldMap,
			Object entityObject, ResultSet rs, int startIndex) throws Exception {

		Iterator<Entry<String,Field>> fieldMapIterator = fieldMap.entrySet().iterator();

		for( int i = 1; fieldMapIterator.hasNext(); i++ ) {

			Entry<String,Field> entry = fieldMapIterator.next();

			Object value = getValueFromResultSet( rs,  startIndex + i, entry.getValue() );

			setEntityFieldValue( tableMetaInfo, entry.getKey(), entityObject, value);		
		}
	}

	private static void processResult( LinkedHashMap<String,Map<Object,ResultJoinData>> tableToEntityMap) throws Exception {

		/**
		 * We would be building the  result starting from the inner most child.
		 * So Iterate the LinkedHashMap in Reverse Order */

		/** Create reverse iterator **/
		Iterator<String> tableToObjMapItr = new LinkedList<String>(tableToEntityMap.keySet()).descendingIterator();

		for( ;tableToObjMapItr.hasNext();) {

			String tableName = tableToObjMapItr.next();

			Map<Object, ResultJoinData> entityJointMap = tableToEntityMap.get(tableName);

			if( null != entityJointMap ) {

				for( ResultJoinData joinData : entityJointMap.values() ) {

					Object entityObject = joinData.getEntity();

					// Add this entityObject to parent entities
					for( ParentEntityData parentData : joinData.getParentEntitySet() ) {

						Object parentEntity = parentData.getParentEntity();
						AssociationMetaInfo associationInfo = parentData.getAssociationInfo();

						Field parentField = associationInfo.getParentField();
						parentField.setAccessible(true);

						if( null != associationInfo.getParentContainerClass() ) {

							@SuppressWarnings("unchecked")
							Collection<Object> collection = (Collection<Object>) parentField.get( parentEntity );

							collection.add( entityObject );

						}else {

							// It is one to one mapping. Assign the object directly.
							// Preserve if value is already set.

							if( null == parentField.get( parentEntity ) ) {

								parentField.set(parentEntity, entityObject);
							}
						}
					}
				}
			}
		}
	}

	private static ResultJoinData getResultJoinData( Map<String,Map<Object,ResultJoinData>> tableToObjectMap,
			String tableAlias, Object idValue ) {

		ResultJoinData resultJoinData = null;

		Map<Object,ResultJoinData> idToObjectMap = tableToObjectMap.get( tableAlias );

		if( null != idToObjectMap ) {

			resultJoinData = idToObjectMap.get( idValue );
		}

		return resultJoinData;
	}

	private static ResultJoinData createResultJoinData(  Map<String,Map<Object,ResultJoinData>> tableToObjectMap,
			String tableName, TableMetaInfo tableMetaInfo, Object idValue ) throws Exception {

		Map<Object,ResultJoinData> idToObjMap = tableToObjectMap.get( tableName );

		if( null == idToObjMap ) {

			idToObjMap = new LinkedHashMap<Object,ResultJoinData>();			
			tableToObjectMap.put(tableName, idToObjMap);
		}

		ResultJoinData ResultJoinData = idToObjMap.get( idValue );

		if( null == ResultJoinData ) {

			Object object = constructEntityObject( tableMetaInfo );

			ResultJoinData = new ResultJoinData( object );

			idToObjMap.put(idValue, ResultJoinData);
		}

		return ResultJoinData;
	}

	private static Object constructEntityObject( TableMetaInfo tableMetaInfo ) throws Exception {

		Class<?> type = tableMetaInfo.getTableClass();

		if( null == type ) {
			throw new RuntimeException( " Class Type is not available ");
		}

		// Get Constructor
		Constructor<?> constructor = type.getConstructor();

		// Create Object
		Object entity = constructor.newInstance();

		for( Field field : tableMetaInfo.getCollectionFieldList() ) {

			Collection<Object> collection = (Collection<Object>)createCollectionObject( field.getType() );

			field.setAccessible(true);
			field.set(entity, collection);
		}

		return entity;
	}

	/** 
	 * Create container class
	 * 
	 * Supports following container classes
	 * 		1. TreeSet
	 * 		2. LinkedHashSet
	 * 		3. HashSet
	 * 		4. Set ( creates HashSet )
	 * 		5. ArrayList
	 * 		6. LinkedList
	 * 		7. List ( creates ArrayList )
	 * 		8. Collection ( creates ArrayList )  
	 * 	
	 * 
	 */
	private static Collection<Object> createCollectionObject( Class<?> containerClass ){

		Collection<Object> container = null;

		if( TreeSet.class.isAssignableFrom(containerClass) ) {
			container = new TreeSet<Object>();
		}else if( LinkedHashSet.class.isAssignableFrom(containerClass) ) {
			container = new LinkedHashSet<Object>();
		}else if( HashSet.class.isAssignableFrom(containerClass) ) {
			container = new HashSet<Object>();
		}else if( Set.class.isAssignableFrom(containerClass) ) {
			container = new HashSet<Object>();
		}else if( ArrayList.class.isAssignableFrom(containerClass) ) {
			container = new ArrayList<Object>();
		}else if( LinkedList.class.isAssignableFrom(containerClass) ) {
			container = new LinkedList<Object>();
		}else if( List.class.isAssignableFrom(containerClass) ) {
			container = new ArrayList<Object>();
		}else {
			container = new ArrayList<Object>();
		}

		return container;
	}
	
	/**
	 * Delete entity by Id
	 * 
	 * @param conn SQL Connection
	 * @param inId Id of entity to be deleted
	 * @return number of entities deleted
	 * @throws Exception If the delete operation fails
	 */
	public int deleteById( Connection conn, ID inId) throws Exception {

		String whereClause = processor.getTableName() + "." + processor.getIdColumnName() + " = ? ";

		return delete( conn, whereClause, inId);
	}
	
	/**
	 * Delete entity using Where criteria
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param params Query Parameters
	 * @return number of entities deleted
	 * @throws Exception If the delete operation fails
	 */
	public int delete( Connection conn, String whereClause, Object... params) throws Exception {

		int count = 0;
		
		boolean autoCommit = conn.getAutoCommit();

		if( autoCommit ) {
			
			try {
				
				// Disable Auto commits
				conn.setAutoCommit(false);
				
				//Call the method
				count = delete0( conn, whereClause, params );
				
				conn.commit();
				
			}catch(Throwable ex ) {

				conn.rollback();

				throw new Exception(ex);

			}finally {

				conn.setAutoCommit(autoCommit);
			}
			
		}else {
			
			// Manual commit is already enabled.
			// Call the method
			count = delete0( conn, whereClause, params );
		}

		return count;
	}
		
	private int delete0( Connection conn, String whereClause, Object... params) throws Exception {

		// Find all objects for the where clause
		List<T> entities = findMultipleWithWhere(conn, whereClause, params);

		for( T entity : entities ) {
			deleteEntitiesRecursive( conn, entity );
		}

		int count = entities.size();

		return count;
	}

	/**
	 * Delete entity using Where criteria
	 * 
	 * @param conn SQL Connection
	 * @param whereClause WHERE Criteria for the query
	 * @param varMap Query Parameter Map
	 * @return number of entities deleted
	 * @throws Exception If the delete operation fails
	 */
	public int delete( Connection conn, String whereClause, Map<String,Object> varMap) throws Exception {
		
		int count = 0;
		
		boolean autoCommit = conn.getAutoCommit();

		if( autoCommit ) {
			
			try {
				
				// Disable Auto commits
				conn.setAutoCommit(false);
				
				//Call the method
				count = delete0( conn, whereClause, varMap );
				
				conn.commit();
				
			}catch(Throwable ex ) {

				conn.rollback();

				throw new Exception(ex);

			}finally {

				conn.setAutoCommit(autoCommit);
			}
			
		}else {
			
			// Manual commit is already enabled.
			// Call the method
			count = delete0( conn, whereClause, varMap );
		}

		return count;
	}
	
	private int delete0( Connection conn, String whereClause, Map<String,Object> varMap) throws Exception {

		// Find all objects for the where clause
		List<T> entities = findMultiple(conn, whereClause, varMap);

		for( T entity : entities ) {
			deleteEntitiesRecursive( conn, entity );
		}

		int count = entities.size();

		return count;
	}


	private void deleteEntitiesRecursive( Connection conn, Object toDelete ) throws Exception {

		TableMetaInfo tableMetaInfo = EntityProcessor.getTableMetaInfo( toDelete );

		if( null != tableMetaInfo  ) {

			for( AssociationMetaInfo associationInfo : tableMetaInfo.getAssociations() ) {

				if( associationInfo instanceof ManyToManyMetaInfo ) {

					/** For ManyToMany, delete entries from bridgeTable **/
					deleteManyToManyBridgeData(conn, toDelete, tableMetaInfo, (ManyToManyMetaInfo)associationInfo );
				}

				// Check whether cascade is set
				if( associationInfo.getCascades().contains( CascadeType.ALL) 
						|| associationInfo.getCascades().contains( CascadeType.REMOVE) ) {

					// Get child object from Association Field.
					Object childObject = associationInfo.getParentField().get( toDelete );

					if( null != childObject ) {

						if (Collection.class.isAssignableFrom( childObject.getClass() )) {
							// It it collection
							@SuppressWarnings("unchecked")
							Collection<Object> collection = (Collection<Object>)childObject;

							for( Object collectionEntry : collection ) {

								deleteEntitiesRecursive( conn, collectionEntry );
							}

						}else {
							deleteEntitiesRecursive( conn, childObject );
						}
					}
				}
			}

			// Child entries deleted. Delete this entry
			deleteEntityFromTable( conn, tableMetaInfo, toDelete );
		}
	}

	private void deleteManyToManyBridgeData( Connection conn, Object entity, 
			TableMetaInfo tableMetaInfo, ManyToManyMetaInfo manyToManyAssociation) throws Exception {

		String bridgeTable = manyToManyAssociation.getBridgeTableName();

		Map<String,Object> varMap = new HashMap<String,Object>();

		for( ColumnJoinInfo joinInfo : manyToManyAssociation.getParentToBridgeJoinList() ) {

			Object value = getEntityFieldValueInSqlType(tableMetaInfo, joinInfo.getParentColumn(), entity );

			varMap.put( joinInfo.getChildColumn() , value);
		}

		List<String> columnList = new ArrayList<String>();

		varMap.keySet().forEach( (columnName) -> { columnList.add( columnName + "=:" + columnName );} );

		String deleteSql = " DELETE FROM " + bridgeTable + " WHERE "+ String.join( ",", columnList);

		logger.debug( "deleteSql : " + deleteSql );

		// Execute query to delete entry from database
		dbQuery.update( conn, deleteSql, varMap);
	}


	private void deleteEntityFromTable( Connection conn, TableMetaInfo tableMetaInfo, Object toDelete  ) throws Exception {

		//DELETE FROM table_name WHERE table_name.ID = ?

		String tableName = tableMetaInfo.getTableName();

		String idColumnName = tableMetaInfo.getIdColumnName();

		Object idValue = getIDValue(toDelete);

		String deleteSql = " DELETE FROM " + tableName + " WHERE " + tableName + "." + idColumnName + " = ? ";

		logger.debug( "deleteSql : " + deleteSql );

		// Execute query to delete entry from database
		dbQuery.update( conn, deleteSql, idValue);

	}
	
	/**
	 * Save an Entity
	 * 
	 * @param conn SQL Connection
	 * @param inItem Entity Object to be saved.
	 * @return Instance of Saved entity instance 
	 * @throws Exception If the entity save operation fails
	 */
	public T save(Connection conn, T inItem) throws Exception {

		T value = null;
		
		boolean autoCommit = conn.getAutoCommit();

		if( autoCommit ) {
			
			try {
				
				// Disable Auto commits
				conn.setAutoCommit(false);
				
				//Call the method
				value = save0( conn, inItem );
				
				conn.commit();
				
			}catch(Throwable ex ) {

				conn.rollback();

				throw new Exception(ex);

			}finally {

				conn.setAutoCommit(autoCommit);
			}
			
		}else {
			// Manual commit is already enabled.
			// Call the method
			value = save0( conn, inItem );
		}

		return value;
	}
		
	@SuppressWarnings("unchecked")
	private T save0(Connection conn, T inItem) throws Exception {

		T existingEntity = null;

		ID idValue = (ID) getIDValue(inItem);

		if( null != idValue ) {
			existingEntity = findById(conn, idValue);
		}

		// Update including Nested entities
		TableMetaInfo tableMetaInfo = EntityProcessor.getTableMetaInfo( inItem );

		// Check whether PERSIST or MERGE
		CascadeType cascadeType =  isIdPresentInDatabase( conn, tableMetaInfo, idValue ) 
				? CascadeType.MERGE 
						: CascadeType.PERSIST;

		Map<String,Object> savedDataMap = updateEntityRecursive( conn, tableMetaInfo, existingEntity, inItem, cascadeType);

		idValue = (ID) savedDataMap.get( tableMetaInfo.getIdColumnName() );

		T value = findById(conn, idValue);

		return value;
	}

	/**
	 * Saves list of entities.
	 * 
	 * @param conn SQL Connection
	 * @param entities List of entities to be saved to database.
	 * @return List of saved entities.
	 * @throws Exception If the entity save operation fails
	 */
	public List<T> saveAll( Connection conn, Iterable<T> entities) throws Exception {
		
		List<T> result = null;
		
		boolean autoCommit = conn.getAutoCommit();

		if( autoCommit ) {
			
			try {
				
				// Disable Auto commits
				conn.setAutoCommit(false);
				
				//Call the method
				result = saveAll0( conn, entities );
				
				conn.commit();
				
			}catch(Throwable ex ) {

				conn.rollback();

				throw new Exception(ex);

			}finally {

				conn.setAutoCommit(autoCommit);
			}
			
		}else {
			// Manual commit is already enabled.
			// Call the method
			result = saveAll0( conn, entities );
		}

		return result;
	}
	
	@SuppressWarnings("unchecked")
	private List<T> saveAll0( Connection conn, Iterable<T> entities) throws Exception {

		// To use batch insert/update the we need to check 
		// whether the input contains non null value for @Id field
		// Based on that we need to call either insert or update.
		// But keeping same insertion order in return list will be difficult.
		// So we are simply calling save here.
		// Which will take care of calling either insert or update based on @Id field value.
		// But we will not get the benefit of BATCH operation.

		List<T> result = new ArrayList<T>();

		for (Object inItem : entities) {

			T existingEntity = null;

			ID idValue = (ID) getIDValue(inItem);

			if( null != idValue ) {
				existingEntity = findById(conn, idValue);
			}

			// Update including Nested entities
			TableMetaInfo tableMetaInfo = EntityProcessor.getTableMetaInfo( inItem );

			// Check whether PERSIST or MERGE
			CascadeType cascadeType =  isIdPresentInDatabase( conn, tableMetaInfo, idValue ) 
					? CascadeType.MERGE 
							: CascadeType.PERSIST;
			Map<String,Object> savedDataMap = updateEntityRecursive( conn, tableMetaInfo, existingEntity, inItem, cascadeType);

			idValue = (ID) savedDataMap.get( tableMetaInfo.getIdColumnName() );

			result.add( findById(conn, idValue) );
		}

		return result;
	}

	/** Update nested entities **/
	private Map<String,Object> updateEntityRecursive( Connection conn,
			TableMetaInfo tableMetaInfo, Object existing, Object inItem, CascadeType cascadeType ) throws Exception{

		Map<String,Object> inItemSavedDataMap = null;

		if( null != existing &&  ! existing.getClass().equals(inItem.getClass() ) ){
			throw new Exception( "Entity Classes are not same ! " + existing.getClass() + " and " + inItem.getClass() );
		}

		// Generate column and data to be updated to database
		Map<String,Object> inItemDataMap = generateDataMapForTable(tableMetaInfo, inItem);

		/** For SingleTarget Association, ID of Child entity should be set in this entity.
		 * So save those entities before saving this entities
		 */
		for( SingleTargetMetaInfo singleTargetAssociation : tableMetaInfo.getSingleTargertAssociations() ) {

			// Check whether cascade is set
			if( singleTargetAssociation.getCascades().contains( CascadeType.ALL) 
					|| singleTargetAssociation.getCascades().contains( cascadeType) ) {

				TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( singleTargetAssociation.getChildTableName() );

				Map<String,Object> childSaveDataMap = null;

				Object existingChildObj = null != existing ?
						singleTargetAssociation.getParentField().get(existing)
						: null;

				Object inItemChildObj =  null != inItem ?
						singleTargetAssociation.getParentField().get(inItem)
						: null;

				if( null != existingChildObj && null != inItemChildObj ) {
					// Entities are present in both Database and InItem

					// existing will always contain valid Id, because it is coming from database
					if(  getIDValue(existingChildObj)
							.equals( getIDValue(inItemChildObj)  ) ) {

						// Both are having same id, so update entity.							
						childSaveDataMap = updateEntityRecursive( conn, childTableMetaInfo, existingChildObj, inItemChildObj, cascadeType );
					}else {
						// Ids are different or inItem is new one.
						// delete existing
						deleteEntitiesRecursive(conn, existingChildObj);

						// save new one ( insert or update )
						childSaveDataMap = updateEntityRecursive(conn, childTableMetaInfo, null, inItemChildObj, cascadeType);
					}

				}else if( null != existingChildObj ){

					// Entity is present in database But not in inItem 
					//( null check is there in first 'if' condition )
					// Delete existing from database
					deleteEntitiesRecursive(conn, existingChildObj);

				}else if( null != inItemChildObj ){

					// Entity is present in inItem but in in database. 
					//( null check is there in first 'if' condition )
					// So save the item to database.
					childSaveDataMap = updateEntityRecursive( conn, childTableMetaInfo, existingChildObj, inItemChildObj, cascadeType );
				}

				ColumnJoinInfo columnJoin = singleTargetAssociation.getColumnJoin();
				if( null != childSaveDataMap ) {
					// childSaveDataMap will be non-null, if we save the child entity
					// Update the details to saveDataMap
					inItemDataMap.put( columnJoin.getParentColumn(), childSaveDataMap.get(columnJoin.getChildColumn()));
				}else {
					// save null value to parent. Because child entity is not present. 
					inItemDataMap.remove( columnJoin.getParentColumn() );
				}
			}
		}

		// Save inItem to inItem Table and get values for inItem Table.
		inItemSavedDataMap = saveAndReturnTableDataMap( conn, tableMetaInfo, inItemDataMap );

		// set the join column values to association entity and save them
		for( AssociationMetaInfo associationInfo : tableMetaInfo.getAssociations() ) {

			/**** One To Many Association ***
			 * Also check whether cascade is set*/
			if( associationInfo instanceof OneToManyMetaInfo 
					&& (associationInfo.getCascades().contains( CascadeType.ALL) 
							|| associationInfo.getCascades().contains( cascadeType)) ) {

				// Read the nested entities to Collection
				Collection<Object> existingChildObjList = new ArrayList<Object>();

				Collection<Object> inItemChildObjList = new ArrayList<Object>();

				getAssociationCollectionObjects( associationInfo, existing, inItem, existingChildObjList, inItemChildObjList);

				OneToManyMetaInfo oneToManyAssociation = (OneToManyMetaInfo)associationInfo;

				TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( associationInfo.getChildTableName() );

				for( Object existingChild : existingChildObjList ) {

					// existing is from Database, So ID will be never NULL.
					Object existingIdValue = getIDValue(existingChild);

					Object inItemChildWithSameId = null;

					for( Object inItemChild : inItemChildObjList ) {

						Object inItemIdValue = getIDValue(inItemChild);

						if( existingIdValue.equals(inItemIdValue) ) {

							inItemChildWithSameId = inItemChild;
							break;
						}
					}

					if( null == inItemChildWithSameId ) {

						// InItem Child with same id not present.
						// So DELETE the entry from database.
						deleteEntitiesRecursive(conn, existingChild);

					}else {

						// UPDATE existing items
						// Some times inItem child entities might not have join column values set. Set them
						for( ColumnJoinInfo columnJoin : oneToManyAssociation.getColumnJoinList() ) {

							Object parentValue = inItemSavedDataMap.get( columnJoin.getParentColumn() );

							setEntityFieldValue( childTableMetaInfo, columnJoin.getChildColumn(), inItemChildWithSameId, parentValue );
						}

						// UPDATE items
						updateEntityRecursive( conn, childTableMetaInfo, existingChild, inItemChildWithSameId, cascadeType );

						// Remove the item from inItemChildObjList.
						// Finally, we will save the remaining items from inItemChildObjList as new entries.
						inItemChildObjList.remove(inItemChildWithSameId);
					}
				}

				// ADD pending items from inItemChildObjList to repository.
				for( Object inItemChild : inItemChildObjList ) {

					// Some times inItem child entities might not have join column values set. Set them
					for( ColumnJoinInfo columnJoin : oneToManyAssociation.getColumnJoinList() ) {

						Object parentValue = inItemSavedDataMap.get( columnJoin.getParentColumn() );

						setEntityFieldValue( childTableMetaInfo, columnJoin.getChildColumn(), inItemChild, parentValue );
					}

					// INSERT items
					updateEntityRecursive( conn, childTableMetaInfo, null, inItemChild, cascadeType );
				}

			}else if( associationInfo instanceof ManyToManyMetaInfo ) {

				/**** Many To Many Association ****/
				ManyToManyMetaInfo manyToManyAssociation = (ManyToManyMetaInfo)associationInfo;

				// Read the nested entities to Collection
				Collection<Object> existingChildObjList = new ArrayList<Object>();

				Collection<Object> inItemChildObjList = new ArrayList<Object>();

				getAssociationCollectionObjects( associationInfo, existing, inItem, existingChildObjList, inItemChildObjList);

				TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( associationInfo.getChildTableName() );

				for( Object existingChild : existingChildObjList ) {

					// existing is from Database, So ID will be never NULL.
					Object existingIdValue = getIDValue(existingChild);

					Object inItemChildWithSameId = null;

					for( Object inItemChild : inItemChildObjList ) {

						Object inItemIdValue = getIDValue(inItemChild);

						if( existingIdValue.equals(inItemIdValue) ) {

							inItemChildWithSameId = inItemChild;
							break;
						}
					}

					if( null == inItemChildWithSameId ) {

						// InItem Child with same id not present.
						// So DELETE the entry from database if cascade is set

						// Delete from bridgeTable first. Otherwise, there may be foreign key constraint error.
						Map<String,Object> existingChildDataMap = generateDataMapForTable(childTableMetaInfo, existingChild);

						deleteFromBridgeTable(conn, childTableMetaInfo, manyToManyAssociation, inItemDataMap, existingChildDataMap);

						// if cascade is set, delete entity
						if( associationInfo.getCascades().contains( CascadeType.ALL) 
								|| associationInfo.getCascades().contains( CascadeType.REMOVE) ) {

							deleteEntitiesRecursive(conn, existingChild);
						}

					}else {

						// if cascade is set, UPDATE
						if( associationInfo.getCascades().contains( CascadeType.ALL) 
								|| associationInfo.getCascades().contains( cascadeType) ) {

							// Delete from bridgeTable first. Otherwise, there may be foreign key constraint error.
							Map<String,Object> existingChildDataMap = generateDataMapForTable(childTableMetaInfo, existingChild);

							deleteFromBridgeTable(conn, childTableMetaInfo, manyToManyAssociation, inItemDataMap, existingChildDataMap);

							Map<String,Object> childEntitySavedDataMap 
							= updateEntityRecursive( conn, childTableMetaInfo, existingChild, inItemChildWithSameId, cascadeType );

							// Insert to bridge table
							insertToBridgeTable(conn, childTableMetaInfo, manyToManyAssociation, inItemSavedDataMap, childEntitySavedDataMap);
						}

						// Remove the item from inItemChildObjList.
						// Finally, we will save the remaining items from inItemChildObjList as new entries.
						inItemChildObjList.remove(inItemChildWithSameId);
					}
				}

				// If cascade is set, Save the entry and Insert in to bridge table.
				if( associationInfo.getCascades().contains( CascadeType.ALL) 
						|| associationInfo.getCascades().contains( cascadeType) ) {

					for( Object inItemChild : inItemChildObjList ) {

						Map<String,Object> childEntitySavedDataMap = updateEntityRecursive( conn, childTableMetaInfo, null, inItemChild, cascadeType );

						// insert to bridge table
						insertToBridgeTable(conn, childTableMetaInfo, manyToManyAssociation, inItemSavedDataMap, childEntitySavedDataMap);
					}

				}else {

					// If entry is present in database, Add to bridge table.
					for( Object inItemChild : inItemChildObjList ) {

						Map<String,Object> childEntityDataMap = generateDataMapForTable(childTableMetaInfo, inItemChild);

						if( isIdPresentInDatabase( conn, childTableMetaInfo, childEntityDataMap.get( childTableMetaInfo.getIdColumnName() ) ) ) {

							// insert to bridge table
							insertToBridgeTable(conn, childTableMetaInfo, manyToManyAssociation, inItemSavedDataMap, childEntityDataMap);
						}
					}
				}
			}
		}

		return inItemSavedDataMap;
	}

	private void deleteFromBridgeTable( Connection conn, TableMetaInfo tableMetaInfo, ManyToManyMetaInfo manyToManyAssociation,
			Map<String,Object> parentDataMap, Map<String,Object> childDataMap ) throws SQLException {

		// DELETE from bridgeTable
		// where association join from both parent to bridge and bridge to child

		Map<String,Object> varMap = new HashMap<String,Object>();

		for( ColumnJoinInfo columnJoinInfo :  manyToManyAssociation.getParentToBridgeJoinList()) {

			varMap.put( columnJoinInfo.getChildColumn(), parentDataMap.get( columnJoinInfo.getParentColumn() ));
		}

		for( ColumnJoinInfo columnJoinInfo :  manyToManyAssociation.getBridgeToChildJoinList()) {

			varMap.put( columnJoinInfo.getParentColumn(), childDataMap.get( columnJoinInfo.getChildColumn() ));
		}

		List<String> columnList = new ArrayList<String>();

		varMap.keySet().forEach( (columnName) -> { columnList.add( columnName + "=:" + columnName );} );

		String columnListStr = String.join( " AND ", columnList);

		String deleteSql = "DELETE FROM " + manyToManyAssociation.getBridgeTableName() + " WHERE " + columnListStr;

		logger.debug( "deleteSql : " + deleteSql );

		dbQuery.update( conn, deleteSql, varMap);
	}

	private void insertToBridgeTable( Connection conn, TableMetaInfo tableMetaInfo, ManyToManyMetaInfo manyToManyAssociation,
			Map<String,Object> parentDataMap, Map<String,Object> childDataMap ) throws SQLException {
		// Insert in to bridgeTable set values, 
		// association join from both parent to bridge and bridge to child ( new values )

		Map<String,Object> varMap = new HashMap<String,Object>();

		for( ColumnJoinInfo columnJoinInfo :  manyToManyAssociation.getParentToBridgeJoinList()) {

			varMap.put( columnJoinInfo.getChildColumn(), parentDataMap.get( columnJoinInfo.getParentColumn() ));
		}

		for( ColumnJoinInfo columnJoinInfo :  manyToManyAssociation.getBridgeToChildJoinList()) {

			varMap.put( columnJoinInfo.getParentColumn(), childDataMap.get( columnJoinInfo.getChildColumn() ));
		}

		String keysJoined = String.join(", ", varMap.keySet());

		String values = ":" + String.join(", :", varMap.keySet());

		String insertSql = "INSERT INTO " + manyToManyAssociation.getBridgeTableName() + " (" + keysJoined + ") VALUES (" + values + ")";

		logger.debug( "insertSql : " + insertSql );

		dbQuery.update(conn, insertSql, varMap);
	}

	@SuppressWarnings("unchecked")
	private void getAssociationCollectionObjects( AssociationMetaInfo associationInfo, Object existing,  Object inItem,
			Collection<Object> existingChildObjList, Collection<Object> inItemChildObjList) throws Exception
	{

		Object existingChildObj = null != existing ?
				associationInfo.getParentField().get(existing)
				: null;

		Object inItemChildObj =  null != inItem ?
				associationInfo.getParentField().get(inItem)
				: null;

		if( null != associationInfo.getParentContainerClass() ) {

			// Entity is type of collection
			if( null != existingChildObj ) {
				existingChildObjList.addAll( (Collection<Object>) existingChildObj );
			}

			if( null != inItemChildObj ) {
				inItemChildObjList.addAll( (Collection<Object>) inItemChildObj );
			}

		} else {

			// Entity is single object
			// Put the entity to collection for easy processing in next steps
			if( null != existingChildObj ) {
				existingChildObjList.add(existingChildObj);
			}

			if( null != inItemChildObj ) {
				inItemChildObjList.add(inItemChildObj);
			}
		}
	}

	private Map<String,Object> saveAndReturnTableDataMap(Connection conn, 
			TableMetaInfo tableMetaInfo, Map<String,Object> saveDataMap) throws SQLException {

		Object idValue = null;

		// Check whether entity is present in Database
		if( isIdPresentInDatabase( conn, tableMetaInfo, saveDataMap.get( tableMetaInfo.getIdColumnName() ) ) ) {
			idValue = updateTableDataMap( conn, tableMetaInfo, saveDataMap );
		}else {
			idValue = insertTableDataMap( conn, tableMetaInfo, saveDataMap );
		}

		// Read Table data to Map
		String selectSql = " SELECT " +  String.join(", ", tableMetaInfo.getDatabaseColumnList() ) 
		+ " FROM " + tableMetaInfo.getTableName()
		+ " WHERE " + tableMetaInfo.getIdColumnName() + " =? ";

		logger.debug( "selectSql : " + selectSql );

		return dbQuery.query(conn, selectSql, toEntityTypeMap(tableMetaInfo), idValue);
	}

	protected static ThrowableFunction<ResultSet,Map<String, Object>> toEntityTypeMap( TableMetaInfo tableMetaInfo ){

		List<String> databaseColumnList = tableMetaInfo.getDatabaseColumnList();

		return ( (ResultSet rs) -> {

			Map<String, Object> result = null;

			if (rs.next()) {

				result = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);

				ResultSetMetaData rsmd = rs.getMetaData();
				int cols = rsmd.getColumnCount();

				for (int i = 1; i <= cols; i++) {

					String columnName = databaseColumnList.get( i -1 );

					Field field = getPrimitiveFieldForColumnName(tableMetaInfo, columnName);

					Object value = getValueFromResultSet( rs, i, field );

					result.put(columnName, value);
				}
			}

			return result;

		} );

	}
	
	private static Field getPrimitiveFieldForColumnName( TableMetaInfo tableMetaInfo, String columnName ) {

		Field field = tableMetaInfo.getIdColumnName().equals(columnName) ? tableMetaInfo.getIdField() 
				: tableMetaInfo.getPrimitiveFieldMap().get(columnName);

		if( null == field ) {

			for( SingleTargetMetaInfo association : tableMetaInfo.getSingleTargertAssociations() ) {

				if( columnName.equals(association.getColumnJoin().getParentColumn()) ) {

					TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( association.getChildTableName() );

					field = getPrimitiveFieldForColumnName( childTableMetaInfo, association.getColumnJoin().getChildColumn() );
					break;
				}
			}
		}

		return field;
	}

	/** Get's a value from a {@code ResultSet} while performing some additional null checks.**/
	private static Object getValueFromResultSet(ResultSet rs, int index, Field field ) throws SQLException {

		Class<?> targetType = field.getType();

		Object value = rs.getObject(index);

		if( null != value ) {

			try {

				Class<?> sourceType = value.getClass(); 

				if( sourceType == Clob.class 
						|| "oracle.sql.CLOB".equals( sourceType.getName() ) ) {

					Clob clob = (Clob)value;

					// Convert To String
					value = clob.getSubString(1, (int) clob.length() );

				}else if( sourceType == Blob.class 
						|| "oracle.sql.BLOB".equals( sourceType.getName() ) ) {

					Blob blob = (Blob)value;

					//Convert To Byte Array
					value = blob.getBytes(1, (int) blob.length());
				}

				// Check and convert using convert Annotation 
				value = getConvertedValueFromDatabase( value, field);

				//value = CommonConvert.convertToCompatibleType( value, targetType );

			}catch( Exception ex ) {

				throw new SQLException( "Could not convert value at index :" + index 
						+ " Object Type : " + value.getClass() + " Field Type " +  targetType + " Field name " +  field.getName(),  ex);
			}
		}

		return value;
	}

	@SuppressWarnings("unchecked")
	private static Object getConvertedValueFromDatabase( Object inItem, Field field) throws Exception {

		Object value = inItem;

		Convert convert = field.getAnnotation( Convert.class );

		ConverterType converterType = field.getAnnotation( ConverterType.class );

		if( null != convert ) {

			if( ! convert.converter().isAssignableFrom( AttributeConverter.class ) ) {
				throw new Exception( " Convert is not a AttributeConverter type " );
			}

			value = DataConverter.convertToEntityAttribute( value, convert.converter() );

		}else if( null != converterType ) {

			value = DataConverter.convertToEntityAttribute( value, converterType.value() );
		}

		return value;
	}

	protected static void setEntityFieldValue( TableMetaInfo tableMetaInfo, String columnName, Object entity, Object value ) throws Exception {

		if( null != value ) {

			Field field = tableMetaInfo.getIdColumnName().equals(columnName) ? tableMetaInfo.getIdField() 
					: tableMetaInfo.getPrimitiveFieldMap().get(columnName);

			if( null != field ) {

				field.setAccessible( true );

				Object newValue = DataConverter.convertToCompatibleType( value, field.getType() ) ;

				field.set(entity, newValue);

			}else {

				for( SingleTargetMetaInfo singleTargetAssociation : tableMetaInfo.getSingleTargertAssociations() ) {

					Object childEntity = singleTargetAssociation.getParentField().get(entity);

					// We can set to child, only if child is not null.
					if( null != childEntity ) {

						ColumnJoinInfo columnJoin = singleTargetAssociation.getColumnJoin();

						if( columnName.equals( columnJoin.getParentColumn()) ) {

							TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( singleTargetAssociation.getChildTableName() );
							setEntityFieldValue( childTableMetaInfo, columnJoin.getChildColumn(), childEntity, value);

							break;
						}
					}
				}
			}
		}

	}


	protected static Object getEntityFieldValue( TableMetaInfo tableMetaInfo, String columnName, Object entity ) throws Exception {

		Object value = null;

		Field field = tableMetaInfo.getIdColumnName().equals(columnName) ? tableMetaInfo.getIdField() 
				: tableMetaInfo.getPrimitiveFieldMap().get(columnName);

		if( null != field ) {

			field.setAccessible( true );
			value = field.get(entity);

		}else {

			for( SingleTargetMetaInfo singleTargetAssociation : tableMetaInfo.getSingleTargertAssociations() ) {

				Object childEntity = singleTargetAssociation.getParentField().get(entity);

				// We can get from child, only if child is not null.
				if( null != childEntity ) {

					ColumnJoinInfo columnJoin = singleTargetAssociation.getColumnJoin();

					if( columnName.equals( columnJoin.getParentColumn()) ) {

						TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( singleTargetAssociation.getChildTableName() );
						value = getEntityFieldValue( childTableMetaInfo, columnJoin.getChildColumn(), childEntity );

						break;
					}
				}
			}

		}

		return value;
	}

	protected static Object getEntityFieldValueInSqlType( TableMetaInfo tableMetaInfo, String columnName, Object entity ) throws Exception {

		Object value = null;

		Field field = tableMetaInfo.getIdColumnName().equals(columnName) ? tableMetaInfo.getIdField() 
				: tableMetaInfo.getPrimitiveFieldMap().get(columnName);

		if( null != field ) {

			field.setAccessible( true );
			// Get the value and convert to SQL Type
			value = DataConverter.convertValueToSql(field, field.get(entity) );

		}else {

			for( SingleTargetMetaInfo singleTargetAssociation : tableMetaInfo.getSingleTargertAssociations() ) {

				Object childEntity = singleTargetAssociation.getParentField().get(entity);

				// We can set to child, only child is not null.
				if( null != childEntity ) {

					ColumnJoinInfo columnJoin = singleTargetAssociation.getColumnJoin();

					if( columnName.equals( columnJoin.getParentColumn()) ) {

						TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( singleTargetAssociation.getChildTableName() );
						value = getEntityFieldValueInSqlType( childTableMetaInfo, columnJoin.getChildColumn(), childEntity );

						break;
					}
				}
			}

		}

		return value;
	}

	
	protected static Object getIDValue( Object inItem )  throws Exception {

		TableMetaInfo tableMetaInfo = EntityProcessor.getTableMetaInfo( inItem );
		return getEntityFieldValue( tableMetaInfo, tableMetaInfo.getIdColumnName(), inItem );
	}

	protected static Map<String,Object> generateDataMapForTable(TableMetaInfo tableMetaInfo, Object entity ) throws Exception{

		Map<String,Object> dataMap =  new TreeMap<String,Object>( String.CASE_INSENSITIVE_ORDER );

		// Add ID Value
		Object value = getEntityFieldValueInSqlType( tableMetaInfo, tableMetaInfo.getIdColumnName(), entity );
		dataMap.put(tableMetaInfo.getIdColumnName(), value );

		// Add primitive Fields
		for( Entry<String, Field> entry : tableMetaInfo.getPrimitiveFieldMap().entrySet() ) {
			
			// Set value only if entry is not present OR it is null in paramMap
			if( ! dataMap.containsKey( entry.getKey() ) || null == dataMap.get( entry.getKey()  )) {
				
				value = getEntityFieldValueInSqlType( tableMetaInfo,  entry.getKey(), entity );
				dataMap.put( entry.getKey(), value);
			}
		}

		// Add SingleTarget Fields
		for( SingleTargetMetaInfo singleTargetAssociation : tableMetaInfo.getSingleTargertAssociations() ) {
			
			ColumnJoinInfo columnJoin = singleTargetAssociation.getColumnJoin();

			// Set value only if entry is not present OR it is null in paramMap
			if( ! dataMap.containsKey( columnJoin.getParentColumn() ) 
					|| null == dataMap.get( columnJoin.getParentColumn() )) {

				value = null;
				
				Object childEntity = singleTargetAssociation.getParentField().get(entity);

				// We can set to child, only if child is not null.
				if( null != childEntity ) {

					TableMetaInfo childTableMetaInfo = EntityProcessor.getTableMetaInfo( singleTargetAssociation.getChildTableName() );
					value = getEntityFieldValueInSqlType( childTableMetaInfo, columnJoin.getChildColumn(), childEntity );
				}
				
				dataMap.put( columnJoin.getParentColumn(), value);
			}
			
		}

		return dataMap;
	}

	protected Object insertTableDataMap( Connection conn, 
			TableMetaInfo tableMetaInfo, Map<String,Object> saveDataMap ) throws SQLException {

		String[] generatdKeys = new String[] { tableMetaInfo.getIdColumnName() }; 

		String keysJoined = String.join(", ", saveDataMap.keySet());

		String values = ":" + String.join(", :", saveDataMap.keySet());

		String insertSql = "INSERT INTO " + tableMetaInfo.getTableName() + " (" + keysJoined + ") values (" + values + ")";

		logger.debug( "insertSql : " + insertSql );

		// Get generated ID
		Object idValue =  dbQuery.insert(conn, insertSql, generatdKeys, DbQuery.<Object>toID(), saveDataMap);

		if( null == idValue ) {

			// Get the idValue from entity.
			idValue = saveDataMap.get( tableMetaInfo.getIdColumnName() );
		}

		return idValue;
	}

	private Object updateTableDataMap( Connection conn, 
			TableMetaInfo tableMetaInfo, Map<String,Object> saveDataMap ) throws SQLException {

		Object idValue = saveDataMap.get( tableMetaInfo.getIdColumnName() );

		/** Update data only if values are different except updatable fields**/
		if( ! isEntitySameInDatabase( conn, tableMetaInfo, saveDataMap  ) ) {

			String whereClause = tableMetaInfo.getTableName() + "." + tableMetaInfo.getIdColumnName() 
			+ " =:" + tableMetaInfo.getIdColumnName();

			Map<String,Object> whereVarMap = new HashMap<String,Object>();

			whereVarMap.put(tableMetaInfo.getIdColumnName(), saveDataMap.get(tableMetaInfo.getIdColumnName()));

			List<String> columnList = new ArrayList<String>();

			saveDataMap.keySet().forEach( (columnName) -> { columnList.add( columnName + "=:" + columnName );} );

			String columnListStr = String.join( ",", columnList);

			String updateSql = "UPDATE " + tableMetaInfo.getTableName() + " SET " + columnListStr;

			// Convert Named Parameter query to '?' query.
			List<Object> paramList = new ArrayList<Object>();

			String parameterizedUpdateSql = DbQuery.parseNamedParameterQuery( updateSql, saveDataMap, paramList );

			String parameterizedWhereClause = DbQuery.parseNamedParameterQuery( whereClause, whereVarMap, paramList );

			parameterizedUpdateSql += " WHERE " + parameterizedWhereClause;

			Object[] params = paramList.toArray(new Object[0]);

			logger.debug( "updateSql : " + parameterizedUpdateSql );

			int rowsUpdated = dbQuery.update(conn, parameterizedUpdateSql, params );

			if( 0 == rowsUpdated) {

				throw new SQLException( "Update failed for ID :" + idValue );
			}

		}

		return idValue;
	}	

	/** Check whether entity is present in database **/
	private boolean isIdPresentInDatabase( Connection conn, TableMetaInfo tableMetaInfo, Object idValue ) throws SQLException {

		boolean bret = false;

		if( null != idValue ) {

			String tableName = tableMetaInfo.getTableName( );

			String idSelectSql = " SELECT " + tableMetaInfo.getIdColumnName() + " FROM " + tableName 
					+ " WHERE " + tableName + "." + tableMetaInfo.getIdColumnName() + " = ? ";

			logger.debug( "idSelectSql : " + idSelectSql );

			Object idValueFromDB =  dbQuery.query(conn, idSelectSql, DbQuery.<Object>toID(), idValue);

			bret = null != idValueFromDB;
		}

		return bret;
	}
	
	/** Check whether entity is present in database **/
	private boolean isEntitySameInDatabase( Connection conn, TableMetaInfo tableMetaInfo, Map<String,Object> entityDataMap ) throws SQLException {
		
		// Create Data map with Non Update fields
		Map<String,Object> nonAutoUpdateDataMap = new HashMap<String,Object>();
		
		for( Entry<String,Object> entry : entityDataMap.entrySet() ) {
			
			String key = entry.getKey();
			
			if( ! tableMetaInfo.getAutoUpdateColumnSet().contains( key ) ) {
				nonAutoUpdateDataMap.put(key, entry.getValue());
			}			
		}
		
		boolean bret = false;
		
		List<String> columnList = new ArrayList<String>();
		
		nonAutoUpdateDataMap.keySet().forEach( (columnName) -> { columnList.add( columnName + "=:" + columnName );} );

		String whereClause = String.join( " AND ", columnList);
		
		String idSelectSql = " SELECT " + tableMetaInfo.getIdColumnName() + " FROM " + tableMetaInfo.getTableName() 
				+ " WHERE " + whereClause;
		
		logger.debug( "idSelectSql : " + idSelectSql );
		
		Object idValueFromDB =  dbQuery.query(conn, idSelectSql, DbQuery.<Object>toID(), nonAutoUpdateDataMap);
		
		bret = null != idValueFromDB;

		return bret;
	}
}

