package io.github.codesakshi.simplejpa.repository;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import io.github.codesakshi.simplejpa.repository.Annotations.UpdateTimeStamp;

/**
 * Parse the entity to generate select SQL and Association detailss 
 * 
 * @author anilalps
 *
 * @param <T> Entity Type
 * @param <ID> ID Type
 */
public class EntityProcessor<T,ID> {

	@SuppressWarnings("serial")
	private static List<Class<? extends Annotation>> AUTO_UPDATE_ANNOTATIONS = new ArrayList<Class<? extends Annotation>>() {
		{ 
			add(UpdateTimeStamp.class); 
		}
	};
	
	/** class to store Column Join Information*/
	public static class ColumnJoinInfo{

		private String parentColumn;
		private String childColumn;

		public ColumnJoinInfo(String parentColumn, String childColumn) {
			super();
			this.parentColumn = parentColumn;
			this.childColumn = childColumn;
		}

		public String getParentColumn() {
			return parentColumn;
		}

		public String getChildColumn() {
			return childColumn;
		}
	}

	// Base class for Associations
	public static class AssociationMetaInfo{

		protected String childTableName;

		protected String parentTableName;

		protected Field parentField;

		protected Class<?> parentContainerClass;
		
		protected List<CascadeType> cascades;

		public AssociationMetaInfo(String childTableName, String parentTableName, Field parentField, 
				Class<?> parentContainerClass, CascadeType[] cascades) {
			super();
			this.childTableName = childTableName;
			this.parentTableName = parentTableName;
			this.parentField = parentField;
			this.parentContainerClass = parentContainerClass;
			this.cascades = new ArrayList<CascadeType>( Arrays.asList( cascades ));
		}

		public String getChildTableName() {
			return childTableName;
		}

		public String getParentTableName() {
			return parentTableName;
		}

		public Field getParentField() {
			return parentField;
		}

		public Class<?> getParentContainerClass() {
			return parentContainerClass;
		}
				
		public List<CascadeType> getCascades() {
			return cascades;
		}
	};

	public static class SingleTargetMetaInfo extends AssociationMetaInfo {

		protected ColumnJoinInfo columnJoin; 

		public SingleTargetMetaInfo(String childTableName, String parentTableName, 
				Field parentField,Class<?> parentContainerClass, CascadeType[] cascades) {
			
			super(childTableName, parentTableName, parentField, parentContainerClass, cascades);
		}
		
		public ColumnJoinInfo getColumnJoin() {
			return columnJoin;
		}

		public void setColumnJoin(ColumnJoinInfo columnJoin) {
			this.columnJoin = columnJoin;
		}
	};

	public static class OneToManyMetaInfo extends AssociationMetaInfo {

		protected List<ColumnJoinInfo> columnJoinList = new ArrayList<ColumnJoinInfo>();
		
		public OneToManyMetaInfo(String childTableName, String parentTableName,
				Field parentField, Class<?> parentContainerClass, CascadeType[] cascades) {
			
			super(childTableName, parentTableName, parentField, parentContainerClass, cascades);			
		}

		public List<ColumnJoinInfo> getColumnJoinList() {
			return columnJoinList;
		}
	};

	public static class ManyToManyMetaInfo extends AssociationMetaInfo {

		protected String bridgeTableName;

		protected List<ColumnJoinInfo> parentToBridgeJoinList = new ArrayList<ColumnJoinInfo>();

		protected List<ColumnJoinInfo> bridgeToChildJoinList = new ArrayList<ColumnJoinInfo>();

		public ManyToManyMetaInfo(String childTableName, String parentTableName, Field parentField,
				Class<?> parentContainerClass, CascadeType[] cascades) {
			
			super(childTableName, parentTableName, parentField, parentContainerClass, cascades);
		}
		
		public String getBridgeTableName() {
			return bridgeTableName;
		}

		public void setBridgeTableName(String bridgeTableName) {
			this.bridgeTableName = bridgeTableName;
		}

		public List<ColumnJoinInfo> getParentToBridgeJoinList() {
			return parentToBridgeJoinList;
		}

		public List<ColumnJoinInfo> getBridgeToChildJoinList() {
			return bridgeToChildJoinList;
		}
	};

	/**
	 *  Internal structure to keep Fields and Join Info
	 */
	public static class TableMetaInfo{

		/** table name **/
		protected String tableName;

		/** Class Type of this table */
		protected Class<?> tableClass;

		protected String idColumnName;

		protected Field idField;

		/** List of primitive fields in the object **/
		protected Map<String,Field> columnFieldMap = new LinkedHashMap<String,Field>();

		/** Associations ****/
		protected List<AssociationMetaInfo> multipleTargetAssociations = new ArrayList<AssociationMetaInfo>();
		
		/** Many to One associations ***/
		protected List<SingleTargetMetaInfo> singleTargertAssociations = new ArrayList<SingleTargetMetaInfo>();

		/** List of Collection fields in this entity. Used for Object creation**/
		protected List<Field> collectionFieldList;
		
		/** Columns in Database table including Many To One association **/
		protected List<String> databaseColumnList;
		
		protected Set<String> autoUpdateColumnSet;

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		
		public Class<?> getTableClass() {
			return tableClass;
		}

		public void setTableClass(Class<?> tableClass) {
			this.tableClass = tableClass;
		}

		public int getColumnCount() {
			return columnFieldMap.size() + 1; // 1 = Idfield
		}

		public String getIdColumnName() {
			return idColumnName;
		}

		public void setIdColumnName(String idColumnName) {
			this.idColumnName = idColumnName;
		}

		public Field getIdField() {
			return idField;
		}

		public void setIdField(Field idField) {
			this.idField = idField;
		}

		public Map<String, Field> getPrimitiveFieldMap() {
			return columnFieldMap;
		}

		public void addColumnFieldEntry( String columnName, Field field) {
			this.columnFieldMap.put(columnName, field);
		}

		@SuppressWarnings("serial")
		public List<AssociationMetaInfo> getAssociations() {
				
			return new ArrayList<AssociationMetaInfo>() {
			{ addAll(singleTargertAssociations); addAll(multipleTargetAssociations); } };
		}

		public void addAssociation( OneToManyMetaInfo association) {
			
			this.multipleTargetAssociations.add(association);
		}
		
		public void addAssociation( ManyToManyMetaInfo association) {
			
			this.multipleTargetAssociations.add(association);
		}		
		
		public void addAssociation( SingleTargetMetaInfo association) {
	
			this.singleTargertAssociations.add(association);
		}

		public List<SingleTargetMetaInfo> getSingleTargertAssociations() {
			return singleTargertAssociations;
		}

		public List<Field> getCollectionFieldList() {
			return collectionFieldList;
		}

		public void setCollectionFieldList(List<Field> collectionFieldList) {
			this.collectionFieldList = collectionFieldList;
		}
		
		public List<String> getDatabaseColumnList() {
			return databaseColumnList;
		}
		
		public void setDatabaseColumnList( List<String> databaseColumnList) {
			this.databaseColumnList = databaseColumnList;
		}

		public Set<String> getAutoUpdateColumnSet() {
			return autoUpdateColumnSet;
		}

		public void setAutoUpdateColumnSet(Set<String> autoUpdateColumnList) {
			this.autoUpdateColumnSet = autoUpdateColumnList;
		}
	}

	/** Singleton Manager class to store all entity details for application ***/
	private static class TableMetaInfoManager{

		protected static TableMetaInfoManager instance = null;

		public static TableMetaInfoManager getInstance() {

			if( null == instance ) {
				instance = new TableMetaInfoManager();
			}

			return instance;
		}

		// Hide constructor
		protected TableMetaInfoManager() {}

		protected Map<String,TableMetaInfo> tableMetaInfoMap = new ConcurrentHashMap<String,TableMetaInfo>();

		public void addTableMetaInfoMap( String tableName, TableMetaInfo tableMetaInfo ) {
			this.tableMetaInfoMap.put( tableName, tableMetaInfo);
		}

		public TableMetaInfo getTableMetaInfo( String tableName ) {
			return this.tableMetaInfoMap.get( tableName );
		}
	}

	/**
	 * Get TableMetaInfo 
	 * @param tableName name of the table
	 * @return TableMetaInfo of the argument given
	 */
	public static TableMetaInfo getTableMetaInfo( String tableName ) {
		
		return TableMetaInfoManager.getInstance().getTableMetaInfo(tableName);
	}
	
	/**
	 * Get TableMetaInfo
	 * 
	 * @param entityClass Class of the entity
	 * @return TableMetaInfo of the argument given
	 */
	public static TableMetaInfo getTableMetaInfo( Class<?> entityClass ) {
		
		return getTableMetaInfo( getTableName( entityClass ) );
	}
	
	/**
	 *  Get TableMetaInfo
	 *  
	 * @param entity entity Object
	 * @return TableMetaInfo of the argument given
	 */
	public static TableMetaInfo getTableMetaInfo( Object entity ) {
		
		return getTableMetaInfo( entity.getClass() );
	}	
	
	/**
	 * Add TableMetaInfo
	 * 
	 * @param tableName name of the table
	 * @param tableMetaInfo TableMetaInfo of the given table
	 */
	public static void addTableMetaInfoMap( String tableName, TableMetaInfo tableMetaInfo ) {
		
		TableMetaInfoManager.getInstance().addTableMetaInfoMap( tableName, tableMetaInfo);
	}

	/** END static methods to access TableMetaInfoManager **/
	
	
	/*** EntityProcessor Implementation starts ***/
	protected String rootTableName;

	protected List<AssociationMetaInfo> associationList = new ArrayList<AssociationMetaInfo>();

	protected String fullSelectSql;
	
	/**
	 * Constructor
	 *  
	 * @param entityClass Class of the entity
	 * @param idClass Class of the @id field
	 */
	public EntityProcessor( Class<T> entityClass, Class<ID> idClass )  {

		Field idField = getFieldByAnnotation( entityClass, Id.class );

		if( null == idField ) {
			throw new RuntimeException( entityClass  + " Do not have @Id Annotated field");
		}

		if( ! idClass.equals( idField.getType()  ) ) {

			throw new RuntimeException( idClass  + " Do not match with @Id Annotated field");
		}

		// get root table name
		rootTableName = getTableName( entityClass );

		generateEntityDetails( entityClass, rootTableName );

		// generate Association List
		generateAssociationListAndSql();

	}

	private static void generateEntityDetails( Class<?> entityClass, String tableName ) throws RuntimeException {

		// check if already processed
		if( null == getTableMetaInfo(  tableName ) ) {

			if( ! isEntity( entityClass ) ){
				throw new RuntimeException( entityClass + " is not an Entity class. Entity Class should have @Entity Annotation ");
			}

			Field idField = getFieldByAnnotation( entityClass, Id.class );

			if( null == idField ) {
				throw new RuntimeException( entityClass  + " Do not have @Id Annotated field");
			}

			TableMetaInfo tableMetaInfo = new TableMetaInfo();

			addTableMetaInfoMap(tableName, tableMetaInfo);
			
			tableMetaInfo.setTableName( tableName );
			
			tableMetaInfo.setTableClass( entityClass );

			List<Field> fieldList = getEntityFields( entityClass );

			Field field = fieldList.get(0);

			String columnName = getColumnName( field );

			tableMetaInfo.setIdField(field);

			tableMetaInfo.setIdColumnName(columnName);

			for( int i = 1; i < fieldList.size(); i ++ ) {

				field = fieldList.get(i);

				if( field.isAnnotationPresent(ManyToOne.class ) ){

					processManyToOneField( tableName, entityClass, tableMetaInfo, field );
					
				}else if( field.isAnnotationPresent(OneToOne.class )){
					
					processOneToOneField( tableName, entityClass, tableMetaInfo, field );

				}else if( field.isAnnotationPresent(OneToMany.class )){

					processOneToManyField( tableName, entityClass, tableMetaInfo, field );

				}else if( field.isAnnotationPresent(ManyToMany.class ) ){

					processManyToManyField( tableName, entityClass, tableMetaInfo, field );

				}else {
					/*** Primitive field **/
					columnName = getColumnName( field );
					tableMetaInfo.addColumnFieldEntry(columnName, field);
				}
			}

			processExtra( tableMetaInfo, entityClass);
		}
	}

	private static List<Field> getEntityFields( Class<?> entityClass ){

		List<Field> fieldList = new ArrayList<Field>();

		// We need to add IdField at the beginning
		Field IdField = null;

		for (Field field : entityClass.getDeclaredFields()) {

			field.setAccessible(true);

			if( ( ! Modifier.isStatic( field.getModifiers() ) ) &&
					! field.isAnnotationPresent(Transient.class) ) {

				if( field.isAnnotationPresent(Id.class) ) {

					IdField = field;
					
				}else {
					
					fieldList.add( field );
				}
			}
		}

		if( null == IdField ) {
			throw new RuntimeException("Id Field is missing for entitiy : " + entityClass.getName() );
		}

		// Insert id field at the beginning
		fieldList.add(0, IdField);

		return fieldList;
	}

	private static void processManyToOneField( String tableName, Class<?> entityClass, TableMetaInfo tableMetaInfo, Field field ) throws RuntimeException {

		ManyToOne annotatoin = field.getAnnotation(ManyToOne.class);
		
		Class<?> childEntityClass =  annotatoin.targetEntity();
		if( void.class == childEntityClass ) {
			childEntityClass = getChildEntityClass( field );
		}

		String childTableName = getTableName( childEntityClass );

		// Populate nested objects
		generateEntityDetails( childEntityClass, childTableName );

		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
		String columnName = ( null != joinColumn  ) ? joinColumn.name() : field.getName();
			
		/** 
		 * for ManyToOne, childIdColumn will be Id column from child entity
		 */	
		String childIdColumnName = getColumnNameForIdField( childEntityClass );

		Class<?> parentContainerClass = getContainerClassFromField(field);
		
		// Add Association details
		SingleTargetMetaInfo info = new SingleTargetMetaInfo( childTableName, tableName, 
				field, parentContainerClass, annotatoin.cascade() );

		info.setColumnJoin( new ColumnJoinInfo(columnName, childIdColumnName));
		tableMetaInfo.addAssociation( info);
	}
	
	private static void processOneToOneField( String tableName, Class<?> entityClass, TableMetaInfo tableMetaInfo, Field field ) throws RuntimeException {

		OneToOne annotatoin = field.getAnnotation(OneToOne.class);
		
		Class<?> childEntityClass =  annotatoin.targetEntity();
		if( void.class == childEntityClass ) {
			childEntityClass = getChildEntityClass( field );
		}

		String childTableName = getTableName( childEntityClass );

		// Populate nested objects
		generateEntityDetails( childEntityClass, childTableName );

		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
		String columnName = ( null != joinColumn  ) ? joinColumn.name() : field.getName();
			
		/** 
		 * for OneToOne, childIdColumn will be Id column from child entity
		 */	
		String childIdColumnName = getColumnNameForIdField( childEntityClass );

		Class<?> parentContainerClass = getContainerClassFromField(field);
		
		// Add Association details
		SingleTargetMetaInfo info = new SingleTargetMetaInfo( childTableName, tableName, 
				field, parentContainerClass, annotatoin.cascade() );

		info.setColumnJoin( new ColumnJoinInfo(columnName, childIdColumnName));
		tableMetaInfo.addAssociation( info);
	}	

	private static void processOneToManyField( String tableName, Class<?> entityClass, TableMetaInfo tableMetaInfo, Field field ) throws RuntimeException {

		OneToMany annotatoin = field.getAnnotation(OneToMany.class);
		
		Class<?> childEntityClass =  annotatoin.targetEntity();
		if( void.class == childEntityClass ) {
		
			childEntityClass = getChildEntityClass( field );
		}

		String childTableName = getTableName( childEntityClass );

		// Populate nested objects
		generateEntityDetails( childEntityClass, childTableName );		

		/** 
		 * for OneToMany, defaultRefColumn will be column from parent entity
		 */

		String parentDefaultRefColumn = getColumnNameForIdField( entityClass );

		Class<?> parentContainerClass = getContainerClassFromField(field);

		// Add Association details
		OneToManyMetaInfo info = new OneToManyMetaInfo( childTableName,tableName, field,
				parentContainerClass, annotatoin.cascade() );
		
		List<ColumnJoinInfo> joinInfoList = new ArrayList<ColumnJoinInfo>();
		
		// Add JoinColumns
		JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
		if( null != joinColumns  ) {
			joinInfoList.addAll( getColumnJoins(joinColumns.value(), parentDefaultRefColumn));
		}
			
		// Add JoinColumn		
		JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
		if( null != joinColumn  ) {
			joinInfoList.add( getColumnJoin(joinColumn, parentDefaultRefColumn ));
		}
		
		// If none specified, take default values
		if( null == joinColumns && null == joinColumn) {
			
			String columnName = getColumnName( field );
			joinInfoList.add( new ColumnJoinInfo(columnName, parentDefaultRefColumn) );
		}

		info.getColumnJoinList().addAll( joinInfoList );

		tableMetaInfo.addAssociation( info);
	}

	private static void processManyToManyField( String tableName, Class<?> entityClass, TableMetaInfo tableMetaInfo, Field field ) throws RuntimeException {

		ManyToMany annotatoin = field.getAnnotation(ManyToMany.class);
		
		Class<?> childEntityClass =  annotatoin.targetEntity();
		
		if( void.class == childEntityClass ) {
			childEntityClass = getChildEntityClass( field );
		}

		String childTableName = getTableName( childEntityClass );

		// Populate nested objects
		generateEntityDetails( childEntityClass, childTableName );

		String parentDefaultRefColumn = getColumnNameForIdField( entityClass );

		Class<?> parentContainerClass = getContainerClassFromField(field);
		
		// Add Association details
		ManyToManyMetaInfo info = new ManyToManyMetaInfo( childTableName,tableName, field, 
				parentContainerClass, annotatoin.cascade() );
			
		JoinTable joinTable = field.getAnnotation(JoinTable.class);
		if( null == joinTable  ) {
			
			throw new RuntimeException( " ManyToMany annotation should specify 'JoinTable' Annotation also!" );
		}
		
		info.setBridgeTableName( joinTable.name() );

		// Prepare Join column list for Parent Entity to Bridge Table
		List<ColumnJoinInfo> joinInfoList = getColumnJoins(joinTable.joinColumns(), parentDefaultRefColumn);
		
		info.getParentToBridgeJoinList().addAll( joinInfoList );
		
		//Prepare Join column list for Bridge Table to child Entity 
		String childDefaultRefColumn = getColumnNameForIdField( childEntityClass );
		
		List<ColumnJoinInfo> inverseJoinColumnList = getInverseJoinColumns( joinTable.inverseJoinColumns(), childDefaultRefColumn );

		info.getBridgeToChildJoinList().addAll( inverseJoinColumnList );

		tableMetaInfo.addAssociation( info);

	}

	private static void processExtra( TableMetaInfo tableMetaInfo, Class<?> entityClass ) {

		// Set collection fields. Used for constructing collection member variables
		List<Field> collectionFields =  getCollectionFieldList(entityClass);

		tableMetaInfo.setCollectionFieldList(collectionFields);
		
		// Generate and set  fetch SQL columns for this table
		@SuppressWarnings("serial")
		List<String> databaseColumnList = new ArrayList<String>() {
			{ 
				add( tableMetaInfo.getIdColumnName() );
				addAll( tableMetaInfo.getPrimitiveFieldMap().keySet() ); 
			}
		};

		tableMetaInfo.getSingleTargertAssociations().forEach( ( singleTargerAssociation) -> { 
			databaseColumnList.add( singleTargerAssociation.getColumnJoin().getParentColumn() );
		});
				
		tableMetaInfo.setDatabaseColumnList(databaseColumnList);

		// Add Auto update fields
		List<Field> fieldList = getEntityFields( entityClass );

		Set<String> autoUpdateColumnSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER); 

		for( Field field : fieldList ) {

			for( Class<? extends Annotation> updatableClass : AUTO_UPDATE_ANNOTATIONS ) {

				if( field.isAnnotationPresent( updatableClass) ) {
					autoUpdateColumnSet.add( getColumnName( field ) );
					break;
				}
			}
		}

		tableMetaInfo.setAutoUpdateColumnSet( autoUpdateColumnSet );
	}
	
	private static List<ColumnJoinInfo> getColumnJoins( JoinColumn[] joinColumnArr, String parentDefaultRefColumn ){

		List<ColumnJoinInfo> joinList = new ArrayList<ColumnJoinInfo>();

		for( int i = 0; i < joinColumnArr.length; i ++ ) {

			ColumnJoinInfo columnJoin = getColumnJoin(joinColumnArr[i], parentDefaultRefColumn);

			joinList.add( columnJoin);

		}

		return joinList;
	}
	
	private static ColumnJoinInfo getColumnJoin( JoinColumn joinColumn, String parentDefaultRefColumn ){
	
		String childColumn = joinColumn.name();
		String parentRefColumn = joinColumn.referencedColumnName();

		if( parentRefColumn.isEmpty() ) {
			parentRefColumn = parentDefaultRefColumn;
		}
		
		/** for ColumnJoinInfo, parentColumn column will be from parent table 
		 * and childReferenceColumn column will be from child table 
		 */
		return new ColumnJoinInfo(parentRefColumn, childColumn );
	}

	private static List<ColumnJoinInfo> getInverseJoinColumns( JoinColumn[] joinColumnArr, String childDefaultRefColumn ){

		List<ColumnJoinInfo> joinList = new ArrayList<ColumnJoinInfo>();

		for( int i = 0; i < joinColumnArr.length; i ++ ) {

			JoinColumn joinColumn = joinColumnArr[i];
			
			String parentColumn = joinColumn.name();

			String childReferenceColumn = joinColumn.referencedColumnName();

			if( childReferenceColumn.isEmpty() ) {
				childReferenceColumn = childDefaultRefColumn;
			}

			/** for ColumnJoinInfo, parentColumn column will be from parent table 
			 * and childReferenceColumn column will be from child table 
			 */
			ColumnJoinInfo columnJoin = new ColumnJoinInfo( parentColumn, childReferenceColumn );

			joinList.add( columnJoin);

		}

		return joinList;

	}

	/** get Collection fields in Entity Class. Used for Object creation **/
	private static List<Field> getCollectionFieldList( Class<?> entityClass ){

		List<Field> fieldList = new ArrayList<Field>();

		for (Field field : entityClass.getDeclaredFields()) {

			field.setAccessible(true);

			if( ( ! Modifier.isStatic( field.getModifiers() ) ) && 
					! field.isAnnotationPresent(Transient.class) ) {

				if (Collection.class.isAssignableFrom( field.getType() )) {

					fieldList.add( field );
				}
			}
		}

		return fieldList;
	}

	private static Class<?> getChildEntityClass(Field field) {

		// Join can be of two types. 
		// 1. Collection<E>
		// 2. One to One ... where Class itself is used.

		Class<?> fieldClass = field.getType();

		if (Collection.class.isAssignableFrom(fieldClass)) {

			// It is collection. get inner type
			ParameterizedType parameterized = (ParameterizedType) field.getGenericType();

			if( 0 == parameterized.getActualTypeArguments().length ) {
				throw new RuntimeException( fieldClass + " Do not have an entity type set");
			}

			Class<?> innerClass = (Class<?>) parameterized.getActualTypeArguments()[0];

			if( ! isEntity( innerClass ) ){
				throw new RuntimeException( innerClass + " is not an Entity class. Entity Class should have @Entity Annotation");
			}

			return innerClass;

		}else {

			if( ! isEntity( fieldClass ) ){
				throw new RuntimeException( fieldClass + " is not an Entity class. Entity Class should have @Entity Annotation");
			}

			return fieldClass;
		}

	}
	
	private static boolean isEntity(  Class<?> clazz ) {
		return clazz.isAnnotationPresent(Entity.class);
	}

	private static Field getIdField( Class<?> clazz) {
		return getFieldByAnnotation( clazz, Id.class );
	}

	private static Class<?> getContainerClassFromField(Field field) {

		// Join can be of two types. 
		// 1. Collection<E>
		// 2. To One ... where Class itself is used.

		Class<?> fieldClass = field.getType();
		if (Collection.class.isAssignableFrom( fieldClass ) ) {

			return fieldClass;
		}

		return null;
	}

	private static String getColumnNameForIdField( Class<?> entityClass ) {

		Field idField = getIdField(entityClass);

		return getColumnName( idField );
	}

	private void generateAssociationListAndSql( ) {

		StringBuilder selectColumnBuff = new StringBuilder();

		StringBuilder joinOperationBuff = new StringBuilder();

		Set<String> cycleCheckSet = new HashSet<String>();

		cycleCheckSet.add(rootTableName);

		generateAssociationListAndSql(rootTableName, rootTableName, 
				selectColumnBuff, joinOperationBuff, associationList, cycleCheckSet, new HashSet<String>() );

		// Remove ',' at last position.
		selectColumnBuff.setLength( selectColumnBuff.length() - 1);

		selectColumnBuff.append( " from " + rootTableName );

		selectColumnBuff.append( joinOperationBuff );

		fullSelectSql = String.valueOf( selectColumnBuff );		
	}

	/***  Generate Association Information and Query. Starting from root, Removing cycle  * */
	private static void generateAssociationListAndSql( String parentTableName, String parentTableAlias, 
			StringBuilder selectColumnBuff, StringBuilder joinOperationBuff,
			List<AssociationMetaInfo> associationList, Set<String> cycleCheckSet, Set<String> aliasSet ){

		TableMetaInfo tableMetaInfo = getTableMetaInfo(parentTableName);

		// Add IdColummnName
		selectColumnBuff.append( parentTableAlias + "." + tableMetaInfo.getIdColumnName() + ", " );

		// Add additional columns
		tableMetaInfo.getPrimitiveFieldMap().keySet().forEach( (column ) ->{

			selectColumnBuff.append( parentTableAlias + "." + column + "," );

		});

		for( AssociationMetaInfo associationInfo : tableMetaInfo.getAssociations() ) {

			String childTableName = associationInfo.getChildTableName();

			// check whether any cycle dependency is there.
			if( ! cycleCheckSet.contains( childTableName ) ) {

				String childTableAlias = generateTableAlias( aliasSet, childTableName);

				if( associationInfo instanceof SingleTargetMetaInfo ) {

					SingleTargetMetaInfo singleTargetAssociation = (SingleTargetMetaInfo)associationInfo;

					generateSqlJoinOperation( joinOperationBuff, parentTableAlias, childTableName, childTableAlias, 
							Arrays.asList(singleTargetAssociation.getColumnJoin()) );

				}else if( associationInfo instanceof OneToManyMetaInfo ) {

					OneToManyMetaInfo oneToManyAssociation = (OneToManyMetaInfo)associationInfo;

					generateSqlJoinOperation( joinOperationBuff, parentTableAlias, childTableName, childTableAlias, 
							oneToManyAssociation.getColumnJoinList()  );

				}else if( associationInfo instanceof ManyToManyMetaInfo ) {

					ManyToManyMetaInfo manyToManyAssociation = (ManyToManyMetaInfo)associationInfo;

					String bridgeTable = manyToManyAssociation.getBridgeTableName();

					// Always Generate Alias for Bridge table-- for sake of future issues
					String bridgeTableAlias = generateTableAlias( aliasSet, bridgeTable);

					// Generate Join for Parent and Bridge
					generateSqlJoinOperation( joinOperationBuff, parentTableAlias, bridgeTable, bridgeTableAlias, 
							manyToManyAssociation.getParentToBridgeJoinList()  );

					// generate Join for Bridge and Child
					generateSqlJoinOperation( joinOperationBuff, bridgeTableAlias, childTableName, childTableAlias, 
							manyToManyAssociation.getBridgeToChildJoinList()  );
					
				}

				associationList.add(associationInfo);

				cycleCheckSet.add(childTableName);

				generateAssociationListAndSql( childTableName, childTableAlias, 
						selectColumnBuff, joinOperationBuff, associationList, cycleCheckSet, aliasSet );

				cycleCheckSet.remove(childTableName);
			}

		}
	}

	private static void generateSqlJoinOperation( StringBuilder joinOperationBuff, String tableAlias, 
			String childTableName, String childTableAlias, List<ColumnJoinInfo> joinInfoList ) {

		joinOperationBuff.append( " LEFT JOIN " + childTableName + " " + childTableAlias );
		joinOperationBuff.append( " ON " );

		List<String> joinColumns = new ArrayList<String>();

		// example: Deck.asfId  = User.deckid and Deck.otherId = User.someId
		for( ColumnJoinInfo joinInfo : joinInfoList ) {

			String entry =  tableAlias + "." + joinInfo.getParentColumn() + " = " + childTableAlias + "." + joinInfo.getChildColumn();
			joinColumns.add( entry );
		}

		joinOperationBuff.append( String.join(" and ", joinColumns) + " " );

	}

	private static String generateTableAlias( Set<String> aliasSet, String tabeleName ) {

		String tableAlias = tabeleName;
		if( aliasSet.contains(tabeleName) ) {
			// C mean column
			tableAlias = "c" + String.valueOf( aliasSet.size() ) + "_" +  tabeleName;
		}

		aliasSet.add(tableAlias);

		return tableAlias;
	}

	/**
	 * Get Associations of this entity
	 * @return Associations of this entity
	 */
	public List<AssociationMetaInfo> getAssociationList() {
		return associationList;
	}
	
	/**
	 *  
	 * @return select sql with join table details
	 */
	public String getFullSelectSql() {

		return fullSelectSql;
	}
	
	/**
	 * 
	 * @return table name of the entity
	 */
	public String getTableName() {
		return rootTableName;
	}
	
	/**
	 * Get Root table TableMetaInfo
	 * @return TableMetaInfo for this entity
	 */
	public TableMetaInfo getTableMetaInfo() {
		return getTableMetaInfo(rootTableName);
	}

	/**
	 * Get @id Column Name
	 * @return Id Column Name for this entity
	 */
	public String getIdColumnName() {
		return getTableMetaInfo().getIdColumnName();
	}
	
	@SuppressWarnings("unchecked")
	private static Field getFieldByAnnotation( Class<?> clazz, Class annotationClazz) {

		Field ret = null;

		for (Field field : clazz.getDeclaredFields()) {

			field.setAccessible(true);

			if ( ( ! Modifier.isStatic( field.getModifiers() ) ) && 
					field.isAnnotationPresent( annotationClazz )) {
				
				ret = field;
				break;
			}
		}

		return ret;
	}

	/**
	 * Get Table name of the given entity Class
	 * 
	 * @param entityClass Class for getting Table Name
	 * @return Table name of the given entity Class
	 */
	public static String getTableName( Class<?> entityClass ) {

		String name = null;
		Table  table = entityClass.getAnnotation(Table.class);
		if( null != table ) {
			name = table.name();
		}

		if( null == name ) {
			name = entityClass.getSimpleName();
		}

		return name;
	}

	/**
	 * Get Column Name of the given field
	 * 
	 * @param field Field for getting column name
	 * @return Column Name of the given field
	 */
	private static String getColumnName(Field field ) {

		String name = null;
		Column column = field.getAnnotation(Column.class);
		if( null != column  ) {

			name = column.name();

		}else {

			if( field.isAnnotationPresent(ManyToOne.class)
					|| field.isAnnotationPresent(OneToOne.class) ) {
				
				JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
				if( null != joinColumn  ) {
					name = joinColumn.name();
				}
			}
		}

		if( null == name ) {
			name = field.getName();
		}

		return name;
	}
}

