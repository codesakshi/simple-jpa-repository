
# Simple JPA Repository

This little library allows you to communicate with database with out writing single SQL query in your application. This was created to be lightweight and meant for a quick and simple access to your database. It is meant for projects which want to interact with their database in a simple, but easy way, without bloating the source code with SQL queries.

If your project demands complex SQL queries, you can use DbQuery Class present in the library to execute SQL queries and use 'java.util.function.Function' interface to map data from SQL ResultSet to your desired POJO Class.

Feature requests are always welcome. Just open an issue. Also, if my documentation is missing something or unclear, please let me know.


## Features
#### 1. Based On javax.persistence-api
#### 2. Support following JPA Annotations
 * Entity - To to mark a Class as an Entity. Fields in Entity Annotation are not supported
 * Table
 * Column
 * Id
 * JoinColumn
 * JoinColumns
 * JoinTable
 * CascadeType ( ALL, MERGE, PERSIST and REMOVE are supported. DETACH and REFRESH are not applicable )
 * Transient
#### 3. Support following Association operations
 * OneToOne
 * OneToMany
 * ManyToOne
 * ManyToMany( Using Bridge Table )
 
#### 4. Support following library specific Annotation.
 * UpdateTimeStamp() - Automatically update time in Database for an Entity. ( Not specified in javax.persistence-api )
#### 5. Support Native SQL queries
Native SQL queries are supported. You can use Native SQL query in the 'WHERE' clause or Native SQL query for full operation

## Not Supported
#### 1. Connection Management
Connection creation, management, pooling are not in scope. You can use Connection Pool libraries. ( example: HikariCP)
#### 2. JPA query is not supported
All queries will be based on underlying Database Dialect. So it is not easy to change Database with out modifying SQL queries used in the application.
#### 3. Composite Keys are not supported
@ id can be used only in a single column in the database. Composite Keys are not supported.
#### 4. EntityManger is not supported.
	
## Setup

Include the Maven artifact:

```xml
	
	<!-- https://mvnrepository.com/artifact/io.github.codesakshi/simple-jpa-repository -->
	<dependency>
	    <groupId>io.github.codesakshi</groupId>
	    <artifactId>simple-jpa-repository</artifactId>
	    <version>1.0.1</version>
	</dependency>

```

Or include the [JAR](https://mvnrepository.com/artifact/io.github.codesakshi/simple-jpa-repository/latest) in your project

## Entity and Repository

Entity represents table rows in the database where every field in the Entity is equivalent to a table column. Repository as services which operate on the Entity classes. Repository will map the data in the Entity class to rows in the Database Table. Repository makes it easy to save and retrieve data from data from database even if the entity class contains nested associated entity.

#### Example Entity
Subject.java
```java
@Entity
@Table(name="SUBJECT")
public class Subject {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name="name")
	private String name;
	
	// Getter and Setter Methods
	
```
Repository can be instantiated in two ways.
#### 1. Create Repository object by passing template arguments.

Example:

```java

	Repository<Subject,Integer> repository = new Repository<Subject,Integer>(Subject.class,Integer.class);
```
First argument is Entity Class and Second Argument is Type of Id field in the entity class.

#### 2.Extending Repository Class

Example:

```java

	public class SubjectRepository extends Repository<Subject,Integer> {
		
	}
```
Here also, first template argument is Entity Class and second argument is Type of Id field in the entity class.

Following operations are supported in Repository
  * findById
  * findAll
  * save
  * saveAll
  * deleteById
  * delete

You can always extend the Repository Class and implement your own methods.

## Example

Let's say we have a database to store details that have following structure:

`subject`\
&nbsp;&nbsp;&nbsp;&nbsp;`id`\
&nbsp;&nbsp;&nbsp;&nbsp;`name`


Subject.java
```java
@Entity
@Table(name="SUBJECT")
public class Subject {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name="name")
	private String name;
	
	// Getter and Setter Methods
	
```

### Insert Single Entity
In this scenario, the ID will be created by the Database( AUTO_INCREMENT ).

```java
	void insertSubject( Connection conn ) throws Exception {

		Repository<Subject,Integer> repo = new Repository<Subject,Integer>(Subject.class,Integer.class);

		Subject inItem = new Subject();

		inItem.setName( "Physics");

		Subject value = repo.save(conn, inItem);
	}
```
Returned value will contain the Id generated by the Database.

```js
	{
		"name": "Physics",
		"id": 1
	}
```

### Insert Multiple Entity

```java
	void insertMultipleSubject( Connection conn ) throws Exception {

		Repository<Subject,Integer> repo = new Repository<Subject,Integer>(Subject.class,Integer.class);

		List<Subject> subjects = new ArrayList<Subject>();
		
		Subject inItem1 = new Subject();

		inItem1.setName( "Chemistry");

		subjects.add(inItem1);
		
		Subject inItem2 = new Subject();

		inItem2.setName( "Biology");

		subjects.add(inItem2);
		
		List<Subject> values = repo.saveAll(conn, subjects);
	}
```	

and the response will be
```js
	[
		{
			"name": "Chemistry",
			"id": 2
		}, {
			"name": "Biology",
			"id": 3
		}
	]
```	

### Implement Custom SQL operations
Implement a Repository Class and implement custom methods
```java
public class SubjectRepository extends Repository<Subject,Integer> {
	
	public Subject findByName( Connection conn, String name ) throws Exception {
		
		String whereClause = " Subject.name = :name";
		
		Map<String,Object> varMap = new HashMap<String,Object>();
		varMap.put( "name", name);
		
		return findSingleWithWhere(conn, whereClause, varMap);
		
	}
}
```	

### Lets do some complex save operation
Suppose We have following entities.

`student`\
&nbsp;&nbsp;&nbsp;&nbsp;`id`\
&nbsp;&nbsp;&nbsp;&nbsp;`name`

Student.java

```java
@Entity
@Table(name = "STUDENT")
public class Student {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name = "NAME")
	private String name;

	// Getter and Setter Methods
	
```

`teacher`\
&nbsp;&nbsp;&nbsp;&nbsp;`id`\
&nbsp;&nbsp;&nbsp;&nbsp;`name`\
&nbsp;&nbsp;&nbsp;&nbsp;`subject`  ( foreign key from subject table )\
&nbsp;&nbsp;&nbsp;&nbsp;`schoolId` ( foreign key from school table )

Teacher.java

```java
@Entity
@Table(name = "TEACHER")
public class Teacher {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name = "NAME")
	private String name;

	@ManyToOne()
	@JoinColumn( name = "SUBJECT_ID")	
	private Subject subject;
	
	@Column(name = "SCHOOL_ID")
	private Integer schoolId; 

	@ManyToMany()
	@JoinTable(
		  name = "TEACHER_STUDENT_GROUP", 
		  joinColumns = @JoinColumn(name = "TEACHER_ID"), 
		  inverseJoinColumns = @JoinColumn(name = "STUDENT_ID")
		  )
	private Set<Student> studentSet;

	// Getter and Setter Methods
		
```	
Where TEACHER_STUDENT_GROUP is bridge table to store ManyToMany Association detail

```sql
create table TEACHER_STUDENT_GROUP
(
  TEACHER_ID int,
  STUDENT_ID int,
  FOREIGN KEY (TEACHER_ID) REFERENCES TEACHER(ID),
  FOREIGN KEY (STUDENT_ID) REFERENCES STUDENT(ID)
);
```

`school`\
&nbsp;&nbsp;&nbsp;&nbsp;`id`\
&nbsp;&nbsp;&nbsp;&nbsp;`name`

School.java

```java
@Entity
@Table(name="SCHOOL")
public class School {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name = "NAME")
	private String name;
	
	@OneToMany( cascade= {CascadeType.ALL} )
	@JoinColumn(name="SCHOOL_ID")
	private List<Teacher> teachers;

	// Getter and Setter Methods
	
```

Saving bunch of entities together

```java
	public	void saveBunchOfData( Connection conn ) throws Exception {
		
		Repository<School,Integer> schoolRepo = new Repository<School,Integer>(School.class,Integer.class);
		
		School school1 = new School();
		school1.setName( "Albert School");	
		
		Teacher teacher1 = new Teacher();		
		teacher1.setName( "Alice" );
		Subject subject1 = new Subject();
		subject1.setId( 1); 
		teacher1.setSubject(subject1);
		
		Teacher teacher2 = new Teacher();		
		teacher2.setName( "Bob" );
		Subject subject2 = new Subject();
		subject2.setId( 2);
		teacher1.setSubject(subject2);
				
		List<Teacher> teachers = new ArrayList<Teacher>();
		
		teachers.add(teacher1);
		teachers.add(teacher2);
		school1.setTeachers(teachers);
				
		Student student1 = new Student();
		student1.setName( "Charlie");
		
		Student student2 = new Student();
		student2.setName( "David");
		
		
		Set<Student> studentSet = new HashSet<Student>();
		studentSet.add(student1);
		studentSet.add(student2);
		teacher1.setStudentSet(studentSet);
		
		School value = schoolRepo.save(conn, school1);
		
		String json = objectMapper.writeValueAsString(value);

		System.out.println( json );
	}
```

And the response will be

```js
	{
		"name": "Albert School",
		"teachers": [{
			"name": "Alice",
			"subject": {
				"name": "Chemistry",
				"id": 2
			},
			"studentSet": [{
				"name": "Charlie",
				"id": 2
			}, {
				"name": "David",
				"id": 1
			}],
			"id": 2
		}, {
			"name": "Bob",
			"subject": null,
			"studentSet": [],
			"id": 3
		}],
		"id": 2
	}
```	

### UpdateTimeStamp annotation

```sql

create table Deck
(
  ID int NOT NULL AUTO_INCREMENT,
  name varchar(255),
  lastModified DATE,
  PRIMARY KEY (ID)
);

```
Deck.java
```java

@Entity
@Table(name = "Deck")
public class Deck {

	@Id
	@Column(name = "ID")
	private Integer Id;
	
	@Column(name = "NAME")
	private String name;

	@UpdateTimeStamp()
	private Date lastModified;
	
	// Getter and Setter Methods
		
```
When saving the Deck Entity, lastModified field will automatically updated to current TIMESTAMP.

```java

	public void saveDeck( Connection conn ) throws Exception {

		Repository<Deck,Integer> repo = new Repository<Deck,Integer>(Deck.class,Integer.class);

		Deck inItem = new Deck();

		inItem.setName( "First Deck");

		Deck value = repo.save(conn, inItem);
	}

```
The response will be 

```js
{
	"name": "First Deck",
	"lastModified": "2023-04-26",
	"id": 2
}
```

## Tips and Tricks
### Pagination in Oracle ( Supported starting from Oracle version 8a )

```java
	public List<Subject> findWithRange( Connection conn, int start, int count) throws Exception {
		
		String whereClause = " 1=1 OFFSET :start ROWS FETCH NEXT :count ROWS ONLY";
		
		Map<String,Object> varMap = new HashMap<String,Object>();
		varMap.put( "start", start);
		varMap.put( "count", count );
		
		return findMultipleWithWhere(conn, whereClause, varMap);
	}
	
```
