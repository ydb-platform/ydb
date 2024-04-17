# {{ ydb-short-name }} dialect for Hibernate #

## Overview {#overview}

This is a guide to using [Hibernate](https://hibernate.org/) with {{ ydb-short-name }}.

Hibernate is an Object-Relational Mapping (ORM) framework for Java that facilitates the mapping of object-oriented models to SQL. Each database management system (DBMS) partially implements the SQL standard, interpreting certain constructs in its own way. This article describes the specifics of how {{ ydb-short-name }} integrates with Hibernate.

## Installation {#install-dialect}

Add the following dependency to your project:

{% list tabs %}

- Maven

    ```xml
    <!-- Set an actual versions -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version>${ydb.jdbc.version}</version>
    </dependency>

    <dependency>
        <groupId>tech.ydb.dialects</groupId>
        <artifactId>hibernate-ydb-dialect</artifactId>
        <version>${hibernate.ydb.dialect.version}</version> 
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        // Set an actual versions
        implementation "tech.ydb.dialects:hibernate-ydb-dialect:$ydbDialectVersion"
        implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
    }
    ```

{% endlist %}

If you use Hibernate version 5, you need `<artifactId>hibernate-ydb-dialect-v5</artifactId>` for Maven or `implementation 'tech.ydb.dialects:hibernate-ydb-dialect-v5:$version` for Gradle.

## Configuration {#configuration-dialect}

Configure Hibernate to use the custom {{ ydb-short-name }} dialect by updating your [persistence.xml](https://docs.jboss.org/hibernate/orm/6.4/introduction/html_single/Hibernate_Introduction.html#configuration-jpa) file:

```xml
<property name="hibernate.dialect">tech.ydb.hibernate.dialect.YdbDialect</property>
```

Or, if you are using programmatic configuration:

```java
public static Configuration basedConfiguration() {
    return new Configuration()
            .setProperty(AvailableSettings.DRIVER, YdbDriver.class.getName())
            .setProperty(AvailableSettings.DIALECT, YdbDialect.class.getName());
}
```

## Usage {#using}

Use this custom dialect just like any other Hibernate dialect. Map your entity classes to database tables and use Hibernate's session factory to perform database operations.

Table of comparison of Java types descriptions with [{{ ydb-short-name }} types](../yql/reference/types/primitive.md):

| Java type                                                           | {{ ydb-short-name }} type   |
|---------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                   | `Bool`                      |
| `String`, enum with `@Enumerated(EnumType.STRING)`                  | `Text` (synonym `Utf8`)     |
| `java.time.LocalDate`                                               | `Date`                      |
| `java.math.BigDecimal`, `java.math.BigInteger`                      | `Decimal(22,9)`             |
| `double`, `Double`                                                  | `Double`                    |
| `float`, `Float`                                                    | `Float`                     |
| `int`, `java.lang.Integer`                                          | `Int32`                     |
| `long`, `java.lang.Long`                                            | `Int64`                     |
| `short`, `java.lang.Short`                                          | `Int16`                     |
| `byte`, `java.lang.Byte`, enum with `@Enumerated(EnumType.ORDINAL)` | `Int8`                      |
| `[]byte`                                                            | `Bytes`  (synonym `String`) |
| `java.time.LocalDateTime`                                           | `Datetime`                  |
| `java.time.Instant`                                                 | `Timestamp`                 |

Database schema generation based on hibernate entities is supported.

For example, for the Group class:

```java
@Getter
@Setter
@Entity
@Table(name = "Groups", indexes = @Index(name = "group_name_index", columnList = "GroupName"))
@NamedQuery(
        name = "Group.findGroups",
        query = "SELECT g FROM Group g " +
                "JOIN Plan p ON g.id = p.planId.groupId " +
                "JOIN Lecturer l ON p.planId.lecturerId = l.id " +
                "WHERE p.planId.courseId = :CourseId and l.id = :LecturerId"
)
public class Group {

    @Id
    @Column(name = "GroupId")
    private int id;
  
    @Column(name = "GroupName")
    private String name;
  
    @OneToMany(mappedBy = "group")
    private List<Student> students;
  
    @Override
    public int hashCode() {
      return id;
    }
}
```

The following `Groups` table will be created and a secondary index, `group_name_index`, will be added to the `GroupName` column:

```sql
create table Groups (
    GroupId Int32 not null,
    GroupName Text,
    primary key (GroupId)
);

alter table Groups
  add index group_name_index global 
       on (GroupName);
```

If you evolve the Group entity by adding the `deparment` field:

```java
@Column
private String department;
```

Hibernate, at the start of the application, update the database schema if the `update` mode is set:

```properties
jakarta.persistence.schema-generation.database.action=update
```

The result of changing the scheme is:

```sql
alter table Groups 
   add column department Text
```

{% note warning %}

Hibernate is not designed to manage database schemas.

{% endnote %}

{% note info %}

You can use [Liquibase](./liquibase.md) or Flyway to manage your database schema.

{% endnote %}

### Example with Spring Data JPA

Configure [Spring Data JPA](https://spring.io/projects/spring-data-jpa/) with Hibernate to use custom {{ ydb-short-name }} dialect by updating your `application.properties`:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Create simple entity and repository:

```kotlin
@Entity
@Table(name = "employee")
data class Employee(
    @Id
    val id: Long,

    @Column(name = "full_name")
    val fullName: String,

    @Column
    val email: String,

    @Column(name = "hire_date")
    val hireDate: LocalDate,

    @Column
    val salary: java.math.BigDecimal,

    @Column(name = "is_active")
    val isActive: Boolean,

    @Column
    val department: String,

    @Column
    val age: Int,
)

interface EmployeeRepository : CrudRepository<Employee, Long>

fun EmployeeRepository.findByIdOrNull(id: Long): Employee? = this.findById(id).orElse(null)
```

Example using:

```kotlin
val employee = Employee(
    1,
    "Example",
    "example@bk.com",
    LocalDate.parse("2023-12-20"),
    BigDecimal("500000.000000000"),
    true,
    "YDB AppTeam",
    23
)

employeeRepository.save(employee)

assertEquals(employee, employeeRepository.findByIdOrNull(employee.id))

employeeRepository.delete(employee)

assertNull(employeeRepository.findByIdOrNull(employee.id))
```

An example of a simple Spring Data JPA repository can be found at the following [link](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
