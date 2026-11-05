# {{ ydb-short-name }} dialect for Hibernate

## Overview {#overview}

This is a guide to using [Hibernate](https://hibernate.org/) with {{ ydb-short-name }}.

Hibernate is an Object-Relational Mapping (ORM) framework for Java that facilitates the mapping of object-oriented models to SQL.

## Installation {#install-dialect}

Add the following dependency to your project:

{% list tabs %}

- Maven

    ```xml
    <!-- Set actual versions -->
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
        // Set actual versions
        implementation "tech.ydb.dialects:hibernate-ydb-dialect:$ydbDialectVersion"
        implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
    }
    ```

{% endlist %}

If you use Hibernate version 5, you need `<artifactId>hibernate-ydb-dialect-v5</artifactId>` for Maven or `implementation 'tech.ydb.dialects:hibernate-ydb-dialect-v5:$version` for Gradle instead of the similar package without the `-v5` suffix.

## Configuration {#configuration-dialect}

Configure Hibernate to use the custom {{ ydb-short-name }} dialect by updating your [persistence.xml](https://docs.jboss.org/hibernate/orm/6.4/introduction/html_single/Hibernate_Introduction.html#configuration-jpa) file:

```xml
<property name="hibernate.dialect">tech.ydb.hibernate.dialect.YdbDialect</property>
```

Or, if you are using programmatic configuration:

{% list tabs group=lang %}

- Java

  ```java
  import org.hibernate.cfg.AvailableSettings;
  import org.hibernate.cfg.Configuration;

  public static Configuration basedConfiguration() {
      return new Configuration()
              .setProperty(AvailableSettings.JAKARTA_JDBC_DRIVER, YdbDriver.class.getName())
              .setProperty(AvailableSettings.DIALECT, YdbDialect.class.getName());
  }
  ```

- Kotlin

  ```kotlin
  import org.hibernate.cfg.AvailableSettings
  import org.hibernate.cfg.Configuration

  fun basedConfiguration(): Configuration = Configuration().apply {
      setProperty(AvailableSettings.JAKARTA_JDBC_DRIVER, YdbDriver::class.name)
      setProperty(AvailableSettings.DIALECT, YdbDialect::class.name)
  }
  ```

{% endlist %}

## Usage {#using}

Use this custom dialect just like any other Hibernate dialect. Map your entity classes to database tables and use Hibernate's session factory to perform database operations.

Table of comparison of Java types descriptions with [{{ ydb-short-name }} types](../../yql/reference/types/primitive.md):

| Java type                                                                      | {{ ydb-short-name }} type   |
|--------------------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                              | `Bool`                      |
| `String`, enum with annotation `@Enumerated(EnumType.STRING)`                  | `Text` (synonym `Utf8`)     |
| `java.time.LocalDate`                                                          | `Date`                      |
| `java.math.BigDecimal`                                                       | `Decimal(22,9)`             |
| `java.math.BigInteger`                                                       | `Decimal(22,0)` (Hibernate 6 and 7), `Decimal(22,9)` (Hibernate 5) |
| `double`, `Double`                                                             | `Double`                    |
| `float`, `Float`                                                               | `Float`                     |
| `int`, `java.lang.Integer`                                                     | `Int32`                     |
| `long`, `java.lang.Long`                                                       | `Int64`                     |
| `short`, `java.lang.Short`                                                     | `Int16`                     |
| `byte`, `java.lang.Byte`, enum with annotation `@Enumerated(EnumType.ORDINAL)` | `Int8`                      |
| `[]byte`                                                                       | `Bytes`  (synonym `String`) |
| `java.time.LocalDateTime` (timezone will be set to `UTC`)                      | `Datetime`                  |
| `java.time.Instant` (timezone will be set to `UTC`)                            | `Timestamp`                 |

{{ ydb-short-name }} dialect supports database schema generation based on Hibernate entities.

For example, for the `Group` class:

{% list tabs group=lang %}

- Java

  ```java
  @Getter
  @Setter
  @Entity
  @Table(name = "Groups", indexes = @Index(name = "group_name_index", columnList = "GroupName"))
  public class Group {

      @Id
      @Column(name = "GroupId")
      private int id;

      @Column(name = "GroupName")
      private String name;

      @OneToMany(mappedBy = "group")
      private List<Student> students;
  }
  ```

- Kotlin

  ```kotlin
  @Entity
  @Table(name = "Groups", indexes = [Index(name = "group_name_index", columnList = "GroupName")])
  data class Group(
      @Id
      @Column(name = "GroupId")
      val id: Int,

      @Column(name = "GroupName")
      val name: String,

      @OneToMany(mappedBy = "group")
      val students: List<Student>
  )
  ```

{% endlist %}

The following `Groups` table will be created, and the `GroupName` will be indexed by a global secondary index named `group_name_index`:

```yql
CREATE TABLE Groups (
    GroupId Int32 NOT NULL,
    GroupName Text,
    PRIMARY KEY (GroupId)
);

ALTER TABLE Groups
  ADD INDEX group_name_index GLOBAL
    ON (GroupName);
```

If you evolve the Group entity by adding the `department` field:

{% list tabs group=lang %}

- Java

  ```java
  @Column
  private String department;
  ```

- Kotlin

  ```kotlin
  @Column
  val department: String
  ```

{% endlist %}

At the start of the application, Hibernate will update the database schema if the `update` mode is set in properties:

```properties
jakarta.persistence.schema-generation.database.action=update
```

The result of changing the schema is:

```yql
ALTER TABLE Groups
   ADD COLUMN department Text
```

{% note warning %}

Hibernate is not designed to manage database schemas. You can manage your database schema using [Liquibase](../migration/liquibase.md) or [Flyway](../migration/flyway.md).

{% endnote %}

{{ ydb-short-name }} dialect supports `@OneToMany`, `@ManyToOne` and `@ManyToMany` relationships.

For example, for `@OneToMany` generates a SQL script:

{% list tabs %}

- FetchType.LAZY

  ```yql
  SELECT
      g1_0.GroupId,
      g1_0.GroupName
  FROM
      Groups g1_0
  WHERE
      g1_0.GroupName='M3439'

  SELECT
      s1_0.GroupId,
      s1_0.StudentId,
      s1_0.StudentName
  FROM
      Students s1_0
  WHERE
      s1_0.GroupId=?
  ```

- FetchType.EAGER

  ```yql
  SELECT
      g1_0.GroupId,
      g1_0.GroupName,
      s1_0.GroupId,
      s1_0.StudentId,
      s1_0.StudentName
  FROM
      Groups g1_0
  JOIN
      Students s1_0
          on g1_0.GroupId=s1_0.GroupId
  WHERE
      g1_0.GroupName='M3439'
  ```

{% endlist %}

## Native {{ ydb-short-name }} types {#native-types}

The table above describes the default mappings. To select native date and interval types explicitly, use the `@JdbcTypeCode` annotation from `org.hibernate.annotations` with constants from `tech.ydb.hibernate.dialect.code.YdbJdbcCode`. Decimal precision and scale are configured separately with `@Column`.

The following examples target Hibernate 6 and 7. Extended date and interval support requires `hibernate-ydb-dialect` version 1.5.1 or later for Hibernate 6, or version 1.7.0 or later for Hibernate 7, with [JDBC driver](../../reference/languages-and-apis/jdbc-driver/maven.md) version 2.3.11 or later. Arbitrary Decimal parameters also require the support described [below](#decimal). The separate Hibernate 5 dialect does not register these codes, and the Hibernate 5 API does not provide `@JdbcTypeCode`.

### Extended date and interval types {#extended-datetime}

To store dates outside the range of `Date`, `Datetime`, and `Timestamp`, or extended intervals, select the corresponding JDBC code:

| Java type | Annotation | {{ ydb-short-name }} type |
|---|---|---|
| `java.time.LocalDate` | `@JdbcTypeCode(YdbJdbcCode.DATE_32)` | `Date32` |
| `java.time.LocalDateTime` | `@JdbcTypeCode(YdbJdbcCode.DATETIME_64)` | `Datetime64` |
| `java.time.Instant` | `@JdbcTypeCode(YdbJdbcCode.TIMESTAMP_64)` | `Timestamp64` |
| `java.time.Duration` | `@JdbcTypeCode(YdbJdbcCode.INTERVAL_64)` | `Interval64` |

For example, add the following fields to an entity class:

```java
@JdbcTypeCode(YdbJdbcCode.DATE_32)
private LocalDate eventDate;

@JdbcTypeCode(YdbJdbcCode.DATETIME_64)
private LocalDateTime eventDatetime;

@JdbcTypeCode(YdbJdbcCode.TIMESTAMP_64)
private Instant eventTimestamp;

@JdbcTypeCode(YdbJdbcCode.INTERVAL_64)
private Duration eventInterval;
```

The annotation selects the type for both schema generation and JDBC parameter binding. For example, `eventDate` can store `LocalDate.of(1960, 1, 1)`, which falls outside the `Date` range.

Using only `@Column(columnDefinition = "Date32")` changes the generated column definition without selecting the required parameter binding. Use `@JdbcTypeCode` for existing tables created with Liquibase or Flyway as well, matching the field's type to the column's type.

The extended range does not increase precision: `Datetime64` stores seconds, while `Timestamp64` and `Interval64` store microseconds. `LocalDateTime` values are interpreted in UTC. For value ranges, see [Date and time](../../yql/reference/types/primitive.md#datetime).

### Decimal precision and scale {#decimal}

`java.math.BigDecimal` maps to `Decimal(22,9)` by default, while `java.math.BigInteger` maps to `Decimal(22,0)`. To select different parameters, use the `@Column` annotation from `jakarta.persistence`:

```java
@Column(precision = 35, scale = 10)
private BigDecimal amount;
```

Here, `BigDecimal` is `java.math.BigDecimal`. The annotation selects `Decimal(35,10)` for both schema generation and JDBC parameter binding. Valid parameters are `1 ≤ precision ≤ 35` and `0 ≤ scale ≤ precision`; see [Primitive data types](../../yql/reference/types/primitive.md#numeric).

To select the type automatically from `precision` and `scale`, use `@Column` without `@JdbcTypeCode` on the same field. An explicit JDBC code, including `SqlTypes.DECIMAL` or `SqlTypes.NUMERIC`, bypasses this mechanism.

{% note info %}

Configuring JDBC binding through `@Column(precision = ..., scale = ...)` requires a Hibernate dialect build that includes [parameterized Decimal support](https://github.com/ydb-platform/ydb-java-dialects/pull/274). In earlier releases, this annotation changes only the generated column definition, while standard JDBC binding of `BigDecimal` continues to use `Decimal(22,9)`.

{% endnote %}

Using only `@Column(columnDefinition = "Decimal(35,10)")` does not set the precision and scale of JDBC parameters. If the column definition is specified explicitly, add `precision = 35` and `scale = 10` to the same annotation.

For an existing table created with [Liquibase](../migration/liquibase.md#decimal) or Flyway, set `@Column` precision and scale to match the physical column, even when Hibernate does not manage the schema. For example, a Liquibase column declared with `type="Decimal(35,10)"` corresponds to the `amount` field shown above.

The build with parameterized Decimal support removes the previous Decimal constants from `YdbJdbcCode`. When upgrading, replace `@JdbcTypeCode(YdbJdbcCode.DECIMAL_p_s)` with `@Column(precision = p, scale = s)`, preserving the column's precision and scale. For example, update an entity field mapped to `Decimal(31,9)` as follows:

```diff
-@JdbcTypeCode(YdbJdbcCode.DECIMAL_31_9)
+@Column(precision = 31, scale = 9)
 private BigDecimal amount;
```

## Example with Spring Data JPA

Configure [Spring Data JPA](https://spring.io/projects/spring-data-jpa/) with Hibernate to use custom {{ ydb-short-name }} dialect by updating your `application.properties`:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Create a simple entity and repository:

{% list tabs group=lang %}

- Java

  ```java
  @Data
  @Entity
  @Table(name = "employee")
  public class Employee {
      @Id
      private long id;

      @Column(name = "full_name")
      private String fullName;

      @Column
      private String email;

      @Column(name = "hire_date")
      private LocalDate hireDate;

      @Column
      private java.math.BigDecimal salary;

      @Column(name = "is_active")
      private boolean isActive;

      @Column
      private String department;

      @Column
      private int age;
  }

  public interface EmployeeRepository implements CrudRepository<Employee, Long> {}
  ```

- Kotlin

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

{% endlist %}

Usage example:

{% list tabs group=lang %}

- Java

  ```java
  Employee employee = new Employee(
      1,
      "Example",
      "example@bk.com",
      LocalDate.parse("2023-12-20"),
      BigDecimal("500000.000000000"),
      true,
      "YDB AppTeam",
      23
  );

  /* The following SQL will be executed:
  INSERT INTO employee (age,department,email,full_name,hire_date,is_active,limit_date_password,salary,id)
  VALUES (?,?,?,?,?,?,?,?,?)
  */
  employeeRepository.save(employee);

  assertEquals(employee, employeeRepository.findById(employee.getId()).get());

  /* The following SQL will be executed:
  DELETE FROM employee WHERE id=?
   */
  employeeRepository.delete(employee);

  assertNull(employeeRepository.findById(employee.getId()).orElse(null));
  ```

- Kotlin

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

  /* The following SQL will be executed:
  INSERT INTO employee (age,department,email,full_name,hire_date,is_active,limit_date_password,salary,id)
  VALUES (?,?,?,?,?,?,?,?,?)
  */
  employeeRepository.save(employee)

  assertEquals(employee, employeeRepository.findByIdOrNull(employee.id))

  /* The following SQL will be executed:
  DELETE FROM employee WHERE id=?
   */
  employeeRepository.delete(employee)

  assertNull(employeeRepository.findByIdOrNull(employee.id))
  ```

{% endlist %}

An example of a simple Spring Data JPA repository can be found at the following [link](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
