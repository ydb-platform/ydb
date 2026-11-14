# {{ ydb-short-name }} dialect for Hibernate

## Introduction {#overview}

This guide covers using [Hibernate](https://hibernate.org/orm/) with {{ ydb-short-name }}.

Hibernate is an Object-Relational Mapping (ORM) framework for Java that simplifies mapping object-oriented models to database tables.

## Installing the {{ ydb-short-name }} dialect {#install-dialect}

Examples for various build systems:

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

If you use Hibernate version 5, you need `<artifactId>hibernate-ydb-dialect-v5</artifactId>` for Maven or `implementation "tech.ydb.dialects:hibernate-ydb-dialect-v5:$version"` for Gradle instead of the similar package without the `-v5` suffix.

## Configuring the dialect {#configuration-dialect}

Configure Hibernate to use the {{ ydb-short-name }} dialect by updating your [persistence.xml](https://docs.jboss.org/hibernate/orm/6.4/introduction/html_single/Hibernate_Introduction.html#configuration-jpa) file:

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

Use this dialect just like any other Hibernate dialect. Map your entity classes to database tables and use Hibernate's session factory to perform database operations.

Table of Java type mappings to [{{ ydb-short-name }} types](../../yql/reference/types/primitive.md):

| Java type                                                                   | {{ ydb-short-name }} type   |
|-----------------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                           | `Bool`                      |
| `String`, enum with `@Enumerated(EnumType.STRING)` annotation               | `Text` (synonym `Utf8`)     |
| `java.time.LocalDate`                                                       | `Date`                      |
| `java.math.BigDecimal`                                                       | `Decimal(22,9)`             |
| `java.math.BigInteger`                                                       | `Decimal(22,0)` (Hibernate 6 and 7), `Decimal(22,9)` (Hibernate 5) |
| `double`, `Double`                                                          | `Double`                    |
| `float`, `Float`                                                            | `Float`                     |
| `int`, `java.lang.Integer`                                                  | `Int32`                     |
| `long`, `java.lang.Long`                                                    | `Int64`                     |
| `short`, `java.lang.Short`                                                  | `Int16`                     |
| `byte`, `java.lang.Byte`, enum with `@Enumerated(EnumType.ORDINAL)` annotation | `Int8`                      |
| `[]byte`                                                                    | `Bytes`  (synonym `String`) |
| `java.time.LocalDateTime` (timezone will be set to `UTC`)                   | `Datetime`                  |
| `java.time.Instant` (timezone will be set to `UTC`)                         | `Timestamp`                 |

The {{ ydb-short-name }} dialect supports database schema generation based on Hibernate entities.

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

The following `Groups` table and a secondary index `group_name_index` on the `GroupName` column will be generated:

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

Hibernate will update the database schema at application startup if the `update` mode is set:

```properties
jakarta.persistence.schema-generation.database.action=update
```

The result of the schema change:

```yql
ALTER TABLE Groups
   ADD COLUMN department Text
```

{% note warning %}

Hibernate is not designed to manage database schemas. You can use [Liquibase](../migration/liquibase.md) or [Flyway](../migration/flyway.md) to manage your database schema.

{% endnote %}

The {{ ydb-short-name }} dialect supports `@OneToMany`, `@ManyToOne`, and `@ManyToMany` relationships.

For example, the generated SQL script for `@OneToMany`:

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

The table above describes the default mappings. To explicitly select native date and interval types, use the `@JdbcTypeCode` annotation from `org.hibernate.annotations` and the `YdbJdbcCode` constants from `tech.ydb.hibernate.dialect.code`. Decimal precision and scale are set separately using `@Column`.

The following examples are for Hibernate 6 and 7. Extended dates and intervals are supported in `hibernate-ydb-dialect` starting from version 1.5.1 for Hibernate 6 and 1.7.0 for Hibernate 7; the [JDBC driver](../../reference/languages-and-apis/jdbc-driver/maven.md) version 2.3.11 or newer is required. Arbitrary Decimal is supported starting from version 1.8.0, see [below](#decimal). In the separate dialect for Hibernate 5, these codes are not registered, and the `@JdbcTypeCode` annotation is not available in the Hibernate 5 API.

### Extended date and interval types {#extended-datetime}

To store dates outside the range of regular `Date`, `Datetime`, and `Timestamp` or extended intervals, select the appropriate JDBC code:

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

The annotation sets the type both for schema generation and for passing values via JDBC. For example, the `eventDate` field will be able to store `LocalDate.of(1960, 1, 1)`, which is outside the `Date` range.

A single `@Column(columnDefinition = "Date32")` annotation changes the column definition during schema generation but does not select the appropriate way to pass values. Use `@JdbcTypeCode` also for existing tables created with Liquibase or Flyway; the field type must match the column type.

The extended range does not increase precision: `Datetime64` stores seconds, while `Timestamp64` and `Interval64` store microseconds. `LocalDateTime` values are interpreted in UTC. Value ranges are described in the [Date and time](../../yql/reference/types/primitive.md#datetime) section.

#### Extended types by default in the JDBC driver {#force-signed-datetimes}

{% note tip %}

If your project does not need to mix regular and extended date types, you can avoid annotating every field and instead enable the `forceSignedDatetimes=true` property in the [JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver) version 2.3.11 or newer, for example in the [JDBC URL](../../reference/languages-and-apis/jdbc-driver/properties.md#jdbc-url): `jdbc:ydb:grpc://localhost:2136/local?forceSignedDatetimes=true`. The property is disabled by default. With it, the driver passes parameters of standard JDBC types `DATE` and `TIMESTAMP`, as well as `java.time.LocalDate`, `java.time.LocalDateTime`, `java.time.Instant`, `java.time.Duration`, `java.sql.Date`, and `java.sql.Timestamp` values as `Date32`, `Datetime64`, `Timestamp64`, and `Interval64`.

The property only affects passing values via JDBC, so consider the following in Hibernate:

* Schema generation does not change: without `@JdbcTypeCode`, the dialect still creates `Date`, `Datetime`, `Timestamp`, and `Interval` columns. Create tables with extended types in advance, for example via [Liquibase](../migration/liquibase.md#extended-datetime).
* Without annotations, `LocalDate` (`Date32`) and `Instant` (`Timestamp64`) switch to extended types. The dialect passes `LocalDateTime` explicitly as `Datetime`, so the `@JdbcTypeCode(YdbJdbcCode.DATETIME_64)` annotation is required for `Datetime64` in any case.
* Explicit `@JdbcTypeCode(YdbJdbcCode.*)` codes are not affected by the property.

{% endnote %}

### Decimal with specified precision and scale {#decimal}

For `java.math.BigDecimal`, the default is `Decimal(22,9)`, and for `java.math.BigInteger` — `Decimal(22,0)`. To set other parameters, use the `@Column` annotation from `jakarta.persistence`:

```java
@Column(precision = 35, scale = 10)
private BigDecimal amount;
```

Here `BigDecimal` is the `java.math.BigDecimal` class. The annotation sets `Decimal(35,10)` both for schema generation and for passing parameters via JDBC. Valid values are `1 ≤ precision ≤ 35` and `0 ≤ scale ≤ precision`; see [Primitive data types](../../yql/reference/types/primitive.md#numeric) for details.

To automatically select the type based on `precision` and `scale`, use `@Column` without `@JdbcTypeCode` on the same field. An explicit JDBC code, including `SqlTypes.DECIMAL` or `SqlTypes.NUMERIC`, bypasses this mechanism.

{% note info %}

JDBC binding configuration via `@Column(precision = ..., scale = ...)` is supported in `hibernate-ydb-dialect` starting from version [1.8.0](https://github.com/ydb-platform/ydb-java-dialects/blob/main/hibernate-dialect/CHANGELOG.md). In earlier releases, this annotation only changes the generated column definition, while the standard `BigDecimal` passing via JDBC remains bound to `Decimal(22,9)`.

{% endnote %}

A single `@Column(columnDefinition = "Decimal(35,10)")` annotation does not set the precision and scale of JDBC parameters. If the column definition is specified explicitly, add `precision = 35` and `scale = 10` to the same annotation.

For an existing table created with [Liquibase](../migration/liquibase.md#decimal) or Flyway, specify the precision and scale of the physical column in `@Column`, even if Hibernate does not manage the schema. For example, a column with `type="Decimal(35,10)"` in Liquibase corresponds to the `amount` field from the example above.

Starting from version 1.8.0, the previous Decimal constants in `YdbJdbcCode` have been removed. When upgrading, replace `@JdbcTypeCode(YdbJdbcCode.DECIMAL_p_s)` with `@Column(precision = p, scale = s)`, preserving the column precision and scale. For example, for `Decimal(31,9)`, change the entity field as follows:

```diff
-@JdbcTypeCode(YdbJdbcCode.DECIMAL_31_9)
+@Column(precision = 31, scale = 9)
 private BigDecimal amount;
```

## Example with Spring Data JPA {#integration-with-spring-data-jpa-example}

Configure Spring Data JPA to use the {{ ydb-short-name }} dialect by updating your `application.properties`:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Let's create a simple entity and repository:

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

An example of a simple Spring Data JPA application can be found at the following [link](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
