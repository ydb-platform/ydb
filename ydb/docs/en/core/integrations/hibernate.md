# {{ ydb-short-name }} dialect for Hibernate #

## Overview {#overview}

This is a guide to using Hibernate with YDB.

Hibernate is an Object-Relational Mapping (ORM) framework for Java that facilitates the mapping of object-oriented models. Each database management system (DBMS) partially implements the SQL standard, interpreting certain constructs in its own way.

## Installation {#install-dialect}

Add the following dependency to your project:

{% list tabs %}

- Maven

    ```xml
    <dependency>
        <groupId>tech.ydb.dialects</groupId>
        <artifactId>hibernate-ydb-dialect</artifactId>
        <!-- Set actual version -->
        <version>${hibernate.ydb.dialect.version}</version> 
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        implementation 'tech.ydb.dialects:hibernate-ydb-dialect:$version' // Set actual version
    }
    ```

{% endlist %}

If you use Hibernate version 5, you need `<artifactId>hibernate-ydb-dialect-v5</artifactId>`.

## Configuration {#configuration-dialect}

Configure Hibernate to use the custom YDB dialect by updating your hibernate.cfg.xml:

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

### Example Spring Data JPA

Configure Spring Data JPA with Hibernate to use custom YDB dialect by updating your application.properties:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
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
    "Kirill Kurdyukov",
    "kurdyukov-kir@ydb.tech",
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
