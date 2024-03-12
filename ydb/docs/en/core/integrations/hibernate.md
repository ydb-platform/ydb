## Hibernate dialect ##

### Overview ###

This is a guide to using Hibernate with YDB.

Hibernate is an Object-Relational Mapping (ORM) framework for Java that facilitates the mapping of object-oriented models. Each database management system (DBMS) partially implements the SQL standard, interpreting certain constructs in its own way.

In order to have a unified ORM framework across various DBMSs, an entity called a dialect is introduced at the client side, which resolves conflicting issues. For example, whether the database supports certain types, `1` or `0`, will be interpreted as a boolean type, or if you need to explicitly generate `TRUE` and `FALSE`, and so on.

### Installation

For Maven, add the following dependency to your pom.xml:

```xml
<dependency>
    <groupId>tech.ydb.dialects</groupId>
    <artifactId>hibernate-ydb-dialect</artifactId>
    <!-- Set actual version -->
    <version>${hibernate.ydb.dialect.version}</version> 
</dependency>
```

For Gradle, add the following to your build.gradle (or build.gradle.kts):

```groovy
dependencies {
    implementation 'tech.ydb.dialects:hibernate-ydb-dialect:$version' // Set actual version
}
```

If you use Hibernate version 5, you need `<artifactId>hibernate-ydb-dialect-v5</artifactId>`.

### Configuration

Configure Hibernate to use the custom YDB dialect
by updating your hibernate.cfg.xml:

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

## Usage

Use this custom dialect just like any other Hibernate dialect.
Map your entity classes to database tables and use Hibernate's
session factory to perform database operations.

## Integration with Spring Data JPA

Configure Spring Data JPA with Hibernate to use custom YDB dialect
by updating your application.properties:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
```

An example of a simple Spring Data JPA repository can be found at the following
[link](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).

## Support and Contact

For support, you can open issues in the [repository](https://github.com/ydb-platform/ydb-java-dialects/tree/main/hibernate-dialect) issue tracker with tag `hibernate-v6` or `hibernate-v5`.
