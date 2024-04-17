# Диалект {{ ydb-short-name }} для Hibernate 

## Введение {#overview}

Это руководство использования [Hibernate](https://hibernate.org/orm/) с {{ ydb-short-name }}. 

Hibernate - это фреймворк объектно-реляционного отображения (ORM) для Java, облегчающий процесс маппинга объектно-ориентированных моделей. 

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Чтобы воспользоваться диалектом YDB в ваш проект нужно добавить зависимость самого диалекта и [JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver):

{% list tabs %}

- Maven

    ```xml
    <dependency>
        <groupId>tech.ydb.dialects</groupId>
        <artifactId>hibernate-ydb-dialect</artifactId>
        <!-- Set an actual version -->
        <version>${hibernate.ydb.dialect.version}</version> 
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        implementation 'tech.ydb.dialects:hibernate-ydb-dialect:$version' // Set an actual version
    }
    ```

{% endlist %}

В случае использования Hibernate 5 версии нужен artifactId равный `hibernate-ydb-dialect-v5` для Maven или `implementation 'tech.ydb.dialects:hibernate-ydb-dialect:$version'` для Gradle.

## Конфигурация диалекта {#configuration-dialect}

После добавления зависимости в проект нужно явно указать ваш диалект. Сконфигурировать Hibernate c помощью hibernate.cfg.xml:

```xml
<property name="hibernate.dialect">tech.ydb.hibernate.dialect.YdbDialect</property>
```

Или в Java коде:

```java
public static Configuration basedConfiguration() {
    return new Configuration()
            .setProperty(AvailableSettings.DIALECT, YdbDialect.class.getName());
}
```

## Использование {#using}

Используйте этот диалект так же, как и любой другой диалект Hibernate. Сопоставьте классы сущностей с таблицами базы данных и используйте фабрику сессий Hibernate для выполнения операций с базой данных.

### Пример Spring Data JPA {#integration-with-spring-data-jpa-example}

Настройте Spring Data JPA для использования диалекта YDB, обновив свой application.properties:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Создадим простую сущность и репозиторий:

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

Пример использования:

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

Пример простого приложения Spring Data JPA можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
