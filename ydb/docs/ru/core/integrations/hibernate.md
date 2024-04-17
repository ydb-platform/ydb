# Диалект {{ ydb-short-name }} для Hibernate 

## Введение {#overview}

Это руководство использования [Hibernate](https://hibernate.org/orm/) с {{ ydb-short-name }}. 

Hibernate - это фреймворк объектно-реляционного отображения (ORM) для Java, облегчающий процесс маппинга объектно-ориентированных моделей. 

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Чтобы воспользоваться диалектом {{ ydb-short-name }} требуются зависимости диалекта {{ ydb-short-name }} и [JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver):

Примеры для различных систем сборки:

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

В случае использования Hibernate 5 версии нужен artifactId равный `hibernate-ydb-dialect-v5` для Maven или `implementation "tech.ydb.dialects:hibernate-ydb-dialect-v5:$version"` для Gradle.

## Конфигурация диалекта {#configuration-dialect}

После добавления зависимости в проект нужно явно указать ваш диалект. Сконфигурировать Hibernate c помощью [persistence.xml](https://docs.jboss.org/hibernate/orm/6.4/introduction/html_single/Hibernate_Introduction.html#configuration-jpa):

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

Таблица сопоставления Java типов с [{{ ydb-short-name }} типами](../yql/reference/types/primitive.md):

| Java type                                                           | {{ ydb-short-name }} type   |
|---------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                   | `Bool`                      |
| `String`, enum with `@Enumerated(EnumType.STRING)`                  | `Text` (синоним `Utf8`)     |
| `java.time.LocalDate`                                               | `Date`                      |
| `java.math.BigDecimal`, `java.math.BigInteger`                      | `Decimal(22,9)`             |
| `double`, `Double`                                                  | `Double`                    |
| `float`, `Float`                                                    | `Float`                     |
| `int`, `java.lang.Integer`                                          | `Int32`                     |
| `long`, `java.lang.Long`                                            | `Int64`                     |
| `short`, `java.lang.Short`                                          | `Int16`                     |
| `byte`, `java.lang.Byte`, enum with `@Enumerated(EnumType.ORDINAL)` | `Int8`                      |
| `[]byte`                                                            | `Bytes`  (синоним `String`) |
| `java.time.LocalDateTime`                                           | `Datetime`                  |
| `java.time.Instant`                                                 | `Timestamp`                 |

Поддержана генерация схемы базы данных по Hibernate сущностям.

Например, для класса Group: 

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

Будет сгенерирована следующая таблица `Groups` и вторичный индекс `group_name_index` к колонке `GroupName`:

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

Если эволюционировать сущность Group путем добавления поля `deparment`:

```java
@Column
private String department;
```

Hibernate при старте приложения обновит схему базы данных, если установлен режим `update`:

```properties
jakarta.persistence.schema-generation.database.action=update
```

Результат изменения схемы:

```sql
alter table Groups 
   add column department Text
```

{% note warning %}

Hibernate не предназначен для управления схемами баз данных.

{% endnote %}

{% note info %}

Вы можете использовать [Liquibase](./liquibase.md) для управления схемой базы данных.

{% endnote %}

### Пример с Spring Data JPA {#integration-with-spring-data-jpa-example}

Настройте Spring Data JPA для использования диалекта {{ ydb-short-name }}, обновив свой `application.properties`:

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
