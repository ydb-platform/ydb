# Диалект {{ ydb-short-name }} для Hibernate 

## Введение {#overview}

Это руководство использования [Hibernate](https://hibernate.org/orm/) с {{ ydb-short-name }}. 

Hibernate - это фреймворк объектно-реляционного отображения (ORM) для Java, облегчающий процесс отображения объектно-ориентированных моделей.

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Примеры для различных систем сборки:

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

Если вы используете Hibernate версии 5, вам понадобится `<artifactId>hibernate-ydb-dialect-v5</artifactId>` для Maven или `implementation "tech.ydb.dialects:hibernate-ydb-dialect-v5:$version"` для Gradle вместо аналогичного пакета без `-v5` суффикса.

## Конфигурация диалекта {#configuration-dialect}

Сконфигурируйте Hibernate для использования диалекта {{ ydb-short-name }}, обновив [persistence.xml](https://docs.jboss.org/hibernate/orm/6.4/introduction/html_single/Hibernate_Introduction.html#configuration-jpa) файл:

```xml
<property name="hibernate.dialect">tech.ydb.hibernate.dialect.YdbDialect</property>
```

Или, если вы используете программную настройку:

{% list tabs %}

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

## Использование {#using}

Используйте этот диалект так же, как и любой другой диалект Hibernate. Сопоставьте классы сущностей с таблицами базы данных и используйте фабрику сессий Hibernate для выполнения операций с базой данных.

Таблица сопоставления Java типов с [{{ ydb-short-name }} типами](../yql/reference/types/primitive.md):

| Java type                                                                   | {{ ydb-short-name }} type   |
|-----------------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                           | `Bool`                      |
| `String`, enum с аннотацией `@Enumerated(EnumType.STRING)`                  | `Text` (синоним `Utf8`)     |
| `java.time.LocalDate`                                                       | `Date`                      |
| `java.math.BigDecimal`, `java.math.BigInteger`                              | `Decimal(22,9)`             |
| `double`, `Double`                                                          | `Double`                    |
| `float`, `Float`                                                            | `Float`                     |
| `int`, `java.lang.Integer`                                                  | `Int32`                     |
| `long`, `java.lang.Long`                                                    | `Int64`                     |
| `short`, `java.lang.Short`                                                  | `Int16`                     |
| `byte`, `java.lang.Byte`, enum с аннотацией `@Enumerated(EnumType.ORDINAL)` | `Int8`                      |
| `[]byte`                                                                    | `Bytes`  (синоним `String`) |
| `java.time.LocalDateTime` (timezone будет установлена в `UTC`)              | `Datetime`                  |
| `java.time.Instant` (timezone будет установлена в `UTC`)                    | `Timestamp`                 |

Диалект {{ ydb-short-name }} поддерживает генерацию схемы базы данных на основе объектов Hibernate.

Например, для класса `Group`: 

{% list tabs %}

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

Будет сгенерирована следующая таблица `Groups` и вторичный индекс `group_name_index` к колонке `GroupName`:

```sql
CREATE TABLE Groups (
    GroupId Int32 NOT NULL,
    GroupName Text,
    PRIMARY KEY (GroupId)
);

ALTER TABLE Groups
  ADD INDEX group_name_index GLOBAL 
       ON (GroupName);
```

Если эволюционировать сущность Group путем добавления поля `deparment`:

{% list tabs %}

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

Hibernate при старте приложения обновит схему базы данных, если установлен режим `update`:

```properties
jakarta.persistence.schema-generation.database.action=update
```

Результат изменения схемы:

```sql
ALTER TABLE Groups 
   ADD COLUMN department Text
```

{% note warning %}

Hibernate не предназначен для управления схемами баз данных. Вы можете использовать [Liquibase](./liquibase.md) или [Flyway](./flyway.md) для управления схемой базы данных.

{% endnote %}

Диалект {{ ydb-short-name }} поддерживает `@OneToMany`, `@ManyToOne`, `@ManyToMany` связи.

Например, сгенерированный SQL скрипт для `@OneToMany`:

{% list tabs %}

- FetchType.LAZY

  ```sql
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

  ```sql
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

### Пример с Spring Data JPA {#integration-with-spring-data-jpa-example}

Настройте Spring Data JPA для использования диалекта {{ ydb-short-name }}, обновив свой `application.properties`:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Создадим простую сущность и репозиторий:

{% list tabs %}

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

Пример использования:

{% list tabs %}

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

Пример простого приложения Spring Data JPA можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
