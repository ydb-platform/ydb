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

## Использование {#using}

Используйте этот диалект так же, как и любой другой диалект Hibernate. Сопоставьте классы сущностей с таблицами базы данных и используйте фабрику сессий Hibernate для выполнения операций с базой данных.

Таблица сопоставления Java типов с [{{ ydb-short-name }} типами](../../yql/reference/types/primitive.md):

| Java type                                                                   | {{ ydb-short-name }} type   |
|-----------------------------------------------------------------------------|-----------------------------|
| `bool`, `Boolean`                                                           | `Bool`                      |
| `String`, enum с аннотацией `@Enumerated(EnumType.STRING)`                  | `Text` (синоним `Utf8`)     |
| `java.time.LocalDate`                                                       | `Date`                      |
| `java.math.BigDecimal`                                                       | `Decimal(22,9)`             |
| `java.math.BigInteger`                                                       | `Decimal(22,0)` (Hibernate 6 и 7), `Decimal(22,9)` (Hibernate 5) |
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

Будет сгенерирована следующая таблица `Groups` и вторичный индекс `group_name_index` к колонке `GroupName`:

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

Если эволюционировать сущность Group путем добавления поля `deparment`:

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

Hibernate при старте приложения обновит схему базы данных, если установлен режим `update`:

```properties
jakarta.persistence.schema-generation.database.action=update
```

Результат изменения схемы:

```yql
ALTER TABLE Groups
   ADD COLUMN department Text
```

{% note warning %}

Hibernate не предназначен для управления схемами баз данных. Вы можете использовать [Liquibase](../migration/liquibase.md) или [Flyway](../migration/flyway.md) для управления схемой базы данных.

{% endnote %}

Диалект {{ ydb-short-name }} поддерживает `@OneToMany`, `@ManyToOne`, `@ManyToMany` связи.

Например, сгенерированный SQL скрипт для `@OneToMany`:

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

## Нативные типы {{ ydb-short-name }} {#native-types}

Таблица выше описывает сопоставления по умолчанию. Для явного выбора нативных типов дат и интервалов используйте аннотацию `@JdbcTypeCode` из `org.hibernate.annotations` и константы `YdbJdbcCode` из `tech.ydb.hibernate.dialect.code`. Точность и масштаб Decimal задаются отдельно с помощью `@Column`.

Следующие примеры предназначены для Hibernate 6 и 7. Расширенные даты и интервалы поддерживаются в `hibernate-ydb-dialect` начиная с версии 1.5.1 для Hibernate 6 и 1.7.0 для Hibernate 7; необходим [JDBC-драйвер](../../reference/languages-and-apis/jdbc-driver/maven.md) версии 2.3.11 или новее. Произвольный Decimal поддерживается начиная с версии 1.8.0, см. [ниже](#decimal). В отдельном диалекте для Hibernate 5 эти коды не зарегистрированы, а аннотация `@JdbcTypeCode` отсутствует в API Hibernate 5.

### Расширенные типы дат и интервалов {#extended-datetime}

Чтобы хранить даты вне диапазона обычных `Date`, `Datetime` и `Timestamp` или расширенные интервалы, выберите соответствующий JDBC-код:

| Java-тип | Аннотация | Тип {{ ydb-short-name }} |
|---|---|---|
| `java.time.LocalDate` | `@JdbcTypeCode(YdbJdbcCode.DATE_32)` | `Date32` |
| `java.time.LocalDateTime` | `@JdbcTypeCode(YdbJdbcCode.DATETIME_64)` | `Datetime64` |
| `java.time.Instant` | `@JdbcTypeCode(YdbJdbcCode.TIMESTAMP_64)` | `Timestamp64` |
| `java.time.Duration` | `@JdbcTypeCode(YdbJdbcCode.INTERVAL_64)` | `Interval64` |

Например, добавьте следующие поля в класс сущности:

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

Аннотация задаёт тип как при генерации схемы, так и при передаче значений через JDBC. Например, поле `eventDate` сможет хранить `LocalDate.of(1960, 1, 1)`, которое не входит в диапазон `Date`.

Одна аннотация `@Column(columnDefinition = "Date32")` меняет определение столбца при генерации схемы, но не выбирает нужный способ передачи значений. Используйте `@JdbcTypeCode` и для существующих таблиц, созданных с помощью Liquibase или Flyway; при этом тип поля должен совпадать с типом столбца.

Расширенный диапазон не увеличивает точность: `Datetime64` хранит секунды, а `Timestamp64` и `Interval64` — микросекунды. Значения `LocalDateTime` интерпретируются в UTC. Диапазоны значений описаны в разделе [Дата и время](../../yql/reference/types/primitive.md#datetime).

#### Расширенные типы по умолчанию в JDBC-драйвере {#force-signed-datetimes}

{% note tip %}

Если в проекте не нужно смешивать обычные и расширенные типы дат, можно не аннотировать каждое поле, а включить свойство `forceSignedDatetimes=true` в [JDBC-драйвере](https://github.com/ydb-platform/ydb-jdbc-driver) версии 2.3.11 или новее, например в [JDBC URL](../../reference/languages-and-apis/jdbc-driver/properties.md#jdbc-url): `jdbc:ydb:grpc://localhost:2136/local?forceSignedDatetimes=true`. По умолчанию свойство выключено. С ним драйвер передаёт параметры стандартных JDBC-типов `DATE` и `TIMESTAMP`, а также значения `java.time.LocalDate`, `java.time.LocalDateTime`, `java.time.Instant`, `java.time.Duration`, `java.sql.Date` и `java.sql.Timestamp` как `Date32`, `Datetime64`, `Timestamp64` и `Interval64`.

Свойство действует только на передачу значений через JDBC, поэтому в Hibernate учитывайте следующее:

* Генерация схемы не меняется: без `@JdbcTypeCode` диалект по-прежнему создаёт столбцы `Date`, `Datetime`, `Timestamp` и `Interval`. Создавайте таблицы с расширенными типами заранее, например через [Liquibase](../migration/liquibase.md#extended-datetime).
* Без аннотаций на расширенные типы переключаются `LocalDate` (`Date32`) и `Instant` (`Timestamp64`). `LocalDateTime` диалект передаёт явно как `Datetime`, поэтому для `Datetime64` аннотация `@JdbcTypeCode(YdbJdbcCode.DATETIME_64)` нужна в любом случае.
* Явные коды `@JdbcTypeCode(YdbJdbcCode.*)` свойство не затрагивает.

{% endnote %}

### Decimal с заданной точностью и масштабом {#decimal}

Для `java.math.BigDecimal` по умолчанию используется `Decimal(22,9)`, а для `java.math.BigInteger` — `Decimal(22,0)`. Чтобы задать другие параметры, используйте аннотацию `@Column` из `jakarta.persistence`:

```java
@Column(precision = 35, scale = 10)
private BigDecimal amount;
```

Здесь `BigDecimal` — класс `java.math.BigDecimal`. Аннотация задаёт `Decimal(35,10)` как для генерации схемы, так и для передачи параметров через JDBC. Допустимы значения `1 ≤ precision ≤ 35` и `0 ≤ scale ≤ precision`; подробнее см. [Примитивные типы данных](../../yql/reference/types/primitive.md#numeric).

Для автоматического выбора типа по `precision` и `scale` используйте `@Column` без `@JdbcTypeCode` на том же поле. Явный JDBC-код, в том числе `SqlTypes.DECIMAL` или `SqlTypes.NUMERIC`, обходит этот механизм.

{% note info %}

Настройка JDBC-привязки через `@Column(precision = ..., scale = ...)` поддерживается в `hibernate-ydb-dialect` начиная с версии [1.8.0](https://github.com/ydb-platform/ydb-java-dialects/blob/main/hibernate-dialect/CHANGELOG.md). В более ранних выпусках эта аннотация меняет только генерируемое определение столбца, а стандартная передача `BigDecimal` через JDBC остаётся привязанной к `Decimal(22,9)`.

{% endnote %}

Одна аннотация `@Column(columnDefinition = "Decimal(35,10)")` не задаёт точность и масштаб JDBC-параметров. Если определение столбца указано явно, добавьте к той же аннотации `precision = 35` и `scale = 10`.

Для существующей таблицы, созданной с помощью [Liquibase](../migration/liquibase.md#decimal) или Flyway, укажите в `@Column` точность и масштаб физического столбца, даже если Hibernate не управляет схемой. Например, столбцу `type="Decimal(35,10)"` в Liquibase соответствует поле `amount` из примера выше.

Начиная с версии 1.8.0 прежние константы Decimal в `YdbJdbcCode` удалены. При обновлении замените `@JdbcTypeCode(YdbJdbcCode.DECIMAL_p_s)` на `@Column(precision = p, scale = s)`, сохранив точность и масштаб столбца. Например, для `Decimal(31,9)` измените поле сущности так:

```diff
-@JdbcTypeCode(YdbJdbcCode.DECIMAL_31_9)
+@Column(precision = 31, scale = 9)
 private BigDecimal amount;
```

## Пример с Spring Data JPA {#integration-with-spring-data-jpa-example}

Настройте Spring Data JPA для использования диалекта {{ ydb-short-name }}, обновив свой `application.properties`:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

Создадим простую сущность и репозиторий:

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

Пример использования:

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

Пример простого приложения Spring Data JPA можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
