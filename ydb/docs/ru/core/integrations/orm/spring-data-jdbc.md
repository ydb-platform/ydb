# Диалект {{ ydb-short-name }} для Spring Data JDBC

Это руководство предназначено для использования [Spring Data JDBC](https://spring.io/projects/spring-data-jdbc) с {{ ydb-short-name }}.

Spring Data JDBC является частью экосистемы [Spring Data](https://spring.io/projects/spring-data), которая предоставляет упрощенный подход к взаимодействию с реляционными базами данных посредством использования SQL и простых Java объектов. В отличие от [Spring Data JPA](https://spring.io/projects/spring-data-jpa), который построен на базе JPA (Java Persistence API), Spring Data JDBC предлагает более прямолинейный способ работы с базами данных, исключающий сложности, связанные с ORM (Object-Relational Mapping).

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Для интеграции {{ ydb-short-name }} с вашим проектом Spring Data JDBC потребуется добавить две зависимости: {{ ydb-short-name }} JDBC Driver и расширение Spring Data JDBC для {{ ydb-short-name }}.

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
        <artifactId>spring-data-jdbc-ydb</artifactId>
        <version>${spring.data.jdbc.ydb}</version>
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        // Set actual versions
        implementation "tech.ydb.dialects:spring-data-jdbc-ydb:$ydbDialectVersion"
        implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
    }
    ```

{% endlist %}

## Использование {#using}

После импорта всех необходимых зависимостей диалект готов к использованию. Рассмотрим простой пример Spring Data JDBC приложения.

```properties
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

```java
@Table(name = "Users")
public class User implements Persistable<Long> {
    @Id
    private Long id = ThreadLocalRandom.current().nextLong();

    private String login;
    private String firstname;
    private String lastname;

    @Transient
    private boolean isNew;

    // Конструкторы, геттеры и сеттеры

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }
}
```

Для сущности `User` таблицы `Users` создадим репозиторий:

```java
public interface SimpleUserRepository extends CrudRepository<User, Long> {
}
```

Давайте сохраним нового пользователя и проверим, что он был успешно сохранен:

```java
@Component
public class UserRepositoryCommandLineRunner implements CommandLineRunner {

    @Autowired
    private SimpleUserRepository repository;

    @Override
    public void run(String... args) {
        User user = new User();
        user.setLogin("johndoe");
        user.setFirstname("John");
        user.setLastname("Doe");
        user.setNew(true);  // Устанавливаем флаг новой сущности

        // Сохранение пользователя
        User savedUser = repository.save(user);

        // Проверка сохранения пользователя
        assertThat(repository.findById(savedUser.getId())).contains(savedUser);

        System.out.println("User saved with ID: " + savedUser.getId());
    }
}
```

### View Index {#viewIndex}

Для генерации конструкций `VIEW INDEX` из методов репозитория необходимо использовать аннотацию `@ViewIndex`.
Аннотация `@ViewIndex` имеет два поля:

- `indexName` - название индекса.
- `tableName` - название таблицы, к которой привязан `VIEW INDEX`, особенно полезно при использовании аннотаций `@MappedCollection`.

Рассмотрим простой пример с индексом таблицы Users по полю `login`:

```java
public interface SimpleUserRepository extends CrudRepository<User, Long> {

    @ViewIndex(indexName = "login_index")
    User findByLogin(String login);
}
```

Запрос, который сгенерирует этот метод, будет выглядеть следующим образом:

```yql
SELECT `Users`.`id`        AS `id`,
       `Users`.`login`     AS `login`,
       `Users`.`lastname`  AS `lastname`,
       `Users`.`firstname` AS `firstname`
FROM `Users` VIEW login_index AS `Users`
WHERE `Users`.`login` = ?
```

### YdbType {#ydbType}

Для указания конкретного типа данных в {{ ydb-short-name }} можно использовать аннотацию `@YdbType` над полем сущности.
Пример использования:

```java
    @YdbType("Json")
    private String jsonColumn;
    @YdbType("JsonDocument")
    private String jsonDocumentColumn;
    @YdbType("Uint8")
    private byte uint8Column;
    @YdbType("Uint16")
    private short uint16Column;
    @YdbType("Uint32")
    private int uint32Column;
    @YdbType("Uint64")
    private long uint64Column;
```

Использование аннотации `@YdbType` позволяет точно указать типы данных, поддерживаемые {{ ydb-short-name }}, что обеспечивает корректное взаимодействие с базой данных.

Пример простого приложения Spring Data JDBC можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jdbc).
