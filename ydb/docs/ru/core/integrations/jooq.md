# Расширение JOOQ для использования с {{ ydb-short-name }}

Это руководство предназначено для использования [JOOQ](https://www.jooq.org/) с {{ ydb-short-name }}.

JOOQ — это библиотека для Java, которая позволяет создавать типобезопасные SQL-запросы путем генерации Java-классов из схемы базы данных и использования удобных конструкторов запросов.

## Генерация Java классов

Генерировать Java классы можно любыми инструментами представленными на [официальном сайте JOOQ](https://www.jooq.org/doc/latest/manual/code-generation/codegen-configuration/), используя две зависимости {{ ydb-short-name }} JDBC Driver и расширение JOOQ для {{ ydb-short-name }}. Также требуется указать два параметра:

- `database.name`: `tech.ydb.jooq.codegen.YdbDatabase` (обязательная настройка)
- `strategy.name`: `tech.ydb.jooq.codegen.YdbGeneratorStrategy` (рекомендуется к использованию)

Рассмотрим на примере `maven` плагина:

```xml
<plugin>
    <groupId>org.jooq</groupId>
    <artifactId>jooq-codegen-maven</artifactId>
    <version>3.19.11</version>
    <executions>
        <execution>
            <goals>
                <goal>generate</goal>
            </goals>
        </execution>
    </executions>
    <dependencies>
        <dependency>
            <groupId>tech.ydb.jdbc</groupId>
            <artifactId>ydb-jdbc-driver</artifactId>
            <version>${ydb.jdbc.version}</version>
        </dependency>
        <dependency>
            <groupId>tech.ydb.dialects</groupId>
            <artifactId>jooq-ydb-dialect</artifactId>
            <version>${jooq.ydb.version}</version>
        </dependency>
    </dependencies>
    <configuration>
        <jdbc>
            <driver>tech.ydb.jdbc.YdbDriver</driver>
            <url>jdbc:ydb:grpc://localhost:2136/local</url>
        </jdbc>
        <generator>
            <strategy>
                <name>tech.ydb.jooq.codegen.YdbGeneratorStrategy</name>
            </strategy>
            <database>
                <name>tech.ydb.jooq.codegen.YdbDatabase</name>
                <!-- исключение системных таблицы -->
                <excludes>.sys.*</excludes> 
            </database>
            <target>
                <packageName>ydb</packageName>
                <directory>./src/main/java</directory>
            </target>
        </generator>
    </configuration>
</plugin>
```

Пример сгенерированных классов из [туториал по YQL](../dev/yql-tutorial/create_demo_tables.md):

```
ydb/DefaultCatalog.java
ydb/default_schema
ydb/default_schema/tables
ydb/default_schema/tables/Seasons.java
ydb/default_schema/tables/records
ydb/default_schema/tables/records/SeriesRecord.java
ydb/default_schema/tables/records/EpisodesRecord.java
ydb/default_schema/tables/records/SeasonsRecord.java
ydb/default_schema/tables/Series.java
ydb/default_schema/tables/Episodes.java
ydb/default_schema/Indexes.java
ydb/default_schema/Keys.java
ydb/default_schema/Tables.java
ydb/default_schema/DefaultSchema.java
```

## Использование

Для интеграции {{ ydb-short-name }} с JOOQ в ваш проект потребуется добавить две зависимости: {{ ydb-short-name }} JDBC Driver и расширение JOOQ для {{ ydb-short-name }}.

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

Для получения `YdbDSLContext` (расширение `org.jooq.DSLContext`) используйте класс `tech.ydb.jooq.YDB`. Например:

```java
String url = "jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]";
Connection conn = DriverManager.getConnection(url);

YdbDSLContext dsl = YDB.using(conn);
```

или

```java
String url = "jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]";
try (CloseableYdbDSLContext dsl = YDB.using(url)) {
    // ...
}
```

### Интеграция с Spring Boot

Расширим `JooqAutoConfiguration.DslContextConfiguration` собственным `YdbDSLContext`. Например:

```java
@Configuration
public class YdbJooqConfiguration extends JooqAutoConfiguration.DslContextConfiguration {

    @Override
    public YdbDSLContextImpl dslContext(org.jooq.Configuration configuration) {
        return YdbDSLContextImpl(configuration);
    }
}
```

```properties
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

`YdbDSLContext` готов к использованию. Рассмотрим примеры специфичных YQL запросов:

Команда [`UPSERT`](../yql/reference/syntax/upsert_into.md):

```java
public void upsert(YdbDSLContext context) {
  context.upsertInto(EPISODES)
          .set(record)
          .execute();    
}
```

Команда [`REPLACE`](../yql/reference/syntax/replace_into.md): 

```java
public void replace(YdbDSLContext context) {
    ydbDSLContext.replaceInto(EPISODES)
            .set(record)
            .execute();
}
```

Чтобы указать индекс в конструкции `VIEW`, необходимо использовать конструкцию `useIndex` у `Table`:

```java
var record = ydbDSLContext.selectFrom(SERIES.useIndex(Indexes.TITLE_NAME.name))
    .where(SERIES.TITLE.eq(title))
    .fetchOne();
```

В остальном диалект {{ ydb-short-name }} следует документации JOOQ.

Пример простого приложения Spring Boot можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-jooq).