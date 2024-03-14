# Диалект {{ ydb-short-name }} для Hibernate 

## Введение {#overview}

Это руководство использования Hibernate с {{ ydb-short-name }}. 

[Hibernate](https://hibernate.org/orm/) - это фреймворк объектно-реляционного отображения (ORM) для Java, облегчающий процесс маппинга объектно-ориентированных моделей. 

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Чтобы воспользоваться диалектом YDB в ваш проект нужно добавить зависимость самого диалекта и [JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver) в свой проект:

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

В случае использования Hibernate 5 версии нужен artifactId равный `hibernate-ydb-dialect-v5`.

## Конфигурация диалекта {#configuration-dialect}

Сконфигурировать Hibernate c помощью hibernate.cfg.xml:

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

## Интеграция с Spring Data JPA {#integration-with-spring-data-jpa}

Настройте Spring Data JPA для использования диалекта YDB, обновив свой application.properties:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
```

Пример простого приложение Spring Data JPA можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).
