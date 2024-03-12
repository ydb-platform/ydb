## Диалект Hibernate ##

### Введение ### 

Это руководство использования Hibernate с YDB. 

Hibernate - это фреймворк объектно-реляционного отображения (ORM) для Java, облегчающий процесс маппинга объектно-ориентированных моделей. Каждая СУБД частично реализует стандарт SQL, интерпретируя те или иные конструкции по - своему. 

Для того чтобы иметь единый ORM фреймворк для различных СУБД, вводится на клиенте такая сущность как диалект, которая разрешает спорные моменты.
Например, какие типы поддерживает база данных, `1` или `0` - будет являться типом bool, или явно нужно генерировать `TRUE` или `FALSE` и так далее.

### Установка диалекта YDB ###

Чтобы воспользоваться диалектом YDB в ваш проект нужно добавить следующие зависимости (пример для maven):

```xml
<dependency>
    <groupId>tech.ydb.dialects</groupId>
    <artifactId>hibernate-ydb-dialect</artifactId>
    <!-- Set actual version -->
    <version>${hibernate.ydb.dialect.version}</version> 
</dependency>
```

В случае использования Hibernate 5 версии нужен `<artifactId>hibernate-ydb-dialect-v5</artifactId>`.

А также зависимость актуальной версии [JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver). 

### Конфигурация диалекта ###

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

### Использование ###

Используйте этот диалект так же, как и любой другой диалект Hibernate. Сопоставьте классы сущностей с таблицами базы данных и используйте фабрику сессий Hibernate для выполнения операций с базой данных.

### Интеграция с Spring Data JPA ### 

Настройте Spring Data JPA для использования диалекта YDB, обновив свой application.properties:

```properties
spring.jpa.properties.hibernate.dialect=tech.ydb.hibernate.dialect.YdbDialect

spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
```

Пример простого приложение Spring Data JPA можно найти по [ссылке](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-data-jpa).

### Контакты ###

В случае нахождения каких-либо проблем вы можете открыть `issue` в [репозитории](https://github.com/ydb-platform/ydb-java-dialects/tree/main/hibernate-dialect) с соответствующим тегом (`hibernate-v6` или `hibernate-v5`).
