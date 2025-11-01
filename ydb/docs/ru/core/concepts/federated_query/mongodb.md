# Работа с базами данных MongoDB

В этом разделе приведена основная информация о работе с внешней NoSQL-базой данных [MongoDB](https://www.mongodb.com/).

Для работы с внешней базой данных MongoDB необходимо выполнить следующие шаги:

1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.

    ```yql
    CREATE OBJECT mongodb_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```

1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий целевую базу данных внутри кластера MongoDB. Для соединения с MongoDB используется [нативный TCP-протокол](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/) (`PROTOCOL="NATIVE"`). Параметр `LOCATION` содержит сетевой адрес экземпляра MongoDB, к которому осуществляется подключение. В `DATABASE_NAME` указывается имя базы данных (например, `master`). Для аутентификации во внешнюю базу используются значения параметров `LOGIN` и `PASSWORD_SECRET_NAME`. Включить шифрование соединений с внешней базой данных можно с помощью параметра `USE_TLS="TRUE"`.

    Параметр `READING_MODE` определяет то, каким образом документ из MongoDB будет представлен в реляционном формате: опция `TABLE` подразумевает отображение каждого корневого поля документа в отдельную колонку таблицы.

    В параметре `UNSUPPORTED_TYPE_DISPLAY_MODE` указывается политика обработки типов данных MongoDB, которые пока не поддерживаются в {{ ydb-full-name }}. Можно выбрать один из двух вариантов: в случае `UNSUPPORTED_OMIT` поля неподдерживаемых типов будут пропущены в результате запроса, а с `UNSUPPORTED_AS_STRING` запрос вернёт значение подобных полей в сериализованном виде как тип `Utf8`. Подробнее о том, как система типов MongoDB отображается в {{ ydb-full-name }}, можно прочитать в разделе [«Поддерживаемые типы данных»](#supported-data-types).

    Опция `UNEXPECTED_TYPE_DISPLAY_MODE` отвечает за представление значений, которые оказались несовместимы с выведенной структурой документов MongoDB. При чтении данных из внешнего источника MongoDB {{ ydb-full-name }} автоматически выводит структуру документов коллекции с помощью небольшого скана. Поскольку данные MongoDB неструктурированы, отдельные документы коллекции могут не соответствовать полученной схеме. С опцией `UNEXPECTED_AS_NULL` подобные поля будут пропускаться вне зависимости от выведенного типа, а с опцией `UNEXPECTED_AS_STRING` значения полей неприводимых типов будут сериализованы в строки `Utf8`, если ожидаемый тип подобных полей в выведенной схеме — `Utf8`.

    ```yql
    CREATE EXTERNAL DATA SOURCE mongodb_datasource WITH (
        SOURCE_TYPE="MongoDB",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="<login>",
        PASSWORD_SECRET_NAME="mongodb_datasource_user_password",
        USE_TLS="TRUE",
        READING_MODE="TABLE",
        UNSUPPORTED_TYPE_DISPLAY_MODE="UNSUPPORTED_OMIT",
        UNEXPECTED_TYPE_DISPLAY_MODE="UNEXPECTED_AS_NULL"
    );
    ```

1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов {#query}

Для работы с MongoDB используется следующая форма SQL-запроса:

```yql
SELECT * FROM mongodb_datasource.<collection_name>
```

где:

- `mongodb_datasource` - идентификатор внешнего источника данных;
- `<collection_name>` - имя коллекции внутри внешнего источника данных.

## Ограничения

При работе с кластерами MongoDB существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

    |Тип данных {{ ydb-short-name }}|
    |----|
    |`Bool`|
    |`Int8`|
    |`Uint8`|
    |`Int16`|
    |`Uint16`|
    |`Int32`|
    |`Uint32`|
    |`Int64`|
    |`Uint64`|
    |`Float`|
    |`Double`|
    |`String`|
    |`Utf8`|

## Поддерживаемые типы данных {#supported-data-types}

MongoDB — это NoSQL-СУБД, предназначенная для работы с неструктурированными и полуструктурированными данными. В отличие от реляционных баз данных, MongoDB хранит JSON-подобные документы, которые, как правило, не соответствуют единому формату или структуре. В связи с этим для преобразования данных MongoDB в реляционный формат, необходимый для выполнения SQL-запросов, {{ ydb-full-name }} автоматически выводит схему MongoDB при выполнении запроса с помощью небольшого сканирования коллекции. В случае обработки документов, в которых одинаковые поля представлены разными неприводимыми типами (например, `Int32` и `String`), они будут представлены в запросе в сериализованном виде, а тип {{ ydb-full-name }} для них будет соответствовать `Optional<Utf8>`.

Любые поля MongoDB, кроме `_id`, по умолчанию могут быть опущены или содержать значения `NULL`, и в системе типов {{ ydb-full-name }} они должны отображаться в [опциональные](../../yql/reference/types/optional.md) типы. Поскольку поле `_id` в разных документах одной коллекции может иметь разные типы, в системе типов {{ ydb-full-name }} оно также будет опциональным.

Ниже приведена таблица соответствия типов MongoDB и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются и не могут быть использованы в пушдауне предикатов. Однако поля таких типов всё ещё могут использоваться в качестве возвращаемого значения запроса в сериализованном виде как `Optional<Utf8>` при включении параметра `UNSUPPORTED_TYPE_DISPLAY_MODE=UNSUPPORTED_AS_STRING`.

|Тип данных MongoDB|Тип данных {{ ydb-full-name }}
|---|---|
|`Boolean`|`Optional<Bool>`|
|`Int32`|`Optional<Int32>`|
|`Int64`|`Optional<Int64>`|
|`Double`|`Optional<Double>`|
|`String`|`Optional<Utf8>`|
|`Binary`|`Optional<String>`|
|`ObjectId`|`Optional<String>`|
