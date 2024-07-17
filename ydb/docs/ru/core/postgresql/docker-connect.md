# Подключение через PostgreSQL-протокол

## Запуск {{ ydb-short-name }} с включенным PostgreSQL

Сейчас функционал postgres-совместимости доступен в образе: `ghcr.io/ydb-platform/local-ydb:nightly`.

Команды для запуска локального докер-контейнера с YDB и открытыми портами postgres и Web-UI.

{% note tip %}

В этом примере контейнеры намеренно создаются так, чтобы их состояние удалялось после завершения работы. Это упрощает инструкцию и позволяет многократно запускать тесты в известном (чистом) окружении, не думая о поломках.

Для того чтобы состояние контейнера сохранялось: нужно убрать переменную окружения YDB_USE_IN_MEMORY_PDISKS.

{% endnote %}

{% list tabs %}

- Docker-compose

    Для запуска через конфигурационный файл docker-compose он уже должен быть [установлен в системе](https://docs.docker.com/compose/install/standalone/)

    docker-compose.yaml:
    ```
    services:
        ydb:
            image: ghcr.io/ydb-platform/local-ydb:nightly
            ports:
            - "5432:5432"
            - "8765:8765"
            environment:
            - "YDB_USE_IN_MEMORY_PDISKS=true"
            - "POSTGRES_USER=${YDB_PG_USER:-root}"
            - "POSTGRES_PASSWORD=${YDB_PG_PASSWORD:-1234}"
            - "YDB_EXPERIMENTAL_PG=1"
    ```

    запуск:
    ```bash
    docker-compose up -d --pull=always
    ```

- Docker-команда

    ```bash
    docker run --name ydb-postgres -d --pull always -p 5432:5432 -p 8765:8765 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=1234 -e YDB_EXPERIMENTAL_PG=1 -e YDB_USE_IN_MEMORY_PDISKS=true ghcr.io/ydb-platform/local-ydb:nightly
    ```

{% endlist %}


После запуска контейнера можно подключаться к нему через postgres-клиенты на порт 5432, база local или открыть [веб-интерфейс](http://localhost:8765) на порту 8765.

## Подключение к запущенному контейнеру через psql

При выполнении этой команды запустится интерактивный консольный клиент postgres. Все последующие запросы нужно вводить внутри этого клиента.

```bash
docker run --rm -it --network=host postgres:14 psql postgresql://root:1234@localhost:5432/local
```

### Первый Hello world

```sql
SELECT 'Hello, world!';
```

Вывод:
```
    column0
---------------
 Hello, world!
(1 row)
```

### Создание таблицы
Основная цель существования систем управления базами данных - сохранение данных для последующего извлечения. Как система, базирующаяся на SQL, основной абстракцией для хранения данных является таблица. Чтобы создать нашу первую таблицу, выполните следующий запрос:

```sql

CREATE TABLE example
(
    key int4,
    value text,
    PRIMARY KEY (key)
);
```

### Добавление тестовых данных
Теперь давайте заполним нашу таблицу первыми данными. Самый простой способ - использовать литералы.

```sql
INSERT INTO example (key, value)
VALUES (123, 'hello'),
       (321, 'world');
```

### Запрос к тестовым данным

```sql
SELECT COUNT(*) FROM example;
```

Вывод:
```
 column0
---------
       2
(1 row)
```


## Остановка контейнера

Эта команда остановит запущенный контейнер и удалит все хранящиеся в нём данные.

{% list tabs %}

- Docker-compose

    В папке с исходным файлом docker-compose.yaml выполнить команду, которая остановит контейнер и удалит его данные:

    ```bash
    docker-compose down -vt 1
    ```
    {% note info %}

    Для остановки контейнера с сохранением данных уберите из конфига переменную YDB_USE_IN_MEMORY_PDISKS и используйте команду остановки:

    ```bash
    docker-compose stop
    ```

    {% endnote %}

- Docker-команда

    Эта команда остановит и удалит данные:

    ```bash
    docker rm -f ydb-postgres
    ```

{% endlist %}
