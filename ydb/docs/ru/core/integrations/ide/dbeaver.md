# Подключение к {{ ydb-short-name }} с помощью DBeaver

[DBeaver](https://dbeaver.com) — бесплатный кроссплатформенный инструмент управления базами данных с открытым исходным кодом, обеспечивающий визуальный интерфейс для подключения к различным базам данных и выполнения SQL-запросов. Он поддерживает множество систем управления базами данных, включая MySQL, PostgreSQL, Oracle и SQLite.

DBeaver позволяет работать с {{ ydb-short-name }} по протоколу Java DataBase Connectivity ([JDBC](https://ru.wikipedia.org/wiki/Java_Database_Connectivity)). Данная статья демонстрирует, как настроить такую интеграцию.

## Подключение JDBC-драйвера {{ ydb-name }} к DBeaver {#dbeaver_ydb}

Для подключения к {{ ydb-name }} из DBeaver понадобится JDBC-драйвер. Для загрузки JDBC-драйвера выполните следующие шаги:
1. Перейдите в [репозиторий ydb-jdbc-driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases).
1. Выберите последний релиз (отмечен тегом `Latest`) и сохраните файл `ydb-jdbc-driver-shaded-<driver-version>.jar`.

Для подключения загруженного JDBC-драйвера выполните следующие шаги:
1. Выберите в верхнем меню DBeaver пункт **База данных**, а затем подпункт **Управление драйверами**:

    ![](./_assets/dbeaver-driver-management_ru.png)

1. Чтобы создать новый драйвер, в открывшемся окне **Менеджер Драйверов** нажмите кнопку **Новый**:

    ![](./_assets/dbeaver-driver-create-new-driver_ru.png)

1. В открывшемся окне **Создать драйвер**, в поле **Имя драйвера**, укажите `YDB`:

    ![](./_assets/dbeaver-driver-create-new-driver-set-name_ru.png)

1. Перейдите в раздел **Библиотеки**, нажмите кнопку **Добавить файл**, укажите путь к скачанному ранее JDBC-драйверу {{ ydb-short-name }} (файлу `ydb-jdbc-driver-shaded-<driver-version>.jar`) и нажмите кнопку **OK**:

    ![](./_assets/dbeaver-driver-management-driver_ru.png)


1. В списке драйверов появится пункт **YDB**. Дважды кликните по новому драйверу и перейдите на вкладку **Библиотеки**, нажмите кнопку **Найти Класс** и в выпадающем списке выберите `tech.ydb.jdbc.YdbDriver`.

    {% note warning %}

    Обязательно явно выберите пункт выпадающего списка `tech.ydb.jdbc.YdbDriver`, нажав на него. В противном случае DBeaver будет считать, что драйвер не был выбран.

    {% endnote %}

    ![](./_assets/dbeaver-driver-management-driver_set.png)

## Создание подключения к {{ ydb-name }} {#dbeaver_ydb_connection}

Для создания подключения необходимо выполнить следующие шаги:

1. В DBeaver создайте новое соединение, указав тип соединения `YDB`.
1. В открывшемся окне перейдите в раздел **Главное**.
1. В подразделе **Общие**, в поле ввода **JDBC URL**, укажите следующую строку соединения:

    ```
    jdbc:ydb:<ydb_endpoint>/<ydb_database>?useQueryService=true
    ```

    Где:
    - `ydb_endpoint` — [эндпойнт](../../concepts/connect.md#endpoint) кластера {{ydb-name}}, к которому будут выполняться подключение.
    - `ydb_database` — путь к [базе данных](../../concepts/glossary.md#database) в кластере {{ydb-name}}, к которой будут выполняться запросы.

    ![](./_assets/dbeaver-ydb-connection.png)

1. В поля **Пользователь** и **Пароль** введите логин и пароль для подключения к базе данных. Полный список способов аутентификации и строк подключения к {{ ydb-name }} приведён в описании [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver).
1. Нажмите кнопку **Тест соединения ...** для проверки настроек.

    Если все настройки указаны верно, то появится сообщение об успешном тестировании соединения:

    ![](./_assets/dbeaver-connection-test.png =400x)

1. Нажмите кнопку **Готово** для сохранения соединения.

## Работа с {{ ydb-name }} {#dbeaver_ydb_connection}

С помощью DBeaver можно просматривать список и структуру таблиц:

![](./_assets/dbeaver-table-structure.png)

А также выполнять запросы к данным:

![](./_assets/dbeaver-query.png)
