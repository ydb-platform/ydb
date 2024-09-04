# Apache Superset

Apache Superset это современная платформа для анализа и визуализации данных.

Поддержка [сетевого протокола PostgreSQL](https://ydb.tech/docs/ru/postgresql/intro) в {{ ydb-short-name }} позволяет использовать [Apache Superset](https://superset.apache.org/) для запросов и визуализации данных из {{ ydb-short-name }}.

## Пререквизиты

Для работы понадобятся:

- [Apache Superset](https://superset.apache.org/).
- PostgreSQL драйвер, установленный в Apache Superset.

  Следуйте [документации Apache Superset](https://superset.apache.org/docs/configuration/databases#installing-database-drivers), чтобы установить драйвер для PostgreSQL.

## Создание подключения к {{ ydb-short-name }} {#add-database-connection}

Для создания подключения к {{ ydb-short-name }} из Apache Superset с использованием сетевого протокола PostgreSQL необходимо выполнить следующие шаги:

1. В верхнем меню Apache Superset, наведите курсор на **Settings** и выберите в выпадающем списке **Database Connections**.

1. Нажмите кнопку **+ DATABASE**.
    Откроется окно мастера **Connect a database**.

1. На первом шаге мастера, нажмите кнопку **PostgreSQL**.

1. На втором шаге мастера, введите данные о подключении к {{ ydb-short-name }} в следующие поля:

   * **HOST**. [Эндпоинт](https://ydb.tech/docs/ru/concepts/connect#endpoint) кластера {{ ydb-short-name }}, к которому осуществляется подключение.
   * **PORT**. Порт эндпойнта {{ ydb-short-name }}.
   * **DATABASE NAME**. Путь к [базе данных](https://ydb.tech/docs/ru/concepts/glossary#database) в кластере {{ ydb-short-name }}, к которой будут выполняться запросы.
   * **USERNAME**. Логин для подключения к базе данных {{ ydb-short-name }}.
   * **PASSWORD**. Пароль для подключения к базе данных {{ ydb-short-name }}.
   * **DISPLAY NAME**. Наименование соединения с {{ ydb-short-name }} в Apache Superset.

   ![](_assets/superset-ydb-connection-details.png =400x)

1. Нажмите кнопку **CONNECT**.

1. Нажмите кнопку **FINISH**, чтобы сохранить подключение.

## Создание набора данных (dataset) {#create-dataset}

Для создания набора данных из таблицы {{ ydb-short-name }} необходимо выполнить следующие шаги:

1. В верхнем меню Apache Superset, наведите курсор на **Settings** и выберите в выпадающем списке **SQL query**.

1. В выпадающем списке **DATABASE** выберите подключение к {{ ydb-short-name }}.

1. В выпадающем меню **SCHEMA** выберите `public`.

   {% note alert %}

   В настоящее время {{ ydb-short-name }} не предоставляет схемы таблиц через сетевой протокол PostgreSQL. Выбор таблицы в выпадающем списке **SEE TABLE SCHEMA** можно пропустить.

   {% endnote %}

1. Введите текст SQL-запроса в правой части страницы. Например, `SELECT * FROM <наименование_таблицы>`.

   {% note tip %}

   Если вы хотите создать набор данных из таблицы, которая расположена в поддиректории {{ ydb-short-name }}, необходимо указать путь к таблице в самом наименовании таблицы. Например, `SELECT * FROM '<путь/к/таблице/наименование_таблицы>'`.

   {% endnote %}

1. Нажмите кнопку **RUN**, чтобы проверить SQL-запрос.

   ![](_assets/superset-sql-query.png)

1. Нажмите на стрелку рядом с кнопкой **SAVE** и выберите **Save dataset** в выпадающем списке.

   Откроется диалоговое окно **Save or Overwrite Dataset**.

1. В открывшемся окне **Save or Overwrite Dataset** выберите **Save as new**, введите наименование набора данных и нажмите **SAVE & EXPLORE**.

После создания наборов данных вы можете использовать данные из {{ ydb-short-name }} для создания диаграмм в Apache Superset. См. документацию [Apache Superset](https://superset.apache.org/docs/intro/).
