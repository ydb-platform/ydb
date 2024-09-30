# Apache Superset

Apache Superset — это современная платформа для анализа и визуализации данных.

[Режим совместимости с PostgreSQL в {{ ydb-short-name }}](../../postgresql/intro.md) позволяет использовать [Apache Superset](https://superset.apache.org/) для выполнения запросов и визуализации данных из {{ ydb-short-name }}. В этом случае Apache Superset работает с {{ ydb-short-name }} как с PostgreSQL.

{% include [../../postgresql/_includes/alert_preview.md](../../postgresql/_includes/alert_preview.md) %}

## Создание подключения к {{ ydb-short-name }} {#add-database-connection}

Чтобы создать подключение к {{ ydb-short-name }} из Apache Superset с использованием сетевого протокола PostgreSQL, выполните следующие шаги:

1. В верхнем меню Apache Superset наведите курсор на **Settings** и выберите в выпадающем списке пункт **Database Connections**.
1. Нажмите кнопку **+ DATABASE**.

    Откроется окно мастера **Connect a database**.

1. На первом шаге мастера нажмите кнопку **PostgreSQL**.
1. На втором шаге мастера введите данные для подключения к {{ ydb-short-name }} в следующие поля:

    * **HOST** — [эндпоинт](../../concepts/connect.md#endpoint) кластера {{ ydb-short-name }}, к которому осуществляется подключение.
    * **PORT** — порт эндпоинта {{ ydb-short-name }}.
    * **DATABASE NAME** — путь к [базе данных](../../concepts/glossary.md#database) в кластере {{ ydb-short-name }}, к которой будут выполняться запросы.
    * **USERNAME** — логин для подключения к базе данных {{ ydb-short-name }}.
    * **PASSWORD** — пароль для подключения к базе данных {{ ydb-short-name }}.
    * **DISPLAY NAME** — наименование соединения с {{ ydb-short-name }} в Apache Superset.

    ![](_assets/superset-ydb-connection-details.png =400x)

1. Нажмите кнопку **CONNECT**.
1. Нажмите кнопку **FINISH**, чтобы сохранить подключение.

## Создание набора данных (dataset) {#create-dataset}

Чтобы создать набор данных из таблицы {{ ydb-short-name }}, выполните следующие шаги:

1. В верхнем меню Apache Superset наведите курсор на кнопку **+** и выберите в выпадающем списке пункт **SQL query**.
1. В выпадающем списке **DATABASE** выберите подключение к {{ ydb-short-name }}.
1. В выпадающем меню **SCHEMA** выберите `public`.

    {% note alert %}

    В настоящее время {{ ydb-short-name }} не предоставляет схемы таблиц через сетевой протокол PostgreSQL. Поэтому выбор таблицы в выпадающем списке **SEE TABLE SCHEMA** можно пропустить.

    {% endnote %}

1. Введите текст SQL-запроса в правой части страницы. Например, `SELECT * FROM <наименование_таблицы>`.

    {% note tip %}

    Если вы хотите создать набор данных из таблицы, которая расположена в поддиректории {{ ydb-short-name }}, необходимо указать путь к таблице в самом наименовании таблицы. Например:

    ```yql
    SELECT * FROM "<путь/к/таблице/наименование_таблицы>";
    ```

    {% endnote %}

1. Нажмите кнопку **RUN**, чтобы проверить SQL-запрос.

    ![](_assets/superset-sql-query.png)

1. Нажмите на стрелку рядом с кнопкой **SAVE** и выберите **Save dataset** в выпадающем списке.

    Откроется диалоговое окно **Save or Overwrite Dataset**.

1. В открывшемся окне **Save or Overwrite Dataset** выберите **Save as new**, введите наименование набора данных и нажмите **SAVE & EXPLORE**.

После создания наборов данных вы можете использовать данные из {{ ydb-short-name }} для создания диаграмм в Apache Superset. См. документацию [Apache Superset](https://superset.apache.org/docs/intro/).

## Создание диаграммы {#create-chart}

Теперь давайте создадим пример диаграммы с использованием набора данных из таблицы `episodes`, которая описана в [Туториале по YQL](../../dev/yql-tutorial/index.md).

Таблица `episodes` содержит следующие колонки:

* series_id;
* season_id;
* episode_id;
* title;
* air_date.

Предположим, что мы хотим построить круговую диаграмму, в которой бы было видно, сколько серий содержит каждый сезон сериала.

Чтобы создать диаграмму, выполните следующие шаги:

1. В верхнем меню Apache Superset наведите курсор на **+** и выберите в выпадающем списке пункт **Chart**.
1. В выпадающем списке **Choose a dataset**, выберите набор данных из таблицы `episodes`.
1. На панели **Choose chart type**, выберите тип диаграммы `Pie chart`.
1. Нажмите кнопку **CREATE NEW CHART**.
1. На панели **Query** настройте диаграмму:

    * В выпадающем списке **DIMENSIONS** выберите колонку `season_id`.
    * В поле **METRIC** введите функцию `COUNT(title)`.
    * В поле **FILTERS** введите фильтр `series_id in (2)`.

1. Нажмите кнопку **CREATE CHART**.

    Круговая диаграмма появится на панели справа.

    ![](_assets/superset-sample-chart.png)

1. Нажмите кнопку **SAVE**.

    Откроется диалоговое окно **Save chart**.

1. В открывшемся окне **Save chart** в поле **CHART NAME** введите наименование диаграммы.
1. Нажмите кнопку **SAVE**.