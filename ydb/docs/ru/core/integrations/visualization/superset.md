# Apache Superset

[Apache Superset](https://superset.apache.org/) — современная платформа для анализа и визуализации данных. В этой статье описано, как создавать визуализации на основе данных, хранящихся в {{ ydb-short-name }}.

## Установка необходимых зависимостей {#prerequisites}

Для работы с {{ ydb-short-name }} необходимо установить драйвер [ydb-sqlalchemy](https://pypi.org/project/ydb-sqlalchemy).

Способ установки зависит от способа развёртывания Superset. Дополнительную информацию см. в [официальной документации Superset](https://superset.apache.org/docs/configuration/databases/#installing-drivers-in-docker-images).

## Создание подключения к {{ ydb-short-name }} {#add-database-connection}

Подключение к {{ ydb-short-name }} доступно в двух вариантах:

1. Нативное подключение с помощью SQLAlchemy-драйвера (начиная с версии 5.0.0);
1. Подключение с использованием сетевого протокола PostgreSQL.

Рекомендуется использовать нативное подключение, когда это возможно.

### Нативное подключение с помощью SQLAlchemy-драйвера

Чтобы создать подключение к {{ ydb-short-name }} из Apache Superset **версии 5.0.0 и выше**, выполните следующие шаги:

1. В верхнем меню Apache Superset наведите курсор на **Settings** и выберите в выпадающем списке пункт **Database Connections**.
1. Нажмите кнопку **+ DATABASE**.

    Откроется окно мастера **Connect a database**.

1. На первом шаге мастера выберите **YDB** из списка **Supported databases**. Если опция **YDB** недоступна, убедитесь, что [установлены все необходимые компоненты](#prerequisites).
1. На втором шаге мастера введите данные для подключения к {{ ydb-short-name }} в следующие поля:

    * **Display Name** — наименование соединения с {{ ydb-short-name }} в Apache Superset;
    * **SQLAlchemy URI** — строка вида `ydb://{host}:{port}/{database_name}`, где **host** и **port** — соответствующие значения из [эндпоинта](../../concepts/connect.md#endpoint) кластера {{ ydb-short-name }}, **database_name** — путь к [базе данных](../../concepts/glossary.md#database).

    ![](_assets/superset-ydb-connection-details.png =400x)

1. Опционально, с помощью поля `Secure Extra` на вкладке `Advanced / Security` можно указать параметры аутентификации.

    Определите параметры следующим образом:

    {% list tabs group=auth-type %}

    - Пароль {#static-credentials}

        ```json
        {
            "credentials": {
                "username": "...",
                "password": "..."
            }
        }
        ```

    - Токен {#access-token-credentials}

        ```json
        {
            "credentials": {
                "token": "...",
            }
        }
        ```



    - Сервисный аккаунт {#service-account-credentials}

        ```json
        {
            "credentials": {
                "service_account_json": {
                    "id": "...",
                    "service_account_id": "...",
                    "created_at": "...",
                    "key_algorithm": "...",
                    "public_key": "...",
                    "private_key": "..."
                }
            }
        }
        ```

    {% endlist %}

1. Нажмите кнопку **CONNECT**.
1. Нажмите кнопку **FINISH**, чтобы сохранить подключение.

Для дополнительной информации о возможностях конфигурации соединения обратитесь к [разделу {{ ydb-short-name }} официальной документации](https://superset.apache.org/docs/configuration/databases#ydb).

### Подключение с использованием сетевого протокола PostgreSQL

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

    ![](_assets/superset-ydb-pg-connection-details.png =400x)

1. Нажмите кнопку **CONNECT**.
1. Нажмите кнопку **FINISH**, чтобы сохранить подключение.

## Создание набора данных (dataset) {#create-dataset}

Чтобы создать набор данных из таблицы {{ ydb-short-name }}, выполните следующие шаги:

1. В верхнем меню Apache Superset наведите курсор на кнопку **+** и выберите в выпадающем списке пункт **SQL query**.
1. В выпадающем списке **DATABASE** выберите подключение к {{ ydb-short-name }}.

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