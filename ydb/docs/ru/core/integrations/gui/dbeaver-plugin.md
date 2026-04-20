# Подключение к {{ ydb-short-name }} с помощью плагина DBeaver

[DBeaver](https://dbeaver.com) — бесплатный кроссплатформенный инструмент управления базами данных с открытым исходным кодом, обеспечивающий визуальный интерфейс для подключения к различным базам данных и выполнения SQL-запросов.

[YDB DBeaver Plugin](https://github.com/ydb-platform/ydb-dbeaver-plugin) — расширение DBeaver с нативной поддержкой {{ ydb-short-name }}. В отличие от [подключения через JDBC-драйвер](dbeaver.md), плагин предоставляет специализированный интерфейс для работы с объектами {{ ydb-short-name }}: иерархический навигатор таблиц, топиков, представлений, внешних источников данных, поддержку всех способов аутентификации, редактор [YQL](../../concepts/glossary.md#yql), визуализацию планов выполнения, мониторинг сессий и кластера, управление правами доступа (ACL) и другие возможности.

## Ключевые возможности плагина {#features}

- Подключение к {{ ydb-name }} со всеми способами [аутентификации](../../security/authentication.md): анонимная, статическая, по токену, по сервисному аккаунту, по метаданным.
- Иерархический навигатор объектов: таблицы, топики, внешние источники данных, внешние таблицы, представления.
- Системные объекты: [системные представления](../../dev/system-views.md) (`.sys`), [пулы ресурсов](../../concepts/glossary.md#resource-pool) и [классификаторы пулов ресурсов](../../concepts/glossary.md#resource-pool-classifier).
- Редактор [YQL](../../concepts/glossary.md#yql) с подсветкой 150+ ключевых слов и встроенных функций.
- Визуализация плана выполнения запроса (`EXPLAIN` / `EXPLAIN ANALYZE`).
- Мониторинг активных сессий через [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Дашборд кластера: загрузка CPU, использование дисков, памяти, сетевой трафик, статус узлов (обновление каждые 5 секунд).
- Управление [правами доступа (ACL)](../../security/authorization.md#right): выдача, отзыв, просмотр разрешений.
- Управление [потоковыми запросами](../../concepts/glossary.md#streaming-query): просмотр, изменение, запуск, остановка.
- [Федеративные запросы](../../concepts/query_execution/federated_query/index.md) через внешние источники данных (S3, базы данных).
- [Конвертер SQL-запросов](../sql-dialect-converter.md) из других диалектов (PostgreSQL, MySQL, ClickHouse и других) в YQL.
- Специализированные редакторы для типов данных `JSON`, `JSONDOCUMENT`, `YSON`.

## Требования {#requirements}

Для работы плагина требуется DBeaver Community Edition версии 24.x или новее.

## Установка плагина {#installation}

Плагин устанавливается через механизм установки расширений DBeaver из URL-репозитория, что обеспечивает автоматическое получение обновлений.

1. Откройте DBeaver. В верхнем меню выберите **Help → Install New Software...**.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-context.png)

    {% endcut %}

1. Нажмите кнопку **Add...** справа от поля **Work with:**.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-add.png)

    {% endcut %}

1. В открывшемся окне **Add Repository** укажите имя репозитория (например, `YDB Plugin`) и вставьте в поле **Location** следующий URL:

    ```text
    https://storage.yandexcloud.net/ydb-dbeaver-plugin
    ```

    Нажмите **Add**. DBeaver загрузит метаданные репозитория.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-add-repository.png)

    {% endcut %}

1. В списке компонентов появится категория **DBeaver YDB Support**. Отметьте её и нажмите **Next >**.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-ydb-support.png)

    {% endcut %}

1. На экране **Install Details** убедитесь, что в списке присутствуют оба компонента (`org.jkiss.dbeaver.ext.ydb` и `org.jkiss.dbeaver.ext.ydb.ui`), и нажмите **Next >**.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-install-details.png)

    {% endcut %}

1. DBeaver может показать предупреждение о неподписанном содержимом. Это ожидаемое поведение — JAR-файлы плагина не подписаны коммерческим сертификатом. Нажмите **Install Anyway**.

    {% note info %}

    Eclipse, на котором основан DBeaver, проверяет подписи JAR-файлов для подтверждения подлинности. Этот плагин с открытым исходным кодом распространяется без подписи, исходный код доступен в [репозитории](https://github.com/ydb-platform/ydb-dbeaver-plugin).

    {% endnote %}

1. Ознакомьтесь с лицензией (Apache License 2.0), выберите **I accept the terms of the license agreements** и нажмите **Finish**.

    {% cut "Скриншот" %}

    ![](./_assets/dbeaver-plugin-license.png)

    {% endcut %}

1. DBeaver установит плагин и предложит перезапуск. Нажмите **Restart Now**. После перезапуска плагин станет активным.

## Создание подключения к {{ ydb-name }} {#connection}

Для создания подключения к {{ ydb-name }} выполните следующие шаги:

1. В верхнем меню выберите **Database → New Database Connection** (или нажмите `Ctrl+Shift+N`).
1. В поле поиска введите `YDB`. Выберите **YDB** из списка и нажмите **Next**.
1. Откроется страница настройки подключения к {{ ydb-name }}. Заполните поля:

    | Поле | Описание | Пример |
    |------|----------|--------|
    | **Host** | Хост [эндпойнта](../../concepts/connect.md#endpoint) кластера {{ ydb-name }} | `ydb.example.com` |
    | **Port** | Порт (по умолчанию `2135`) | `2135` |
    | **Database** | Путь к [базе данных](../../concepts/glossary.md#database) | `/Root/database` |
    | **Monitoring URL** | URL [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md) с путём к базе данных, используется для дашборда (необязательно) | `http://ydb.example.com:8765/monitoring/tenant?name=%2FRoot%2Fdatabase` |
    | **Use secure connection** | Использовать защищённое соединение (`grpcs://`) | ☑ |
    | **Enable autocomplete API** | Автодополнение через API {{ ydb-short-name }} | ☑ |

1. Выберите способ аутентификации в выпадающем списке **Auth type** (см. [Способы аутентификации](#auth-methods)).
1. Нажмите кнопку **Test Connection** для проверки настроек. При успешном подключении появится диалог с временем соединения в миллисекундах.
1. Нажмите кнопку **Finish**. Подключение появится в панели **Database Navigator**.

## Способы аутентификации {#auth-methods}

Плагин поддерживает все способы [аутентификации](../../security/authentication.md), доступные в {{ ydb-short-name }}. Способ выбирается в выпадающем списке **Auth type** на странице настройки подключения.

### Anonymous {#auth-anonymous}

Подключение без учётных данных. Используется для локальных или тестовых установок {{ ydb-short-name }}. Дополнительных полей заполнять не требуется.

### Static (логин и пароль) {#auth-static}

Аутентификация по логину и паролю. Укажите имя пользователя в поле **User** и пароль в поле **Password**. Используется, если на сервере {{ ydb-short-name }} включена [аутентификация по логину и паролю](../../security/authentication.md#static-credentials).

{% note info %}

В managed-инсталляциях {{ ydb-name }} аутентификация по логину и паролю отключена: управляемые сервисы используют централизованную систему управления доступом облачной платформы ([IAM](https://yandex.cloud/ru/docs/iam/)).

{% endnote %}

### Token {#auth-token}

Аутентификация по [IAM-](https://yandex.cloud/ru/docs/iam/concepts/authorization/iam-token) или [OAuth-токену](https://yandex.cloud/ru/docs/iam/concepts/authorization/oauth-token). Введите токен в поле **Token**. Токен передаётся в заголовке каждого запроса.

### Service Account {#auth-service-account}

Аутентификация по ключу [сервисного аккаунта](https://yandex.cloud/ru/docs/iam/concepts/users/service-accounts) Yandex Cloud. Укажите путь к JSON-файлу с ключом в поле **SA Key File** (для выбора файла используйте кнопку **...**). Подробнее о том, как создать авторизованный ключ, см. в [документации Yandex Cloud](https://yandex.cloud/ru/docs/iam/operations/authentication/manage-authorized-keys).

Формат файла ключа:

```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

### Metadata {#auth-metadata}

Аутентификация через [сервис метаданных Yandex Cloud](https://yandex.cloud/ru/docs/compute/operations/vm-metadata/get-vm-metadata). Плагин получает IAM-токен от сервиса метаданных виртуальной машины. Используется, только когда DBeaver запущен на виртуальной машине Yandex Cloud.

## Навигатор объектов {#object-navigator}

После подключения в панели **Database Navigator** отображается иерархия объектов {{ ydb-short-name }}. Корневой узел — подключение, внутри — путь к базе данных, который содержит следующие папки:

- **Tables** — таблицы, организованные по поддиректориям согласно пути в {{ ydb-short-name }} (например, таблица по пути `folder1/subfolder/mytable` будет вложена в `folder1 → subfolder`).
- **Topics** — [топики](../../concepts/datamodel/topic.md).
- **Views** — [представления](../../concepts/datamodel/view.md).
- **External Data Sources** — [внешние источники данных](../../concepts/glossary.md#external-data-source).
- **External Tables** — [внешние таблицы](../../concepts/glossary.md#external-table).
- **System Views (.sys)** — [системные представления](../../dev/system-views.md), такие как `partition_stats`, `query_sessions`.
- **Resource Pools** — [пулы ресурсов](../../concepts/glossary.md#resource-pool).

## Работа с плагином {#capabilities}

### Редактор YQL {#yql-editor}

Откройте **SQL Editor** (`F3` или двойной клик по подключению). Редактор поддерживает:

- Подсветку синтаксиса [YQL](../../yql/reference/index.md): ключевые слова (`UPSERT`, `REPLACE`, `EVALUATE`, `PRAGMA`, `WINDOW` и 145+ других), типы данных, встроенные функции.
- Автодополнение имён таблиц, колонок и функций.
- Выполнение запросов: `Ctrl+Enter` — текущий запрос, `Ctrl+Shift+Enter` — весь скрипт.

Пример YQL-запроса:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

### EXPLAIN и план выполнения {#explain}

Нажмите **Explain** (или `Ctrl+Shift+E`), чтобы получить [план выполнения запроса](../../dev/query-plans-optimization.md). Плагин отображает:

- **Text plan** — дерево операций в текстовом виде.
- **Diagram** — графическое представление в виде DAG.
- **SVG plan** — интерактивная визуализация.

`EXPLAIN ANALYZE` дополнительно показывает статистику выполнения (количество строк, время выполнения).

### Менеджер сессий {#session-manager}

Кликните правой кнопкой по подключению и выберите **Manage Sessions**, либо используйте пункт меню **Database → Manage Sessions**. В открывшемся представлении отображаются все активные сессии с текущим запросом, состоянием и длительностью (данные из системного представления [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). Галочка **Hide Idle** скрывает сессии без активного запроса.

### Дашборд кластера {#cluster-dashboard}

Откройте вкладку **Dashboard** в редакторе подключения (требует заполненного поля **Monitoring URL** при настройке подключения).

{% note warning %}

Дашборд доступен только при работе с self-hosted инсталляциями {{ ydb-short-name }}, где есть доступ к [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). В Yandex Cloud Managed Service for {{ ydb-short-name }} Embedded UI не публикуется, поэтому данные дашборда недоступны — для мониторинга используйте [средства облачной платформы](https://yandex.cloud/ru/docs/ydb/operations/monitoring).

{% endnote %}

Дашборд отображает в реальном времени (обновление каждые 5 секунд):

- Загрузку CPU по узлам.
- Использование дискового пространства.
- Использование памяти.
- Сетевой трафик.
- Количество выполняющихся запросов.
- Статус узлов кластера.

### Стриминговые запросы {#streaming-queries}

В навигаторе раскройте папку **Streaming Queries**. Для каждого запроса доступны:

- Просмотр исходного YQL.
- Просмотр ошибок (issues).
- Просмотр плана выполнения.
- Действия: **Start**, **Stop**, **Alter**.

### Конвертер SQL-диалектов {#convert-dialect}

Плагин позволяет преобразовать SQL-запрос, написанный на другом диалекте (PostgreSQL, MySQL, ClickHouse и других), в YQL. Конвертер доступен на вкладке **Convert Dialect** в редакторе подключения.

Чтобы преобразовать запрос:

1. В выпадающем списке **Source Dialect** выберите исходный диалект SQL. Список диалектов запрашивается с внешнего сервиса плагина при первом открытии вкладки.
1. Вставьте исходный SQL-код в поле **Input SQL**.
1. Нажмите **Convert**. Результат появится в нижнем поле.
1. Нажмите **Copy**, чтобы скопировать результат в буфер обмена.

Подробнее о принципах работы конвертера, поддерживаемых диалектах и ограничениях см. в статье [Конвертер SQL-диалектов в YQL](../sql-dialect-converter.md).

{% note warning %}

Для преобразования плагин отправляет исходный запрос на внешний HTTPS-сервис. Не используйте конвертер для запросов, содержащих конфиденциальные данные.

{% endnote %}

### Создание объектов {#create-objects}

Кликните правой кнопкой по папке или объекту и выберите **Create New**:

- **Create Table** — создать новую таблицу.
- **Create Topic** — создать новый топик.
- **Create Resource Pool** — создать пул ресурсов.

## Обновление плагина {#updates}

DBeaver использует механизм Eclipse P2 для обнаружения и установки обновлений. При установке плагина DBeaver запоминает источник — URL репозитория. Когда публикуется новая версия, DBeaver сравнивает установленную версию с версией в репозитории и предлагает обновление одним из двух способов:

1. Автоматически при следующем запуске DBeaver (если включена проверка обновлений в **Window → Preferences → Install/Update → Automatic Updates**).
1. Вручную через **Help → Check for Updates**: выберите доступное обновление и пройдите те же шаги, что и при первой установке (лицензия → предупреждение о неподписанном содержимом → перезапуск).

