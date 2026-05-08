# Подключение к {{ ydb-short-name }} с помощью плагина VS Code

[Visual Studio Code](https://code.visualstudio.com) — бесплатный кроссплатформенный редактор кода с открытым исходным кодом, поддерживающий широкую экосистему расширений для работы с базами данных, языковых серверов и инструментов разработки.

[YDB for VS Code](https://github.com/ydb-platform/ydb-vscode-plugin) — расширение VS Code с нативной поддержкой {{ ydb-short-name }}. Плагин предоставляет специализированный интерфейс для работы с объектами {{ ydb-short-name }}: иерархический навигатор таблиц, топиков, представлений, внешних источников данных, поддержку всех способов аутентификации, редактор [YQL](../../concepts/glossary.md#yql) с подсветкой синтаксиса, визуализацию планов выполнения, мониторинг сессий и кластера, управление правами доступа (ACL), встроенный [MCP-сервер](https://modelcontextprotocol.io/) для AI-ассистентов и другие возможности.

## Ключевые возможности плагина {#features}

- Подключение к {{ ydb-name }} со всеми способами [аутентификации](../../security/authentication.md): анонимная, статическая, по токену, по сервисному аккаунту, по метаданным.
- Иерархический навигатор объектов: таблицы ([строковые](../../concepts/glossary.md#row-oriented-table) и [колоночные](../../concepts/glossary.md#column-oriented-table)), [топики](../../concepts/datamodel/topic.md), [представления](../../concepts/datamodel/view.md), [внешние источники данных](../../concepts/glossary.md#external-data-source), [внешние таблицы](../../concepts/glossary.md#external-table), [трансферы](../../concepts/transfer.md), [потоковые запросы](../../concepts/glossary.md#streaming-query).
- Системные объекты: [системные представления](../../dev/system-views.md) (`.sys`), [пулы ресурсов](../../concepts/glossary.md#resource-pool).
- Редактор [YQL](../../concepts/glossary.md#yql) с подсветкой синтаксиса, автодополнением таблиц и колонок.
- Выполнение запросов и визуализация результатов: таблица, JSON, диаграмма.
- Визуализация [плана выполнения запроса](../../dev/query-execution-optimization/query-plans-optimization.md) (`EXPLAIN`).
- Мониторинг активных сессий через [`.sys/query_sessions`](../../dev/system-views.md#query-sessions).
- Дашборд кластера на базе [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md): загрузка CPU, использование памяти, сетевой трафик (обновление каждые 10 секунд).
- Управление [правами доступа (ACL)](../../security/authorization.md#right): просмотр разрешений на объекты базы данных.
- Генерация DDL-скриптов ([`CREATE`](../../yql/reference/syntax/create_table/index.md)) для любого объекта базы данных.
- Управление [потоковыми запросами](../../concepts/glossary.md#streaming-query): просмотр, запуск, остановка.
- [Конвертер SQL-запросов](../sql-dialect-converter.md) из других диалектов (PostgreSQL, MySQL, ClickHouse и других) в YQL.
- Встроенный [MCP-сервер](#mcp) — прямой доступ к базам данных из AI-ассистентов (Claude Code и других).
- [Семантический поиск по документации YQL](#rag) (RAG) для AI-assisted написания запросов.

## Требования {#requirements}

Для работы плагина требуется Visual Studio Code версии 1.75.0 или новее.

## Установка плагина {#installation}

Плагин можно установить из VS Code Marketplace или из `.vsix`-файла на странице GitHub Releases.

### Установка из VS Code Marketplace {#install-marketplace}

1. Откройте панель расширений в VS Code (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. В строке поиска введите `YDB for VS Code` и выберите расширение от издателя `ydb-tech` ([прямая ссылка](https://marketplace.visualstudio.com/items?itemName=ydb-tech.ydb-vscode-plugin)).
1. Нажмите **Install**.

Альтернативно, установить плагин из Marketplace можно одной командой в терминале:

```bash
code --install-extension ydb-tech.ydb-vscode-plugin
```

### Установка из VSIX-файла {#install-vsix}

Способ подходит, если нужна конкретная версия или нет доступа к Marketplace.

1. Перейдите на [страницу GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases) и скачайте файл `ydb-vscode-plugin-*.vsix` нужной версии.

1. Установите расширение одним из способов:

    - **Через терминал:**

        ```bash
        code --install-extension ydb-vscode-plugin-X.X.X.vsix
        ```

        Где `X.X.X` — номер скачанной версии.

    - **Через интерфейс VS Code:**

        1. Откройте панель расширений (`Ctrl+Shift+X` / `Cmd+Shift+X`).
        1. Нажмите `...` (три точки) в правом верхнем углу панели.
        1. Выберите **Install from VSIX...**.
        1. Укажите путь к скачанному `.vsix`-файлу.

После установки любым из способов перезагрузите VS Code. В панели Activity Bar появится значок **YDB**.

## Создание подключения к {{ ydb-name }} {#connection}

1. Нажмите на значок **YDB** в Activity Bar слева.
1. В панели **Connections** нажмите кнопку **Add Connection** (значок `+`).
1. Откроется форма создания подключения. Заполните поля:

    | Поле | Описание | Пример |
    |------|----------|--------|
    | **Connection Name** | Произвольное имя подключения | `my-ydb` |
    | **Host** | Хост [эндпойнта](../../concepts/connect.md#endpoint) кластера {{ ydb-name }} | `ydb.example.com` |
    | **Port** | Порт (по умолчанию `2135`) | `2135` |
    | **Database** | Путь к [базе данных](../../concepts/glossary.md#database) | `/Root/database` |
    | **Monitoring URL** | URL [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md), используется для дашборда (заполняется автоматически по хосту, можно переопределить) | `http://ydb.example.com:8765` |
    | **Secure connection (grpcs)** | Использовать защищённое соединение (`grpcs://`) | ☑ |
    | **Use RAG** | Включить [поиск по документации YQL](#rag) для данного подключения | ☑ |

1. При необходимости укажите путь к пользовательскому CA-сертификату (PEM) в поле **CA Certificate File** — для подключений с нестандартным TLS. Если поле не заполнено, используется встроенный сертификат Yandex Cloud.

1. Выберите способ аутентификации в выпадающем списке **Auth type** (см. [Способы аутентификации](#auth-methods)).

1. Нажмите **Test Connection** для проверки настроек. При успешном подключении появится сообщение об успехе.

1. Нажмите **Save**. Подключение появится в панели **Connections**.

## Способы аутентификации {#auth-methods}

Плагин поддерживает все способы [аутентификации](../../security/authentication.md), доступные в {{ ydb-short-name }}. Способ выбирается в выпадающем списке **Auth type** на форме создания подключения.

### Anonymous {#auth-anonymous}

Подключение без учётных данных. Используется для локальных или тестовых установок {{ ydb-short-name }}. Дополнительных полей заполнять не требуется.

### Static Credentials (логин и пароль) {#auth-static}

Аутентификация по логину и паролю. Укажите имя пользователя в поле **Username** и пароль в поле **Password**. Используется, если на сервере {{ ydb-short-name }} включена [аутентификация по логину и паролю](../../security/authentication.md#static-credentials).

{% note info %}

В managed-инсталляциях {{ ydb-name }} аутентификация по логину и паролю отключена: управляемые сервисы используют централизованную систему управления доступом облачной платформы ([IAM](https://yandex.cloud/ru/docs/iam/)).

{% endnote %}

### Access Token {#auth-token}

Аутентификация по [IAM-](https://yandex.cloud/ru/docs/iam/concepts/authorization/iam-token) или [OAuth-токену](https://yandex.cloud/ru/docs/iam/concepts/authorization/oauth-token). Введите токен в поле **Token**. Токен передаётся в заголовке каждого запроса.

{% note warning %}

IAM-токен имеет ограниченный [срок жизни — не более 12 часов](https://yandex.cloud/ru/docs/iam/concepts/authorization/iam-token#lifetime), после чего его необходимо получить заново и обновить в настройках подключения. Для долгоживущих подключений используйте аутентификацию по [сервисному аккаунту](#auth-service-account) или [сервису метаданных](#auth-metadata).

{% endnote %}

### Service Account Key File {#auth-service-account}

Аутентификация по ключу [сервисного аккаунта](https://yandex.cloud/ru/docs/iam/concepts/users/service-accounts) Yandex Cloud. Укажите путь к JSON-файлу с ключом в поле **Service Account Key File** (для выбора файла используйте кнопку **Browse**). Подробнее о том, как создать авторизованный ключ, см. в [документации Yandex Cloud](https://yandex.cloud/ru/docs/iam/operations/authentication/manage-authorized-keys).

Формат файла ключа:

```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

### Metadata Service {#auth-metadata}

Аутентификация через [сервис метаданных Yandex Cloud](https://yandex.cloud/ru/docs/compute/operations/vm-metadata/get-vm-metadata). Плагин получает IAM-токен от сервиса метаданных виртуальной машины. Используется, только когда VS Code запущен на виртуальной машине Yandex Cloud.

## Навигатор объектов {#object-navigator}

После подключения кликните на подключение в панели **Connections** — откроется панель **Navigator** с иерархией объектов {{ ydb-short-name }}. Навигатор содержит следующие разделы:

- **Tables** — таблицы (строковые и колоночные), организованные по поддиректориям согласно пути в {{ ydb-short-name }}.
- **System Views** — [системные представления](../../dev/system-views.md) (`.sys`), такие как `query_sessions`, `partition_stats`.
- **Views** — [представления](../../concepts/datamodel/view.md).
- **Topics** — [топики](../../concepts/datamodel/topic.md).
- **External Data Sources** — [внешние источники данных](../../concepts/glossary.md#external-data-source).
- **External Tables** — [внешние таблицы](../../concepts/glossary.md#external-table).
- **Resource Pools** — [пулы ресурсов](../../concepts/glossary.md#resource-pool).
- **Transfers** — трансферы данных.
- **Streaming Queries** — [потоковые запросы](../../concepts/glossary.md#streaming-query).

Правый клик на любом объекте в навигаторе открывает контекстное меню с доступными действиями.

## Работа с плагином {#capabilities}

### Рабочее пространство запросов {#query-workspace}

Откройте рабочее пространство запросов через `Ctrl+Shift+Q` (`Cmd+Shift+Q` на macOS) или нажмите **Open Query Workspace** в контекстном меню подключения. В рабочем пространстве можно писать и выполнять YQL-запросы, просматривать историю и результаты.

Для быстрого открытия редактора с предзаполненным запросом кликните правой кнопкой по таблице или представлению в навигаторе и выберите:

- **Show Preview** — `SELECT` первых 100 строк.
- **Make Query** — `SELECT` с именем объекта.

### Редактор YQL {#yql-editor}

Редактор поддерживает:

- Подсветку синтаксиса [YQL](../../yql/reference/index.md): ключевые слова, типы данных, встроенные функции.
- Автодополнение имён таблиц и колонок (по схеме подключённой базы данных).
- Выполнение запроса: `Ctrl+Enter` (`Cmd+Enter` на macOS).

Пример YQL-запроса:

```yql
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

Результаты выполнения отображаются в панели **Results** в виде таблицы, JSON или диаграммы (переключение вкладками).

### EXPLAIN и план выполнения {#explain}

Выберите **Explain YQL Query** в контекстном меню редактора или в командной палитре (`Ctrl+Shift+P`), чтобы получить [план выполнения запроса](../../dev/query-execution-optimization/query-plans-optimization.md). Плагин отображает дерево операций плана в текстовом виде.

### Менеджер сессий {#session-manager}

В панели **Sessions** (Activity Bar → YDB) отображаются все активные сессии с текущим запросом, состоянием и длительностью (данные из системного представления [`.sys/query_sessions`](../../dev/system-views.md#query-sessions)). Кнопка **Toggle Hide Idle** скрывает сессии без активного запроса.

### Дашборд кластера {#cluster-dashboard}

В панели **Database Load** (Activity Bar → YDB) отображается нагрузка на кластер в реальном времени (обновление каждые 10 секунд):

- Загрузка CPU (% и количество ядер).
- Использование памяти (% и объём).
- Сетевой трафик.

{% note warning %}

Дашборд доступен только при работе с self-hosted инсталляциями {{ ydb-short-name }}, где есть доступ к [{{ ydb-short-name }} Embedded UI](../../reference/embedded-ui/index.md). В Yandex Cloud Managed Service for {{ ydb-short-name }} Embedded UI не публикуется, поэтому данные дашборда недоступны — для мониторинга используйте [средства облачной платформы](https://yandex.cloud/ru/docs/ydb/operations/monitoring).

{% endnote %}

### Потоковые запросы {#streaming-queries}

В навигаторе раскройте раздел **Streaming Queries**. Для каждого запроса доступны:

- Просмотр исходного YQL (**View Source**).
- Запуск и остановка (**Start** / **Stop**).

### Конвертер SQL-диалектов {#convert-dialect}

Плагин позволяет преобразовать SQL-запрос, написанный на другом диалекте (PostgreSQL, MySQL, ClickHouse и других), в YQL. Конвертер доступен в панели **Convert Dialect** на Activity Bar.

Чтобы преобразовать запрос:

1. В выпадающем списке **Source Dialect** выберите исходный диалект SQL.
1. Вставьте исходный SQL-код в поле **Input SQL**.
1. Нажмите **Convert**. Результат появится в нижнем поле.
1. Скопируйте результат для использования в редакторе YQL.

Подробнее о принципах работы конвертера, поддерживаемых диалектах и ограничениях см. в статье [Конвертер SQL-диалектов в YQL](../sql-dialect-converter.md).

{% note warning %}

Для преобразования плагин отправляет исходный запрос на внешний HTTPS-сервис. Конвертер не работает без доступа в интернет. Не используйте конвертер для запросов, содержащих конфиденциальные данные.

{% endnote %}

### Просмотр разрешений {#permissions}

Кликните правой кнопкой по объекту в навигаторе и выберите **View Permissions** для просмотра [прав доступа (ACL)](../../security/authorization.md#right), назначенных на этот объект.

### Генерация DDL {#ddl}

Кликните правой кнопкой по таблице, топику или другому объекту в навигаторе и выберите **Create DDL** для получения `CREATE`-скрипта объекта.

### Создание объектов {#create-objects}

Кликните правой кнопкой по соответствующей папке в навигаторе, чтобы создать новый объект:

- **New Row Table** / **New Column Table** — создать строковую или колоночную таблицу.
- **New Topic** — создать топик.
- **New View** — создать представление.
- **New Object Storage Data Source** / **New YDB Data Source** — создать внешний источник данных.
- **New CSV/JSON/Parquet External Table** — создать внешнюю таблицу.
- **New Transfer** — создать трансфер.
- **New Streaming Query** — создать потоковый запрос.

## Интеграция с AI-ассистентами (MCP) {#mcp}

Плагин запускает встроенный [MCP-сервер](https://modelcontextprotocol.io/) (Model Context Protocol), который позволяет AI-ассистентам (Claude Code и другим) напрямую выполнять запросы к базам данных {{ ydb-short-name }}, настроенным в плагине.

### Настройка порта {#mcp-port}

Сервер запускается на порту **3333** (только localhost) по умолчанию. Изменить порт можно в настройках VS Code:

```json
{
  "ydb.mcpPort": 3333
}
```

Если порт уже занят, расширение покажет предупреждение и продолжит работу без MCP.

### Подключение Claude Code {#mcp-claude}

1. Убедитесь, что расширение YDB запущено в VS Code и в панели **Connections** добавлено хотя бы одно подключение (см. [Создание подключения](#connection)).

1. Зарегистрируйте MCP-сервер в Claude Code глобально для текущего пользователя:

    ```bash
    claude mcp add --scope user --transport sse ydb http://localhost:3333/sse
    ```

    Флаг `--scope user` сохраняет конфигурацию глобально — сервер будет доступен из любого каталога, в котором запускается Claude Code. Без этого флага используется [scope `local`](https://docs.claude.com/en/docs/claude-code/mcp#mcp-installation-scopes) (по умолчанию), и регистрацию придётся повторять для каждого проекта.

1. Проверьте подключение:

    ```bash
    claude mcp list
    ```

### Доступные MCP-инструменты {#mcp-tools}

| Инструмент | Параметры | Описание |
|------------|-----------|----------|
| `ydb_list_connections` | — | Список всех подключений, настроенных в плагине |
| `ydb_query` | `connection`, `sql` | Выполнить YQL-запрос |
| `ydb_describe_table` | `connection`, `path` | Получить схему таблицы (колонки, первичный ключ) |
| `ydb_list_directory` | `connection`, `path?` | Список объектов в директории базы данных |
| `ydb_list_all` | `connection`, `path?`, `limit?`, `offset?` | Рекурсивный список всех объектов |
| `ydb_yql_help` | `query`, `connection?` | Поиск по документации YQL (требует включённого [RAG](#rag)) |

Параметр `connection` — имя подключения, как оно отображается в панели **Connections**.

## Поиск по документации YQL (RAG) {#rag}

Плагин поддерживает семантический и ключевой поиск по документации YQL с помощью встроенного RAG-индекса (Retrieval-Augmented Generation). Это позволяет AI-ассистентам (через инструмент `ydb_yql_help`) находить релевантные фрагменты синтаксического справочника при написании запросов.

### Включение RAG {#rag-enable}

1. При создании или редактировании подключения установите флаг **Use RAG**.
1. Плагин автоматически определит версию {{ ydb-short-name }} и загрузит соответствующий индекс из облака при первом подключении.

### Режимы поиска {#rag-modes}

- **Ключевой поиск (keyword)** — работает без дополнительных зависимостей, используется по умолчанию.
- **Семантический поиск (vector)** — требует локально запущенного [Ollama](https://ollama.com) с моделью эмбеддингов [`nomic-embed-text`](https://ollama.com/library/nomic-embed-text). См. [Установка Ollama и модели эмбеддингов](#install-ollama).

Статус RAG (Running / Not running) и статус Ollama отображаются прямо в форме подключения.

### Установка Ollama и модели эмбеддингов {#install-ollama}

[Ollama](https://ollama.com) — локальный сервер для запуска языковых моделей и моделей эмбеддингов. Плагин обращается к нему по HTTP, чтобы преобразовывать тексты документации и поисковые запросы в векторы для семантического поиска.

1. Установите Ollama по инструкции на [официальной странице загрузки](https://ollama.com/download) (поддерживаются macOS, Windows и Linux).

1. Убедитесь, что сервис Ollama запущен и доступен на `http://localhost:11434`:

    ```bash
    curl http://localhost:11434/api/tags
    ```

    Ответ в формате JSON со списком моделей означает, что сервис работает.

1. Скачайте модель эмбеддингов `nomic-embed-text` (~270 МБ):

    ```bash
    ollama pull nomic-embed-text
    ```

1. Убедитесь, что модель доступна:

    ```bash
    ollama list
    ```

    В выводе должна появиться строка `nomic-embed-text`.

1. Откройте форму подключения в плагине и убедитесь, что статус Ollama отображается как **Running**. Если статус **Not running** — проверьте, что сервис Ollama запущен и доступен на `http://localhost:11434`.

{% note info %}

URL Ollama (`http://localhost:11434`) и имя модели (`nomic-embed-text`) уже заданы в плагине по умолчанию — переопределять настройки `ydb.ragOllamaUrl` и `ydb.ragOllamaModel` нужно только при нестандартной установке.

Использовать другую модель эмбеддингов нельзя: индексы, публикуемые плагином в облаке, построены именно на `nomic-embed-text`, и при смене модели векторы запроса и документации окажутся несовместимы.

{% endnote %}

## Обновление плагина {#updates}

Способ обновления зависит от того, как был установлен плагин.

### Обновление из VS Code Marketplace {#update-marketplace}

Если плагин установлен из [Marketplace](#install-marketplace), VS Code обновляет его автоматически.

Чтобы обновить плагин вручную:

1. Откройте панель расширений (`Ctrl+Shift+X` / `Cmd+Shift+X`).
1. В строке поиска введите `YDB for VS Code` и откройте установленное расширение.
1. Если доступна новая версия, появится кнопка **Update** — нажмите её.

### Обновление из VSIX-файла {#update-vsix}

Если плагин установлен из [VSIX-файла](#install-vsix), автоматическое обновление недоступно — VS Code не отслеживает обновления для расширений, установленных вручную.

1. Скачайте новый `.vsix`-файл со страницы [GitHub Releases](https://github.com/ydb-platform/ydb-vscode-plugin/releases).
1. Установите его поверх старой версии тем же способом, что и при первой установке — VS Code автоматически заменит предыдущую версию.
