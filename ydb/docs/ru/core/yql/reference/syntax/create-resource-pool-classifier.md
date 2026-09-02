# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` создаёт [пул классификаторов ресурсов](../../../concepts/glossary.md#resource-pool-classifier.md).

## Синтаксис

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

- `name` — имя создаваемого классификатора пула ресурсов. Должно быть уникальным. Имя не должно содержать символы, запрещённые для схемных объектов.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — позволяет задавать значения параметров, определяющих поведение классификатора пула ресурсов.

### Общие параметры

* `RANK` (Int64) — опциональное поле, задающее порядок выбора классификатора пула ресурсов. Если значение не указано, берётся максимальный существующий `RANK` и к нему прибавляется 1000. Допустимые значения: уникальное число в диапазоне $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — имя пула ресурсов, в который направляются запросы, удовлетворяющие предикатам классификатора.
* `ACTION` (Enum) — действие, применяемое к запросу при срабатывании классификатора. Допустимое значение — `reject`: запрос отклоняется, пользователь получает ошибку вида `Request is rejected by classifier '<name>' (rank=<rank>)`.

{% note info %}

Параметры `RESOURCE_POOL` и `ACTION` взаимоисключающие: в классификаторе должен быть указан ровно один из них.

{% endnote %}

### Параметры-предикаты

Предикат — условие, проверяемое для входящего запроса. Классификатор срабатывает, если выполнены **все** его предикаты (логическое **AND**). Для реализации логики **OR** необходимо создать несколько классификаторов с разными значениями `RANK`. Классификаторы обрабатываются в порядке возрастания `RANK`; обработка останавливается на первом сработавшем — к запросу применяется его `ACTION` или он направляется в его `RESOURCE_POOL`.

Список предикатов:

* `MEMBER_NAME` (String) — SID пользователя или группы, от имени которых поступил запрос. Подробнее — [ниже](#member-name).
* `HAS_PATH` (String) — путь к объекту YDB, к которому обращается запрос; поддерживает wildcard `*`. Подробнее — [ниже](#has-path).
* `HAS_APP_NAME` (String) — идентификатор клиентского приложения. Подробнее — [ниже](#has-app-name).
* `HAS_FULL_SCAN` (String) — путь к объекту, по которому ожидается полное сканирование; поддерживает wildcard `*`. Подробнее — [ниже](#has-full-scan).
* `HAS_STREAM` (Bool) — признак стримингового запроса. Подробнее — [ниже](#has-stream).

#### MEMBER_NAME {#member-name}

`MEMBER_NAME` сравнивается посимвольно с [SID](../../../concepts/glossary.md#access-sid) пользователя или любым SID группы из его токена аутентификации. Формат SID зависит от того, каким способом пользователь пришёл в систему.

- **Встроенные пользователи {{ ydb-short-name }} (логин/пароль)** — SID совпадает с именем пользователя, без суффикса. Например, `user1`. Подробнее — [{#T}](../../../security/authentication.md#static-credentials).
- **Облачные пользователи (Access Service)** — SID имеет вид `<subject_id>@as`, где `<subject_id>` — идентификатор пользователя в IAM. Суффикс задаётся параметром [`access_service_domain`](../../../reference/configuration/auth_config.md#iam-auth-config) (по умолчанию `as`). Например, `ajeb89hv69nujke769fa@as`. Подробнее — [{#T}](../../../security/authentication.md#iam).
- **LDAP** — SID имеет вид `<логин>@<домен>`, где домен задаётся параметром [`ldap_authentication_domain`](../../../reference/configuration/auth_config.md#ldap-auth-config) (по умолчанию `ldap`). Например, `user1@ldap`. Подробнее — [{#T}](../../../security/authentication.md#ldap).
- **Внешние провайдеры идентификации (OIDC)** — SID имеет вид `<логин>@<домен>`, где домен задаётся параметром `external_idp_authentication_domain` в [конфигурации аутентификации](../../../reference/configuration/auth_config.md) (по умолчанию `sso`). Например, `user1@sso`.

В качестве значения можно указать как SID конкретного пользователя, так и SID группы. Ко всем аутентифицированным пользователям автоматически добавляется группа `all-users@well-known` — её удобно использовать, если нужно направить в пул запросы от всех аутентифицированных клиентов.

**Пример.** Направить запросы пользователя `user1@ldap` в пул `olap`:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_user WITH (
    RANK=100,
    RESOURCE_POOL='olap',
    MEMBER_NAME='user1@ldap'
);
```

#### HAS_PATH {#has-path}

`HAS_PATH` сравнивает пути объектов YDB, к которым обращается запрос, с указанной маской. Маска поддерживает wildcard `*`, соответствующий любой последовательности символов в пути. Предикат срабатывает, если хотя бы один объект в плане запроса соответствует маске.

**Пример.** Направить запросы к архивным таблицам в пул `pool_archive`:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_archive WITH (
    RANK=700,
    RESOURCE_POOL='pool_archive',
    HAS_PATH='/Root/db/archive/*'
);
```

#### HAS_APP_NAME {#has-app-name}

`HAS_APP_NAME` сравнивает значение с идентификатором клиентского приложения. Значение задаётся клиентом через заголовок `x-ydb-application-name` при создании сессии; сравнение — на точное совпадение (без wildcard).

{% note warning %}

Значение `HAS_APP_NAME` задаётся клиентом и не аутентифицируется сервером — не используйте его как средство контроля доступа. Для эффективной изоляции сочетайте с `MEMBER_NAME` или направляйте нераспознанные запросы в sandbox-пул с жёсткими лимитами.

{% endnote %}

**Пример.** Направить запросы от Embedded UI в пул `pool_adhoc`:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_adhoc_ui WITH (
    RANK=200,
    RESOURCE_POOL='pool_adhoc',
    HAS_APP_NAME='ydb-ui'
);
```

#### HAS_FULL_SCAN {#has-full-scan}

`HAS_FULL_SCAN` определяет запросы, содержащие полное сканирование указанных объектов. Полное сканирование — чтение таблицы без ограничения по ключу или диапазону ключей. Аргумент — маска пути к объекту с поддержкой wildcard `*`; предикат срабатывает, если в плане запроса есть хотя бы один такой объект. Поддерживаются как row-store (OLTP), так и column-store (OLAP) таблицы. Объекты, к которым понятие full scan неприменимо (например, топики), не учитываются.

**Особенности определения полного сканирования**

- **`LIMIT` не отменяет полное сканирование.** Без условия по ключу формируется физический план, обрабатывающий всю таблицу целиком; `LIMIT` ограничивает только размер результата.
- **Вторичные индексы.** Полное сканирование индексной таблицы вторичного индекса засчитывается по её собственному пути. Например, у таблицы

    ```yql
    CREATE TABLE orders (
        Id Uint64 NOT NULL,
        Status Utf8,
        PRIMARY KEY (Id),
        INDEX by_status GLOBAL ON (Status)
    );
    ```

    индексная таблица имеет путь `/Root/orders/by_status/indexImplTable`. Запрос `SELECT * FROM orders VIEW by_status` выполняет полное сканирование именно индексной таблицы — основная `/Root/orders` не сканируется. Поэтому:

    - `HAS_FULL_SCAN='/Root/orders'` — **не сработает**;
    - `HAS_FULL_SCAN='/Root/orders/by_status/indexImplTable'` или `HAS_FULL_SCAN='/Root/orders/*'` — **сработает**.

**Пример.** Отклонять запросы, вызывающие полное сканирование архива заказов:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_fullscan_reject WITH (
    RANK=100,
    ACTION='reject',
    HAS_FULL_SCAN='/Root/db/orders_archive/*'
);
```

#### HAS_STREAM {#has-stream}

`HAS_STREAM` определяет, является ли запрос [стриминговым](create-streaming-query.md) — то есть выполняет длительное непрерывное чтение и/или запись из потоков данных. Допустимые значения:

- `true` — классификатор срабатывает на стриминговых запросах;
- `false` — классификатор срабатывает на нестриминговых запросах.

**Пример.** Направить стриминговые запросы в пул `pool_stream`:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_stream WITH (
    RANK=500,
    RESOURCE_POOL='pool_stream',
    HAS_STREAM=true
);
```

## Замечания {#remarks}

Если в DDL для создания классификатора пула ресурсов не указан `RANK`, то по умолчанию ему будет присвоено значение $RANK = MAX(existing\_ranks) + 1000$. Все значения `RANK` должны быть уникальными, чтобы обеспечить строго детерминированный порядок выбора пула ресурсов в случае конфликтующих условий. Такое поведение выбрано для возможности добавлять новые классификаторы пулов ресурсов между уже существующими.

Также возможно наличие классификатора, который ссылается на несуществующий пул ресурсов или к которому у пользователя нет доступа. В таком случае такие классификаторы будут пропускаться.

С ограничениями на число классификаторов можно ознакомиться на странице [ограничений](../../../../concepts/limits-ydb#resource_pool).

## Разрешения

Требуется [разрешение](./grant.md#permissions-list) `USE` на базу данных.

Пример выдачи такого разрешения:

```yql
GRANT 'USE' ON `/my_db` TO `user1@domain`;
```

## Примеры {#examples}

Ниже — сводный пример, комбинирующий несколько классификаторов и предикатов: отклонение полных сканов архивных таблиц, изоляция стриминговых запросов и выделение пула под интерактивные запросы админа из Embedded UI.

*Создание ресурсных пулов*

```yql
CREATE RESOURCE POOL pool_stream WITH (
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=20
);

CREATE RESOURCE POOL pool_adhoc_admin WITH (
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=10
);
```

*Создание классификаторов*

```yql
-- Отклонять полные сканы архивных таблиц.
CREATE RESOURCE POOL CLASSIFIER cl_fullscan_reject WITH (
    RANK=100,
    ACTION='reject',
    HAS_FULL_SCAN='/Root/db/orders_archive/*'
);

-- Стриминговые запросы направлять в выделенный пул.
CREATE RESOURCE POOL CLASSIFIER cl_stream WITH (
    RANK=200,
    RESOURCE_POOL='pool_stream',
    HAS_STREAM=true
);

-- Запросы админа из Embedded UI — в пул интерактивных запросов.
-- Условие AND: и MEMBER_NAME, и HAS_APP_NAME должны совпасть.
CREATE RESOURCE POOL CLASSIFIER cl_adhoc_admin WITH (
    RANK=300,
    RESOURCE_POOL='pool_adhoc_admin',
    MEMBER_NAME='admin',
    HAS_APP_NAME='ydb-ui'
);
```

Классификаторы обрабатываются в порядке возрастания `RANK`; для запроса срабатывает первый подходящий. Запрос, не удовлетворяющий ни одному классификатору, направляется в пул `default`.

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
