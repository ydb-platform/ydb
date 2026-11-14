# Аутентификация OIDC в CLI {#oidc}

{{ ydb-short-name }} CLI использует [OIDC-провайдер C++ SDK](../ydb-sdk/auth.md#oidc) для получения токенов у внешнего провайдера идентификации (IdP). Это отдельный от {{ ydb-short-name }} сервис, который проверяет учётные данные пользователя или приложения и выдаёт токены. В терминологии OIDC такой сервис выполняет роль [провайдера OpenID](https://openid.net/specs/openid-connect-core-1_0.html#Terminology). Перед подключением выполните [предварительную настройку IdP и сервера](../ydb-sdk/auth.md#oidc-prerequisites). В справочнике SDK описаны [участники обмена и токены](../ydb-sdk/auth.md#oidc-participants), [обнаружение адресов IdP](../ydb-sdk/auth.md#oidc-discovery), [обновление токенов](../ydb-sdk/auth.md#oidc-refresh) и [обработка ошибок](../ydb-sdk/auth.md#oidc-errors).

Ниже приведены настройки CLI; [примеры команд, файлов конфигурации и профилей](#examples) собраны в конце статьи. После настройки проверьте вход [командой](commands/discovery-whoami.md) `discovery whoami`.

## Выбор режима {#oidc-flows}

**Идентификатор приложения** (`client_id`) — значение, которое IdP присваивает зарегистрированному приложению. Оно позволяет определить приложение, запрашивающее токен, и само по себе не является секретом. См. [RFC 6749, раздел 2.2](https://www.rfc-editor.org/rfc/rfc6749.html#section-2.2).

**Области доступа** (`scope`) — именованные разрешения, которые приложение запрашивает у IdP при получении токена. Допустимые значения и их смысл определяет IdP; фактически выданные разрешения могут отличаться от запрошенных. См. [RFC 6749, раздел 3.3](https://www.rfc-editor.org/rfc/rfc6749.html#section-3.3). Права на объекты базы данных настраиваются отдельно на стороне {{ ydb-short-name }}.

**Общие предварительные условия:**

- Узнайте HTTPS-адрес издателя токенов у администратора IdP. Параметр `--oidc-issuer` обязателен для всех трёх режимов, включая готовый токен.
- Для `device` и `client` обеспечьте доступ из окружения запуска CLI к [Discovery-документу и службам IdP](../ydb-sdk/auth.md#oidc-discovery) по HTTPS. В режиме `static` CLI не обращается к IdP: получение и замену токена обеспечивает пользователь или внешний инструмент.
- Для запросов к базе укажите её эндпоинт и путь параметрами `-e` и `-d` либо в профиле. На сервере должны быть настроены [проверка токенов внешнего IdP](../configuration/auth_config.md#external-idp-auth-config) и [права доступа](../../security/authorization.md) для пользователя или приложения. Выданный токен должен соответствовать [требованиям сервера](../../security/authentication.md#token-and-key-requirements).

Выберите режим параметром `--oidc-flow`. Ниже перечислены его обязательные параметры; эквиваленты в переменных окружения приведены в [таблице параметров](#oidc-options), а структура отдельного файла — в [описании конфигурации](#oidc-config-file):

| Режим | Что указать | Когда использовать |
| --- | --- | --- |
| `device` | `--oidc-client-id` | Для входа пользователя по коду устройства. [Описание режима](../ydb-sdk/auth.md#oidc-device-flow). |
| `client` | `--oidc-client-id` и `--oidc-client-secret-file` либо `YDB_OIDC_CLIENT_SECRET` | Для запуска CLI из скриптов без участия пользователя с использованием секрета приложения. [Описание режима и настройки IdP](../ydb-sdk/auth.md#oidc-client-flow). |
| `static` | `--oidc-access-token-file` либо `YDB_OIDC_ACCESS_TOKEN` | Для уже полученного токена без автоматического обновления. [Описание режима](../ydb-sdk/auth.md#oidc-static-example). |

Если режим не указан, файл готового токена или непустая переменная `YDB_OIDC_ACCESS_TOKEN` выбирает `static`, иначе выбирается `device`. Наличие секрета приложения не выбирает `client`: задайте этот режим явно. В режиме `static` нельзя задавать идентификатор приложения, секрет и области доступа; в `device` нельзя передавать секрет или готовый токен; в `client` нельзя передавать готовый токен.

### Вход пользователя по коду устройства {#device}

- **Когда использовать:** для работы от имени пользователя, например запуска команд администратором или работы аналитика в интерактивной SQL-оболочке. Пользователь должен иметь возможность подтвердить вход в браузере. Для скриптов, которые должны работать без участия человека, выберите `client` или заранее подготовленный токен в `static`.
- **Что подготовить:** учётную запись пользователя у IdP и зарегистрированное приложение, которому разрешён [вход по коду устройства](../ydb-sdk/auth.md#oidc-device-flow) без секрета. Браузер может быть открыт на другом устройстве, но должен иметь доступ к странице входа IdP.
- **Что задать:** `--oidc-flow device`, `--oidc-issuer` и `--oidc-client-id`. Дополнительные области доступа передавайте через `--oidc-scope`; запрашивайте только разрешённые для приложения области. Для сохранения сессии между запусками задайте `--oidc-cache-path` и заранее подготовьте [каталог кеша](#oidc-cache).

В режиме `device` CLI выводит ссылку и код в стандартный поток ошибок и ожидает подтверждения входа. Браузер нужно открыть самостоятельно, в том числе на другом устройстве. При отказе или истечении срока действия кода повторите команду. Интерактивная SQL-оболочка также ожидает первоначального входа; `Ctrl+C` прерывает ожидание. Перенаправление ввода или вывода и параметры `--assume-yes` и `auth get-token --force` не заменяют подтверждение пользователя.

**Токен обновления** (`refresh_token`) — учётные данные для получения нового токена доступа у IdP без повторного входа пользователя. Он передаётся только IdP, а не серверу {{ ydb-short-name }}. Определение приведено в [RFC 6749, раздел 1.5](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.5), порядок обмена — в [разделе 6](https://www.rfc-editor.org/rfc/rfc6749.html#section-6). Выдачу токена определяет IdP. Поведение этой области описано в [OpenID Connect Core, раздел 11](https://openid.net/specs/openid-connect-core-1_0.html#OfflineAccess).

Чтобы сохранить вход между запусками, используйте [файловый кеш токенов](#oidc-cache). Без кеша каждый новый процесс начинает вход заново.

### Подключение без участия пользователя {#client}

- **Когда использовать:** для подключения от имени приложения без входа пользователя в браузере, например из CI/CD, скрипта резервного копирования или задания по расписанию. CLI самостоятельно получает и обновляет токен по [правилам SDK](../ydb-sdk/auth.md#oidc-refresh).
- **Что подготовить:** зарегистрируйте приложение в IdP, получите его идентификатор и секрет. Для него должны быть разрешены выдача токенов по секрету приложения (`client_credentials`) и проверка секрета способом `client_secret_basic`; их назначение и настройка описаны в [справочнике SDK](../ydb-sdk/auth.md#oidc-client-flow). Обеспечьте доступ к секрету только тому пользователю, от имени которого запускается CLI.
- **Что задать:** `--oidc-flow client`, `--oidc-issuer`, `--oidc-client-id` и путь к файлу секрета в `--oidc-client-secret-file`. Вместо файла можно передать сам секрет через `YDB_OIDC_CLIENT_SECRET`, например средствами хранения секретов CI/CD. При необходимости задайте согласованные с IdP области через `--oidc-scope`. Режим `client` нужно выбрать явно: наличие секрета не включает его автоматически.

См. вкладку «Секрет приложения (client)» в [примерах подключения](#connection-examples).

### Подключение с готовым токеном {#static}

- **Когда использовать:** если токен уже получен другим инструментом или передан окружением запуска, например для разовой команды или скрипта, которому внешний сервис выдаёт токен перед запуском. Для длительной работы нужно самостоятельно обеспечить замену токена после истечения срока действия.
- **Что подготовить:** действующий токен доступа, предназначенный для вашей базы данных, и адрес его издателя. Сохраните токен в файле с доступом только владельцу либо передайте его через окружение. Секрет приложения и вход в браузере со стороны CLI не требуются.
- **Что задать:** `--oidc-flow static`, `--oidc-issuer` и путь к файлу токена в `--oidc-access-token-file` либо сам токен в `YDB_OIDC_ACCESS_TOKEN`. Не задавайте идентификатор приложения, секрет или области доступа. Файловый кеш в этом режиме не используется.

В режиме `static` CLI использует токен, полученный заранее, и не обновляет его. CLI читает файл токена заново при каждом запуске, удаляет начальные и конечные пробельные символы, а также префикс `Bearer` с одним пробелом, если он есть. Для замены токена в работающем процессе перезапустите CLI. См. вкладку «Готовый токен (static)» в [примерах подключения](#connection-examples).

## Параметры и переменные окружения {#oidc-options}

Параметры OIDC указываются перед подкомандой, как и остальные [параметры соединения](connect.md#command-line-pars).

| Параметр | Переменная окружения | Назначение |
| --- | --- | --- |
| `--oidc-config PATH` | Нет | Путь к [файлу YAML или JSON](#oidc-config-file). |
| `--oidc-issuer URL` | `YDB_OIDC_ISSUER` | HTTPS-адрес издателя, обязательный для всех режимов. Должен точно совпадать с `issuer` в [Discovery-документе](../ydb-sdk/auth.md#oidc-discovery). |
| `--oidc-flow VALUE` | `YDB_OIDC_FLOW` | Режим: `static`, `client` или `device`. |
| `--oidc-client-id VALUE` | `YDB_OIDC_CLIENT_ID` | Идентификатор приложения для `client` и `device`. |
| `--oidc-client-secret-file PATH` | `YDB_OIDC_CLIENT_SECRET` | Файл секрета приложения для `client`; переменная содержит **сам секрет**, а не путь. |
| `--oidc-access-token-file PATH` | `YDB_OIDC_ACCESS_TOKEN` | Файл токена для `static`; переменная содержит **сам токен**, а не путь. |
| `--oidc-scope VALUE` | `YDB_OIDC_SCOPE` | Области доступа для `client` и `device`, разделённые пробелами. Параметр можно повторять. |
| `--oidc-cache-path PATH` | `YDB_OIDC_CACHE_PATH` | Путь к [кешу токенов](#oidc-cache). По умолчанию файловый кеш выключен. |

CLI объединяет области доступа из повторяющихся `--oidc-scope` и строк с пробелами. Повторяющиеся пробельные разделители игнорируются, пустое значение параметра отклоняется. Назначение областей, допустимые символы и автоматическое добавление `openid` описаны в [конфигурации SDK](../ydb-sdk/auth.md#oidc-configuration).

Параметров для передачи самого секрета или токена в командной строке нет. Переменная `YDB_OIDC_CONFIG` также не поддерживается: путь к конфигурации задаётся параметром или профилем.

## Приоритет настроек {#oidc-precedence}

Для OIDC действует [общий порядок выбора источников](connect.md), описанный в статье о подключении к базе данных. При выборе только по окружению OIDC проверяется после других поддерживаемых методов: например, `YDB_TOKEN` имеет приоритет над `YDB_OIDC_ISSUER`. Не совмещайте явно выбранный OIDC с другим методом аутентификации.

При переопределении `issuer` CLI отбрасывает связанные поля другого источника профиля, а при переопределении `client_id` не подставляет файл секрета из другого источника. Укажите полный набор настроек нового приложения. Недостающий секрет можно передать через окружение.

`--oidc-config` нельзя совмещать с явно заданными параметрами `--oidc-*`. Переменные окружения не переопределяют содержимое отдельного файла, но могут предоставить отсутствующий секрет или токен выбранного режима. Указанный в таком файле путь к секрету или токену всегда имеет приоритет над окружением.

Для прямых настроек файл из командной строки или явно выбранного профиля имеет приоритет над переменной с секретом, а окружение имеет приоритет над файлом из активированного профиля. Если выбранный файл пуст или недоступен, CLI возвращает ошибку, не подставляя значение из окружения. Файл невыбранной OIDC-конфигурации не читается.

## Файл конфигурации {#oidc-config-file}

Файл для `--oidc-config` содержит обязательный `issuer`, необязательный `cache_path` и ровно один блок получения учётных данных. YAML и JSON имеют одинаковую структуру.

| Поле или блок | Содержимое |
| --- | --- |
| `issuer` | Непустая строка с HTTPS-адресом издателя. |
| `cache_path` | Необязательный путь к кешу. В режиме `static` не используется. |
| `device_authorization_grant` | Обязательный `client_id`, необязательный список строк `scope`. |
| `client_credentials_grant` | Обязательный `client_id`, путь `client_secret_file` либо секрет из `YDB_OIDC_CLIENT_SECRET`, необязательный список строк `scope`. |
| `static_credentials` | Путь `access_token_file` либо токен из `YDB_OIDC_ACCESS_TOKEN`, необязательный `expires_at`. |

Относительные пути к секрету, токену и кешу разрешаются относительно каталога конфигурации. Путь к самому файлу конфигурации разрешается относительно рабочего каталога запуска. Символ `~` внутри YAML не раскрывается.

`expires_at` задаёт момент истечения срока действия готового токена в целых секундах Unix и имеет приоритет над `exp` из JWT. Нулевое значение означает истёкший токен. При отсутствии поля действуют [правила SDK](../ydb-sdk/auth.md#oidc-static-example). Если токен задан через окружение и срок не указан, блок `static_credentials` может быть пустым.

Неизвестные и повторяющиеся ключи, несколько блоков получения токена, пустые обязательные строки и неверные типы отклоняются. `scope` должен быть списком строк, по одной области в элементе; цифровой `client_id` заключите в кавычки. `expires_at` должен быть неотрицательным целым числом секунд Unix в поддерживаемом диапазоне. Сами значения `access_token` и `client_secret` в файле запрещены. Поля для `refresh_token`, `token_endpoint`, `audience`, тайм-аутов или отключения HTTPS не предусмотрены.

## Профили OIDC {#oidc-profiles}

Для сохранения настроек используйте [команды управления профилями](profile/create.md) `config profile create` или `config profile update`. Они проверяют локальную конфигурацию, но не получают токен у IdP. Первый вход происходит при выполнении команды, которой нужны учётные данные. Новый профиль выберите через `--profile` или активируйте [командой](profile/activate.md) `config profile activate`.

В блоке `authentication` профиля доступны два способа хранения настроек:

- `method: oidc-config`: поле `data` содержит путь к отдельному файлу OIDC.
- `method: oidc`: поле `data` содержит прямые параметры, включая `flow` и строку `scope` с областями через пробелы. В нём можно хранить пути `access_token_file` или `client_secret_file`, но не сами секреты.

Относительные пути в профиле разрешаются относительно рабочего каталога запуска, а не каталога файла профилей. Предпочитайте абсолютные пути. При создании и обновлении задавайте `issuer`, режим, `client_id`, области доступа и путь к кешу явно: эти настройки не копируются из окружения. Из окружения могут читаться секрет или токен; наличие `YDB_OIDC_ACCESS_TOKEN` также влияет на выбор `static`. Значение секрета в профиль не записывается.

Обновление аутентификации заменяет весь блок `authentication`: передавайте полный набор настроек. Параметр `--no-auth` нельзя совмещать с OIDC.

## Файловый кеш токенов {#oidc-cache}

Для сохранения токенов между запусками в режимах `client` и `device` задайте `--oidc-cache-path`, `cache_path` в конфигурации или `YDB_OIDC_CACHE_PATH`. Без этого токены хранятся только в памяти процесса. В режиме `static` файловый кеш не используется.

Заранее создайте родительский каталог с доступом только владельцу: CLI не создаёт его автоматически. Кеш содержит незашифрованные токены, поэтому исключите его из системы контроля версий и общедоступных резервных копий. CLI проверяет, что это обычный файл текущего пользователя с доступом только владельцу: в Unix проверяются права группы и остальных, в Windows — DACL, в macOS — также расширенные ACL. Для существующего файла в Unix обычно подходят права `600`. [Подготовка каталога кеша](#preparation).

Кеш привязан к издателю, режиму, идентификатору приложения, секрету и областям доступа. При изменении этих настроек используйте другой путь или удалите прежний кеш: несовместимый файл не используется и не перезаписывается. Для разных пользователей одного приложения задавайте отдельные файлы. Запись выполняется атомарно через временный файл, но межпроцессной блокировки нет; избегайте одновременного обновления одной сессии несколькими процессами.

Если кеш отсутствует, повреждён или несовместим, CLI получает токены заново. При проблемах доступа выводится предупреждение в стандартный поток ошибок. Ошибка записи не мешает использовать токен в текущем процессе, но следующий запуск может потребовать повторного входа. Удаление кеша не отзывает токены у IdP и не меняет токен работающего процесса. Правила использования сохранённых токенов и обновления описаны в [справочнике SDK](../ydb-sdk/auth.md#oidc-refresh).

## Получение токена для другого инструмента {#oidc-get-token}

[Команда](auth-get-token.md) `auth get-token` выводит строку `Bearer` с пробелом и токеном доступа. Токен обновления и `id_token` не выводятся. `--force` отключает подтверждение вывода секрета, но не вход у IdP; ссылка и код устройства идут в стандартный поток ошибок. Параметр `--timeout` не ограничивает ожидание входа по коду устройства.

Полученный файл можно использовать с `--oidc-access-token-file`. Если токен нужен другому инструменту, проверьте, ожидает ли тот всю строку с префиксом или только значение токена. Команды для всех режимов приведены в [разделе «Получение токена»](#token-examples).

## Примеры {#examples}

### Подготовка {#preparation}

В примерах используются условные адреса `grpcs://ydb.example.com:2135`, `/Root/mydb` и `https://idp.example.com/realms/ydb`. Укажите вместо них эндпоинт и путь вашей базы данных, а также точное значение `issuer` вашего IdP. Параметр `-e` задаёт эндпоинт, а `-d` задаёт путь базы данных. Идентификаторы приложений `ydb-cli` и `ydb-service` и разрешённые области доступа также должны соответствовать настройкам IdP.

Замените путь `/home/user/.config/ydb/` на путь к каталогу конфигурации в вашем окружении. Для хранения конфигурации, секретов и кеша создайте каталог с доступом только владельцу. Например, в Linux или macOS:

```bash
mkdir -p /home/user/.config/ydb
chmod 700 /home/user/.config/ydb
```

`mkdir -p` создаёт каталог, а `chmod 700` разрешает доступ к нему только владельцу. Значения секретов и токенов получите средствами вашего окружения; не помещайте их в общедоступные файлы. CLI не создаёт каталог кеша автоматически. Требования к его защите описаны в [разделе о файловом кеше](#oidc-cache).

### Получение токена {#token-examples}

Начните с получения токена отдельно от запроса к базе данных. Команды получения токена используют `umask 077`, чтобы новый файл был доступен только владельцу. Для уже существующего файла заранее проверьте права: `umask` их не меняет.

Если команда завершилась с ошибкой, проверьте параметры выбранного режима, доступность IdP и его настройки. Сообщения об ошибках и правила повторных попыток описаны в [справочнике SDK](../ydb-sdk/auth.md#oidc-errors).

{% list tabs %}

- Код устройства (device)

  Для приложения `ydb-cli` IdP должен разрешать вход по коду устройства. При необходимости подтвердите вход в браузере. Параметр `--force` разрешает вывод токена в стандартный поток вывода.

  Без профиля:

  ```bash
  umask 077
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow device \
    --oidc-client-id ydb-cli \
    --oidc-scope offline_access \
    auth get-token --force > /home/user/.config/ydb/exported-device-token.txt
  ```

  Если профиль `oidc-device` уже создан, используйте его. Создание профиля показано на вкладке «Код устройства (device)» в [примерах подключения](#connection-examples):

  ```bash
  umask 077
  {{ ydb-cli }} --profile oidc-device auth get-token --force > /home/user/.config/ydb/exported-device-token.txt
  ```

- Секрет приложения (client)

  Сохраните секрет зарегистрированного приложения `ydb-service` в файле (например, `/home/user/.config/ydb/client-secret.txt`), доступном только владельцу.

  Без профиля:

  ```bash
  umask 077
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow client \
    --oidc-client-id ydb-service \
    --oidc-client-secret-file /home/user/.config/ydb/client-secret.txt \
    auth get-token --force > /home/user/.config/ydb/exported-client-token.txt
  ```

  Если профиль `oidc-client` уже создан, используйте его. Создание профиля показано на вкладке «Секрет приложения (client)» в [примерах подключения](#connection-examples):

  ```bash
  umask 077
  {{ ydb-cli }} --profile oidc-client auth get-token --force > /home/user/.config/ydb/exported-client-token.txt
  ```

{% endlist %}

### Подключение к базе данных {#connection-examples}

После получения токена проверьте запрос к базе [командой](commands/discovery-whoami.md) `discovery whoami`. В режимах `device` и `client` CLI сам получает и обновляет токены при выполнении команд. Предварительно вызывать `auth get-token` для каждого запуска не требуется. В режиме `static` используется заранее полученный токен.

Если получение токена прошло успешно, но сервер вернул `UNAUTHORIZED`, проверьте [требования к токену и ключам](../../security/authentication.md#token-and-key-requirements) и [настройки проверки внешнего IdP на сервере](../configuration/auth_config.md#external-idp-auth-config). Если токен принят, но операция запрещена, проверьте [права доступа](../../security/authorization.md).

В примерах **неинтерактивный запуск** выполняет одну команду `discovery whoami` и завершает работу, а **интерактивный запуск** открывает [SQL-оболочку](interactive-cli.md): CLI запускается без подкоманды из терминала. Это не меняет способ получения токена. В режиме `device` оба варианта могут требовать подтверждения в браузере; для запуска CLI из скрипта без участия пользователя выбирайте `client` или `static`.

Во вкладках показаны одинаковые варианты подключения: без профиля и с профилем, без кеша и с кешем, в каждом случае — одна команда и интерактивная оболочка. Для `static` варианты с кешем неприменимы, поскольку этот режим его не использует.

{% list tabs %}

- Код устройства (device)

  **Конфигурация:**

  У приложения `ydb-cli` должен быть разрешён вход по коду устройства. Область `offline_access` оставьте, если она поддерживается вашим IdP для выдачи токена обновления. Область `openid` CLI добавляет самостоятельно.

  Сохраните конфигурацию **без кеша** в `/home/user/.config/ydb/oidc-device.yaml`:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  device_authorization_grant:
    client_id: ydb-cli
    scope:
      - offline_access
  ```

  Для варианта **с кешем** сохраните отдельный файл `/home/user/.config/ydb/oidc-device-cache.yaml`:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  cache_path: oidc-device-cache.json
  device_authorization_grant:
    client_id: ydb-cli
    scope:
      - offline_access
  ```

  Путь `cache_path` задан относительно каталога конфигурации. Токены будут сохраняться в `/home/user/.config/ydb/oidc-device-cache.json`.

  **Без профиля, без кеша:**

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device.yaml \
    discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device.yaml
  ```

  **Без профиля, с кешем:**

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device-cache.yaml \
    discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device-cache.yaml
  ```

  При первом запуске подтвердите вход в браузере. Последующие запуски используют сохранённые токены, пока они пригодны для доступа или обновления. Если возобновить сессию не удаётся, CLI снова запросит подтверждение.

  **С профилем, без кеша:**

  **Профиль со ссылкой на конфигурацию:**

  Создайте профиль, ссылающийся на конфигурацию без кеша:

  ```bash
  {{ ydb-cli }} config profile create oidc-device \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device.yaml
  ```

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-device discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-device
  ```

  Такой профиль содержит ссылку на файл в блоке `authentication`:

  ```yaml
  authentication:
    method: oidc-config
    data: /home/user/.config/ydb/oidc-device.yaml
  ```

  **С профилем, с кешем:**

  Создайте профиль, ссылающийся на конфигурацию с кешем:

  ```bash
  {{ ydb-cli }} config profile create oidc-device-cache \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device-cache.yaml
  ```

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-device-cache discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-device-cache
  ```

  **Прямые параметры вместо файла конфигурации:**

  Для запуска без отдельного файла OIDC передайте параметры непосредственно в командной строке:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow device \
    --oidc-client-id ydb-cli \
    --oidc-scope offline_access \
    discovery whoami
  ```

  **Настройка областей доступа:**

  Параметр `--oidc-scope 'profile offline_access'` и сочетание `--oidc-scope profile --oidc-scope offline_access` задают одинаковый список. Запрашивайте только области, разрешённые для приложения вашим IdP.

  **Профиль с прямыми параметрами:**

  Чтобы сохранить эти параметры непосредственно в профиле, выполните:

  ```bash
  {{ ydb-cli }} config profile create oidc-device-direct \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow device \
    --oidc-client-id ydb-cli \
    --oidc-scope offline_access
  ```

  Такой профиль хранит настройки в `authentication` с `method: oidc`; профиль со ссылкой на файл использует `method: oidc-config`. Выбирайте новый профиль через `--profile oidc-device-direct`. Для сохранения токенов добавьте к команде создания профиля `--oidc-cache-path /home/user/.config/ydb/oidc-device-direct-cache.json`. Запуск без подкоманды открывает интерактивную оболочку.

  Пример содержимого файла профилей после сохранения прямых параметров:

  ```yaml
  profiles:
    oidc-device-direct:
      endpoint: grpcs://ydb.example.com:2135
      database: /Root/mydb
      authentication:
        method: oidc
        data:
          issuer: https://idp.example.com/realms/ydb
          flow: device
          client_id: ydb-cli
          scope: offline_access
  ```

- Секрет приложения (client)

  **Конфигурация:**

  Сохраните секрет приложения `ydb-service` в файле `/home/user/.config/ydb/client-secret.txt`, доступном только владельцу.

  Сохраните конфигурацию **без кеша** в `/home/user/.config/ydb/oidc-client.yaml`:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  client_credentials_grant:
    client_id: ydb-service
    client_secret_file: client-secret.txt
  ```

  Чтобы читать секрет из окружения, удалите `client_secret_file` и задайте `YDB_OIDC_CLIENT_SECRET` средствами окружения исполнения. Переменная содержит сам секрет.

  Для варианта **с кешем** сохраните отдельный файл `/home/user/.config/ydb/oidc-client-cache.yaml`:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  cache_path: oidc-client-cache.json
  client_credentials_grant:
    client_id: ydb-service
    client_secret_file: client-secret.txt
  ```

  Путь `cache_path` задан относительно каталога конфигурации. Токены будут сохраняться в `/home/user/.config/ydb/oidc-client-cache.json`.

  **Без профиля, без кеша:**

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client.yaml \
    discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client.yaml
  ```

  **Без профиля, с кешем:**

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client-cache.yaml \
    discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client-cache.yaml
  ```

  **С профилем, без кеша:**

  Создайте профиль, ссылающийся на конфигурацию без кеша:

  ```bash
  {{ ydb-cli }} config profile create oidc-client \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client.yaml
  ```

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-client discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-client
  ```

  **С профилем, с кешем:**

  Создайте профиль, ссылающийся на конфигурацию с кешем:

  ```bash
  {{ ydb-cli }} config profile create oidc-client-cache \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client-cache.yaml
  ```

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-client-cache discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-client-cache
  ```

  **Прямые параметры вместо файла конфигурации:**

  Для запуска без отдельного файла OIDC передайте параметры непосредственно в командной строке:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow client \
    --oidc-client-id ydb-service \
    --oidc-client-secret-file /home/user/.config/ydb/client-secret.txt \
    discovery whoami
  ```

  Чтобы сохранить эти параметры непосредственно в профиле, выполните:

  ```bash
  {{ ydb-cli }} config profile create oidc-client-direct \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow client \
    --oidc-client-id ydb-service \
    --oidc-client-secret-file /home/user/.config/ydb/client-secret.txt
  ```

  Такой профиль хранит настройки в `authentication` с `method: oidc`; профиль со ссылкой на файл использует `method: oidc-config`. Выбирайте новый профиль через `--profile oidc-client-direct`. Для сохранения токенов добавьте к команде создания профиля `--oidc-cache-path /home/user/.config/ydb/oidc-client-direct-cache.json`. Запуск без подкоманды открывает интерактивную оболочку.

- Готовый токен (static)

  **Конфигурация:**

  Получите токен доступа заранее и сохраните его в `/home/user/.config/ydb/access-token.txt`, доступном только владельцу.

  Сохраните конфигурацию **без кеша** в `/home/user/.config/ydb/oidc-static.yaml`:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  static_credentials:
    access_token_file: access-token.txt
    expires_at: 2000000000
  ```

  Замените `2000000000` фактическим моментом истечения срока действия токена в секундах Unix или опустите `expires_at`, чтобы CLI попытался прочитать `exp` из JWT. Если токен задан через `YDB_OIDC_ACCESS_TOKEN`, удалите `access_token_file`; при отсутствии других полей запишите пустой блок:

  ```yaml
  issuer: https://idp.example.com/realms/ydb
  static_credentials: {}
  ```

  **Без профиля, без кеша:**

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-static.yaml \
    discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-static.yaml
  ```

  **Без профиля, с кешем:**

  Файловый кеш не используется: CLI каждый раз читает исходный файл токена. Параметр `cache_path` не изменит это поведение. Для обоих вариантов запуска используйте команды из примера «Без профиля, без кеша» выше.

  **С профилем, без кеша:**

  Создайте профиль, ссылающийся на конфигурацию без кеша:

  ```bash
  {{ ydb-cli }} config profile create oidc-static \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-static.yaml
  ```

  Неинтерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-static discovery whoami
  ```

  Интерактивный запуск:

  ```bash
  {{ ydb-cli }} --profile oidc-static
  ```

  **С профилем, с кешем:**

  Профиль не включает кеширование готового токена. Для обоих вариантов запуска используйте профиль из примера «С профилем, без кеша» выше: он ссылается на тот же исходный файл токена.

  **Прямые параметры вместо файла конфигурации:**

  Для запуска без отдельного файла OIDC передайте параметры непосредственно в командной строке:

  ```bash
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow static \
    --oidc-access-token-file /home/user/.config/ydb/access-token.txt \
    discovery whoami
  ```

  В этом варианте нет параметра `expires_at`: срок действия определяется по [правилам SDK для готового токена](../ydb-sdk/auth.md#oidc-static-example).

  Чтобы сохранить эти параметры непосредственно в профиле, выполните:

  ```bash
  {{ ydb-cli }} config profile create oidc-static-direct \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-issuer https://idp.example.com/realms/ydb \
    --oidc-flow static \
    --oidc-access-token-file /home/user/.config/ydb/access-token.txt
  ```

  Такой профиль хранит настройки в `authentication` с `method: oidc`; профиль со ссылкой на файл использует `method: oidc-config`. Выбирайте новый профиль через `--profile oidc-static-direct`. Запуск без подкоманды открывает интерактивную оболочку.

{% endlist %}
