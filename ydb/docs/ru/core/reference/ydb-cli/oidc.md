# Аутентификация OIDC в CLI {#oidc}

{{ ydb-short-name }} CLI использует [OIDC-провайдер C++ SDK](../ydb-sdk/auth.md#oidc) для получения токенов у внешнего провайдера идентификации (IdP). Это отдельный от {{ ydb-short-name }} сервис, который проверяет учётные данные пользователя или приложения и выдаёт токены. В терминологии OIDC такой сервис выполняет роль [провайдера OpenID](https://openid.net/specs/openid-connect-core-1_0.html#Terminology). Перед подключением выполните [предварительную настройку IdP и сервера](../ydb-sdk/auth.md#oidc-prerequisites). В справочнике SDK описаны [участники обмена и токены](../ydb-sdk/auth.md#oidc-participants), [обнаружение адресов IdP](../ydb-sdk/auth.md#oidc-discovery), [обновление токенов](../ydb-sdk/auth.md#oidc-refresh) и [обработка ошибок](../ydb-sdk/auth.md#oidc-errors).

Ниже приведены настройки CLI; [примеры команд, файлов конфигурации и профилей](#examples) собраны в конце статьи. После настройки проверьте вход [командой](commands/discovery-whoami.md) `discovery whoami`.

## Выбор режима {#oidc-flows}

**Идентификатор приложения** (`client_id`) — значение, которое IdP присваивает зарегистрированному приложению. Оно позволяет определить приложение, запрашивающее токен, и само по себе не является секретом. См. [RFC 6749, раздел 2.2](https://www.rfc-editor.org/rfc/rfc6749.html#section-2.2).

**Области доступа** (`scope`) — именованные разрешения, которые приложение запрашивает у IdP при получении токена. Допустимые значения и их смысл определяет IdP; фактически выданные разрешения могут отличаться от запрошенных. См. [RFC 6749, раздел 3.3](https://www.rfc-editor.org/rfc/rfc6749.html#section-3.3). Права на объекты базы данных настраиваются отдельно на стороне {{ ydb-short-name }}.

Укажите HTTPS-адрес издателя через `--oidc-issuer` и выберите режим параметром `--oidc-flow`:

| Режим | Что указать | Когда использовать |
| --- | --- | --- |
| `device` | `--oidc-client-id` | Для входа пользователя по коду устройства. [Описание режима](../ydb-sdk/auth.md#oidc-device-flow). |
| `client` | `--oidc-client-id` и `--oidc-client-secret-file` либо `YDB_OIDC_CLIENT_SECRET` | Для автоматических заданий с секретом приложения. [Описание режима и настройки IdP](../ydb-sdk/auth.md#oidc-client-flow). |
| `static` | `--oidc-access-token-file` либо `YDB_OIDC_ACCESS_TOKEN` | Для уже полученного токена без автоматического обновления. [Описание режима](../ydb-sdk/auth.md#oidc-static-example). |

Если режим не указан, файл готового токена или непустая переменная `YDB_OIDC_ACCESS_TOKEN` выбирает `static`, иначе выбирается `device`. Наличие секрета приложения не выбирает `client`: задайте этот режим явно. В режиме `static` нельзя задавать идентификатор приложения, секрет и области доступа; в `device` нельзя передавать секрет или готовый токен; в `client` нельзя передавать готовый токен.

### Вход пользователя по коду устройства {#device}

В режиме `device` CLI выводит ссылку и код в стандартный поток ошибок и ожидает подтверждения входа. Браузер нужно открыть самостоятельно, в том числе на другом устройстве. При отказе или истечении срока действия кода повторите команду. Интерактивная SQL-оболочка также ожидает первоначального входа; `Ctrl+C` прерывает ожидание. Перенаправление ввода или вывода и параметры `--assume-yes` и `auth get-token --force` не заменяют подтверждение пользователя.

**Токен обновления** (`refresh_token`) — учётные данные для получения нового токена доступа у IdP без повторного входа пользователя. Он передаётся только IdP, а не серверу {{ ydb-short-name }}. Определение приведено в [RFC 6749, раздел 1.5](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.5), порядок обмена — в [разделе 6](https://www.rfc-editor.org/rfc/rfc6749.html#section-6). Выдачу токена определяет IdP. Поведение этой области описано в [OpenID Connect Core, раздел 11](https://openid.net/specs/openid-connect-core-1_0.html#OfflineAccess).

Чтобы сохранить вход между запусками, используйте [конфигурацию с кешем](#device-cache). Без кеша каждый новый процесс начинает вход заново.

### Подключение автоматического задания {#client}

В режиме `client` CLI получает токен по идентификатору и секрету зарегистрированного приложения. Участие пользователя в браузере не требуется. Секрет передаётся через файл, доступный только владельцу, или переменную окружения. См. [примеры для этого режима](#client-config).

### Подключение с готовым токеном {#static}

В режиме `static` CLI использует токен, полученный заранее, и не обновляет его. CLI читает файл токена заново при каждом запуске, удаляет начальные и конечные пробельные символы, а также префикс `Bearer` с одним пробелом, если он есть. Для замены токена в работающем процессе перезапустите CLI. См. [примеры с готовым токеном](#static-config).

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

Полученный файл можно использовать с `--oidc-access-token-file`. Если токен нужен другому инструменту, проверьте, ожидает ли тот всю строку с префиксом или только значение токена. Команды для всех режимов приведены на [вкладке «Получение токена»](#token-examples).

## Примеры {#examples}

### Подготовка {#preparation}

В примерах используются условные адреса `grpcs://ydb.example.com:2135`, `/Root/mydb` и `https://idp.example.com/realms/ydb`. Укажите вместо них эндпоинт и путь вашей базы данных, а также точное значение `issuer` вашего IdP. Параметр `-e` задаёт эндпоинт, а `-d` задаёт путь базы данных. Идентификаторы приложений `ydb-cli` и `ydb-service` и разрешённые области доступа также должны соответствовать настройкам IdP.

Замените путь `/home/user/.config/ydb/` на путь к каталогу конфигурации в вашем окружении. Для хранения конфигурации, секретов и кеша создайте каталог с доступом только владельцу. Например, в Linux или macOS:

```bash
mkdir -p /home/user/.config/ydb
chmod 700 /home/user/.config/ydb
```

`mkdir -p` создаёт каталог, а `chmod 700` разрешает доступ к нему только владельцу. Значения секретов и токенов получите средствами вашего окружения; не помещайте их в общедоступные файлы. CLI не создаёт каталог кеша автоматически. Требования к его защите описаны в [разделе о файловом кеше](#oidc-cache).

В примерах **неинтерактивный запуск** выполняет одну команду `discovery whoami` и завершает работу, а **интерактивный запуск** открывает [SQL-оболочку](interactive-cli.md): CLI запускается без подкоманды из терминала. Это не меняет способ получения токена. В режиме `device` оба варианта могут требовать подтверждения в браузере; для автоматического задания без участия пользователя выбирайте `client` или `static`.

Во вкладках показаны одинаковые варианты подключения: без профиля и с профилем, без кеша и с кешем, в каждом случае — одна команда и интерактивная оболочка. Для `static` варианты с кешем неприменимы, поскольку этот режим его не использует.

{% list tabs %}

- Код устройства (device)

  ### Конфигурация {#device-config}

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

  ### Без профиля, без кеша {#device-no-profile}

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

  ### Без профиля, с кешем {#device-cache}

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

  ### С профилем, без кеша {#device-profile}

  #### Профиль со ссылкой на конфигурацию {#file-profile}

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

  ### С профилем, с кешем {#device-profile-cache}

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

  ### Прямые параметры вместо файла конфигурации {#device-direct}

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

  #### Настройка областей доступа {#scopes}

  Параметр `--oidc-scope 'profile offline_access'` и сочетание `--oidc-scope profile --oidc-scope offline_access` задают одинаковый список. Запрашивайте только области, разрешённые для приложения вашим IdP.

  #### Профиль с прямыми параметрами {#direct-profile}

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

  ### Конфигурация {#client-config}

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

  ### Без профиля, без кеша {#client-no-profile}

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

  ### Без профиля, с кешем {#client-cache}

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

  ### С профилем, без кеша {#client-profile}

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

  ### С профилем, с кешем {#client-profile-cache}

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

  ### Прямые параметры вместо файла конфигурации {#client-direct}

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

  ### Конфигурация {#static-config}

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

  ### Без профиля, без кеша {#static-no-profile}

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

  ### Без профиля, с кешем {#static-cache}

  Файловый кеш не используется: CLI каждый раз читает исходный файл токена. Параметр `cache_path` не изменит это поведение. Для обоих вариантов запуска используйте [команды без кеша](#static-no-profile).

  ### С профилем, без кеша {#static-profile}

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

  ### С профилем, с кешем {#static-profile-cache}

  Профиль не включает кеширование готового токена. Для обоих вариантов запуска используйте [профиль без кеша](#static-profile): он ссылается на тот же исходный файл токена.

  ### Прямые параметры вместо файла конфигурации {#static-direct}

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

- Получение токена

  ### Получение токена {#token-examples}

  Команды получения токена используют `umask 077`, чтобы новый файл был доступен только владельцу. Для уже существующего файла заранее проверьте права: `umask` их не меняет. Перенаправление `>` сохраняет стандартный вывод, содержащий `Bearer`, пробел и токен доступа. URL и код устройства остаются в стандартном потоке ошибок.

  #### Код устройства {#device-get-token}

  Используйте [конфигурацию и профиль для этого режима](#device-config).

  При необходимости подтвердите вход в браузере. Параметр `--force` разрешает вывод токена, но не отменяет это подтверждение.

  Без профиля:

  ```bash
  umask 077
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-device.yaml \
    auth get-token --force > /home/user/.config/ydb/exported-device-token.txt
  ```

  С профилем:

  ```bash
  umask 077
  {{ ydb-cli }} --profile oidc-device auth get-token --force > /home/user/.config/ydb/exported-device-token.txt
  ```

  Чтобы использовать кеш, в первом варианте укажите `oidc-device-cache.yaml`, а во втором — профиль `oidc-device-cache`.

  #### Секрет приложения {#client-get-token}

  Используйте [конфигурацию и профиль для этого режима](#client-config).

  Без профиля:

  ```bash
  umask 077
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-client.yaml \
    auth get-token --force > /home/user/.config/ydb/exported-client-token.txt
  ```

  С профилем:

  ```bash
  umask 077
  {{ ydb-cli }} --profile oidc-client auth get-token --force > /home/user/.config/ydb/exported-client-token.txt
  ```

  Чтобы использовать кеш, в первом варианте укажите `oidc-client-cache.yaml`, а во втором — профиль `oidc-client-cache`.

  #### Готовый токен {#static-get-token}

  Используйте [конфигурацию и профиль для этого режима](#static-config).

  Команда выводит уже переданный токен с префиксом `Bearer`, а не получает новый у IdP. Используйте отдельный выходной файл, чтобы перенаправление не очистило исходный токен перед чтением.

  Без профиля:

  ```bash
  umask 077
  {{ ydb-cli }} \
    -e grpcs://ydb.example.com:2135 -d /Root/mydb \
    --oidc-config /home/user/.config/ydb/oidc-static.yaml \
    auth get-token --force > /home/user/.config/ydb/exported-static-token.txt
  ```

  С профилем:

  ```bash
  umask 077
  {{ ydb-cli }} --profile oidc-static auth get-token --force > /home/user/.config/ydb/exported-static-token.txt
  ```

{% endlist %}
