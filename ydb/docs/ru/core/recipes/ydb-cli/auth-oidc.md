# Аутентификация при помощи OIDC в CLI

Аутентификация [OIDC](../../concepts/glossary.md#oidc) позволяет {{ ydb-short-name }} CLI использовать токены внешнего провайдера идентификации для запросов к базе данных. В этом рецепте показаны вход пользователя, подключение автоматического задания, настройка профиля и сохранение токенов.

Перед выполнением команд проверьте [настройку IdP и сервера](../../reference/ydb-cli/oidc.md#prerequisites). Описание режимов, параметров и приоритетов находится в [справочнике OIDC для CLI](../../reference/ydb-cli/oidc.md).

## Подготовка {#preparation}

В примерах используются условные адреса `grpcs://ydb.example.com:2135`, `/Root/mydb` и `https://idp.example.com/realms/ydb`. Укажите вместо них эндпоинт и путь вашей базы данных, а также точное значение `issuer` вашего IdP. Параметр `-e` задаёт эндпоинт, а `-d` задаёт путь базы данных. Идентификаторы приложений `ydb-cli` и `ydb-service` и разрешённые области доступа также должны соответствовать настройкам IdP.

Замените путь `/home/user/.config/ydb/` на путь к каталогу конфигурации в вашем окружении. Для хранения конфигурации, секретов и кеша создайте каталог с доступом только владельцу. Например, в Linux или macOS:

```bash
mkdir -p /home/user/.config/ydb
chmod 700 /home/user/.config/ydb
```

`mkdir -p` создаёт каталог, а `chmod 700` разрешает доступ к нему только владельцу. Значения секретов и токенов получите средствами вашего окружения; не помещайте их в общедоступные файлы. CLI не создаёт каталог кеша автоматически. Требования к его защите описаны в [справочнике](../../reference/ydb-cli/oidc.md#cache).

## Вход пользователя по коду устройства {#device}

Укажите режим `device` и идентификатор приложения, для которого IdP разрешает вход по коду устройства. [Команда](../../reference/ydb-cli/commands/discovery-whoami.md) `discovery whoami` проверит, под какой учётной записью сервер принял запрос:

```bash
{{ ydb-cli }} \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-issuer https://idp.example.com/realms/ydb \
  --oidc-flow device \
  --oidc-client-id ydb-cli \
  --oidc-scope offline_access \
  discovery whoami
```

Откройте выведенную ссылку в браузере, при необходимости введите код и подтвердите вход. CLI пишет инструкции в стандартный поток ошибок и продолжает работу после подтверждения. Если срок действия кода истёк или вход отклонён, запустите команду заново.

Область `offline_access` запрашивается явно. Оставьте её, если она поддерживается вашим IdP для выдачи токена обновления; область `openid` CLI добавляет самостоятельно. Чтобы сохранить вход между запусками, используйте [конфигурацию с кешем](#device-config).

## Подключение автоматического задания {#client}

Выберите режим `client`. Сохраните секрет зарегистрированного приложения в файле `/home/user/.config/ydb/client-secret.txt`, доступном только владельцу, и передайте путь к файлу:

```bash
{{ ydb-cli }} \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-issuer https://idp.example.com/realms/ydb \
  --oidc-flow client \
  --oidc-client-id ydb-service \
  --oidc-client-secret-file /home/user/.config/ydb/client-secret.txt \
  discovery whoami
```

IdP должен разрешать способ получения токена `client_credentials`, проверку приложения через `client_secret_basic` и область доступа `openid`. Значения этих терминов описаны в [разделе о секрете приложения](../../reference/ydb-cli/oidc.md#client). Наличие файла секрета не выбирает режим автоматически, поэтому параметр `--oidc-flow client` обязателен в этом примере.

## Подключение с готовым токеном {#static}

Сохраните полученный вне CLI токен доступа в файле `/home/user/.config/ydb/access-token.txt`, доступном только владельцу. Затем выполните:

```bash
{{ ydb-cli }} \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-issuer https://idp.example.com/realms/ydb \
  --oidc-flow static \
  --oidc-access-token-file /home/user/.config/ydb/access-token.txt \
  discovery whoami
```

Файл может содержать только токен либо токен с префиксом `Bearer` и одним пробелом. CLI не обновляет токен в этом режиме. После замены содержимого файла запустите команду заново. Правила чтения файла описаны в [справочнике статического режима](../../reference/ydb-cli/oidc.md#static).

## Настройка областей доступа {#scopes}

Области доступа можно передать одной строкой или повторяющимися параметрами. Параметр `--oidc-scope 'profile offline_access'` и сочетание параметров `--oidc-scope profile --oidc-scope offline_access` задают одинаковый список. Добавьте нужный вариант перед подкомандой в примере для `client` или `device`.

В отдельном YAML-файле используйте список строк, как в следующем примере. Допустимые значения областей и их назначение описаны в [справочнике](../../reference/ydb-cli/oidc.md#scopes).

## Подключение через отдельный файл {#config-file}

Файл OIDC объединяет параметры одного способа получения токена. Поля и проверка формата описаны в [справочнике конфигурации](../../reference/ydb-cli/oidc.md#config-file).

### Код устройства и кеширование токенов {#device-config}

Сохраните файл `/home/user/.config/ydb/oidc-device.yaml`:

```yaml
issuer: https://idp.example.com/realms/ydb
cache_path: oidc-device-cache.json
device_authorization_grant:
  client_id: ydb-cli
  scope:
    - offline_access
```

Путь `cache_path` задан относительно каталога конфигурации. После успешного входа токены сохранятся в `/home/user/.config/ydb/oidc-device-cache.json`, если каталог существует и запись разрешена. Проверьте аутентификацию с этим файлом:

```bash
{{ ydb-cli }} \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-config /home/user/.config/ydb/oidc-device.yaml \
  discovery whoami
```

### Секрет приложения {#client-config}

Сохраните следующую конфигурацию в `/home/user/.config/ydb/oidc-client.yaml`, а секрет в файле `client-secret.txt` рядом с ней:

```yaml
issuer: https://idp.example.com/realms/ydb
client_credentials_grant:
  client_id: ydb-service
  client_secret_file: client-secret.txt
```

Для подключения используйте предыдущую команду, заменив значение `--oidc-config` на путь к `oidc-client.yaml`. Чтобы читать секрет из окружения, удалите `client_secret_file` и задайте `YDB_OIDC_CLIENT_SECRET` средствами окружения исполнения. Эта переменная содержит сам секрет.

### Готовый токен со сроком действия {#static-config}

Сохраните конфигурацию в `/home/user/.config/ydb/oidc-static.yaml`, а токен в файле `access-token.txt` рядом с ней:

```yaml
issuer: https://idp.example.com/realms/ydb
static_credentials:
  access_token_file: access-token.txt
  expires_at: 2000000000
```

Передайте путь к `oidc-static.yaml` в `--oidc-config`. Замените `2000000000` фактическим моментом истечения срока действия токена в целых секундах Unix или опустите `expires_at`, чтобы CLI попытался прочитать `exp` из JWT.

Если токен передаётся через `YDB_OIDC_ACCESS_TOKEN`, удалите `access_token_file`; при отсутствии других полей блок записывается как `static_credentials: {}`. Правила определения срока действия и требования к обязательным данным приведены в [справочнике](../../reference/ydb-cli/oidc.md#static-config).

## Сохранение настроек в профиле {#profiles}

### Профиль со ссылкой на конфигурацию {#file-profile}

После подготовки `oidc-device.yaml` создайте профиль и проверьте подключение:

```bash
{{ ydb-cli }} config profile create oidc-device \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-config /home/user/.config/ydb/oidc-device.yaml

{{ ydb-cli }} --profile oidc-device discovery whoami
```

В профиль записывается путь к конфигурации. Создание профиля проверяет локальные настройки, но вход у IdP происходит при выполнении `discovery whoami`. Профиль не активируется автоматически, поэтому в примере он выбран через `--profile`.

### Профиль с прямыми параметрами {#direct-profile}

Вместо отдельного файла сохраните параметры OIDC непосредственно в профиле:

```bash
{{ ydb-cli }} config profile create oidc-direct \
  -e grpcs://ydb.example.com:2135 -d /Root/mydb \
  --oidc-issuer https://idp.example.com/realms/ydb \
  --oidc-flow device \
  --oidc-client-id ydb-cli \
  --oidc-scope offline_access \
  --oidc-cache-path /home/user/.config/ydb/oidc-direct-cache.json
```

После создания выберите профиль `oidc-direct` через `--profile` при запуске команды. Прямые настройки сохраняются в файле профилей в следующем виде; ниже приведён пример документа с одним профилем:

```yaml
profiles:
  oidc-direct:
    endpoint: grpcs://ydb.example.com:2135
    database: /Root/mydb
    authentication:
      method: oidc
      data:
        issuer: https://idp.example.com/realms/ydb
        flow: device
        client_id: ydb-cli
        scope: offline_access
        cache_path: /home/user/.config/ydb/oidc-direct-cache.json
```

Если профиль ссылается на отдельный OIDC-файл, его блок `authentication` имеет другой вид:

```yaml
authentication:
  method: oidc-config
  data: /home/user/.config/ydb/oidc-device.yaml
```

В прямом профиле `scope` является строкой, а в отдельном OIDC-файле является списком строк. Профиль хранит пути к секретным файлам, а не сами секреты. Правила обновления профилей и разрешения относительных путей описаны в [справочнике](../../reference/ydb-cli/oidc.md#profiles).

## Сохранение токена для другого инструмента {#get-token}

Используйте созданный выше профиль `oidc-device`, содержащий эндпоинт и путь базы данных. Следующие команды для командной оболочки в Linux или macOS ограничивают доступ к новому файлу и записывают в него результат:

```bash
umask 077
{{ ydb-cli }} --profile oidc-device auth get-token --force > access-token.txt
```

`umask 077` ограничивает права создаваемых файлов, а перенаправление `>` записывает стандартный вывод в `access-token.txt`. Для существующего файла заранее проверьте права доступа: `umask` не меняет их. Результат содержит `Bearer`, пробел и токен доступа.

Параметр `--force` отключает подтверждение вывода токена. При необходимости входа по коду устройства ссылка и код выводятся в стандартный поток ошибок и не попадают в файл. Подробнее о поведении команды и её тайм-ауте см. [получение токена](../../reference/ydb-cli/oidc.md#get-token).

Полученный файл можно использовать с `--oidc-access-token-file`. Если токен нужен другому инструменту, проверьте, ожидает ли тот всю строку с префиксом или только значение токена.
