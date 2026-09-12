# Подключение и аутентификация

{{ ydb-short-name }} DSTool (`ydb-dstool`) обращается к кластеру по двум независимым каналам:

- **gRPC** — вызовы контроллера Blob Storage (BSC) и других служебных API: чтение конфигурации, изменение статуса PDisk, операции с VDisk и группами. Используется как основной интерфейс получения данных и управления.
- **HTTP** — запросы к [{{ ydb-ui-name }}](../ydb-ui/index.md) (Viewer) и мониторингу узла. Например, часть команд обязательно проверяет актуальное состояние узлов и дисков через JSON-интерфейс Viewer.

От выбора эндпоинта и способа аутентификации зависит, какой канал сможет установить соединение и под какой учётной записью сервер выполнит запрос. Ниже описано, как утилита выбирает протокол и хост, как работает [анонимная аутентификация](../../security/authentication.md#anonymous) и как использовать [аутентификацию через токен](#credentials).

Полный список флагов подключения см. в разделе [{#T}](global-options.md).

## Эндпоинты {#endpoints}

Эндпоинт задаётся глобальным параметром `-e` / `--endpoint` в формате `[PROTOCOL://]HOST[:PORT]`. Параметр можно указать несколько раз, в том числе с разными протоколами.

Допустимые протоколы:

| Протокол | Канал | Порт по умолчанию | Шифрование |
|---|---|---|---|
| `grpc` | gRPC | `2135` | нет |
| `grpcs` | gRPC | `2135` | TLS |
| `http` | HTTP Viewer / мониторинг | `8765` | нет |
| `https` | HTTP Viewer / мониторинг | `8765` | TLS |

Если протокол не указан, утилита считает эндпоинт HTTP-адресом Viewer. Если порт не указан, для `grpc`/`grpcs` используется `--grpc-port` (по умолчанию `2135`), для `http`/`https` — `--mon-port` (по умолчанию `8765`).

Примеры:

```bash
# Только HTTP Viewer (локальный кластер без TLS)
ydb-dstool -e http://localhost:8765 cluster list

# Только gRPC. Необходимые HTTP-запросы будут направлены в http://<хост>:8765
ydb-dstool -e grpc://localhost:2135 cluster list

# Рекомендуемый вариант для кластера с аутентификацией и TLS:
# явно заданы оба канала
ydb-dstool \
  -e grpcs://static-node-1.example.com:2135 \
  -e https://static-node-1.example.com:8765 \
  --ca-file /path/to/ca.crt \
  --token-file /path/to/ydb-token \
  cluster list
```

Для `grpcs` и `https` передайте корневой сертификат кластера в `--ca-file`. Флаг `--insecure` отключает проверку сертификата и имени хоста только для HTTPS; на gRPC он не влияет.

## Выбор протокола и хоста {#host-selection}

Каждый внутренний запрос относится к одному из типов: HTTP, gRPC или «любой» (например, команда к BSC может пойти и по gRPC, и по HTTP, в зависимости от протокола выбранного эндпоинта).

Утилита выбирает адрес в следующем порядке:

1. Берёт эндпоинты нужного типа из списка `-e`. Если их несколько, выбирает случайный хост.
2. При ошибке соединения повторяет запрос на других эндпоинтах того же типа (до пяти попыток). Хост, ответивший ошибкой HTTP, помечается как неудачный до конца запуска.
3. Если эндпоинтов нужного типа нет, утилита преобразует указанные адреса в эндпоинты другого типа:
   - для HTTP-запроса из `grpc`/`grpcs://HOST:PORT` получается `{http|https}://HOST:<mon-port>`;
   - для gRPC-запроса из `http`/`https://HOST:PORT` получается `{grpc|grpcs}://HOST:<grpc-port>`.
4. Протокол преобразования:
   - `https`, если среди `-e` есть хотя бы один `https` и нет `http`; иначе `http`;
   - `grpcs`, если среди `-e` есть хотя бы один `grpcs` и нет `grpc`; иначе `grpc`.

Если задан только `grpcs://...:2135`, утилита предупреждает, что HTTP-эндпоинт не указан, и обращается с HTTP-запросами на `http://<хост>:8765`. На кластере с обязательным TLS у мониторинга это приводит к ошибкам. Чтобы избежать преобразования, укажите оба эндпоинта.

Флаг `--use-ip` заставляет утилиту резолвить имя хоста в IP-адрес перед HTTP-запросом.

{% note warning %}

Сообщение `Can't connect to specified addresses` после серии `HTTP Error 403` означает отказ в доступе, а не сетевую недоступность. Проверьте формат токена и [уровень доступа](../configuration/security_config.md#security-access-levels) пользователя.

{% endnote %}

## Анонимная аутентификация {#anonymous}

Если утилита не нашла токен ни в одном из [источников](#token-sources), запросы уходят без аутентификационных данных: HTTP без заголовка `Authorization`, gRPC без `SecurityToken` и без метаданных `x-ydb-auth-ticket`.

Так можно работать с локальным или тестовым кластером, у которого включена [анонимная аутентификация](../../security/authentication.md#anonymous): параметр [`enforce_user_token_requirement`](../configuration/security_config.md) равен `false`, либо не указан вовсе (значение по умолчанию).

Проверить, что токен не подхватывается из окружения, можно так: не задавайте `--token-file` и `--iam-token-file`, очистите `YDB_TOKEN` и `IAM_TOKEN` и убедитесь, что нет файлов `~/.ydb/token` и `~/.ydb/iam_token`. Затем выполните команду, например `ydb-dstool -e http://localhost:8765 cluster list`.

{% note warning %}

Анонимный доступ предназначен только для ознакомительных и локальных развёртываний. Если списки уровней доступа в `security_config` пусты, любой подключившийся клиент получает административные права. Не используйте анонимную аутентификацию на кластерах, доступных по сети.

{% endnote %}

Если на кластере включена обязательная аутентификация (`enforce_user_token_requirement: true`), анонимный запрос будет отклонён. Для HTTP Viewer это обычно ответ `401 Unauthorized` (нет заголовка `Authorization`) или `403 Forbidden` (заголовок есть, но токен не принят).

## Аутентификация через токен {#credentials}

{{ ydb-short-name }} DSTool не принимает логин и пароль в командной строке и не вызывает сервис входа самостоятельно. Утилита передаёт готовый (ранее полученный) [аутентификационный токен](../../concepts/glossary.md#auth-token), получаемый с помощью [{{ ydb-short-name }} CLI](../ydb-cli/auth-get-token.md).

Для получения токена используется штатный механизм [аутентификации](../../security/authentication.md): {{ ydb-short-name }} CLI отправляет учётные данные в сервис `Login`, сервер возвращает токен, далее DSTool подставляет этот токен в каждый запрос.

### Получение токена {#get-token}

```bash
{{ ydb-cli }} --ca-file /path/to/ca.crt \
  -e grpcs://static-node-1.example.com:2135 \
  -d /Root \
  --user <user> \
  auth get-token --force > /tmp/ydb-login.jwt
```

Если пароль не задан флагами `--password-file` или `--no-password`, CLI запросит его интерактивно. Для пользователя `root` с пустым паролем на этапе начального развёртывания добавьте `--no-password`.

### Формат файла токена {#token-file-format}

`--token-file` читает **первую строку** файла. Если в строке одно слово, утилита считает его токеном типа `OAuth`. Если слов два — первое слово трактуется как схема, второе как токен.

Для токена входа укажите схему `Login`. Иначе HTTP Viewer получит заголовок `Authorization: OAuth <токен>` и отклонит запрос (`403 Forbidden`), хотя gRPC-команды к BSC с тем же файлом могут пройти: в gRPC утилита передаёт только тело токена, без схемы.

```bash
{ printf 'Login '; cat /tmp/ydb-login.jwt; } > /path/to/ydb-token
```

Пример содержимого файла:

```text
Login eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

Токен, оканчивающийся на `@builtin` (например `root@builtin`), утилита отправляет без схемы.

### Как токен передаётся в запросах {#token-transport}

| Канал | Куда попадает токен |
|---|---|
| HTTP Viewer | заголовок `Authorization: <схема> <токен>` |
| gRPC BSC / CMS | поле `SecurityToken` (только тело токена) |
| gRPC Distributed Storage и Bridge | метаданные `x-ydb-auth-ticket` (только тело токена) |

### Источники токена {#token-sources}

Утилита выбирает **первый** найденный источник:

1. `--token-file` — по умолчанию схема `OAuth`, если в файле не указана своя.
2. `--iam-token-file` — схема `Bearer`. Взаимоисключающий с `--token-file`.
3. Переменная окружения `YDB_TOKEN` — схема `OAuth`, если не указана своя.
4. Переменная окружения `IAM_TOKEN` — схема `Bearer`.
5. Файл `~/.ydb/token` — схема `OAuth`.
6. Файл `~/.ydb/iam_token` — схема `Bearer`.

Для входа по логину и паролю используйте `--token-file` со схемой `Login` или запишите ту же строку в `YDB_TOKEN` / `~/.ydb/token`.

### Права пользователя {#access-levels}

Успешный вход недостаточен: [SID](../../concepts/glossary.md#access-sid) пользователя должен входить в списки уровней доступа [`security_config`](../configuration/security_config.md#security-access-levels).

- Команды, которые меняют конфигурацию хранилища через BSC, требуют уровня **administration** (`administration_allowed_sids`).
- HTTP-запросы к Viewer, в том числе проверка состояния PDisk, требуют как минимум уровня **viewer** (`viewer_allowed_sids`). Более высокий уровень включает более низкие: administration даёт monitoring и viewer.

Обычно администратора кластера достаточно добавить только в `administration_allowed_sids` (например `root` или группу `ADMINS`). Проверить фактический SID можно командой [`{{ ydb-cli }} discovery whoami`](../ydb-cli/commands/discovery-whoami.md).

## Примеры {#examples}

Анонимный доступ к локальному кластеру:

```bash
ydb-dstool -e http://localhost:8765 cluster list
```

Кластер с TLS и входом по логину и паролю:

```bash
{{ ydb-cli }} --ca-file /path/to/ca.crt \
  -e grpcs://static-node-1.example.com:2135 \
  -d /Root --user root \
  auth get-token --force > /tmp/ydb-login.jwt

{ printf 'Login '; cat /tmp/ydb-login.jwt; } > ~/ydb-token

ydb-dstool \
  -e grpcs://static-node-1.example.com:2135 \
  -e https://static-node-1.example.com:8765 \
  --ca-file /path/to/ca.crt \
  --token-file ~/ydb-token \
  pdisk set --status BROKEN --pdisk-ids '[9:1008]'
```
