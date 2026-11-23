# Подключение и аутентификация

{{ ydb-short-name }} DSTool (`ydb-dstool`) обращается к кластеру по двум независимым каналам:

- **[gRPC](https://grpc.io/)** — вызовы [контроллера Blob Storage](../../concepts/glossary.md#ds-controller) (BSC) и других служебных API: чтение конфигурации, изменение статуса [PDisk](../../concepts/glossary.md#pdisk), операции с [VDisk](../../concepts/glossary.md#vdisk) и [группами хранения](../../concepts/glossary.md#storage-group). Команды BSC идут по gRPC, если выбран эндпоинт `grpc` или `grpcs`, и по HTTP, если выбран `http` или `https`.
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
# Только HTTP Viewer (локальный тестовый кластер без TLS)
ydb-dstool -e http://localhost:8765 cluster list

# Только gRPC. Команде cluster list достаточно gRPC
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

Для `grpcs` и `https` при необходимости укажите доверенный корневой сертификат через `--ca-file`. Этот параметр необязателен, если сертификат сервера проверяется с использованием доверенных сертификатов по умолчанию. Флаг `--insecure` отключает проверку сертификата и имени хоста только для HTTPS; на gRPC он не влияет.

## Выбор протокола и хоста {#host-selection}

Каждый внутренний запрос относится к одному из типов: HTTP, gRPC или «любой» (например, команда к BSC может пойти и по gRPC, и по HTTP, в зависимости от протокола выбранного эндпоинта).

Утилита выбирает адрес в следующем порядке:

1. Берёт эндпоинты нужного типа из списка `-e`. Если их несколько, выбирает случайный хост.
2. При ошибке соединения повторяет запрос на других эндпоинтах того же типа (до пяти попыток). Эндпоинт, на котором HTTP-запрос завершился ошибкой соединения или HTTP-ошибкой, пропускается при последующем обычном выборе до конца запуска, но финальная попытка может выбрать его снова.
3. Если выполнить запрос через эндпоинты нужного типа не удалось, утилита пробует эндпоинты другого типа. Автоматическое преобразование не сохраняет признак использования TLS:
   - HTTP-запрос к хосту `grpc` или `grpcs` уходит на `http://HOST:<mon-port>` (по умолчанию `8765`). Протокол становится `https`, только если среди `-e` есть хотя бы один `https` и нет `http`. Один эндпоинт `grpcs://HOST:2135` HTTPS не включает: утилита предупреждает, что HTTP-эндпоинт не указан, и отправляет HTTP-запросы на `http://HOST:8765` без шифрования. На кластере, где мониторинг требует TLS, это приводит к ошибке;
   - запрос, которому нужен именно gRPC, к хосту `http` или `https` всегда идёт по незашифрованному `grpc` на `--grpc-port`, в том числе если исходный эндпоинт — `https` и задан `--ca-file`. TLS для gRPC используется только при явном эндпоинте `grpcs`. Команды BSC не преобразуются: они используют протокол выбранного эндпоинта. HTTP уже покрывает API BSC, поэтому список только из `http`/`https` не переключает BSC на gRPC.

Чтобы использовать TLS в обоих каналах, явно задайте эндпоинты `grpcs://` и `https://`.

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
  auth get-token --force > ydb-login.jwt
```

Если пароль не задан флагами `--password-file` или `--no-password`, CLI запросит его интерактивно. Для пользователя `root` с пустым паролем на этапе начального развёртывания добавьте `--no-password`.

### Формат файла токена {#token-file-format}

`--token-file` читает **первую строку** файла. Если в строке одно слово, утилита считает его токеном типа `OAuth`. Если слов два через один пробел — первое слово трактуется как схема аутентификации, второе как токен.

Для токена входа укажите схему `Login`, в противном случае HTTP Viewer получит заголовок `Authorization: OAuth <токен>` и отклонит запрос (`403 Forbidden`), хотя gRPC-команды к BSC с тем же файлом могут пройти: в gRPC утилита передаёт только тело токена, без схемы.

```bash
{ printf 'Login '; cat ydb-login.jwt; } > /path/to/ydb-token
```

Пример содержимого файла:

```text
Login eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

Токен, оканчивающийся на `@builtin` (например `root@builtin`), утилита отправляет без указания схемы аутентификации.

### Как токен передаётся в запросах {#token-transport}

| Канал | Куда попадает токен |
|---|---|
| HTTP Viewer | заголовок `Authorization: <схема> <токен>` |
| gRPC BSC / [CMS](../../concepts/glossary.md#cms) | поле `SecurityToken` (только тело токена) |
| gRPC [Distributed Storage](../../concepts/glossary.md#distributed-storage) и [Bridge](../../concepts/glossary.md#bridge) | метаданные `x-ydb-auth-ticket` (только тело токена) |

### Источники токена {#token-sources}

Утилита выбирает **первый** найденный источник:

1. `--token-file` — по умолчанию схема аутентификации `OAuth`, если в файле не указана своя.
2. `--iam-token-file` — используется схема `Bearer`. Взаимоисключающий с `--token-file`.
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
  pdisk list --check-leaked-slots
```
