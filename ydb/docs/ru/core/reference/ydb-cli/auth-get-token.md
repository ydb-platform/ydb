# Получение токена аутентификации

Подкоманда `auth get-token` получает токен с помощью настроенного способа аутентификации. Настройки берутся из профиля, переменных окружения или параметров командной строки.

При [аутентификации OIDC](oidc.md#get-token) команда выводит строку с префиксом `Bearer`, пробелом и токеном доступа. Если требуется вход по коду устройства, ссылка и код выводятся в стандартный поток ошибок; `--force` отключает только подтверждение вывода токена, но не вход у провайдера идентификации.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] auth get-token [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Чтобы посмотреть справку по команде, выполните:

```bash
{{ ydb-cli }} auth get-token --help
```

## Параметры подкоманды {#options}

Параметр | Описание
---|---
`-f, --force` | Вывести токен без запроса подтверждения.
`--timeout` | Общий параметр времени ожидания клиента: допускает единицы времени, например `5s` или `1m`; число без единицы означает миллисекунды. В `auth get-token` он не ограничивает ожидание получения OIDC-токена, в том числе входа по коду устройства.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Получение токена с подтверждением {#with-prompt}

Перед выводом токена в консоль команда по умолчанию запрашивает подтверждение:

```bash
{{ ydb-cli }} -p quickstart auth get-token
```

Результат:

```text
Caution: Your auth token will be printed to console. Use "--force" ("-f") option to print without prompting.
Do you want to proceed? (y/N): y
t1.eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Получение токена без подтверждения {#without-prompt}

Для вывода токена без запроса подтверждения укажите параметр `--force`:

```bash
{{ ydb-cli }} -p quickstart auth get-token --force
```

Результат:

```text
t1.eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Использование в скриптах {#in-scripts}

Команду можно использовать для получения токена в скриптах:

```bash
TOKEN=$({{ ydb-cli }} -p quickstart auth get-token --force)
echo "Token: $TOKEN"
```
