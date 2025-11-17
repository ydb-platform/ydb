# Получение токена аутентификации

С помощью подкоманды `auth get-token` вы можете получить токен аутентификации на основе параметров аутентификации, указанных в профиле, переменных окружения или параметрах командной строки.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] auth get-token [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды получения токена:

```bash
{{ ydb-cli }} auth get-token --help
```

## Параметры подкоманды {#options}

Параметр | Описание
---|---
`-f, --force` | Вывести токен без запроса подтверждения.
`--timeout` | Время ожидания ответа клиента в миллисекундах. После истечения этого времени нет смысла ждать результат.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Получение токена с подтверждением {#with-prompt}

По умолчанию команда запрашивает подтверждение перед выводом токена, так как токен будет выведен в консоль:

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

Для автоматизации или использования в скриптах используйте опцию `--force` для вывода токена без запроса подтверждения:

```bash
{{ ydb-cli }} -p quickstart auth get-token --force
```

Результат:

```text
t1.eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Использование в скриптах {#in-scripts}

Команда может быть использована для получения токена в скриптах:

```bash
TOKEN=$({{ ydb-cli }} -p quickstart auth get-token --force)
echo "Token: $TOKEN"
```

