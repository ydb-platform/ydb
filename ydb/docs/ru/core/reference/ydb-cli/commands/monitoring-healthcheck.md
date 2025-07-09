# Проверка состояния базы данных

{{ ydb-short-name }} имеет встроенную систему самодиагностики, с помощью которой можно получить краткий отчёт о состоянии базы данных и информацию о выявленных проблемах.

Общий вид команды:

```bash
ydb [global options...] monitoring healthcheck [options...]
```

* `global options` — [глобальные параметры](global-options.md),
* `options` — [параметры подкоманды](#options).

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
||`--timeout` | Время, в течение которого должна быть выполнена операция на сервере, мс.||
||`--format` | Формат вывода. Возможные значения:

* `pretty` — обобщенный статус базы данных. Возможные варианты значений приведены в [таблице](../../ydb-sdk/health-check-api.md#selfcheck-result).
* `json` — подробный ответ в формате JSON, содержащий иерархический список обнаруженных проблем. Перечень возможных проблем приведен в документации [Healthcheck API](../../ydb-sdk/health-check-api.md#issues).

Значение по умолчанию — `pretty`.||
|#

## Примеры {#examples}

### Краткий результат проверки {#example-pretty}

```bash
{{ ydb-cli }} --profile quickstart monitoring healthcheck --format pretty
```

Проблем с базой не обнаружено:

```bash
Healthcheck status: GOOD
```

Обнаружена деградация базы данных:

```bash
Healthcheck status: DEGRADED
```

### Подробный результат проверки {#example-json}


```bash
{{ ydb-cli }} --profile quickstart monitoring healthcheck --format json
```

Проблем с базой не обнаружено:

```json
{
 "self_check_result": "GOOD",
 "location": {
  "id": 51059,
  "host": "my-host.net",
  "port": 19001
 }
}
```

Обнаружена деградация базы данных:

```json
{
 "self_check_result": "DEGRADED",
 "issue_log": [
  {
   "id": "YELLOW-b3c0-70fb",
   "status": "YELLOW",
   "message": "Database has multiple issues",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "reason": [
    "YELLOW-b3c0-1ba8",
    "YELLOW-b3c0-1c83"
   ],
   "type": "DATABASE",
   "level": 1
  },
  {
   "id": "YELLOW-b3c0-1ba8",
   "status": "YELLOW",
   "message": "Compute is overloaded",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "reason": [
    "YELLOW-b3c0-343a-51059-User"
   ],
   "type": "COMPUTE",
   "level": 2
  },
  {
   "id": "YELLOW-b3c0-343a-51059-User",
   "status": "YELLOW",
   "message": "Pool usage is over than 99%",
   "location": {
    "compute": {
     "node": {
      "id": 51059,
      "host": "my-host.net",
      "port": 31043
     },
     "pool": {
      "name": "User"
     }
    },
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "type": "COMPUTE_POOL",
   "level": 4
  },
  {
   "id": "YELLOW-b3c0-1c83",
   "status": "YELLOW",
   "message": "Storage usage over 75%",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "type": "STORAGE",
   "level": 2
  }
 ],
 "location": {
  "id": 117,
  "host": "my-host.net",
  "port": 19001
 }
}
```
