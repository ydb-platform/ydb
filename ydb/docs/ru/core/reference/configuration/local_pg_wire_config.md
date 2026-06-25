# local_pg_wire_config

В разделе `local_pg_wire_config` файла конфигурации {{ ydb-short-name }} включается и настраивается встроенный слушатель сетевого протокола PostgreSQL (pgwire).

{% note warning %}

Поддержка сетевого протокола PostgreSQL — экспериментальная функция. На production-кластерах её следует держать отключённой, если вы явно не используете pgwire для тестирования или миграции.

{% endnote %}

## Описание параметров

| Параметр | Тип | Значение по умолчанию | Описание |
| --- | --- | --- | --- |
| `enable_local_pg_wire` | bool | `false` | Включает или отключает слушатель сетевого протокола PostgreSQL. |
| `listening_port` | int32 | `5432` | Порт, на котором pgwire принимает подключения. Используется только при `enable_local_pg_wire: true`. |
| `address` | string | `::` | Адрес, на котором слушатель принимает подключения. |
| `ssl_certificate` | string | — | PEM-сертификат с закрытым ключом для TLS-подключений. |
| `tcp_not_delay` | bool | `true` | Включает опцию сокета `TCP_NODELAY`. |

## Пример заполненного конфига

```yaml
local_pg_wire_config:
  enable_local_pg_wire: true
  listening_port: 5432
  address: "::"
```

Также pgwire можно включить через скрытую опцию командной строки `ydbd` `--pgwire-port` — при этом `enable_local_pg_wire` выставляется в `true` автоматически.

В локальном Docker-образе {{ ydb-short-name }} pgwire включён по умолчанию через переменную окружения `YDB_ENABLE_LOCAL_PGWIRE` (значение по умолчанию `1`). Это отделено от `YDB_EXPERIMENTAL_PG`, которая включает дополнительные экспериментальные feature flags и по умолчанию выключена.
