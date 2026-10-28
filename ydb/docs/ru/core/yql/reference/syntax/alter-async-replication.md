# ALTER ASYNC REPLICATION

Вызов `ALTER ASYNC REPLICATION` изменяет параметры и состояние экземпляра [асинхронной репликации](../../../concepts/async-replication.md).

## Синтаксис {#syntax}

```yql
ALTER ASYNC REPLICATION <name> SET (option = value [, ...])
```

где:

* `name` — имя экземпляра асинхронной репликации.
* `SET (option = value [, ...])` — [параметры](#params) асинхронной репликации.

### Параметры {#params}

* `STATE` — состояние асинхронной репликации. Возможные значения:
  * `PAUSED`: приостановить применение изменений. Потоки изменений на источнике продолжают накапливать записи в пределах времени хранения. Уже подключённые объекты-реплики остаются доступны только для чтения.
  * `STANDBY`: запустить или возобновить репликацию изменений. Для репликации, созданной с `INITIAL_SCAN = FALSE`, подключает заранее восстановленные целевые таблицы. Перед запуском завершите импорт всех таблиц, как описано в [руководстве по первоначальной синхронизации](../../../devops/async-replication-initial-sync.md).
  * `DONE` — [завершение процесса асинхронной репликации](../../../concepts/async-replication.md#done). Для принудительного завершения укажите также `FAILOVER_MODE = 'FORCE'`.
* `FAILOVER_MODE` — режим завершения репликации. Применимо только совместно с `STATE = 'DONE'`. Для `PAUSED` и `STANDBY` этот параметр не требуется. Возможные значения:
  * `FORCE` — принудительное переключение состояния.
* Настройки для аутентификации в базе-источнике одним из способов:

  {% include [x](_includes/async_replication_authentification.md) %}

## Примеры {#examples}

Следующий запрос запускает репликацию после [импорта первоначальных данных](../../../devops/async-replication-initial-sync.md#import):

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'STANDBY');
```

Следующий запрос приостанавливает применение изменений:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'PAUSED');
```

Для возобновления используйте `STATE = 'STANDBY'`. Время приостановки и последующей обработки накопленных изменений должно укладываться во [время хранения записей](../../../concepts/cdc.md#retention-period).

Следующий запрос принудительно завершит процесс асинхронной репликации:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = "DONE", FAILOVER_MODE = "FORCE");
```

Следующий запрос изменяет секрет:

```yql
ALTER ASYNC REPLICATION my_replication SET (TOKEN_SECRET_PATH = "my_token");
```

## См. также

* [Первоначальная синхронизация с помощью резервного копирования и восстановления](../../../devops/async-replication-initial-sync.md)
* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
