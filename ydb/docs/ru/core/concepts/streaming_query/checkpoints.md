# Чекпойнты

Стриминговые запросы в процессе работы записывают свое состояние в т.н. чекпойнт, он сохраняется с хранилище (в текущей или внешней YDB).

Чекпойнт предназначен для восстановления состояния после сбоя (падения нод, завершения запроса по ошибке) или после ручного рестарта запроса.
Чекпойнт состоит из:

- внутреннего состояния тасок (поддерживается только для [MATCH RECOGNIZE](../../../yql/reference/syntax/select/match_recognize) и [GROUP BY HOP](../../..//yql/reference/syntax/select/group-by#group-by-hop)),
- текущих [смещения](../topic#consumer-offset) для входных топиках,
- текущих смещений выходных топиков (для дедубликации данных на выходе при рестарте запроса).

## Очистка

При успешном сохранении чекпойнта предыдущие чекпойнты удаляются из хранилища. Т.е. в большинстве времени для одного запроса в хранилище хранится только один чекпойнт.

## Настройки

| Параметр | Назначение | Значение по умолчаню |
|----------|------------|----------------------|
| `checkpoints_config.checkpointing_period_millis` | Период записи | 30000 |
| `checkpoints_config.state_storage_limits.max_graph_checkpoints_size_bytes` | Максимальный размер всех чекпойнтов (одного запроса) | 30000 |
| `checkpoints_config.state_storage_limits.max_task_state_size_bytes` | Максимальный размер одной таски | ??? |
| `checkpoints_config.state_storage_limits.max_row_size_bytes` | Максимальный размер одной строки при сохранении | 16000000 |
| `checkpoints_config.checkpoint_garbage_config.enabled` | Включение очистки старых чекпойнтов | True |
| `checkpoints_config.local_storage.table_prefix` | Префикс (путь) в хранилище чекпойнтов | "" |

Пример конфига:

```yaml
query_service_config:
  checkpoints_config: !inherit
    TODO
```

## Отключение чекпойнтов

Для отключения (исключительно в тестовых целях) используетcя прагма `dq.DisableCheckpoints`.

```sql

CREATE OR REPLACE STREAMING QUERY `my_queries/query1`
BEGIN
    PRAGMA dq.DisableCheckpoints="True";
    INSERT INTO `source_name`.`output_topic_name` SELECT * FROM `source_name`.`input_topic_name`;
END;
```
