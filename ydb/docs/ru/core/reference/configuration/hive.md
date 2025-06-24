# Настройка Hive

[Hive](../../concepts/glossary.md#hive) — компонент YDB, отвечающий за запуск [таблеток](../../concepts/glossary.md#tablet). В различных ситуациях и под разными паттернами нагрузки может возникнуть потребность в настройке его поведения.  Поведение Hive конфигурируется в секции `hive_config` [конфигурации](../../reference/configuration/hive.md) {{ ydb-short-name }}. Также часть опций из конфигурации доступна для редактирования через интерфейс [Hive web-viewer](../embedded-ui/hive.md#settings). Настройки, выставленные через интерфейс, имеют приоритет над указанными в конфигурации. Ниже перечислены все доступные опции, с указанием соответствующего названия опции в интерфейсе, если опцию возможно редактировать через интерфейс.

## Опции запуска таблеток {#boot}

С помощью этих опций можно регулировать с какой скоростью [запускаются таблетки](../../contributor/hive-booting.md) и как для них [выбираются узлы](../../contributor/hive-booting.md#findbestnode).

#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `max_tablets_scheduled` | MaxTabletsScheduled | Целое число | Максимальное число таблеток, одновременно находящихся в процессе старта на одном узле. | 100 ||
|| `max_boot_batch_size` | MaxBootBatchSize | Целое число | Максимальное число таблеток из [очереди запуска](../../contributor/hive-booting.md#bootqueue) Hive, обрабатываемых за раз. | 1000 ||
|| `node_select_strategy` | NodeSelectStrategy | Перечисление | Стратегия выбора узла для запуска таблетки. Возможные варианты:

- `HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM` — взвешенно-случайный выбор на основе потребления;
- `HIVE_NODE_SELECT_STRATEGY_EXACT_MIN` — выбор узла с минимальным потреблением;
- `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` — выбор случайного узла среди 7% узлов с наименьшим потреблением;
- `HIVE_NODE_SELECT_STRATEGY_RANDOM` — выбор случайного узла.

| `HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P` ||
|| `boot_strategy` | — | Перечисление | Регулирует поведение при запуске большого числа таблеток. Возможные варианты:

* `HIVE_BOOT_STRATEGY_BALANCED` — при достижении лимита в `max_tablets_scheduled` на одном узле останавливает запуск новых таблеток на всех узлах.
* `HIVE_BOOT_STRATEGY_FAST` — при достижении лимита в `max_tablets_scheduled` на одном узле продолжает запуск таблеток на других узлах.

Если при запуске большого количества таблеток один из узлов запускает таблетки чуть медленне других, то при использовании `HIVE_BOOT_STRATEGY_FAST` на этом узле будет запущено меньше таблеток, чем на остальных. При использовании `HIVE_BOOT_STRATEGY_BALANCED` в той же ситуации таблетки будут равномерно распределены по узлам, но их запуск займёт больше времени.

| `HIVE_BOOT_STRATEGY_BALANCED` ||
|| `default_tablet_limit` | — | Вложенная секция | Ограничения на запуск таблеток различных типов на одном узле. Указывается в формате списка, где каждый элемент имеет поля `type` и `max_count`. | Пустая секция ||
|| `default_tablet_preference` | —  | Вложенная секция | Приоритеты по выбору датацентров для запуска таблеток различных типов. Для каждого типа таблетки можно указать несколько групп датацентров. Датацентры внутри одной группы будут иметь одинаковый приоритет, а более ранняя группа будет иметь приориет над последующими. Пример формата:

```yaml
default_tablet_preference:
  - type: Coordinator
    data_centers_preference:
      - data_centers_group:
        - "dc-1"
        - "dc-2"
      - data_centers_group:
        - "dc-3"
```

| Пустая секция ||
|| `system_category_id` | — | Целое число | При указании любого отличного от 0 числа, все координаторы и медиаторы по возможности запускаются в одном и том же датацентре. | 1 ||
|# {wide-content}

### Пример

{% note info %}

В подсекциях `default_tablet_limit` и `default_tablet_preference` нужно указывать типы таблеток. Точные названия типов таблеток указаны в [глоссарии](../../concepts/glossary.md#tablet-types).

{% endnote %}

```yaml
hive_config:
  max_tablets_scheduled: 10
  node_select_strategy: HIVE_NODE_SELECT_STRATEGY_RANDOM
  boot_strategy: HIVE_BOOT_STRATEGY_FAST
  default_tablet_limit:
    - type: PersQueue
      max_count: 15
    - type: DataShard
      max_count: 100
  default_tablet_preference:
    - type: Coordinator
      data_centers_preference:
        - data_centers_group:
          - "dc-1"
          - "dc-2"
        - data_centers_group:
          - "dc-3"
```

## Опции автобалансировки {#autobalancing}

Эти опции регулируют процесс [автобалансировки](../../contributor/hive.md#autobalancing): в каких ситуациях он запускается, сколько таблеток с какими интервалами перевозит, как выбирает узлы и таблетки. Часть опций представлена в двух вариациях: для "emergency-балансировки", то есть балансировки при перегрузке одного или нескольких узлов, и для всех остальных видов балансировки.

#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `min_scatter_to_balance` | MinScatterToBalance | Вещественное число | Порог метрики [Scatter](../../contributor/hive.md#scatter) для ресурсов CPU, Memory, Network. Имеет приоритет ниже, чем параметры ниже. | 0.5 ||
|| `min_cpuscatter_to_balance` | MinCPUScatterToBalance | Вещественное число | Порог метрики Scatter для ресурса CPU. | 0.5 ||
|| `min_memory_scatter_to_balance` | MinMemoryScatterToBalance | Вещественное число | Порог метрики Scatter для ресурса Memory.| 0.5  ||
|| `min_network_scatter_to_balance` | MinNetworkScatterToBalance | Вещественное число | Порог метрики Scatter для ресурса Network. | 0.5 ||
|| `min_counter_scatter_to_balance` | MinCounterScatterToBalance | Вещественное число | Порог метрики Scatter для фиктивного ресурса [Counter](../../contributor/hive.md#counter). | 0.02 ||
|| `min_node_usage_to_balance` | MinNodeUsageToBalance | Вещественное число | Потребление ресурсов на узле ниже данного значения приравнивается к данному значению. Используется для того, чтобы не балансировать таблетки между малозагруженными узлами. | 0.1 ||
|| `max_node_usage_to_kick` | MaxNodeUsageToKick | Вещественное число | Порог потребления ресурсов на узле для запуска emergency-автобалансировки. | 0.9 ||
|| `node_usage_range_to_kick` | NodeUsageRangeToKick | Вещественное число | Минимальная разница в уровне потребления ресурсов между узлами, ниже которой автобалансировка считается нецелесообразной. | 0.2 ||
|| `resource_change_reaction_period` | ResourceChangeReactionPeriod | Целое число секунд | Частота обновления агрегированной статистики потребления ресурсов. | 10 ||
|| `tablet_kick_cooldown_period` | TabletKickCooldownPeriod | Целое число секунд | Минимальный период времени между перемещениями одной таблетки. | 600 ||
|| `spread_neighbours` | SpreadNeighbours | true/false | Запуск таблеток одного схемного объекта (таблицы, топика) по возможности на разных узлах. | true ||
|| `node_balance_strategy` | NodeBalanceStrategy | Перечисление | Стратегия выбора узла, с которого перевозятся таблетки при автобалансировке. Возможные варианты:

- `HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM` — взвешенно-случайный выбор на основе потребления;
- `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` — выбор узла с максимальным потреблением;
- `HIVE_NODE_BALANCE_STRATEGY_RANDOM` — выбор случайного узла.

| `HIVE_NODE_BALANCE_STRATEGY_HEAVIEST` ||
|| `tablet_balance_strategy` | TabletBalanceStrategy | Перечисление | Стратегия выбора таблетки для перевоза при автобалансировке. Возможные варианты:

- `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` — взвешенно-случайный выбор на основе потребления;
- `HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST` — выбор таблетки с максимальным потреблением;
- `HIVE_TABLET_BALANCE_STRATEGY_RANDOM` — выбор случайной таблетки.

| `HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_balance` | MinPeriodBetweenBalance | Вещественное число секунд | Минимальный период времени между двумя итерациями автобалансировки. Не относится к emergency-балансировке. | 0.2 ||
|| `balancer_inflight` | BalancerInflight | Целое число | Число таблеток, одновременно перезапускающихся в процессе автобалансировки. Не относится к emergency-балансировке. | 1 ||
|| `max_movements_on_auto_balancer` | MaxMovementsOnAutoBalancer | Целое число | Число перемещений таблеток за одну итерацию автобалансировки. Не относится к emergency-балансировке. | 1 ||
|| `continue_auto_balancer` | ContinueAutoBalancer | true/false | При включении следующая итерация балансировки запускается, не дожидаясь окончания `resource_change_reaction_period`. | true ||
|| `min_period_between_emergency_balance` | MinPeriodBetweenEmergencyBalance | Вещественное число секунд | Аналогично `min_period_between_balance`, но для emergency-балансировки. | 0.1 ||
|| `emergency_balancer_inflight` | EmergencyBalancerInfligh | Целое число | Аналогично `balancer_inflight`, но для emergency-балансировки. | 1 ||
|| `max_movements_on_emergency_balancer` | MaxMovementsOnEmergencyBalancer | Целое число | Аналогично `max_movements_on_auto_balancer`, но для emergency-балансировки. | 2 ||
|| `continue_emergency_balancer` | ContinueEmergencyBalancer | true/false | Аналогично `continue_auto_balancer`, но для emergency-балансировки. | true ||
|| `check_move_expediency` | CheckMoveExpediency | true/false | Проверка целесообразности перемещений таблеток. Если автобалансировка приводит к повышенному потреблению Hive процессорных ресурсов, можно отключить эту опцию. | true ||
|| `object_imbalance_to_balance` | ObjectImbalanceToBalance | Вещественное число | Порог метрики [дисбаланса таблеток одного объекта](../../contributor/hive.md#imbalance). | 0.02 ||
|| `less_system_tablets_moves` | LessSystemTabletMoves | true/false | Минимизация перемещения системных таблеток при автобалансировке. | true ||
|| `balancer_ignore_tablet_types` | BalancerIgnoreTabletTypes | Список типов таблеток. При выставлении через Hive UI — разделённый точкой с запятой. | Типы таблеток, на которые не распространяется автобалансировка. | Пустой список ||
|# {wide-content}

### Примеры

С помощью такого файла конфигурации можно полностью отключить все виды автобалансировки таблеток между узлами.

```yaml
hive_config:
  min_cpuscatter_to_balance: 1.0
  min_memory_scatter_to_balance: 1.0
  min_network_scatter_to_balance: 1.0
  min_counter_scatter_to_balance: 1.0
  max_node_usage_to_kick: 3.0
  object_imbalance_to_balance: 1.0
```

Таким файлом конфигурации можно отключить все виды автобалансировки между узлами для таблеток, участвующих в распределении транзакций, то есть [координаторов](../../concepts/glossary.md#coordinator) и [медиаторов](../../concepts/glossary.md#mediator). Точные названия типов таблеток указаны в [глоссарии](../../concepts/glossary.md#tablet-types).


```yaml
hive_config:
  balancer_ignore_tablet_types:
    - Coordinator
    - Mediator
```

При использовании Hive UI для такого же эффекта нужно записать `Coordinator;Mediator` в поле ввода для настройки BalancerIgnoreTabletTypes.

## Опции сбора метрик потребления вычислительных ресурсов {metrics}

Hive собирает с каждого узла [метрики потребления вычислительных ресурсов](../../contributor/hive.md#resources) — процессорного времени, оперативной памяти, сети — в целом по узлу и с разделением по таблеткам. Эти настройки позволяют регулировать сбор этих метрик, их нормировку и агрегацию.

#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `max_resource_cpu` | MaxResourceCPU | Целое число микросекунд | Максимальное потребление CPU на узле в секунду. Значение по умолчанию, используется только если узел не предоставляет значение при регистрации в Hive. | 10000000 ||
|| `max_resource_memory` | MaxResourceMemory | Целое число байт | Максимальное потребление памяти на узле. Значение по умолчанию, используется только если узел не предоставляет значение при регистрации в Hive. | 512000000000 ||
|| `max_resource_network` | MaxResourceNetwork | Целое число байт/секунду | Максимальное потребление полосы на узле. Значение по умолчанию, используется только если узел не предоставляет значение при регистрации в Hive. | 1000000000 ||
|| `max_resource_counter` | MaxResourceCounter | Целое число | Максимальное потребление виртуального ресурса Counter на узле. | 100000000 ||
|| `metrics_window_size` | MetricsWindowSize | Целое число миллисекунд | Размер окна, на котором агрегируются метрики потребления ресурсов таблетками. | 60000 ||
|| `resource_overcommitment` | ResourceOvercommitment | Вещественное число | Коэффициент переподписки на ресурсы узлов. | 3.0 ||
|| `pools_to_monitor_for_usage` | — | Названия пулов через запятую | Пулы актор-системы, потребление в которых учитывается при подсчётё потребления ресурсов узла. | System,User,IC ||
|# {wide-content}

## Опции распределения каналов между группами хранения {#storage}

Здесь перечислены опции, связанные с распределением [каналов](../../concepts/glossary.md#channel) таблеток по [группам хранения](../../concepts/glossary.md#storage-group): с учётом различных метрик, с выбором групп, с процессом автобалансировки каналов по группам.


{% note info %}

Эта таблица содержит продвинутые настройки, которые в большинстве случаев не требуют изменения.

{% endnote %}


#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `default_unit_iops` | DefaultUnitIOPS | Целое число | Значение по умолчанию для IOPS одного канала. | 1 ||
|| `default_unit_throughput` | DefaultUnitThroughput | Целое число байт/секунду | Значение по умолчанию для потребления пропускной способности одним каналом. | 1000 ||
|| `default_unit_size` | DefaultUnitSize | Целое число байт | Значение по умолчанию для потребления места на дисках одним каналом. | 100000000 ||
|| `storage_overcommit` | StorageOvercommit | Вещественное число | Коэффициент переподписки на ресурсы групп хранения. | 1.0 ||
|| `storage_balance_strategy` | StorageBalanceStrategy | Перечисление | Выбор параметра используемого для распределения каналов таблеток по группам хранения. Возможные варианты:

- `HIVE_STORAGE_BALANCE_STRATEGY_IOPS` — учитывается только IOPS;
- `HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT` — учитывается только потребление пропускной способности;
- `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` — учитывается только объём занятого места;
- `HIVE_STORAGE_BALANCE_STRATEGY_AUTO` — учитывается тот из параметров выше, чьё потребление максимально;

| `HIVE_STORAGE_BALANCE_STRATEGY_SIZE` ||
|| `storage_safe_mode` | StorageSafeMode | true/false | Проверка превышения максимального потребления ресурсов групп хранения. | true ||
|| `storage_select_strategy` | StorageSelectStrategy | Перечисление | Стратегия выбора группы хранения для канала таблетки. Возможные варианты:

- `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` — взвешенно-случайный выбор на основе потребления;
- `HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN` — выбор группы с минимальным потреблением;
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P` — выбор случайной группы среди 7% групп с наименьшим потреблением;
- `HIVE_STORAGE_SELECT_STRATEGY_RANDOM` — выбор случайной группы;
- `HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN` - выбор группы внтури пула хранения по принципу [Round-robin](https://ru.wikipedia.org/wiki/Round-robin_(%D0%B0%D0%BB%D0%B3%D0%BE%D1%80%D0%B8%D1%82%D0%BC)).
| `HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM` ||
|| `min_period_between_reassign` | MinPeriodBetweenReassign | Целое число секунд | Минимальный период времени между переназначениями групп хранения для каналов одной таблетки. | 300 ||
|| `storage_pool_fresh_period` | StoragePoolFreshPeriod | Целое число миллисекунд | Периодичность обновления информации о пулах хранения. | 60000 ||
|| `space_usage_penalty_threshold` | SpaceUsagePenaltyThreshold | Вещественное число | Минимальное отношение свободного места в целевой группе к свободному месту в исходной группе, при котором целевая группа будет пессимизирована путём применения к весу мультипликативного штрафа при перевозе канала. | 1.1 ||
|| `space_usage_penalty` | SpaceUsagePenalty | Вещественное число | Коэффициент штрафа пессимизации, описанной выше. | 0.2 ||
|| `channel_balance_strategy` | ChannelBalanceStrategy | Перечисление | Стратегия выбора канала для переназначения при балансировке каналов. Возможные варианты:

- `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` — взвешенно-случайный выбор на основе потребления;
- `HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST` — выбор канала с максимальным потреблением;
- `HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM` — выбор случайного канала.

| `HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM` ||
|| `max_channel_history_size` |  MaxChannelHistorySize | Целое число | Максимальный размер истории каналов. | 200 ||
|| `storage_info_refresh_frequency` | StorageInfoRefreshFrequency | Целое число миллисекунд | Частота обновления информации о пулах хранения. | 600000 ||
|| `min_storage_scatter_to_balance` | MinStorageScatterToBalance | Вещественное число | Порог метрики Scatter для групп хранения. | 999 ||
|| `min_group_usage_to_balance` | MinGroupUsageToBalance | Вещественное число | Порог потребления ресурсов группы хранения, ниже которого не запускается балансировка. | 0.1 ||
|| `storage_balancer_inflight` | StorageBalancerInflight | Целое число | Число таблеток, одновременно перезапускающихся во время балансировки каналов. | 1 ||
|# {wide-content}

## Опции отслеживания перезапусков {#restarts}

Hive отслеживает, как часто рестартуют различные узлы и таблетки, чтобы определять проблемные. С помощью данных опций можно настроить, какие именно таблетки или узлы будут считаться проблемными, и как это на них повлияет. На основании этой статистики узлы и таблетки попадают в отчёт [HealthCheck API](../ydb-sdk/health-check-api.md).

### Опции отслеживания перезапусков таблеток

#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `tablet_restart_watch_period` | — | Целое число секунд | Размер окна, на котором собирается статистика о числе рестартов таблетки. **Этот период используется только для статистики, передаваемой в HealthCheck.** | 3600 ||
|| `tablet_restarts_period` | — | Целое число миллисекунд | Размер окна, на котором считается количество рестартов таблетки для пессимизации запуска проблемных таблеток. | 1000 ||
|| `tablet_restarts_max_count` | — | Целое число | Количество рестартов на окне `tablet_restarts_period`, при превышении которого применяется пессимизация. | 2 ||
|| `postopone_start_period` | — | Целое число миллисекунд | Периодичность попыток запуска проблемных таблеток. | 1000 ||
|# {wide-content}

### Опции отслеживания перезапусков узлов

#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `node_restart_watch_period` | — | Целое число секунд | Размер окна, на котором собирается статистика о числе рестартов узла. | 3600 ||
|| `node_restarts_for_penalty` | NodeRestartsForPenalty | Целое число | Количество рестартов на окне `node_restart_watch_period`, после которого узлы получают понижение приоритета. | 3 ||
|# {wide-content}


## Прочее {#misc}

Здесь перечислены дополнительные настройки Hive.


{% note info %}

Эта таблица содержит продвинутые настройки, которые в большинстве случаев не требуют изменения.

{% endnote %}


#|
|| Название параметра в конфигурации | Название параметра в Hive web-viewer | Формат | Описание | Значение по умолчанию ||
|| `drain_inflight` | DrainInflight | Целое число | Число таблеток, одновременно перезапускающихся в процессе плавного перемещения всех таблеток с одного узла (drain). | 10 ||
|| `request_sequence_size` | — | Целое число | Количество идентификаторов таблеток, которое за один раз Hive базы данных запрашивает у корневого Hive. | 1000 ||
|| `min_request_sequence_size` | — | Целое число | Минимальное количество идентификаторов таблеток, которое за один раз корневой Hive выделяет для Hive базы данных. | 1000 ||
|| `max_request_sequence_size` | — | Целое число | Максимальное количество идентификаторов таблеток, которое за один раз выделяет для Hive базы данных. | 1000000 ||
|| `node_delete_period` | — | Целое число секунд | Период неактивности, по истечении которого узел удаляется из базы Hive. | 3600 ||
|| `warm_up_enabled` | WarmUpEnabled | true/false | При включении этой опции при старте базы Hive дожидается подключения всех узлов прежде чем начать запускать таблетки. При выключении все таблетки могут быть запущены на первом подключившемся узле. | true ||
|| `warm_up_boot_waiting_period` | MaxWarmUpBootWaitingPeriod | Целое число миллисекунд | Время ожидания старта всех известных узлов при старте базы. | 30000 ||
|| `max_warm_up_period` | MaxWarmUpPeriod | Целое число секунд | Максимальное время ожидания старта узлов при старте базы. | 600 ||
|| `enable_destroy_operations` | — | true/false | Разрешены ли деструктивные ручные операции. | false ||
|| `max_pings_in_flight` | — | Целое число | Максимальное количество параллельно устаналиваемых соединений с узлами. | 1000 ||
|| `cut_history_deny_list` | — | Список типов таблеток, разделённый запятой | Список типов таблеток, для которых игнорируется операция очистки истории. | ColumnShard,KeyValue,PersQueue,BlobDepot ||
|| `cut_history_allow_list` | — | Список типов таблеток, разделённый запятой | Список типов таблеток, для которых разрешена операция очистки истории. | DataShard ||
|| `scale_recommendation_refresh_frequency` | ScaleRecommendationRefreshFrequency | Целое число миллисекунд | Как часто обновляется рекомендация по количеству вычислительных узлов. | 60000 ||
|| `scale_out_window_size` | ScaleOutWindowSize | Целое число | Количество бакетов, на основе которых принимается решение о рекомендации увеличить число вычислительных узлов. | 15 ||
|| `scale_in_window_size` | ScaleInWindowSize | Целое число | Количество бакетов, на основе которых принимается решение о рекомендации уменьшить число вычислительных узлов. |5 ||
|| `target_tracking_cpumargin` | TargetTrackingCPUMargin | Вещественное число | Допустимое отклонение от целевого значения утилизации CPU при автоскейлинге. | 0.1 ||
|| `dry_run_target_tracking_cpu` | DryRunTargetTrackingCPU | Вещественное число | Целевое значение утилизации CPU для проверки, как работал бы автоскейлинг. | 0 ||
|# {wide-content}
