# table_service_config

Секция `table_service_config` содержит параметры конфигурации для сервиса таблиц, включая настройки спиллинга.

## resource_manager {#resource-manager}

В подсекции `resource_manager` задаются параметры управления ресурсами сервиса таблиц.

### Кэш level table векторного индекса

Level table (`indexImplLevelTable`) векторного индекса [`vector_kmeans_tree`](../../dev/vector-indexes-kmeans-tree-type.md#index-structure) хранит центроиды, к которым {{ ydb-short-name }} обращается на каждом шаге спуска по дереву кластеров во время векторного поиска. Чтобы не перечитывать эти центроиды из распределённого хранилища при каждом запросе, {{ ydb-short-name }} может кэшировать их в памяти на каждом узле базы данных.

Кэш создаётся отдельно на каждом узле, использует LRU-вытеснение и наращивает свой размер до указанного лимита постепенно.

```yaml
table_service_config:
  resource_manager:
    kqp_level_cache_max_size_bytes: 0
    kqp_level_cache_increase_batch_size_bytes: 33554432
```

#### resource_manager.kqp_level_cache_max_size_bytes

**Тип:** `uint64`  
**По умолчанию:** `0` (кэш отключён)  
**Описание:** Максимальный размер кэша level table векторного индекса в байтах на один узел базы данных. Установка ненулевого значения включает кэш.

#### resource_manager.kqp_level_cache_increase_batch_size_bytes

**Тип:** `uint64`  
**По умолчанию:** `33554432` (32 МиБ)  
**Описание:** Шаг увеличения размера кэша level table. Кэш растёт порциями такого размера до значения `kqp_level_cache_max_size_bytes` и уменьшается такими же порциями при снижении лимита.

### Лимиты памяти выполнения запросов {#query-execution-memory-limits}

В подсекции `resource_manager` задаётся порог спиллинга относительно пула памяти запросов на [узле](../../concepts/glossary.md#node). Размер этого пула регулируется параметрами [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit) в `memory_controller_config`.

```yaml
table_service_config:
  resource_manager:
    spilling_percent: 80
```

#### resource_manager.spilling_percent {#spilling-percent}

**Тип:** `double`  
**По умолчанию:** `80`  
**Описание:** Порог заполнения пула памяти запросов, при котором {{ ydb-short-name }} начинает считать [спиллинг](../../concepts/query_execution/spilling.md) предпочтительным способом управления памятью. Базой для расчёта служит пул Query Processor, размер которого задаётся [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit).

Когда суммарное потребление памяти запросами на узле превышает `spilling_percent` процентов от доступного пула, вычислительные операции, поддерживающие спиллинг (Grace Hash Join, агрегации и др.), получают сигнал выгружать промежуточные данные на диск вместо дальнейшего наращивания потребления RAM.

Например, при значении по умолчанию `80` спиллинг активируется, когда пул запросов заполнен примерно на 80%.

Порог применяется:

- к общему пулу памяти запросов на узле (размер — см. [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit));
- к [resource pool](../../concepts/glossary.md#resource-pool), если запрос выполняется в workload-пуле с ограничением `total_memory_limit_percent_per_node`.

{% note info %}

`spilling_percent` не задаёт лимит на объём файлов спиллинга на диске. За дисковые квоты отвечает [`local_file_config.max_total_size`](#local-file-config-max-total-size) в секции `spilling_service_config`.

{% endnote %}

##### Взаимодействие с другими параметрами

| Параметр | Уровень | Роль |
| --- | --- | --- |
| `query_execution_limit_percent` / `query_execution_limit_bytes` | Узел (QP) | Размер пула памяти запросов |
| `spilling_percent` | Запрос / пул | Порог заполнения пула, после которого предпочтителен спиллинг |
| `activities_limit_percent` | Узел | Общий лимит памяти для всех компонентов-активностей (QP, компактизация и др.) |

`spilling_percent` определяет, когда пул запросов на узле настолько заполнен, что дальнейший рост в RAM должен уступить место спиллингу. `activities_limit_percent` ограничивает память активностей в целом и косвенно влияет на доступный объём RAM, но не заменяет пул, относительно которого считается `spilling_percent`.

##### Рекомендации

- Уменьшайте `spilling_percent` (например, до `70`), если нужно раньше переводить тяжёлые запросы на диск и снизить риск исчерпания пула памяти;
- Увеличивайте `spilling_percent` (например, до `90`), если дисковый спиллинг слишком часто снижает производительность, а на узле достаточно RAM;
- Согласовывайте `spilling_percent` с [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit): при увеличении лимита пула запросов можно поднять порог спиллинга, если на узле достаточно RAM.

## spilling_service_config

[Спиллинг](../../concepts/query_execution/spilling.md) — это механизм управления памятью в {{ ydb-short-name }}, который временно сохраняет данные на диск при нехватке оперативной памяти.

### Включение {#enable}

Спиллинг включён по умолчанию. Следующий параметр управляет включением и отключением сервиса спиллинга.

#### local_file_config.enable {#local-file-config-enable}

**Расположение:** `table_service_config.spilling_service_config.local_file_config.enable`  
**Тип:** `boolean`  
**По умолчанию:** `true`  
**Описание:** Включает или отключает сервис спиллинга. При отключении (`false`) [спиллинг](../../concepts/query_execution/spilling.md) не функционирует, что может привести к ошибкам при обработке больших объёмов данных.

##### Возможные ошибки

- `Spilling Service not started` / `Service not started` — попытка использования спиллинга при выключенном Spilling Service. См. [{#T}](../../troubleshooting/spilling/service-not-started.md)

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
```

### Основные параметры конфигурации

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480
```

### Конфигурация директории

#### local_file_config.root

**Тип:** `string`  
**По умолчанию:** `""` (временная директория)  
**Описание:** Файловая директория для сохранения файлов спиллинга.

Для каждого процесса `ydbd` создается отдельная директория с уникальным именем. Директории спиллинга имеют следующий формат имени:

`node_<node_id>_<spilling_service_id>`

Где:

- `node_id` — идентификатор [узла](../../concepts/glossary.md#node)
- `spilling_service_id` — уникальный идентификатор экземпляра, который создается при инициализации [Spilling Service](../../contributor/spilling-service.md) один раз при запуске процесса ydbd

Файлы спиллинга хранятся внутри каждой такой директории.

Пример полного пути к директории спиллинга:

```bash
/tmp/spilling-tmp-<username>/node_1_32860791-037c-42b4-b201-82a0a337ac80
```

Где:

- `/tmp` — значение параметра `root`
- `<username>` — имя пользователя, под которым запускается процесс `ydbd`

**Важные замечания:**

- При запуске процесса все существующие директории спиллинга в указанной директории автоматически удаляются. Директории спиллинга имеют специальный формат имени, который включает идентификатор экземпляра, генерируемый один раз при запуске процесса ydbd. При запуске нового процесса все директории в директории спиллинга, которые соответствуют формату имени, но имеют другой `spilling_service_id` от текущего, удаляются.
- Директория должна иметь достаточные права на запись и чтение для пользователя, под которым запускается ydbd

{% note info %}

Спиллинг выполняется только на [узлах базы данных](../../concepts/glossary.md#database-node).

{% endnote %}

##### Возможные ошибки

- `Permission denied` — недостаточные права доступа к директории. См. [{#T}](../../troubleshooting/spilling/permission-denied.md)

#### local_file_config.max_total_size {#local-file-config-max-total-size}

**Тип:** `uint64`  
**По умолчанию:** `21474836480` (20 GiB)  
**Описание:** Максимальный суммарный размер всех файлов спиллинга на каждом [узле](../../concepts/glossary.md#node). При превышении лимита операции спиллинга завершаются ошибкой. Общий лимит спиллинга во всем кластере равен сумме значений `max_total_size` со всех узлов.

##### Рекомендации

- Устанавливайте значение исходя из доступного дискового пространства

##### Возможные ошибки

- `Total size limit exceeded: X/YMb` — превышен максимальный суммарный размер файлов спиллинга. См. [{#T}](../../troubleshooting/spilling/total-size-limit-exceeded.md)

### Управление памятью {#memory-management}

#### Связь с memory_controller_config

Активация спиллинга тесно связана с настройками контроллера памяти. Подробная конфигурация `memory_controller_config` описана в [отдельной статье](memory_controller_config.md).

Ключевым параметром для спиллинга является **`activities_limit_percent`**, который определяет объем памяти, выделяемый для активностей по обработке запросов. От этого параметра зависит доступная память для пользовательских запросов и, соответственно, частота активации спиллинга.

#### Порог спиллинга в resource_manager

Непосредственный порог, при котором вычислительные операции переключаются на спиллинг, регулируется параметром [`resource_manager.spilling_percent`](#spilling-percent). Он определяет, при каком заполнении пула памяти Query Processor промежуточные данные запроса начинают выгружаться на диск. Размер пула задаётся в [`memory_controller_config`](memory_controller_config.md#query-execution-limit). Подробнее см. раздел [Лимиты памяти выполнения запросов](#query-execution-memory-limits).

#### Влияние на спиллинг

- При увеличении `activities_limit_percent` больше памяти доступно для запросов → спиллинг активируется реже
- При уменьшении `activities_limit_percent` меньше памяти доступно для запросов → спиллинг активируется чаще
- При уменьшении `spilling_percent` спиллинг включается при меньшем заполнении пула запросов
- При увеличении `spilling_percent` задачи дольше наращивают потребление RAM, прежде чем перейти к спиллингу

{% note warning %}

Важно учитывать, что сам спиллинг также требует память. Если установить `activities_limit_percent` слишком высоким, память может все равно закончиться несмотря на спиллинг, поскольку механизм спиллинга сам потребляет ресурсы памяти.

{% endnote %}

### Требования к файловой системе

#### Файловые дескрипторы

{% note info %}

Для получения информации о настройке лимитов файловых дескрипторов при первоначальном развертывании см. раздел [Лимиты файловых дескрипторов](../../devops/deployment-options/manual/initial-deployment/deployment-preparation.md#file-descriptors).

{% endnote %}

### Примеры конфигурации

#### Высоконагруженная система

Для максимальной производительности в высоконагруженных системах рекомендуется увеличить размер спиллинга:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 107374182400   # 100 GiB
```

#### Ограниченные ресурсы

Для систем с ограниченными ресурсами рекомендуется использовать консервативные настройки:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 5368709120     # 5 GiB
```

### Полный пример

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/spilling"
      max_total_size: 53687091200    # 50 GiB
```

## См. также

- [Концепция спиллинга](../../concepts/query_execution/spilling.md)
- [Архитектура Spilling Service](../../contributor/spilling-service.md)
- [Устранение неполадок спиллинга](../../troubleshooting/spilling/index.md)
- [Конфигурация контроллера памяти](memory_controller_config.md)
- [Мониторинг {{ ydb-short-name }}](../../devops/observability/monitoring.md)
- [Диагностика производительности](../../troubleshooting/performance/index.md)
