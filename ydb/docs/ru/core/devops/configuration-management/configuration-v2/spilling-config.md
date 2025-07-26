# Конфигурация спиллинга

## Обзор

[Спиллинг](../../../concepts/spilling.md) — это механизм управления памятью в YDB, который позволяет временно сохранять данные на диск при нехватке оперативной памяти. Данный раздел описывает параметры конфигурации для настройки спиллинга в продакшн-среде.

Все настройки спиллинга находятся в секции `table_service_config`, которая располагается на том же уровне, что и `host_configs`.

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

## Глобальные настройки спиллинга

### enable_query_service_spilling

**Расположение:** `table_service_config.enable_query_service_spilling`  
**Тип:** `boolean`  
**По умолчанию:** `true`  
**Описание:** Глобальная опция, включающая спиллинг в каналах передачи данных между задачами.

```yaml
table_service_config:
  enable_query_service_spilling: true
```

**Важно:** Эта настройка работает совместно с локальной конфигурацией сервиса спиллинга. При отключении (`false`) спиллинг в каналах не функционирует даже при включенном `spilling_service_config`.

### enable_spilling_nodes

**Расположение:** `table_service_config.enable_spilling_nodes`  
**Тип:** `string`  
**Возможные значения:** `"All"` | `"GraceJoin"` | `"Aggregate"` | `"None"`  
**По умолчанию:** `"All"`  
**Описание:** Управляет включением спиллинга в вычислительных узлах.

```yaml
table_service_config:
  enable_spilling_nodes: "All"
```

## Основные параметры конфигурации

### Сервис спиллинга (spilling_service_config)

**Расположение:** `table_service_config.spilling_service_config`

Основная конфигурация сервиса спиллинга определяется в секции `spilling_service_config`:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

#### Enable

**Тип:** `boolean`  
**По умолчанию:** `true`  
**Описание:** Включает или отключает сервис спиллинга. При отключении (`false`) [спиллинг](../../../concepts/spilling.md) не функционирует, что может привести к ошибкам Out of Memory при обработке больших объемов данных.

#### Root

**Тип:** `string`  
**По умолчанию:** `""` (автоматическое определение)  
**Описание:** Директория для сохранения файлов спиллинга. При пустом значении система автоматически создает директорию в формате `{TMP}/spilling-tmp-<username>`.

**Важные особенности:**

- При старте процесса все существующие файлы спиллинга в указанной директории удаляются автоматически
- Директория должна иметь достаточные права для записи

**Рекомендации:**

- Используйте отдельный диск или раздел для спиллинга
- Предпочтительно использовать быстрые накопители (SSD/NVMe)
- Убедитесь в наличии достаточного свободного места

#### MaxTotalSize

**Тип:** `uint64`  
**По умолчанию:** `21474836480` (20 GiB)  
**Описание:** Максимальный суммарный размер всех файлов спиллинга. При превышении лимита операции спиллинга завершаются ошибкой.

**Рекомендации:**

- Устанавливайте значение исходя из доступного дискового пространства

#### MaxFileSize

{% note warning %}

Данная опция является устаревшей (deprecated) и будет удалена в будущих версиях.

{% endnote %}

**Тип:** `uint64`  
**По умолчанию:** `5368709120` (5 GiB)  
**Описание:** Максимальный размер одного файла спиллинга.

#### MaxFilePartSize

{% note warning %}

Данная опция является устаревшей (deprecated) и будет удалена в будущих версиях.

{% endnote %}

**Тип:** `uint64`  
**По умолчанию:** `104857600` (100 MB)  
**Описание:** Максимальный размер одной части файла. Файлы спиллинга могут состоять из нескольких частей, каждая размером до `MaxFilePartSize`. Суммарный размер всех частей не должен превышать `MaxFileSize`.

### Конфигурация пула потоков (TIoThreadPoolConfig)

#### WorkersCount

**Тип:** `uint32`  
**По умолчанию:** `2`  
**Описание:** Количество рабочих потоков для обработки операций ввода-вывода спиллинга.

**Рекомендации:**

- Увеличивайте для высоконагруженных систем
- Учитывайте количество CPU-ядер на сервере

#### QueueSize

**Тип:** `uint32`  
**По умолчанию:** `1000`  
**Описание:** Размер очереди операций спиллинга. Каждая задача отправляет только один блок данных на спиллинг одновременно, поэтому большие значения обычно не требуются.

## Управление памятью

### Связь с memory_controller_config

Активация спиллинга тесно связана с настройками контроллера памяти. Подробная конфигурация `memory_controller_config` описана в [отдельной статье](../../../reference/configuration/index.md#memory-controller-config).

Ключевым параметром для спиллинга является **`activities_limit_percent`**, который определяет объем памяти, выделяемый для фоновых активностей. От этого параметра зависит доступная память для пользовательских запросов и, соответственно, частота активации спиллинга.

**Влияние на спиллинг:**

- При увеличении `activities_limit_percent` остается меньше памяти для выполнения запросов → спиллинг активируется чаще
- При уменьшении `activities_limit_percent` больше памяти доступно для запросов → спиллинг активируется реже

## Требования к файловой системе

### Файловые дескрипторы

Для корректной работы спиллинга необходимо увеличить лимит одновременно открытых файловых дескрипторов до 10000.

## Примеры конфигурации

### Высоконагруженная система

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/ssd/ydb/spill"
      max_total_size: 107374182400   # 100 GiB
      max_file_size: 10737418240     # 10 GiB
      max_file_part_size: 1073741824 # 1 GiB
      io_thread_pool:
        workers_count: 8
        queue_size: 2000
```

### Ограниченные ресурсы

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "GraceJoin"  # Только для join операций
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 5368709120     # 5 GiB
      max_file_size: 1073741824      # 1 GiB
      max_file_part_size: 52428800   # 50 MB
      io_thread_pool:
        workers_count: 1
        queue_size: 500
```

## Устранение неполадок

### Частые проблемы

1. **Service not started...**
    Попытка включить спиллинг при выключенном Spilling Service.
    - Выставите `table_service_config.enable_query_service_spilling: true`
    Подробнее про архитектуру спиллинга читайте в разделе [Архитектура спиллинга в YDB](../../../concepts/spilling.md#архитектура-спиллинга-в-ydb)

2. **Total size limit exceeded...**
   - Увеличьте `MaxTotalSize`

## См. также

- [Концепция спиллинга](../../../concepts/spilling.md)
- [Конфигурация контроллера памяти](../../../reference/configuration/index.md#memory-controller-config)
- [Мониторинг YDB](../../observability/monitoring.md)
- [Диагностика производительности](../../../troubleshooting/performance/index.md)