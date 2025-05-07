# Устройство механизма конфигурации V2

Конфигурация V2 в {{ ydb-short-name }} реализует единый подход к управлению настройками кластера. Как пользоваться этим механизмом, описано в [разделе для DevOps](../devops/configuration-management/configuration-v2/config-overview.md), а эта статья сфокусирована именно на его техническом устройстве. Она будет полезна разработчикам и контрибьюторам {{ ydb-short-name }}, желающим внести изменения в этот механизм, а также всем, кто хочет более глубоко разобраться в происходящем на кластере {{ ydb-short-name }} при изменении конфигурации.

Конфигурация кластера {{ ydb-short-name }} хранится в нескольких местах и различные компоненты кластера координируют синхронизацию между ними:

- в виде набора файлов конфигурации узла в файловой системе каждого узла {{ ydb-short-name }} (необходимы для использования в момент запуска узла для подключения к остальному кластеру);
- в специальной области хранения метаданных на каждом [PDisk](../concepts/glossary.md#pdisk) (кворум из PDisk считается единственным источником правды о конфигурации);
- в локальной базе таблетки [DS Controller](../concepts/glossary.md#ds-controller) (для нужд [распределённого хранилища](../concepts/glossary.md#distributed-storage));
- в локальной базе таблетки [Console](../concepts/glossary.md#console).

Часть параметров вступает в силу на узле сразу после того, как изменённая конфигурация будет доставлена туда, а часть — только после перезапуска узла.

## Распределённая система конфигурации (Distconf)

[Distconf](../concepts/glossary.md#distributed-configuration) — это система управления конфигурацией V2 кластера {{ ydb-short-name }}, основанная на [Node warden](../concepts/glossary.md#node-warden), [узлах хранения](../concepts/glossary.md#storage-node) и их [PDisk](../concepts/glossary.md#pdisk)'ах. Все изменения конфигурации V2, включая первоначальную инициализацию, проходят через неё.

### Внутренние процессы Distconf

- **Binding**
  Узлы хранения формируют ациклический граф. Каждый узел подключается к случайному соседнему таким образом, чтобы не было циклов. В итоге получается дерево из узлов с явно выделенной вершиной-корнем — узлом, который не может никуда подключиться, т.к. все уже подключены к нему. Корневой узел становится **лидером**, через который проходят все команды и который может принимать решения, в том числе рассылать команды через описанный далее механизм Scatter. Привязка узлов происходит через события [`TEvInterconnect::TEvNodesInfo`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_binding.cpp#L5) и [`TEvInterconnect::TEvNodeConnected`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_binding.cpp#L109) в [`distconf_binding.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_binding.cpp).

- **Scatter/Gather**
  Лидер рассылает команды другим узлам по сформированному ранее графу (Scatter) и собирает результаты (Gather). Реализовано в [`distconf_scatter_gather.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_scatter_gather.cpp), задачи рассылки и сбора ответов хранятся в структуре [`TScatterTasks`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf.h#L235) и исполняются посредством методов `IssueScatterTask`, `CompleteScatterTask` и `AbortScatterTask`.

- **Finite State Machine (FSM)**
  В [`distconf_fsm.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_fsm.cpp) реализован конечный автомат, непосредственно управляющий конфигурацией, её изменениями и их распространение по кластеру. Он отвечает за propose и commit изменений, а также смену лидера.

- **Persistent Storage**
  Локальная запись и чтение метаданных ([`TPDiskMetadataRecord`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_distributed_config.proto#L38)) на PDisk'ах с обработкой событий [`TEvStorageConfigLoaded`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf.h#L82)/[`TEvStorageConfigStored`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf.h#L88) через [`TDistributedConfigKeeper`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf.h#L68).

- **InvokeOnRoot**
  Входная точка для команд администратора кластера: запросы [`TEvNodeConfigInvokeOnRoot`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_invoke_common.cpp#L338) обрабатываются в [`distconf_invoke_common.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_invoke_common.cpp), ответы возвращаются в виде [`TEvNodeConfigInvokeOnRootResult`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_distributed_config.proto#L230).

- **Dynamic**
  Узлы баз данных подписываются на события [`TEvNodeWardenDynamicConfigPush`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/node_warden_events.h#L75) через [`ConnectToStaticNode`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/nodewarden/distconf_dynamic.cpp#L10), чтобы получать обновления конфигурации в реальном времени.

- **Self-Heal**
  При использовании Distconf для [статической группы](../concepts/glossary.md#static-group) работает [Self-Heal](../maintenance/manual/selfheal.md) по аналогии с [динамическими группами](../concepts/glossary.md#dynamic-group).

- **Контроллер распределённого хранилища**
  DS-controller получает от Distconf изменения в конфигурации и использует их для работы распределённого хранилища.

- **Локальные YAML-файлы на узлах**
  Хранятся в директории, указанной аргументом запуска сервера `ydbd --config-dir`, и обновляются при получении информации о каждом изменении конфигурации. Эти файлы нужны при старте узла, чтобы обнаружить PDisk'ы и другие узлы кластера, а также установить начальные сетевые соединения. Данные в этих файлах могут быть устаревшими, особенно при выключении узла на длительное время.

### Базовый порядок работы Distconf

1. При старте узла Distconf пытается прочитать конфигурацию с локальных PDisk'ов.
2. Подключается к случайному узлу хранения для проверки актуальности конфигурации.
3. Если не удаётся подключиться, но есть кворум подключённых, становится лидером.
4. Лидер пытается инициировать первоначальную настройку кластера, если она разрешена.
5. Лидер рассылает актуальную конфигурацию всем узлам через Scatter/Gather.
6. Узлы сохраняют полученную конфигурацию в локальной области PDisk для метаданных и в директориях `--config-dir`.

## Итоговое распределение конфигурации

| Место хранения                                | Содержит                                                              |
|-----------------------------------------------|-----------------------------------------------------------------------|
| `TPDiskMetadataRecord` на кворуме из PDisk'ов | Истинную актуальную конфигурацию                                      |
| Локальная директория `--config-dir`           | Исходный YAML для старта (может быть устаревшим)                      |
| Console                                       | Актуальная копия (с минимальной задержкой)                            |
| DS-controller                                 | Подмножество конфигурации, необходимое для распределённого хранилища  |