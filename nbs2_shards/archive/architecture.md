# Архитектура интеграции NBS cells с nbs2

Статус документа: архитектурное исследование; в рамках этой работы меняется
только документ, код интеграции не реализовывался.

Дата последней актуализации контекста: 2026-08-12.

Состояние кода, повторно проверенное при актуализации плана:

- YDB: `9cecd841b167fa84354e3f99c1a418c9e5b3f1a1`; изменения относительно исходной точки плана `0ff00e463a0c128b045d7a2581b8983369fd0b50` просмотрены;
- NBS: `5e95a6646c3cc8af5797ba307f99ec9431aaecbe`; ревизия не изменилась. Локальные untracked файлы не влияют на описанный cells data path.

Между предыдущей проверенной ревизией YDB `e2f1a664c49af07a6180be2b212bbd14871af4e2` и текущей ревизией изменения в `ydb/core/nbs`, `ydb/core/blockstore`, `ydb/services/nbs` и функциональных тестах NBS отсутствуют.

Релевантные изменения YDB после исходной точки:

- `e5c288d4feb`, `6f9b850de4d`, `77c34c5d481`: DDisk map API в
  `DbsController` теперь сохраняет и запрашивает связи
  `PartitionTabletId <-> DDisk/node`. Update/query handlers реализованы, но
  production-код вне самого controller пока не отправляет `UpdateDDiskMap`,
  поэтому live map ещё не гарантированно заполнена. В схеме по-прежнему нет
  logical `DiskId`, `VolumeConfig`, `VolumeTabletId` или `SchemeVersion`, и она
  не заменяет описанный ниже volume resolver;
- `d04797f5c99`: добавлены `SSProxy::DestroyVolume` и transport-операция
  `DeleteTabletChunks` как primitives для удаления диска. Публичный
  `DeletePartition` их пока не вызывает, поэтому законченный delete workflow
  ещё отсутствует;
- `a9a849e0f94`, `989a7c9cab6`, `c23af0e5616`: уменьшен CPU overhead time
  predictor и request log titles, добавлен учёт DDisk io_uring в device
  overestimation monitoring. Архитектура data path не изменилась, но
  performance baselines нужно снимать на одной зафиксированной ревизии и с
  одинаковой конфигурацией;
- `60ea0dcdb13`: добавлена latency monitoring page partition. Она полезна для
  диагностики backend, но не заменяет gateway/session/RPC observability из
  production-шага 3;
- `6a50621b4d3`: общая gRPC-инфраструктура получила поддержку user-facing tracing. Изменение не затрагивает регистрацию NBS-сервисов, multiplexing по полному имени gRPC service или предлагаемый data path gateway.

Остальные изменения в рассмотренном диапазоне не меняют discovery, mount или
I/O-архитектуру этого плана.

Документ предназначен как handoff для человека и для продолжения работы в
новой сессии. Он фиксирует вводные, подтверждённое по коду устройство систем,
рассмотренные варианты, рекомендуемую архитектуру и поэтапный план реализации.

Использованные документы:
- разбор кода и работы с cells: /home/yudovmaksim/nbs/cloud/blockstore/libs/cells/README.md
- настройка и запуск nbs с cells /home/yudovmaksim/nbs/example/setup_cells.txt

> Навигация: [обзор и решения](README.md), [RFC](rfc.md),
> [нагрузочные MVP](mvp_benchmark.md),
> [функциональный MVP](mvp_functional.md), [production](production.md).

## Исходные материалы

### Legacy NBS cells

- Архитектурное описание:
  `/home/yudovmaksim/nbs/cloud/blockstore/libs/cells/README.md`
- Пример настройки и запуска двух cells:
  `/home/yudovmaksim/nbs/example/setup_cells.txt`
- Основная интеграционная точка endpoint/session:
  `/home/yudovmaksim/nbs/cloud/blockstore/libs/endpoints/session_manager.cpp`
- Реализация discovery:
  `/home/yudovmaksim/nbs/cloud/blockstore/libs/cells/impl/describe_volume.cpp`
- Выбор host целевой cell:
  `/home/yudovmaksim/nbs/cloud/blockstore/libs/cells/impl/cell_impl.cpp`
- Classic NBS gRPC contract:
  `/home/yudovmaksim/nbs/cloud/blockstore/public/api/grpc/service.proto`

### nbs2 в YDB

- Корень реализации:
  `/home/yudovmaksim/ydbwork/ydb/ydb/core/nbs/`
- Текущий экспериментальный YDB gRPC API:
  `/home/yudovmaksim/ydbwork/ydb/ydb/services/nbs/`
  `/home/yudovmaksim/ydbwork/ydb/ydb/core/grpc_services/rpc_nbs.cpp`
  `/home/yudovmaksim/ydbwork/ydb/ydb/core/grpc_services/rpc_nbs_io.cpp`
- Схема draft API:
  `/home/yudovmaksim/ydbwork/ydb/ydb/public/api/protos/draft/ydb_nbs.proto`
- Partition tablet и fast path:
  `/home/yudovmaksim/ydbwork/ydb/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/`
- SSProxy:
  `/home/yudovmaksim/ydbwork/ydb/ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/`

Локальное создание nbs2-диска:

```bash
/home/yudovmaksim/ydbwork/ydb/ydb/apps/ydbd/ydbd \
    admin bs config invoke --proto \
    'Command { DefineDDiskPool { BoxId: 1 Name: "ddp1" Geometry { NumFailRealms: 1 NumFailDomainsPerFailRealm: 5 NumVDisksPerFailDomain: 1 RealmLevelBegin: 10 RealmLevelEnd: 10 DomainLevelBegin: 10 DomainLevelEnd: 40 } PDiskFilter { Property { Type: ROT } } NumDDiskGroups: 10 } }'

./ydb-dstool -d -e grpc://localhost:2135 \
    nbs partition create \
    --block-size 4096 \
    --blocks-count 1048576 \
    --pool ddp1 \
    --type=ssd \
    --disk-id disk1
```

Перед переносом classic NBS API следует зафиксировать commit репозитория NBS и
проверять wire/error compatibility именно с этой ревизией. Предпочтительно
переносить минимальный protobuf/service API. Целые server libraries следует
подключать только если dependency spike покажет, что это действительно проще и
не приносит непропорциональный dependency graph.

## Как работают существующие NBS cells

### Discovery выполняется при создании endpoint

`TSessionManager::CreateSessionImpl` вызывает `DescribeVolume` до создания
endpoint. Cell manager параллельно отправляет `DescribeVolume`:

- в локальный NBS service;
- в настроенные peer cells по gRPC.

Первый корректный успешный ответ возвращает:

- `TVolume`;
- `cellId`, через который был найден диск.

Discovery не повторяется для каждого I/O.

### Backend выбирается один раз

В `CreateStorageDataClient`:

- пустой `cellId` означает обычный local storage provider;
- непустой `cellId` означает `CellManager->GetCellEndpoint(cellId, ...)`.

Полученный endpoint предоставляет:

- `IBlockStore` для `MountVolume`, `UnmountVolume` и control plane;
- `IStorage` для `ReadBlocksLocal`, `WriteBlocksLocal`, `ZeroBlocks`.

Для foreign cell `IStorage` является адаптером над NBS gRPC/RDMA client.

### Host для endpoint выбирается независимо от discovery

После успешного `DescribeVolume` cells не сохраняет host, приславший ответ.
`GetCellEndpoint` случайно выбирает один из активных hosts целевой cell.
Текущая сигнатура `GetCellEndpoint(cellId, clientConfig)` не получает
`DiskId`, поэтому выбрать gateway по размещению конкретной partition на этом
этапе невозможно.

Следствие: любой host nbs2 gateway должен уметь обслужить любой диск своей
cell. Нельзя строить решение на том, что gateway находится на том же узле, где
в данный момент работает partition tablet. Запрос, пришедший на произвольный
gateway host, направляется через tablet pipe на текущий node partition; после
migration тот же pipe переподключается к новому node.

Placement-aware выбор gateway допустим позднее как оптимизация лишнего
interconnect hop, но не как условие корректности. Для него потребуется менять
legacy cells: передавать `DiskId` в `GetCellEndpoint`, получать актуальное
размещение partition и переключать уже созданный endpoint после migration.
Даже при такой оптимизации gateway должен уметь обслужить запрос при stale
affinity через tablet pipe.

### Минимальный ожидаемый сетевой контракт

Для рабочего endpoint целевой foreign cell должна поддерживать classic NBS
protocol как минимум для следующих методов:

- `DescribeVolume`;
- `MountVolume`;
- `UnmountVolume`;
- `ReadBlocks`;
- `WriteBlocks`.

`Ping` нужен как operational smoke test, но cells не использует его для
маршрутизации endpoint. `ZeroBlocks` является частью classic API, однако до его
реализации gateway может возвращать `E_NOT_IMPLEMENTED`, если origin NBS не
рекламирует guest-у discard/write-zeroes.

Первый нагрузочный срез выполняется через gRPC. Сразу после него на том же
backend добавляется совместимый с существующим cells RDMA transport: control
plane (`DescribeVolume`, `MountVolume`, `UnmountVolume`) остаётся на gRPC, а
`ReadBlocks`/`WriteBlocks` переводятся на RDMA. Отдельный benchmark-only
протокол не вводится: оба замера проходят через NBS1 cells и отличаются только
data transport.

## Текущее состояние nbs2

### Регистрация диска

`CreatePartition` формирует `NKikimrBlockStore::TVolumeConfig` и отправляет
`TEvSSProxy::TEvCreateVolumeRequest`.

SSProxy создаёт в SchemeShard объект типа `EPathTypeBlockStoreVolume` под
настроенным `SchemeShardDir`. Scheme description содержит:

- `VolumeConfig`, включая `DiskId`, `BlockSize`, block count и media kind;
- стабильный volume tablet ID;
- список partitions;
- стабильные tablet IDs partitions.

Таким образом, для create/describe SchemeShard является фактическим
authoritative registry для соответствия:

```text
DiskId -> BlockStoreVolume
       -> VolumeConfig + VolumeTabletId + PartitionTabletIds + SchemeVersion
```

Это утверждение пока не распространяется на полный lifecycle. В SSProxy уже
добавлен `DestroyVolume`, удаляющий scheme object, а storage transport получил
`DeleteTabletChunks`. Однако текущий публичный `DeletePartition` их не вызывает:
он по-прежнему доходит до partition tablet, handler которого только отвечает
успехом. Поэтому scheme object и данные через этот workflow ещё не удаляются.
Полноценный resize также не является завершённым внешним workflow. До
production rollout необходимо связать новые delete primitives в единый
lifecycle и согласовать delete/resize, закрытие sessions и SchemeShard state.

Отдельной регистрации диска в cells или в SSProxy не требуется. SSProxy здесь
является proxy к SchemeShard, а не самостоятельным registry. После create
достаточно созданного scheme object; cells лениво узнаёт о диске через
`DescribeVolume` gateway при создании endpoint.

### Существующее разрешение `DiskId`

SSProxy уже позволяет получить из логического `DiskId` описание volume и стабильные `VolumeTabletId`/`PartitionTabletId`. Этот механизм можно переиспользовать в gateway для discovery и маршрутизации запросов через tablet pipe.

Существующий draft API `Ydb.Nbs` не используется в целевом data path: он не предоставляет classic NBS contract, ожидаемые cells операции `DescribeVolume`/`MountVolume`/`UnmountVolume`, session semantics и совместимые ответы без YDB Operations envelope. Поэтому направить cells на текущий `Ydb.Nbs` service без compatibility gateway нельзя.

### Локальный vhost nbs2

После готовности fast path partition создаёт:

```text
/tmp/<DiskId>.sock
```

и передаёт vhost server прямую ссылку на локальный `FastPathService`.

Этот endpoint привязан к процессу и узлу, где загружена partition tablet.
Он не является стабильной сетевой точкой cells. Перезапуск partition tablet в
том же процессе теперь частично поддержан через detach/attach `FastPathService`
и durable wrapper. Но migration на другой node оставляет старый локальный socket
без нового storage, а restart всего `ydbd` разрывает локальную vhost-связь.

Кроме того, независимое использование этого локального endpoint и endpoint из
legacy NBS создаёт риск нескольких несогласованных writers.
Сейчас local endpoint открывается безусловно; одного operational соглашения
«не подключаться к socket» недостаточно. Для gateway-сценария нужен явный
config, отключающий его создание.

### Реальные ограничения fast path

- Реализованы local read и write через sglist.
- Существующие `IStorage`/`FastPathService` выполняют локальный I/O до DDisk; gateway должен передавать им запрос после маршрутизации на текущий узел partition.
- Для первого этапа сохраняется явно проверяемое ограничение block size 4096.
- Ряд проверок использует `Y_ABORT_UNLESS`; некорректный внешний запрос нужно
  валидировать до передачи в fast path.
- `ZeroBlocksLocal` сейчас вызывает `Y_ABORT_UNLESS(false)` и не может быть
  доступен внешнему клиенту.
- В nbs2 уже появились `TStorageGate` и durable wrapper для detach/reattach и
  retry локального vhost endpoint. Их error classification и тесты полезно
  переиспользовать, но поверх origin NBS durable client нельзя бездумно ставить
  второй независимый retry loop.
- `VolumeDirect` tablet пока является skeleton и не содержит production
  session state и writer generation, необходимых для writer fencing.

## Почему resolver сначала должен использовать SSProxy/SchemeShard

Cells сам по себе не требует SchemeShard. Ему нужен только корректный ответ
`DescribeVolume`.

SchemeShard выбран для первой реализации потому, что сейчас это уже источник
истины для:

- существования `DiskId`;
- геометрии диска;
- media kind;
- стабильного `VolumeTabletId`;
- стабильного `PartitionTabletId`.

Создание отдельной таблицы `DiskId -> TabletId` потребовало бы синхронизации с
create/delete/resize и создало бы второй источник истины. При этом текущая
реализация delete/resize сама ещё должна быть доведена до согласованного
SchemeShard lifecycle; resolver не решает эту проблему автоматически.

Появившийся в `DbsController` DDisk map не является такой таблицей. Его целевая
схема описывает отношения partition tablet с DirectBlockGroup/DDisk/node для
задач размещения и обратного поиска partitions по storage nodes. Хранение,
update и query handlers уже реализованы, но production producer, отправляющий
`UpdateDDiskMap`, пока отсутствует, поэтому live map может оставаться пустой.
Кроме того, в схеме нет logical `DiskId` и полной геометрии volume. После
подключения producer её можно использовать для placement diagnostics, но не
как источник ответа `DescribeVolume`.

При этом gateway не должен быть архитектурно жёстко связан с SSProxy. Следует
ввести внутренний контракт примерно такого смысла:

```text
INbs2VolumeResolver::Describe(DiskId)
    -> VolumeConfig
    -> stable VolumeTabletId + partition tablet IDs
    -> SchemeVersion
    -> classified error
```

Первая реализация resolver использует локальный SSProxy/SchemeShard. В будущем
её можно заменить dedicated registry/controller, если он станет authoritative
source.

Resolver обязан:

1. быть доступным с любого gateway host;
2. различать окончательный `not found` и временную недоступность;
3. возвращать стабильные tablet IDs, а не текущие actor IDs;
4. возвращать `VolumeTabletId`, который в будущем станет естественной точкой
   authoritative mount/session model;
5. после реализации lifecycle быть согласованным с create/delete/resize.

Для sidecar, который не имеет прямого доступа к локальному SSProxy actor,
потребуется SchemeCache/public scheme client либо отдельный resolver API.

## Рассмотренные варианты интеграции

### Вариант A: classic NBS compatibility gateway внутри ydbd

Схема:

```text
NBS cells
    -> classic NBS gRPC
    -> gateway внутри ydbd
        -> resolver/SSProxy
        -> tablet pipe
        -> nbs2 partition
        -> FastPathService
```

Плюсы:

- legacy NBS и cells не требуют изменений;
- сохраняется существующий NBS wire contract;
- gateway имеет прямой доступ к actor system, SSProxy и tablet pipe;
- можно переиспользовать classic NBS public API; server libraries подключаются
  только при приемлемой стоимости зависимостей;
- nbs2 выглядит для cells как обычная foreign cell.

Минусы:

- необходимо перенести или сгенерировать classic NBS protobuf/service facade;
- появляются дополнительные зависимости; отдельный listener потребуется, если
  нельзя или не следует использовать существующий YDB gRPC server;
- нужно аккуратно развести namespace/protobuf packages classic NBS и nbs2.

Оценка: рекомендуемый вариант.

### Вариант B: отдельный compatibility gateway sidecar

Wire contract и логика такие же, как в варианте A, но gateway работает
отдельным процессом.

Плюсы:

- изоляция зависимостей и отказов от `ydbd`;
- независимый rollout.

Минусы:

- отдельный сервис для эксплуатации;
- sidecar необходимо подключить к YDB actor/tablet infrastructure либо дать
  ему стабильный публичный nbs2 transport;
- прямой локальный SSProxy actor недоступен.

Оценка: вариант рассмотрен, но не выбран. Compatibility gateway остаётся
внутри `ydbd`; отдельный sidecar не требуется.

### Вариант C: расширить draft `Ydb.Nbs` и добавить особый backend в cells

Потребовалось бы:

- добавить в cells новый тип peer endpoint;
- реализовать другой gRPC client;
- добавить в draft API describe/mount/unmount/zero;
- преобразовывать ошибки и данные;
- реализовать session/retry semantics.

Минусы:

- изменения нужны с обеих сторон;
- появляется второй протокол блочного storage;
- существующие NBS client/server библиотеки не переиспользуются;
- всё равно потребуется реализовать stable routing, session semantics и совместимый error model.

Оценка: возможно для короткого эксперимента, но не рекомендуется как итоговая
архитектура.

### Вариант D: прямой tablet pipe из legacy NBS process

В cells появился бы backend, который самостоятельно разрешает nbs2 disk и
ходит в partition tablet.

Плюсы:

- потенциально на один gRPC hop меньше.

Минусы:

- жёсткая связь legacy NBS с YDB actor topology nbs2;
- обычно требуется один физический YDB-кластер;
- нарушается изоляция cells между независимыми кластерами;
- существенно усложняется конфигурация и failure model legacy NBS.

Оценка: допустимо только как узкий same-cluster PoC.

### Вариант E: использовать локальный vhost socket nbs2

Не подходит из-за зависимости от текущего размещения partition tablet,
отсутствия сетевого discovery и невозможности выбрать произвольный gateway
host.

## Итоговая рекомендуемая архитектура

nbs2 должен выглядеть на сетевой границе как самостоятельная classic NBS cell.

Пользовательский NBD/vhost endpoint создаётся стандартным механизмом NBS1. Локальный vhost NBS2 в cells data path не участвует: за gateway переиспользуется внутренний `IStorage`/`FastPathService`, а доставка запроса к текущему узлу partition выполняется через tablet pipe.

```text
VM
  -> vhost/NBD endpoint в legacy NBS cell-a
  -> ISession
  -> TStorageDataClient
  -> cells remote endpoint
  -> classic NBS transport
       control: gRPC
       data: gRPC (MVP-1) или RDMA (MVP-2 и далее)
  -> nbs2 compatibility gateway
       control plane:
         DescribeVolume -> INbs2VolumeResolver -> SSProxy/SchemeShard
         Mount/Unmount  -> gateway process-local state в MVP-1/MVP-2/MVP-3
                         -> VolumeDirect authoritative state в production

       data plane:
         Read/Write
           -> per-session/per-volume gateway actor
          -> MVP-1/MVP-2/MVP-3:
               stable tablet pipe by PartitionTabletId -> partition tablet
          -> production:
               VolumeDirect session/generation check -> partition tablet
           -> IStorage/FastPathService
           -> DDisk
```

Ключевые свойства:

1. Внешний идентификатор всегда логический `DiskId`.
2. `PartitionTabletId` остаётся внутренней стабильной деталью маршрутизации.
3. Любой gateway host обслуживает любой диск cell.
4. Перезапуск или миграция partition восстанавливается через tablet pipe.
5. Ошибки переводятся в семантику classic NBS, необходимую cells.
6. Cells на стороне legacy NBS остаётся без специальных знаний о nbs2.
7. Неопределённый результат in-flight write не повторяется самим gateway:
   retry остаётся ответственностью durable client origin NBS.

### Где выполняется `StartEndpoint`

`StartEndpoint` отправляется в существующий legacy NBS server в cell, где
должен быть создан пользовательский endpoint, например в `cell-a`. Он не
отправляется в nbs2 gateway:

```text
пользователь/control plane
    -> StartEndpoint(disk1) в legacy NBS cell-a
        -> DescribeVolume(disk1) в local service и peer cells
        -> MountVolume(disk1) в найденный nbs2 gateway
        -> создание NBD/vhost socket на host legacy NBS cell-a
```

После запуска I/O идёт из созданного endpoint через `ReadBlocks`/`WriteBlocks`
в nbs2 gateway. Поэтому gateway уже в MVP-1 реализует backend-методы
`DescribeVolume`, `MountVolume`, `UnmountVolume`, `ReadBlocks` и `WriteBlocks`,
но не создаёт guest-facing endpoint и не реализует `StartEndpoint`.

### Сетевой listener внутри `ydbd`

В рекомендуемом варианте listener не является отдельным бинарём. Classic
protobuf service `NCloud.NBlockStore.NProto.TBlockStoreService` регистрируется
на существующем YDB gRPC server внутри того же `ydbd`, где работает nbs2.
Первый spike должен подтвердить, что server корректно multiplex-ит YDB и
classic NBS services по полному protobuf service name, а direct classic NBS
response не проходит через YDB Operations.

Первый deployment выглядит так:

```text
legacy NBS cells
    -> classic NBS gRPC на существующий YDB gRPC port
    -> TBlockStoreService facade внутри ydbd
    -> actor system/SSProxy/tablet pipe nbs2
```

Причины:

- минимальный новый runtime и dependency surface;
- не нужен дополнительный port для первого E2E;
- facade можно независимо включать/выключать конфигурацией;
- cells уже умеет подключаться к такому endpoint.

Отдельный configurable classic NBS port в том же `ydbd` остаётся deployment-вариантом, если существующий YDB endpoint недоступен из сети NBS1 cells, несовместим с требованиями classic NBS к TLS/authentication или параметрам gRPC-соединений либо если classic NBS traffic требуется независимо открывать и закрывать на сетевом уровне. Выбор между отдельным listener и существующим YDB gRPC port пока открыт. Sidecar рассмотрен, но не выбран: gateway остаётся компонентом процесса `ydbd`.

Выбор listener определяет сетевой endpoint и его настройки, но не меняет gateway contract, data path и распределение CPU, памяти или actor-system ресурсов внутри `ydbd`.

В MVP-2 рядом с gRPC service запускается совместимый classic NBS RDMA target.
У него отдельный `RdmaPort`, но это компонент того же процесса `ydbd`, а не
отдельный бинарь. Target обязан говорить на существующем NBS1 RDMA wire
protocol и направлять read/write в тот же nbs2 `IStorage`, что используется
gRPC facade. Сначала предпринимается перенос существующего RDMA target целиком;
минимальный wire-compatible adapter остаётся резервом, если полный перенос
даст неприемлемый dependency graph. Control plane cells при RDMA transport всё
равно остаётся на gRPC.

## Mount/session semantics

Для запуска NBS endpoint недостаточно только read/write. Origin NBS выполняет
`MountVolume` и ожидает:

- успешный ответ;
- описание volume;
- `SessionId`;
- последующее принятие I/O этой session.

### Разрешённый локальный session mode для MVP-1/MVP-2

Для первых нагрузочных E2E допускается явно непроизводственная реализация:

- gateway генерирует непрозрачный синтетический `SessionId`, например GUID,
  возвращает его в `TMountVolumeResponse.SessionId` и хранит только в своей
  in-memory таблице;
- in-memory mount state;
- отсутствие полноценного fencing;
- нормализация входных `LOCAL`/`REMOTE` mount requests к remote semantics
  внутри gateway;
- идемпотентный повторный `MountVolume` для одной комбинации
  `DiskId`/`ClientId`/`InstanceId`/`MountSeqNumber` и одинаковых существенных
  mount/access parameters;
- обязательная проверка `SessionId` для каждого I/O;
- `E_BS_INVALID_SESSION` для неизвестной, завершённой или потерянной после
  restart gateway session, чтобы classic NBS session выполнила remount/retry;
- явное отключение локального auto-vhost endpoint nbs2 конфигурацией.

Origin NBS сохраняет полученный `SessionId` и передаёт его в отдельных полях
`ReadBlocksRequest`, `WriteBlocksRequest`, `ZeroBlocksRequest` и
`UnmountVolumeRequest`. Gateway проверяет, что session существует, относится к
указанному `DiskId` и не была завершена. «Синтетический» здесь означает, что
session не создаётся и не хранится в `VolumeDirect`/SchemeShard и теряется при restart gateway.

В MVP-1/MVP-2 таблица может обслуживать один заранее настроенный диск и один
нагрузочный сценарий. В MVP-3 она становится нормальным process-local session
registry: поддерживает несколько дисков, идемпотентный mount key и полный
lifecycle до authoritative session state и writer fencing. Такой режим должен
быть отмечен feature
flag/config и не считаться готовым к production rollout.

### Требование для корректной эксплуатации

Classic NBS уже имеет authoritative client/mount state, `MountSeqNumber`,
обнаружение conflicting writers и отклонение stale session. Но этот механизм
защищает volumes старого NBS и не распространяется автоматически на nbs2 disk.
nbs2 реализует аналогичную семантику в `VolumeDirect`, выбранном authoritative
владельцем session state и writer generation. Проверка generation обеспечивает
writer fencing.

Fencing означает, что после выдачи права записи более новому writer старый
writer больше не может менять данные. Одной проверки in-memory `SessionId` в
gateway недостаточно: после restart состояние теряется, а два gateway hosts
могут независимо разрешить двух writers.

Mount state находится в `VolumeDirect` tablet и учитывает:

- `ClientId` и `InstanceId`;
- read-only/read-write access mode;
- `MountSeqNumber`, writer generation и writer fencing;
- конфликтующих writers;
- restart gateway;
- проверку `SessionId` и writer generation в `VolumeDirect` на каждом I/O;
- корректное unmount и inactive client cleanup.

Локальный auto-vhost nbs2 постоянно отключён в cells deployment, чтобы не
создавать второй путь вне общей ownership/session model. Его реализация
остаётся в коде для конфигураций nbs2 без cells. Текущий same-process
detach/reattach при перезапуске partition не заменяет authoritative session
state и writer fencing и не решает cross-node migration или restart `ydbd`.

## Error model

Cells принимает решение о наличии диска по ответам `DescribeVolume`, причём
его discovery-модель фактически различает только success, `E_NOT_FOUND` и
retriable error. Произвольный non-retriable error в `DescribeVolume` может
быть ошибочно интерпретирован как отсутствие диска и нарушить внутренний
инвариант cells. Поэтому gateway должен переводить ошибки так:

- отсутствующий path или path неправильного scheme type -> `E_NOT_FOUND`;
- временно недоступен SchemeShard/resolver -> `E_REJECTED`;
- неверный `Headers.CellId` в `DescribeVolume` -> `E_REJECTED`;
- существующий диск с неподдерживаемой для MVP-3 конфигурацией всё равно даёт
  успешный `DescribeVolume`; последующий `MountVolume` отклоняет его через
  `E_NOT_IMPLEMENTED` или `E_ARGUMENT`;
- неверные аргументы/range/payload -> `E_ARGUMENT`;
- неизвестная или stale session -> `E_BS_INVALID_SESSION`;
- временно недоступен tablet/pipe или оборвался in-flight request ->
  `E_REJECTED`;
- неподдерживаемый discard до реализации -> `E_NOT_IMPLEMENTED`;
- внутренние nbs2 ошибки явно переводятся в набор classic NBS error codes,
  зафиксированный выбранной ревизией API;
- внутренние invariant failures не должны быть достижимы пользовательским
  сетевым запросом.

Особенно важно не превращать временную недоступность в `E_NOT_FOUND`: иначе
cells может ошибочно решить, что диска в nbs2 нет.

Проверка `Headers.CellId` нужна именно в `DescribeVolume`: cells использует
этот header при discovery. Mount и data requests уже направляются в выбранный
cell endpoint и не должны зависеть от наличия этого header.
