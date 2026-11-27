# MVP-1/MVP-2: нагрузочный hot path через NBS cells

> Навигация: [обзор и решения](README.md), [RFC](rfc.md),
> [архитектура](architecture.md),
> [функциональный MVP](mvp_functional.md), [production](production.md).

## MVP-1: нагрузочный hot path через gRPC

### Цель MVP-1

Максимально быстро получить корректный read/write путь из NBS1 endpoint через
cells и classic NBS gRPC в настоящий nbs2 partition, чтобы подтвердить работоспособность и измерить gRPC hot path без ожидания полной control-plane реализации.

### Что получим

- минимальный classic NBS facade внутри `ydbd`;
- один вручную настроенный nbs2 disk с фиксированными `DiskId`, `CellId`,
  `BlockSize`, `BlocksCount` и `PartitionTabletId`;
- обязательные для cells gRPC методы `DescribeVolume`, `MountVolume`,
  `UnmountVolume`, `ReadBlocks` и `WriteBlocks`;
- синтетическую process-local session, достаточную для одного нагрузочного
  стенда;
- настоящий data path через nbs2 `IStorage`, tablet pipe и partition;
- NBD/vhost endpoint, создаваемый существующим `StartEndpoint` в NBS1 cell;
- baseline latency, IOPS, bandwidth, CPU и размер допустимого in-flight.

### Что останется за пределами MVP-1

- RDMA data transport;
- разрешение произвольного `DiskId` через SSProxy/SchemeShard;
- несколько дисков и полноценный process-local mount registry;
- гарантированное восстановление после restart/migration;
- authoritative session state, writer generation и writer fencing;
- `ZeroBlocks`/discard;
- multi-host режим: несколько gateway hosts в одной nbs2 cell, любой из
  которых cells может выбрать для обслуживания диска; MVP-1 использует один
  host, а единые session state и writer generation, writer fencing и обработка
  отказов нескольких hosts проверяются в production;
- расширенная диагностика, security и production rollout.

Хардкодить разрешено control-plane данные и выполнять настройку вручную. Нельзя
заменять настоящий cells wire contract или backend синтетическим benchmark
protocol: результат должен измерять тот путь, который будет сохранён далее.

### MVP-1-шаг 1. Зафиксировать контракты и встроить каркас gateway

Цель:

Доказать, что classic NBS gRPC service можно собрать внутри `ydbd`, и заранее
проверить dependency graph полного существующего RDMA target. RDMA server на
этом шаге ещё не запускается, но compile/dependency spike выполняется до
gRPC-замеров, чтобы риск MVP-2 обнаружился как можно раньше.

Логика:

```text
classic NBS client/test
    -> существующий YDB gRPC port
    -> TBlockStoreService facade внутри ydbd
    -> direct classic response
```

Действия:

- зафиксировать source commit classic NBS gRPC proto, RDMA protocol и error
  codes;
- перенести минимальный protobuf/service API и определить таблицу ошибок;
  - BlockStoreService
  - ├── Ping
  - ├── DescribeVolume
  - ├── MountVolume
  - ├── UnmountVolume
  - ├── ReadBlocks
  - ├── WriteBlocks
- попробовать собрать внутри YDB tree существующий classic NBS RDMA target
  целиком; если dependency graph окажется неприемлемым, зафиксировать причины
  и использовать резервный вариант — минимальный wire-compatible adapter к
  nbs2 `IStorage`;
- добавить `GatewayConfig` с `Enabled`, `CellId` и benchmark-параметрами диска;
- зарегистрировать direct-response
  `NCloud.NBlockStore.NProto.TBlockStoreService` на существующем YDB gRPC
  server;
- реализовать `Ping` и каркас обязательных методов;
- для `DescribeVolume` проверять
  `Headers.CellId == GatewayConfig.CellId`;
- не создавать отдельный gateway binary.

Проверка:

- `ydbd` собирается с выключенным gateway;
- выключенный gateway не меняет обычный запуск YDB/nbs2;
- classic `blockstore-client ping` получает direct classic response;
- неправильный `Headers.CellId` даёт `E_REJECTED`;
- wire/error compatibility подтверждена зафиксированной ревизией NBS;
- полный existing RDMA target компилируется внутри YDB tree либо документирован
  конкретный dependency blocker для перехода к минимальному adapter.

### MVP-1-шаг 2. Реализовать захардкоженный benchmark control plane

Цель:

Дать NBS1 cells минимальные корректные ответы для одного заранее созданного
диска, не реализуя пока resolver и полноценный session lifecycle.

Логика:

```text
DescribeVolume(configured DiskId)
    -> TVolume из GatewayConfig

MountVolume(configured DiskId)
    -> синтетический SessionId в памяти gateway
```

Действия:

- задать в benchmark config `DiskId`, `BlockSize = 4096`, `BlocksCount`, media
  kind и стабильный `PartitionTabletId`;
- возвращать `E_NOT_FOUND` для другого `DiskId`;
- реализовать `DescribeVolume`, `MountVolume` и `UnmountVolume` для
  настроенного диска;
- возвращать и проверять синтетический process-local `SessionId`;
- после потери session возвращать `E_BS_INVALID_SESSION`;
- отключить local auto-vhost endpoint nbs2; Данные в nbs2 пойдут через механизм cells.
- не рекламировать discard/write-zeroes и возвращать `E_NOT_IMPLEMENTED` на
  `ZeroBlocks`.

Проверка:

- настроенный disk успешно проходит describe/mount/unmount;
- другой `DiskId` получает `E_NOT_FOUND`;
- I/O с неизвестной или завершённой session получает
  `E_BS_INVALID_SESSION`;
- direct `ZeroBlocks` возвращает `E_NOT_IMPLEMENTED` без вызова
  `ZeroBlocksLocal`;
- local auto-vhost socket не создаётся.

### MVP-1-шаг 3. Провести gRPC в настоящий nbs2 data path

Цель:

Реализовать измеряемый hot path от classic gRPC request до nbs2 partition,
который затем без изменений backend будет использован RDMA target.

Логика:

```text
ReadBlocks/WriteBlocks по gRPC
    -> проверка DiskId/SessionId
    -> общий gateway IStorage/backend adapter
    -> tablet pipe по настроенному PartitionTabletId
    -> partition/FastPathService
    -> DDisk
```

Действия:

- ввести общий для gRPC и будущего RDMA backend поверх nbs2 `IStorage`;
- направлять serialized read/write по настроенному `PartitionTabletId` через стабильный tablet pipe в существующий `FastPathService` NBS2;
- валидировать range, overflow, block alignment и точный размер payload до
  fast path;
- удерживать protobuf buffers и sglist до завершения запроса;
- ограничить число и объём in-flight запросов, чтобы benchmark не измерял
  unbounded queue;
- при разрыве pipe возвращать `E_REJECTED` и не повторять неопределённый write;
- до появления отдельной защиты исключить из workload конкурентные записи в
  один и тот же блок.

Проверка:

- pattern write/read через прямой classic gRPC client даёт одинаковый
  checksum;
- malformed request возвращает `E_ARGUMENT` без `Y_ABORT`;
- write неправильного размера не меняет данные;
- лимит in-flight является наблюдаемым и не допускает неограниченного роста
  памяти;
- data path использует настоящий nbs2 partition, а не benchmark storage stub.

### MVP-1-шаг 4. Выполнить E2E и нагрузочный baseline через NBS1 cells

Цель:

Проверить hot path в требуемой пользовательской конфигурации: endpoint
создаётся NBS1, disk находится через cells, data transport — gRPC.

Логика:

```text
StartEndpoint в NBS1 cell-a
    -> cells Describe/Mount по gRPC
    -> NBD/vhost endpoint в NBS1
    -> Read/Write по gRPC
    -> общий nbs2 backend
```

Действия:

- вручную настроить peer cell `nbs2` с
  `Transport: CELL_DATA_TRANSPORT_GRPC`;
- направить host/`GrpcPort` на gateway внутри `ydbd`;
- выполнить `StartEndpoint` в legacy NBS cell-a;
- провести correctness workload, затем latency/IOPS/bandwidth тесты;
- сохранить размер I/O, queue depth, CPU allocation и конфигурацию стенда для
  сравнения с RDMA.

Проверка:

- `disk1` находится через peer cell и `StartEndpoint` завершается успешно;
- NBD/vhost socket создаётся в NBS1, а не в nbs2 gateway;
- checksum read/write совпадает;
- в workflow не используются draft `Ydb.Nbs` I/O API и прямые `ydb-dstool nbs partition io` команды;
- baseline воспроизводится с зафиксированными параметрами workload.

## MVP-2: нагрузочный hot path через RDMA

### Цель MVP-2

На том же диске, с тем же NBS1 endpoint workflow и тем же nbs2 backend заменить
gRPC data transport на существующий cells RDMA transport и получить сравнимые
нагрузочные результаты.

### Что получим

- classic NBS RDMA target внутри того же `ydbd`;
- точную совместимость с существующим NBS1 cells RDMA protocol;
- gRPC control plane и RDMA `ReadBlocks`/`WriteBlocks`;
- один общий nbs2 backend для gRPC и RDMA;
- сопоставимые gRPC/RDMA latency, IOPS, bandwidth и CPU measurements.

### Что останется за пределами MVP-2

- resolver произвольных дисков;
- полноценный process-local session registry для нескольких disks/clients;
- гарантированное восстановление после рестартов и миграций;
- authoritative session state, writer fencing, multi-host и production
  failure semantics;
- `ZeroBlocks`/discard, TLS/authentication и rollout.

MVP-2 не вводит упрощённый benchmark-only RDMA protocol. NBS1 cells использует
свой штатный `CELL_DATA_TRANSPORT_RDMA`; control plane остаётся на classic NBS
gRPC, как предусмотрено существующей реализацией cells.

### MVP-2-шаг 1. Добавить совместимый classic NBS RDMA target

Цель:

Принять RDMA-запросы существующего cells client внутри `ydbd` и передать их в
тот же backend, который уже измерен через gRPC.

Логика:

```text
NBS1 cells RDMA client
    -> classic NBS RDMA wire protocol
    -> RDMA target внутри ydbd
    -> общий gateway IStorage/backend adapter
    -> tablet pipe -> nbs2 partition
```

Действия:

- использовать зафиксированные classic NBS message IDs, request/response
  layout и error semantics;
- перенести существующий classic NBS RDMA target целиком, адаптировав его
  storage boundary к общему nbs2 backend; минимальный wire-compatible adapter
  использовать только при подтверждённом dependency blocker;
- запустить RDMA target на configurable `RdmaPort` внутри `ydbd`;
- адаптировать RDMA `ReadBlocksLocal`/`WriteBlocksLocal` к общему nbs2 backend;
- применять те же проверки disk/session/range/payload и те же in-flight limits,
  что в gRPC path;
- возвращать контролируемую ошибку для `ZeroBlocks`, пока capability выключена;
- не добавлять отдельный процесс или второй backend path.

Проверка:

- штатный NBS1 RDMA client устанавливает соединение с target;
- protocol/message compatibility подтверждена тестами с зафиксированной
  ревизией NBS;
- pattern write/read через RDMA даёт тот же checksum, что gRPC;
- malformed и stale-session requests не достигают небезопасного fast path;
- gRPC и RDMA используют один и тот же backend implementation.

### MVP-2-шаг 2. Переключить NBS1 cells на штатный RDMA transport

Цель:

Собрать полный cells workflow, в котором control plane остаётся на gRPC, а
data plane автоматически выбирается существующим cells как RDMA.

Логика:

```text
StartEndpoint в NBS1 cell-a
    -> Describe/Mount/Unmount: gRPC
    -> Read/Write: RDMA
    -> тот же nbs2 partition
```

Действия:

- указать gateway `GrpcPort` и валидный `RdmaPort` в host peer cell;
- установить `Transport: CELL_DATA_TRANSPORT_RDMA`;
- включить RDMA client origin NBS и задать `RdmaTransportWorkers > 0`;
- повторно выполнить тот же `StartEndpoint`, не меняя gateway hardcodes и
  backend;
- подтвердить по метрикам/логам, что control идёт по gRPC, data — по RDMA.

Проверка:

- `DescribeVolume`, `MountVolume` и `UnmountVolume` видны на gRPC facade;
- `ReadBlocks`/`WriteBlocks` проходят через RDMA target;
- NBD/vhost workload не требует изменений относительно MVP-1;
- отключение RDMA target приводит к контролируемой транспортной ошибке, а не к
  молчаливому переходу на другой benchmark path.

### MVP-2-шаг 3. Сравнить gRPC и RDMA hot path

Цель:

Получить данные, по которым можно оценить пределы nbs2 I/O и эффект замены
транспорта, не смешивая его с изменением backend или workload.

Логика:

Сравниваются два cells-прогона с одинаковыми disk, data, I/O size, queue depth,
CPU pinning и duration. Единственная целевая переменная — gRPC или RDMA data
transport.

Действия:

- повторить correctness и нагрузочную матрицу MVP-1;
- измерить latency percentiles, IOPS, bandwidth, CPU и ошибки/backpressure;
- проверить несколько размеров I/O и queue depth в пределах заданных лимитов;
- отдельно зафиксировать actor/interconnect/backend latency, чтобы не приписать
  её RDMA transport;
- сохранить конфигурацию и результаты обоих прогонов.

Проверка:

- оба транспорта проходят одинаковый checksum workload;
- результаты воспроизводимы на одной конфигурации стенда;
- нет unbounded in-flight state, memory growth или скрытых write retries;
- различие gRPC/RDMA измерено на одном backend и допускает содержательное
  сравнение.

### Условные оптимизации MVP-2

Эти оптимизации не являются обязательными для первого прогона. Они выполняются
только при подтверждённом измерениями bottleneck, после чего одинаковая
нагрузочная матрица повторяется для gRPC и RDMA.

1. **Multi-queue vhost в NBS1.** Если один vhost executor NBS1 ограничивает
   IOPS при наличии свободных CPU, перенести из nbs2 поддержку обслуживания
   очередей одного endpoint несколькими executor threads. Направление
   детализации: negotiation virtqueues, соотношение `VhostQueuesCount` и
   `VhostThreadsCount`, affinity и потокобезопасность общего device handler.
2. **Несколько I/O actors.** Если один gateway/partition adapter actor или его
   mailbox ограничивает IOPS, реализовать несколько принимающих/отправляющих
   I/O lanes над общим backend. Направление детализации: выбор шардирования,
   несколько tablet-pipe/data adapters, bounded in-flight и сохранение порядка
   пересекающихся writes.

## Бинарные критерии успеха этапов

### MVP-1: cells + gRPC benchmark

MVP-1 считается достигнутым, если одновременно выполняются условия:

1. `StartEndpoint` выполняется в NBS1 cell и создаёт NBD/vhost endpoint.
2. Control и read/write идут через штатный NBS1 cells gRPC transport.
3. I/O достигает настоящего nbs2 partition через общий gateway backend.
4. Pattern write/read даёт одинаковый checksum.
5. В workflow не используются draft `Ydb.Nbs` I/O API, прямые `ydb-dstool nbs partition io` команды и benchmark storage stub.
6. Зафиксирован воспроизводимый gRPC baseline с параметрами workload и стенда.
7. Malformed requests и превышение in-flight limit дают контролируемые ошибки
   без `Y_ABORT` и неограниченного роста памяти.

### MVP-2: cells + RDMA benchmark

MVP-2 считается достигнутым, если одновременно выполняются условия:

1. Тот же `StartEndpoint` и workload работают с
   `CELL_DATA_TRANSPORT_RDMA`.
2. Describe/mount/unmount идут по gRPC, read/write — по штатному cells RDMA.
3. RDMA target совместим с зафиксированной ревизией classic NBS protocol.
4. gRPC и RDMA вызывают один и тот же nbs2 backend и работают с тем же disk.
5. RDMA pattern write/read даёт правильный checksum.
6. Получены сопоставимые gRPC/RDMA latency, IOPS, bandwidth и CPU measurements.
7. Нет скрытых write retries, unbounded in-flight state или отдельного
   benchmark-only RDMA protocol.
