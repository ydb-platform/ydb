# MVP-3: функционально целостная интеграция NBS cells с nbs2

> Навигация: [обзор и решения](README.md), [RFC](rfc.md),
> [архитектура](architecture.md),
> [нагрузочные MVP](mvp_benchmark.md), [production](production.md).

## MVP-3: функционально целостная интеграция

### Цель MVP-3

Убрать benchmark hardcodes MVP-1/MVP-2 и получить нормально настраиваемый
вариант с основной discovery, session и recovery логикой. Он должен быть
пригоден для длительных интеграционных и нагрузочных прогонов, но ещё не
обещает production-grade fencing, security и исчерпывающую failure semantics.

### Что получим

- gateway встроен в `ydbd` и выключен по умолчанию;
- classic NBS gRPC service и RDMA target используют общий backend;
- source revision classic NBS proto/error contract зафиксирована;
- `DiskId` разрешается через `INbs2VolumeResolver`/SSProxy/SchemeShard, а не
  через benchmark config;
- поддерживаются штатные cells gRPC и RDMA data transports;
- одна partition на диск и block size 4096;
- `Ping`, `DescribeVolume`, `MountVolume`, `UnmountVolume`, `ReadBlocks` и
  `WriteBlocks`;
- идемпотентная in-memory session с синтетическим `SessionId`, проверкой
  каждого I/O и `E_BS_INVALID_SESSION`/remount после restart gateway;
- local auto-vhost endpoint постоянно отключён в cells-конфигурации, но
  доступен в конфигурациях nbs2 без cells;
- discard/write-zeroes не рекламируются, `ZeroBlocks` возвращает
  контролируемый `E_NOT_IMPLEMENTED`;
- gateway восстанавливает tablet pipe после restart/migration partition и не
  повторяет неопределённые in-flight writes;
- пользовательский workflow использует стандартный NBS1 endpoint и штатный cells transport, без draft `Ydb.Nbs` I/O API и прямых `ydb-dstool nbs partition io` команд.

### Что останется за пределами MVP-3

- authoritative session state и production-grade multi-writer fencing;
- полноценный `ZeroBlocks`/discard; Более того, реализацию этой фичи ждём отдельно от текущего плана.
- несколько gateway hosts и проверенный multi-host failure handling;
- полный failure matrix и production observability;
- TLS/authentication, QoS, performance tuning и поэтапный rollout;
- multi-partition volumes и произвольный block size;
- сложный resolver cache;
- выбор и реализация отдельного gateway listener, если он потребуется вместо
  существующего YDB gRPC port;
- gateway sidecar: он не входит в принятую архитектуру;
- placement-aware выбор gateway и автоматическая смена gateway host уже
  существующего endpoint.

MVP-3 реализуется следующими пятью шагами. Каждый шаг заканчивается бинарной проверкой перед переходом к следующему.

### MVP-3-шаг 1. Превратить benchmark gateway в постоянный компонент

Цель:

Закрепить реализованные в MVP-1/MVP-2 gRPC facade и RDMA target как один
конфигурируемый gateway с общими lifecycle, error mapping и backend contract.

Логика:

```text
NBS1 cells
    -> gRPC facade / RDMA target внутри ydbd
    -> единый gateway backend
    -> resolver и session/data actors следующих шагов
```

Benchmark-параметры диска пока могут остаться отдельным временным режимом, но
не должны проникать в transport adapters. `GatewayConfig` является условным
именем конфигурационной секции; конкретное имя proto/class выбирается при
реализации.

Действия:

- добавить конфигурацию gateway с `Enabled` и `CellId`; не дублировать
  `SchemeShardDir`, если его уже однозначно задаёт конфигурация SSProxy;
- оформить единый start/stop lifecycle gRPC facade, RDMA target и gateway
  actors;
- вынести transport-independent validation, error mapping и backend calls из
  gRPC/RDMA adapters;
- добавить проверку `Headers.CellId == GatewayConfig.CellId` только для
  `DescribeVolume`;
- сохранить точную classic NBS wire compatibility обоих transports;
- ограничить benchmark disk override отдельным явно непроизводственным flag и
  удалить его после включения resolver на MVP-3-шаге 2;
- убедиться, что classic и nbs2 namespaces/protobuf packages сосуществуют.

Проверка:

- `ydbd` собирается с выключенным gateway;
- выключенный gateway не меняет поведение обычного YDB/nbs2 запуска;
- gRPC facade и RDMA target включаются и корректно останавливаются одним
  gateway lifecycle;
- classic request/response и RDMA protocol проходят compatibility tests;
- неправильный cell ID в `DescribeVolume` даёт `E_REJECTED`;
- одинаковый запрос получает одинаковую server-side validation/error semantics
  независимо от gRPC или RDMA data transport.

### MVP-3-шаг 2. Реализовать `INbs2VolumeResolver` и `DescribeVolume`

Цель:

Научить gateway находить настоящий nbs2 volume по логическому `DiskId` и
возвращать его в формате classic NBS.

Логика:

```text
DescribeVolume("disk1")
    -> TBlockStoreService facade
    -> INbs2VolumeResolver
    -> локальный SSProxy
    -> SchemeShard
    -> classic TVolume
```

На этом шаге запросы всё ещё направляются непосредственно в gateway без cells.
SchemeShard используется как существующий authoritative registry. Ограничения
первого data path, такие как одна partition и block size 4096, проверяются
позднее на mount: существующий volume должен успешно пройти discovery.

Действия:

- ввести внутренний интерфейс `INbs2VolumeResolver`;
- первая реализация resolver отправляет request в локальный SSProxy;
- проверить path type;
- извлечь `VolumeConfig`, `VolumeTabletId`, partition tablet IDs и
  `SchemeVersion`;
- построить classic `TVolume` с явно заполненными `DiskId`, `BlockSize`,
  `BlocksCount`, `StorageMediaKind`, `PartitionsCount = 1`, tablet/config
  version (`VolumeConfig.TabletVersion`/`VolumeConfig.Version`) и
  `VhostDiscardEnabled = false`;
- сопоставлять media kind и другие enum по именованной таблице, а не numeric
  cast;
- не превращать multi-partition или unsupported block size в ошибку
  `DescribeVolume` для существующего volume;
- не добавлять cache до работающей корректной версии.

Проверка:

- существующий диск возвращает правильные `DiskId`, `BlockSize`,
  `BlocksCount`, media kind;
- отсутствующий диск возвращает `E_NOT_FOUND`;
- временно недоступный resolver возвращает `E_REJECTED`;
- path неправильного типа возвращает `E_NOT_FOUND`;
- существующий multi-partition/unsupported-block-size disk успешно проходит
  discovery и детерминированно отклоняется только на `MountVolume`.

### MVP-3-шаг 3. Добавить нормальный process-local `MountVolume`/`UnmountVolume`

Цель:

Заменить single-workload session MVP-1/MVP-2 на process-local registry для
нескольких дисков и клиентов, но явно не претендующий на production fencing.

Логика:

```text
MountVolume
    -> повторный resolve DiskId
    -> проверка поддерживаемой MVP-3-геометрии
    -> создание in-memory session в gateway
    -> TMountVolumeResponse { Volume, SessionId }
```

Gateway генерирует непрозрачный `SessionId` и хранит отображения mount key в
session и session в параметры volume только в памяти процесса. Origin NBS
сохраняет ID и добавляет его в последующие I/O. После restart gateway старая
session получает `E_BS_INVALID_SESSION`, а classic NBS session выполняет
remount. Cells для этой проверки не нужны: используется прямой classic client
session до gateway.

Действия:

- добавить `EnableLocalVhostEndpoint`, выключать его в cells-сценарии и не
  допускать одновременного включения cells gateway и local auto-vhost;
- реализовать явно MVP-3 session mode под feature flag;
- `MountVolume` повторно использует resolver result и возвращает `TVolume`;
- принимать classic `LOCAL` и `REMOTE` mount mode, но внутри gateway
  нормализовать оба к remote semantics;
- сделать mount идемпотентным по `DiskId`, `ClientId`, `InstanceId`,
  `MountSeqNumber` и существенным mount/access parameters;
- сгенерировать/переиспользовать `SessionId` для идемпотентного mount;
- отклонять multi-partition, block size != 4096 и неподдерживаемые access modes
  через явный `E_NOT_IMPLEMENTED`/`E_ARGUMENT`;
- `UnmountVolume` корректно завершает gateway-side state;
- каждый data request проверяет `DiskId` и `SessionId`; неизвестная/stale
  session получает `E_BS_INVALID_SESSION`.

Проверка:

- origin NBS session, направленная непосредственно в gateway, успешно проходит
  mount;
- повторный эквивалентный mount возвращает ту же действующую session;
- I/O с неизвестной или завершённой session получает
  `E_BS_INVALID_SESSION`;
- после restart gateway origin NBS автоматически remount-ит session и
  продолжает I/O без пересоздания endpoint;
- повторные mount/unmount не приводят к утечкам или abort;
- local auto-vhost endpoint не создаётся в cells-конфигурации, но продолжает
  работать в конфигурации nbs2 без cells.

### MVP-3-шаг 4. Довести serialized read/write path до restart/migration

Цель:

Провести classic `ReadBlocks`/`WriteBlocks` к NBS2 partition по стабильному `PartitionTabletId` через tablet pipe и передать их в существующий `FastPathService`.

Логика:

```text
ReadBlocks/WriteBlocks
    -> gateway проверяет DiskId и SessionId
    -> tablet pipe по PartitionTabletId
    -> partition tablet
    -> IStorage/FastPathService
    -> DDisk
```

Gateway владеет pipe и in-flight state. После restart/migration partition pipe переподключается к текущему узлу partition. Если pipe
рвётся во время write, gateway не может знать, была ли запись выполнена, и не
повторяет её самостоятельно: он возвращает `E_REJECTED`, оставляя retry
durable/session client origin NBS.

Действия:

- добавить serialized read/write events, принимаемые через partition tablet
  pipe;
- partition передаёт запрос существующему `IStorage`/`FastPathService`;
- gateway actor владеет pipe и таблицей in-flight requests/cookies;
- при pipe disconnect завершает неопределённые in-flight requests через
  `E_REJECTED`, не повторяет внутри gateway потенциально выполненные writes и
  восстанавливает pipe для последующих запросов;
- валидировать request block size, переполнение и границы block range, точный
  размер write payload, block alignment, request limits и transport-specific
  message size до fast path; gateway не дополняет короткий payload нулями;
- учитывать lifetime protobuf buffers и sglist до завершения future.

Проверка:

- pattern write/read даёт одинаковый checksum;
- malformed request возвращает ошибку без `Y_ABORT`;
- partition tablet restart/migration восстанавливается через переподключение tablet pipe;
- disconnect во время write не вызывает скрытый повтор внутри gateway;
- после восстановления pipe новые I/O продолжаются, а `E_REJECTED` для
  прерванных запросов корректно обрабатывает durable NBS client.

### MVP-3-шаг 5. Проверить функциональный E2E обоих cells transports

Цель:

Проверить уже без benchmark hardcodes полный пользовательский путь через
существующий `StartEndpoint` legacy NBS и убедиться, что cells воспринимает
nbs2 как обычную foreign cell как с gRPC, так и с RDMA data transport.

Логика:

```text
пользователь/control plane
    -> StartEndpoint(disk1) в legacy NBS cell-a
        -> cells вызывает DescribeVolume в nbs2 gateway
        -> legacy NBS вызывает MountVolume в nbs2 gateway
        -> legacy NBS создаёт у себя NBD/vhost endpoint
        -> endpoint посылает ReadBlocks/WriteBlocks по gRPC или RDMA
        -> оба transport adapter вызывают общий nbs2 backend
```

`StartEndpoint` уже реализован в старом NBS; nbs2 gateway не получает этот RPC
и не создаёт guest-facing socket. На этом шаге в cells не добавляется новый
backend: достаточно конфигурации peer cell. Для локального кластера достаточно
одного gateway host. В production каждый настроенный host должен уметь
обслужить любой `DiskId` через tablet pipe.

Действия:

- добавить в конфигурацию legacy NBS peer cell, указывающую на nbs2 gateway;
- выполнить сценарий сначала с gRPC:

```protobuf
Cells {
    CellId: "nbs2"
    GrpcPort: <ydb-grpc-port-or-dedicated-gateway-port>
    Transport: CELL_DATA_TRANSPORT_GRPC
    DescribeVolumeHostCount: 1
    MinCellConnections: 1
    Hosts { Fqdn: "localhost" }
}
```

- затем выполнить тот же сценарий с `CELL_DATA_TRANSPORT_RDMA`, `RdmaPort` и
  включёнными RDMA workers;

- убедиться, что `Cells.CellId` legacy NBS совпадает с
  `GatewayConfig.CellId` в `ydbd`;
- запустить `StartEndpoint` для nbs2 disk именно в legacy NBS cell-a;
- не добавлять специальную nbs2-логику в cells.

Проверка:

- legacy disk по-прежнему находится в своей NBS cell;
- `disk1` находится через nbs2 gateway;
- `StartEndpoint` успешно выполняется в legacy cell-a, а не в nbs2 gateway;
- NBD/vhost socket создаётся на стороне legacy NBS;
- NBD/vhost read/write проходит в nbs2 через gRPC и RDMA;
- оба transport используют resolver, session registry и общий backend
  MVP-3, а не benchmark disk override;
- в пользовательском сценарии не используются draft `Ydb.Nbs` I/O API и прямые `ydb-dstool nbs partition io` команды.

## Бинарные критерии успеха этапа

### MVP-3: функционально целостный E2E

MVP-3 считается достигнутым, если одновременно выполняются условия:

1. `disk1` создан только через `ydb-dstool nbs partition create`.
2. `blockstore-client describevolume`, обращённый к legacy NBS cell-a,
   находит `disk1` через peer cell `nbs2`.
3. `StartEndpoint` для `disk1` отправляется и успешно выполняется в legacy NBS
   cell-a; nbs2 gateway этот RPC не получает.
4. NBD/vhost endpoint создаётся на стороне legacy NBS cell-a, и через него
   записывается тестовый pattern.
5. Прочитанные данные имеют тот же checksum.
6. В workflow не используются draft `Ydb.Nbs` I/O API и прямые `ydb-dstool nbs partition io` команды.
7. После перезапуска/migration partition tablet endpoint восстанавливает I/O
   либо получает retriable errors до восстановления, но не требует пересоздания
   диска и ручного обновления маршрутизации.
8. После restart gateway потерянная MVP-3 session получает
   `E_BS_INVALID_SESSION`, origin NBS выполняет remount, и endpoint продолжает
   I/O без ручного пересоздания.
9. Guest не видит discard/write-zeroes capability, а прямой `ZeroBlocks` до
   реализации возвращает `E_NOT_IMPLEMENTED` и не приводит к падению процесса.
10. Несуществующий диск и недоступная nbs2 cell возвращают соответственно
    `E_NOT_FOUND` и `E_REJECTED`.
11. Существующий диск с неподдерживаемой MVP-3-геометрией успешно проходит
    `DescribeVolume`, но получает детерминированную ошибку на `MountVolume`.
12. Неверный `Headers.CellId` в `DescribeVolume` возвращает `E_REJECTED`.
13. Malformed/overflow range и payload неправильного размера возвращают
    `E_ARGUMENT` без `Y_ABORT` и без изменения данных.
14. Local auto-vhost socket не создаётся в cells-конфигурации; конфигурация
    без cells по-прежнему может включить `EnableLocalVhostEndpoint`.
15. Существующие legacy NBS cross-cell тесты продолжают проходить.
16. Один и тот же функциональный сценарий проходит через gRPC и RDMA без
    benchmark disk override.
