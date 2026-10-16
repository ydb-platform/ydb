# RFC: интеграция NBS1 control plane с NBS2 backend

## 1. Метаданные

| Поле                                             | Значение                                   |
|--------------------------------------------------|--------------------------------------------|
| Статус                                           | Черновик                                   |
| Авторы                                           | TBD                                        |
| Рецензенты                                       | TBD                                        |
| Создан                                           | 2026-08-11                                 |
| Обновлён                                         | 2026-08-14                                 |
| Целевая ревизия YDB                              | `3529cbcc660f1bb87c2e4e691675f584589f047b` |
| Кандидат на source revision classic NBS contract | `5e95a6646c3cc8af5797ba307f99ec9431aaecbe` |

Связанные документы:

- [обзор и текущие решения](README.md);
- [исследование и архитектурное обоснование](architecture.md);
- [MVP-1/MVP-2: gRPC и RDMA benchmark](mvp_benchmark.md);
- [MVP-3: функциональная интеграция](mvp_functional.md);
- [production и выкатка](production.md).

Этот RFC фиксирует предлагаемую границу системы и обязательные свойства решения. Детали реализации будут проработаны отдельно.

В документе `cell` обозначает NBS shard, внутри которого могут одновременно работать NBS1 и NBS2.
Исходная cell — cell, в которой NBS1 выполняет `StartEndpoint`;
cell-владелец — cell, в SchemeShard которой зарегистрирован диск.
Для локального диска это одна и та же cell, для меж-cell доступа — разные.

## 2. Решаемая проблема

В NBS2 появляется новый тип диска, далее условно называемый `fast_disk`, но существующая инфраструктура NBS1 пока не умеет работать с ним как с обычным диском. Пользователь не может через единый NBS1 workflow найти такой диск по `DiskId`, подключить его и выполнять read-write I/O как внутри своей cell, так и между cells.

Необходимо сделать NBS2-диски доступными через существующую пользовательскую и административную модель NBS1, сохранив корректность сессий и конкурентной записи, восстановление после отказов и прежнее поведение NBS1-дисков. Отдельный пользовательский путь для NBS2 нежелателен, поскольку он разделит управление дисками, клиентские сценарии и эксплуатационные инструменты.

## 3. Обзор решения

NBS1 сохраняет пользовательский и административный control plane, discovery и создание NBD/vhost endpoint. NBS2 создаёт и обслуживает `fast_disk`, предоставляя совместимые с NBS1 операции управления сессией и I/O.

После discovery NBS1 определяет реализацию диска и его cell. NBS1-диски продолжают использовать существующие пути. Для `fast_disk` NBS1 находит host текущей NBS2 partition и направляет туда запросы независимо от того, находится диск в исходной или другой cell.

| Расположение диска | Реализация диска | `MountVolume`/`UnmountVolume` | I/O |
|---|---|---|---|
| Cell-владелец совпадает с исходной | NBS1 | Локальный NBS1 service | Существующий локальный NBS1 storage path |
| Cell-владелец совпадает с исходной | NBS2 | gRPC на host локальной NBS2 partition | gRPC/RDMA на тот же NBS2 host |
| Cell-владелец отличается от исходной | NBS1 | NBS1 service выбранного host cell-владельца | Существующий cells gRPC/RDMA путь к NBS1 |
| Cell-владелец отличается от исходной | NBS2 | gRPC на host NBS2 partition в cell-владельце | gRPC/RDMA на тот же NBS2 host |

`StartEndpoint` остаётся в NBS1. Для `fast_disk` control-plane операции выполняются по gRPC, а I/O — по gRPC или RDMA на том же NBS2 host. NBS2 frontend обслуживает только локальную partition и не пересылает запросы между hosts.

## 4. Технические ограничения текущей реализации

На текущий момент сквозной интеграции NBS1 и NBS2 нет.

- **NBS1 не умеет использовать NBS2 как дисковый backend.** NBS1 не распознаёт `fast_disk` и не может построить для него backend client. NBS2, со своей стороны, пока не предоставляет полный совместимый интерфейс classic NBS для управления сессией и выполнения I/O. Существующий локальный vhost NBS2 решает только node-local сценарий.

- **NBS cells не поддерживает NBS2.** Текущая реализация cells работает только с NBS1 backend: для диска своей cell выбирается локальный NBS1, а для другой cell — один из NBS1 hosts. Cells не умеет определить, что диск обслуживается NBS2, найти host его текущей partition и обновить этот маршрут после рестарта или миграции.

- **Отсутствуют необходимые модули маршрутизации внутри NBS2.** SchemeShard хранит описание диска и идентификаторы tablets, но не сообщает, на каком host сейчас работает partition. Требуются:
  - модуль, определяющий текущий host partition и его сетевые адреса;
  - локальный реестр NBS2 host, который знает о запущенных на нём partitions и предоставляет frontend локальные обработчики session RPC и I/O.

- **Не завершены управление сессиями и восстановление после смены маршрута.** NBS2 пока не обрабатывает classic `MountVolume`/`UnmountVolume` и не хранит необходимое session/writer state. NBS1-клиент не умеет переключать весь NBS2 backend для того же `DiskId`, а существующая retry-логика может небезопасно повторить операцию записи.

Для интеграции необходимо добавить совместимый NBS2 backend API, поддержку NBS2 в механизме cells, модули определения и проверки размещения partition, а также отказоустойчивое управление сессиями и безопасное восстановление маршрута.

## 5. Что хотим сделать

1. Оставить административные операции, включая discovery, create/delete/resize и `StartEndpoint`, а также создание guest-facing NBD/vhost endpoint в NBS1.
2. Использовать существующий SchemeShard metadata namespace каждой cell и единый логический формат `DiskId` для NBS1- и NBS2-дисков.
3. Создавать `fast_disk` только в NBS2 и возвращать его полное описание через существующий NBS1 discovery path.
4. Для NBS1-диска сохранять существующий backend, а для `fast_disk` строить по `DataRoute` NBS2 `Service` для gRPC session RPC и NBS2 `Storage` для gRPC/RDMA I/O как внутри исходной cell, так и между cells.
5. Для NBS2 направлять `MountVolume`, `UnmountVolume` и I/O непосредственно на host текущей partition и не добавлять межузловой forwarding в NBS2 frontend.
6. Сохранить существующее поведение NBS1-дисков, не требуя placement-aware маршрутизации для них в рамках этого RFC.
7. Использовать общий transport-independent data frontend core и одинаковую validation/error model для gRPC и RDMA.
8. В production автоматически восстанавливать placement route и переключать весь NBS2 backend client после restart или migration partition; повторный mount требуется после `E_BS_INVALID_SESSION`.
9. Обрабатывать `MountVolume`/`UnmountVolume` в NBS2, хранить session/writer authorization в PartitionTablet и проверять её непосредственно в partition-owned I/O path.
10. Не добавлять сценарий, в котором NBS2 инициирует discovery или I/O к NBS1-диску.
11. При hard failure или сетевом разделении не позволять старой incarnation partition продолжать запись после активации новой.
12. Описать все состояния session и переходы между ними;

## 6. Что не хотим

- реализовывать `StartEndpoint` или создание guest-facing socket в NBS2;
- создавать отдельный пользовательский control plane NBS2;
- добавлять отдельный путь прямого пользовательского `MountVolume` для `fast_disk` в обход `StartEndpoint` и управляемой NBS1 `TSession`;
- добавлять отдельный внутренний NBS1 -> NBS2 `RegisterSession`/`UnregisterSession` поверх выбранного classic `MountVolume`/`UnmountVolume` backend API;
- реализовывать NBS2-клиент для доступа к NBS1-дискам;
- использовать local NBS2 vhost socket как backend управляемого NBS1 endpoint;
- создавать отдельный benchmark-only RDMA protocol;
- пересылать запрос с NBS2 frontend на удалённую partition через tablet pipe или actor interconnect;
- обязательно менять существующий data path NBS1-дисков на strict-affinity маршрутизацию;
- поддерживать multi-partition `fast_disk` в рамках первого варианта;
- определять в этом RFC полную create/delete/resize lifecycle-интеграцию; RFC фиксирует принадлежность этих операций NBS1 и создание `fast_disk` в NBS2, а детальный внутренний контракт определяется отдельно;

## 7. Предлагаемая архитектура

**TODO: разобрать подробнее.**

Получение маршрута для `fast_disk`:

```text
NBS1 control service
    -> local/peer NBS1 control endpoint
    -> SSProxy/SchemeShard: DiskId -> TBlockStoreVolumeDescription
    -> placement resolver для NBS2: PartitionTabletId -> NodeId
    -> NBS2 host address resolver: NodeId -> gRPC/RDMA address
```

Session и data path для `fast_disk`:

```text
guest
    -> NBD/vhost endpoint в NBS1
    -> backend client для fast_disk
         Service -> gRPC NBS2 frontend на host partition
              -> MountVolume/UnmountVolume
              -> local partition registry/dispatcher
              -> lifetime-bound session-control target текущей incarnation
              -> PartitionTablet transaction
                   -> persisted session/writer authorization
         Storage -> gRPC facade / RDMA target того же host
              -> общий data frontend core
              -> local partition registry/dispatcher
              -> session-checking IStorage handle текущей incarnation
                   -> проверка session/access mode
                   -> проверка writer generation для изменяющих операций
              -> FastPathService: I/O
              -> Regions/DDisk

         PartitionTablet владеет persisted authorization,
         жизненным циклом session и публикацией связанной пары
         session-control target + IStorage handle
```

Решение состоит из следующих логических компонентов:

- **NBS1 session manager и backend client** — после discovery сохраняют существующий NBS1 backend для обычного диска либо создают по `DataRoute` NBS2 `Service` и `Storage`; существующая `TSession` инициирует mount/remount/unmount независимо от типа backend;
- **NBS1 control service** — обслуживает discovery и административные операции для NBS1- и NBS2-дисков, но не завершает runtime mount `fast_disk`;
- **SSProxy/SchemeShard** — является единым источником постоянного `TBlockStoreVolumeDescription`, содержащего `VolumeConfig` и tablet IDs; тип диска указывает, какая реализация создала и обслуживает volume;
- **placement resolver cell-владельца** — разрешает `PartitionTabletId` NBS2-диска в текущий `NodeId`;
- **NBS2 host address resolver** — сопоставляет `NodeId` с routable gRPC/RDMA адресами NBS2 host; если набор адресов зависит от исходной cell, она должна передаваться или надёжно определяться по аутентифицированному соединению;
- **NBS2 frontend** — общее название classic NBS gRPC facade, RDMA target, data frontend core и local partition registry/dispatcher;
- **classic NBS gRPC facade** — принимает `MountVolume`, `UnmountVolume` и data requests; session RPC через local partition registry/dispatcher передаёт локальному session-control target, а data requests — в общий data frontend core;
- **RDMA target** — принимает data requests и передаёт их в общий data frontend core; mount/unmount при RDMA data transport остаются на gRPC endpoint того же NBS2 host;
- **data frontend core** — выполняет общую validation, error mapping и resource accounting для gRPC/RDMA I/O, не владея отказоустойчивым session state;
- **local partition registry/dispatcher** — новый компонент, который получает от локальной partition связанную и привязанную к времени жизни её текущей incarnation пару: session-control target для mount/unmount и `IStorage` handle для I/O. Оба объекта публикуются и отзываются атомарно; запрос по `DiskId`/`PartitionTabletId` передаётся только локальному объекту этой пары, а её отсутствие означает устаревший placement;
- **PartitionTablet** — должен транзакционно обрабатывать mount/unmount, владеть отказоустойчивым состоянием session/writer authorization и жизненным циклом публикуемой пары session-control target и I/O handle;
- **session-checking I/O handle** — по `Headers.ClientId` и `SessionId` проверяет принадлежащую клиенту session и access mode для каждого I/O, а для изменяющей операции дополнительно проверяет соответствие текущему writer generation;
- **FastPathService** — существующая реализация `IStorage`, которой владеет partition и которую текущий local vhost вызывает напрямую; она выполняет I/O в regions/DDisk и сейчас не реализует описанную session/writer-проверку.

Для выбора NBS2 host не требуется отдельный постоянный реестр `DiskId -> backend`: SchemeShard уже содержит полное описание с `VolumeConfig` и tablet IDs. Тип `fast_disk` используется при создании и построении маршрута, а изменяемое расположение partition разрешается отдельно и не записывается как постоянная volume metadata. Optional `DataRoute` является вычисленным результатом discovery, а не вторым источником истины о принадлежности диска.

Точный источник placement (`PartitionTabletId -> NodeId`) и способ публикации сетевых endpoints остаются открытыми вопросами. Текущая публикация tablet state в Node Whiteboard не является готовым authoritative resolver, а vhost registry индексирует endpoints по socket path, а не по `DiskId` или tablet ID. Поэтому требуются отдельные или доработанные placement resolver и local partition registry.

Direct partition может запускаться только на host с включёнными NBS2 service и frontend. Placement/configuration должны обеспечивать это свойство; одной регистрации tablet constructors на узле недостаточно.

Для production отзыв связанной пары session-control target и I/O handle при управляемом shutdown или migration должен атомарно запрещать приём новых запросов старой incarnation. Уже принятые session RPC и I/O должны быть завершены, безопасно отклонены либо переведены в явно определённое состояние с неопределённым результатом до того, как новая incarnation начнёт принимать изменяющие операции. Этого недостаточно при crash или сетевом разделении: необходим crash-safe placement/incarnation lease либо доказанная гарантия существующего tablet/DDisk lifecycle, физически запрещающая старой incarnation писать после активации новой. Конкретная реализация этой границы и session-проверки — actor hop через partition, обёртка над `FastPathService` либо расширение самого `FastPathService` — должна быть выбрана после измерения влияния на hot path.

## 8. Контракт NBS2 backend

### Граница совместимости

NBS2 frontend реализует совместимое подмножество classic protobuf service `NCloud.NBlockStore.NProto.TBlockStoreService` из одной зафиксированной source revision, кандидат которой указан в метаданных RFC. Ответы используют classic NBS semantics и не оборачиваются в YDB Operations. Детальные требования к protobuf и RDMA wire compatibility приведены в разделе 14.

### Поддерживаемые операции

| RPC | Назначение |
|---|---|
| `Ping` | Compatibility/liveness check конкретного NBS2 endpoint |
| `MountVolume` | Создание или идемпотентное восстановление session в локальной PartitionTablet |
| `UnmountVolume` | Отзыв session в локальной PartitionTablet |
| `ReadBlocks` | Чтение через общий frontend core |
| `WriteBlocks` | Запись через общий frontend core |
| `ZeroBlocks` | Discard/write-zeroes после готовности внутреннего NBS2 path |

`DescribeVolume` остаётся операцией NBS1 и не входит в runtime API NBS2 frontend.

### Контракт `DataRoute`

`DataRoute` должен содержать достаточно информации для создания NBS2 backend client:

- обязательный gRPC endpoint для `MountVolume`/`UnmountVolume` и data fallback;
- опциональный RDMA endpoint;
- поддерживаемые data transports;
- гарантию принадлежности session- и data-endpoints одному NBS2 host/incarnation;
- опциональную generation маршрута для обновления cache и диагностики.

Для распознанного `fast_disk` поддерживаемый `DataRoute` обязателен; отсутствие или непонимание маршрута является ошибкой совместимости. Точное wire-представление `DataRoute` остаётся открытым вопросом 20.1.

### Использование транспортов

`MountVolume` и `UnmountVolume` выполняются по gRPC. Data I/O выполняется по gRPC или RDMA на том же NBS2 host. Оба data transport используют общий backend contract и error model.

## 9. Discovery и runtime-session flow

### Диск в исходной cell

```text
StartEndpoint(disk1) в NBS1 cell-a
    -> локальный DescribeVolume(disk1)
    -> SSProxy/SchemeShard
    <- Volume + пустой CellId
    -> для fast_disk: получить DataRoute
         как часть DescribeVolume или отдельным resolve
    -> создать backend client:
         NBS1 disk:
              Service = локальный NBS1 service
              Storage = существующий local NBS1 storage
         fast_disk:
              Service = gRPC NBS2 frontend из DataRoute
              Storage = gRPC/RDMA NBS2 endpoint того же host
    -> MountVolume через Service выбранного backend
         NBS1 disk -> существующий NBS1 mount path
         fast_disk -> NBS2 frontend -> локальная PartitionTablet transaction
    -> создать NBD/vhost endpoint на NBS1 host
```

Пустой `CellId` означает только принадлежность диска исходной cell. Для NBS1-диска отсутствие `DataRoute` по-прежнему выбирает локальный NBS1 `StorageProvider`; для `fast_disk` требуется прямой NBS2 client, а отсутствие поддерживаемого маршрута является ошибкой. Режим remote mount для `fast_disk` выбирается по NBS2 backend route, а не по `CellId`: даже для диска своей cell `MountVolume` отправляется по сети в NBS2 frontend.

### Диск в другой cell

```text
StartEndpoint(disk1) в NBS1 cell-a
    -> cells discovery
         -> DescribeVolume(disk1, CellId=cell-b) на NBS1 control endpoints cell-b
         -> SSProxy/SchemeShard cell-b
         -> при NBS2-диске: placement resolver -> NBS2 hostB
    <- Volume + CellId=cell-b
    -> для fast_disk: получить DataRoute
         как часть DescribeVolume или отдельным resolve
    -> создать backend client:
         NBS1 disk:
              Service/Storage = существующий GetCellEndpoint(cell-b)
         fast_disk:
              Service = gRPC NBS2 frontend hostB
              Storage = gRPC/RDMA NBS2 endpoint hostB
    -> MountVolume через Service выбранного backend
         NBS1 disk -> NBS1 service cell-b
         fast_disk -> NBS2 frontend hostB -> локальная PartitionTablet transaction
    -> создать NBD/vhost endpoint на NBS1 host cell-a
```

В меж-cell `DescribeVolume` `Headers.CellId` идентифицирует и валидирует уже выбранную для запроса cell, но не определяет конкретный NBS2 host. NBS1 control endpoint, ответивший на `DescribeVolume`, не обязан совпадать с NBS2 runtime endpoint. Стабильный список NBS1 control endpoints cell-владельца используется для первичного discovery и повторного placement resolve; после resolve gRPC session RPC и I/O `fast_disk` идут на NBS2 host из `DataRoute`.

### Общие правила discovery и административных операций

В описании volume нужен явный признак, по которому NBS1 отличает `fast_disk` от обычного NBS1-диска и выбирает NBS2 backend. Таким признаком может быть отдельное значение `StorageMediaKind`. NBS1 create path должен переводить его в создание direct NBS2 tablets; classic NBS backend не должен создавать диск этого типа.

Административные lifecycle operations остаются в NBS1. Для `CreateVolume(StorageMediaKind=fast_disk)` NBS1 должен инициировать создание соответствующей NBS2 metadata/tablets и зарегистрировать полное описание volume в SchemeShard. Runtime session lifecycle (`MountVolume`/`UnmountVolume`) делегируется NBS2 backend. Детальный внутренний create/delete/resize контракт не определяется этим RFC.

Если SchemeShard не находит `DiskId`, NBS1 возвращает `E_NOT_FOUND`. Временная недоступность SchemeShard, placement resolver или NBS2 host address resolver является retriable ошибкой и не должна маскироваться под отсутствие диска.

## 10. Data-plane flow

### NBS1-диск

Существующие NBS1 data paths сохраняются: для диска исходной cell используется local storage provider, для диска другой cell — штатный cells gRPC/RDMA client и существующая серверная маршрутизация NBS1.

### NBS2-диск

```text
NBS1 endpoint
    -> gRPC ReadBlocks/WriteBlocks
       или RDMA ReadBlocksLocal/WriteBlocksLocal
    -> placement-specific NBS2 host
         -> gRPC facade или RDMA target
         -> frontend core
              -> validation и resource/in-flight limits
         -> local partition registry/dispatcher
              -> lifetime-bound I/O handle текущей partition на этом host
         -> session-checking IStorage handle, опубликованный partition
              -> проверка session/access mode
              -> для write/zero: проверка текущего writer generation
         -> FastPathService: read/write
         -> Regions/DDisk
```

gRPC и RDMA adapters проверяют формат сообщения и ограничения своего транспорта, после чего передают I/O в общий frontend core. Он одинаково для обоих transports проверяет `DiskId`, session fields, диапазон блоков, payload и resource limits, включая количество и суммарный объём in-flight запросов. Данные запроса и связанные с ними сетевые буферы должны оставаться доступными до полного завершения операции чтения или записи. Диапазон проверяется на overflow, alignment и выход за границы volume; write payload должен точно соответствовать запрошенному диапазону.

Strict affinity является обязательным свойством NBS2 session и data path. Frontend не обслуживает partition через межузловой tablet pipe. Если local session-control target или I/O handle отсутствует либо отозван, старый host возвращает `E_REJECTED` с отдельным признаком устаревшего маршрута до начала операции. Route-aware слой NBS1 перехватывает этот результат до общей retry-логики, приостанавливает отправку новых I/O и session RPC, повторно получает `DataRoute` и атомарно переключает связанные `Service` и `Storage` под существующей `TSession` на новый host. In-flight операции с известным результатом завершаются до переключения; session RPC с неопределённым результатом после переключения разрешается повторять только по идемпотентному mount/unmount contract, а потенциально выполненный write автоматически не повторяется. Если сохранённый в PartitionTablet `SessionId` остаётся действителен в новой incarnation, remount не требуется; `E_BS_INVALID_SESSION` после переключения запускает mount через новый NBS2 `Service`. Текущий NBS1 switchable client так не умеет: конкретный механизм сериализации backend operations, переключения всей пары и drain старых запросов является частью реализации. Для раннего нагрузочного прототипа допустимы закреплённый placement и ручное восстановление; production-вариант должен выполнять восстановление автоматически.

`E_REJECTED` сам по себе не доказывает, что запрос безопасно повторить. Запрос с неопределённым результатом после разрыва соединения не содержит признака устаревшего маршрута, и NBS1 не должен автоматически повторять потенциально выполненный write без дедупликации по `RequestId` или другой явно зафиксированной семантики. Точное представление stale-route indication и его интеграция с существующим durable retry остаются открытым вопросом.

При полном падении host stale-route indication получить невозможно. Transport error должен запустить повторный resolve и переключение всего NBS2 backend client для последующих запросов, но не автоматический replay write с неопределённым результатом. Безопасное повторение read, идемпотентный повтор mount/unmount и судьба исходного write определяются retry contract отдельно от восстановления маршрута.

## 11. Ключевые инварианты

1. NBS1 является единой пользовательской и административной точкой control plane и владельцем пользовательских endpoint для NBS1- и NBS2-дисков; runtime session RPC `fast_disk` обрабатывает NBS2 backend.
2. Единственный внешний идентификатор volume — логический `DiskId`; его уникальность в пространстве доступных discovery cells является внешним требованием к control plane и конфигурации deployment.
3. SchemeShard является authoritative источником постоянной volume metadata, включая тип диска и tablet IDs.
4. `CellId` определяет cell-владельца; для NBS1-диска отсутствие `DataRoute` сохраняет существующий backend, а для `fast_disk` поддерживаемый `DataRoute` обязателен и выбирает NBS2 session/data backend.
5. `fast_disk` создаётся как NBS2 volume, а его данные обслуживаются только NBS2; metadata остаётся в SchemeShard cell-владельца.
6. NBS2 `MountVolume`, `UnmountVolume` и I/O направляются на host текущей partition; frontend не выполняет межузловой forwarding.
7. Устаревший placement обнаруживается по отсутствующей или отозванной паре lifetime-bound local session-control target и I/O handle до выполнения новой операции и приводит к повторному resolve и переключению пары `Service`/`Storage` на стороне NBS1.
8. В production partition-owned I/O path проверяет session и access mode для каждого I/O, а актуальность writer — для каждой изменяющей операции; состояние frontend не является источником истины.
9. После необходимой доработки retry policy NBS1 не повторяет write с неопределённым результатом; текущее поведение durable client этому инварианту не соответствует.
10. NBS2 не инициирует discovery, mount или I/O к NBS1-дискам.
11. Неподдерживаемая capability не рекламируется guest-у.
12. gRPC и RDMA data adapters NBS2 используют один backend contract, validation и error model; session RPC доступны по gRPC и изменяют то же partition-owned состояние.
13. Direct partition запускается только на host с включёнными NBS2 service и frontend.
14. Новая incarnation partition не принимает изменяющие операции, пока crash-safe механизм не исключил запись через старую incarnation.
15. Успешная смена writer не допускает последующей фиксации уже принятой операции предыдущего writer.

## 12. Управление сессиями и защита от устаревшего writer (writer fencing)

NBS1 управляет жизненным циклом пользовательского endpoint, а его клиентская `TSession` инициирует `MountVolume`, remount и `UnmountVolume`. Для `fast_disk` эти RPC отправляются по gRPC в NBS2 frontend на host из `DataRoute`. Frontend проверяет strict affinity и передаёт запрос локальной PartitionTablet, которая транзакционно создаёт, восстанавливает или отзывает session state. Отдельный NBS1 -> NBS2 `RegisterSession` не используется.

Успешный `MountVolume` возвращается только после сохранения session state в PartitionTablet; response содержит полный classic `Volume`, непустой `SessionId` и согласованный `InactiveClientsTimeout`. Поскольку classic mount request не содержит `SessionId`, PartitionTablet находит существующую session по устойчивой client identity и параметрам mount либо создаёт и фиксирует новую; повторный mount с той же identity и эквивалентными параметрами возвращает актуальный `SessionId`. `SessionId` не может существовать только в process-local состоянии frontend. `UnmountVolume` идемпотентно отзывает эту session в той же PartitionTablet, а повтор после потерянного успешного ответа возвращает сохранённый успешный terminal result.

Process-local registry сопоставляет `DiskId` и `PartitionTabletId` с lifetime-bound парой session-control target и I/O handle текущей локальной partition, но не хранит authoritative session state. После рестарта frontend registry восстанавливается из регистраций живых partitions. Потеря registry не даёт права авторизовать операцию, но сама по себе не аннулирует сохранённую session: NBS1 повторяет placement resolve и переключает NBS2 backend, а remount выполняется при `E_BS_INVALID_SESSION`.

До production partition должна хранить данные, позволяющие по `Headers.ClientId` и `SessionId` определить актуальную session, её access mode и соответствие текущему writer generation. Принадлежность session клиенту и access mode проверяются на каждом I/O; перед каждой изменяющей операцией partition выполняет writer fencing и отклоняет запрос предыдущего writer. Writer generation определяется по сохранённой session и не передаётся в каждом classic I/O request. Проверки только в памяти frontend недостаточно.

Проверки generation только в начале I/O недостаточно: write предыдущего writer может уже находиться в обработке во время смены generation. До успешного ответа на mount нового writer система должна отозвать право старой generation на новые операции и дождаться завершения или безопасного отклонения уже принятых изменяющих операций. Альтернатива — проверять generation в точке фактической фиксации записи. Конкретный механизм остаётся частью session state machine.

Точная state machine PartitionTablet, формат `SessionId`/generation, устойчивая client identity и ключ идемпотентности mount/unmount, взаимодействие с периодическим remount клиентской `TSession`, семантика `InactiveClientsTimeout` и автоматическое завершение неактивных сессий остаются открытыми вопросами. Transport `RequestId` не может быть единственным ключом идемпотентности, поскольку periodic remount использует новый `RequestId`; точная роль `InstanceId` и `IdempotenceId` также должна быть определена. `MountSeqNumber` участвует в writer fencing, но не должен считаться единственным ключом идемпотентности. Обязательное свойство решения: после рестарта frontend, рестарта или миграции partition и смены NBS1 endpoint устаревший writer не может изменить данные.

`VolumeDirect` не является обязательным компонентом выбранного single-partition data path. Если в будущем появятся multi-partition volumes или потребуется единая volume-level координация, необходимость отдельного volume-level компонента должна быть рассмотрена заново.

Local auto-vhost NBS2 постоянно выключен для дисков, которыми управляет NBS1, иначе появляется второй путь к данным вне общей модели session ownership и writer fencing. Реализация auto-vhost остаётся в коде для автономных конфигураций NBS2.

## 13. Error model

| Ситуация | Наблюдаемый результат |
|---|---|
| Disk отсутствует или path имеет неверный scheme type | `E_NOT_FOUND` |
| SchemeShard, placement resolver или NBS2 host address resolver временно недоступен | `E_REJECTED` |
| Неверный `CellId` в меж-cell `DescribeVolume` | `E_REJECTED` |
| NBS2 partition отсутствует на выбранном host или route устарел до начала session/data operation | `E_REJECTED` + отдельный машиночитаемый stale-route indication |
| Неверный range, alignment или payload | `E_ARGUMENT` |
| Конфликт read-write mount | `E_BS_MOUNT_CONFLICT` |
| Неизвестная, завершённая или stale session в data request | `E_BS_INVALID_SESSION` |
| Повтор `UnmountVolume` для уже успешно отозванной session той же client identity | Идемпотентный успешный terminal result |
| Разрыв соединения во время mount/unmount | Transport error; повтор разрешён только по зафиксированному idempotency contract |
| Разрыв соединения после передачи I/O; результат операции неизвестен | Transport error без stale-route indication; автоматический retry write запрещён |
| Отключённый zero/discard | `E_NOT_IMPLEMENTED` |

Существующий volume с неподдерживаемой frontend геометрией должен успешно проходить discovery, после чего `MountVolume` возвращает детерминированный `E_NOT_IMPLEMENTED` или `E_ARGUMENT`. Это не позволяет cells ошибочно интерпретировать unsupported capability как отсутствие disk.

Route-aware слой может повторять I/O после разрешения нового маршрута только при наличии stale-route indication, гарантирующего, что операция на старом host не начиналась. `E_REJECTED` без этого признака и transport error после передачи запроса не дают такой гарантии для write. Mount/unmount должны иметь отдельный идемпотентный retry contract. Конкретный совместимый способ кодирования признака должен быть выбран до реализации автоматического recovery.

Внутренние NBS2 ошибки преобразуются только в коды ошибок зафиксированной classic NBS revision. Пользовательский сетевой запрос не должен приводить к `Y_ABORT` или другой process-wide invariant failure.

## 14. Совместимость

- classic NBS protobuf package, полный service name, номера полей, коды ошибок, IDs RDMA-сообщений и wire layout копируются из одной зафиксированной source revision;
- solution-specific optional поля, значения enum и error indications добавляются обратно совместимо, получают новые номера и явно документируются относительно зафиксированной source revision;
- изменение source revision требует protobuf compatibility tests, RDMA wire tests и повторного E2E через NBS1;
- изменения discovery/backend selection в NBS1 должны быть обратно совместимы: существующие NBS1-диски без нового `DataRoute` обслуживаются по прежним путям;
- добавление `fast_disk` не должно менять результат discovery или выбор backend client для существующих media kinds;
- `fast_disk` нельзя создавать или показывать endpoint hosts, пока capability gate не подтвердит поддержку media discriminator, `DataRoute`, NBS2 `MountVolume`/`UnmountVolume` и fail-closed поведения всеми потенциальными NBS1 hosts;
- gRPC session API и gRPC/RDMA data adapters NBS2 проверяются общим набором frontend contract tests;
- NBS2 frontend выключен по умолчанию; выключенный frontend не регистрирует classic service/RDMA target и не меняет обычные пути `ydbd`;
- namespace и protobuf symbol конфликты между YDB, NBS2 и classic NBS должны выявляться build/compatibility тестами.

## 15. Безопасность и эксплуатация

Для classic NBS интерфейса NBS2 frontend переиспользует совместимые настройки и эксплуатационные правила NBS1. Необходимые отклонения должны быть явно перечислены и обоснованы, а не образовывать неявную параллельную модель конфигурации.

Из NBS1 переиспользуются или сохраняются совместимыми:

- идентификаторы cells и настройки подключений: gRPC/RDMA ports, transport, таймауты и количество соединений;
- защищённый gRPC: `SecureGrpcPort`, доверенный сертификат центра сертификации (`CA`, `RootCertsFile`), клиентские и серверные сертификаты, а также существующий способ передачи authentication metadata;
- ограничения обработки запросов: максимальный размер сообщения, request timeout, квота памяти, число workers и объём незавершённых запросов;
- classic NBS error semantics и параметры throttling/QoS;
- принятые в NBS1 форматы request logs, метрик и диагностической информации;
- для RDMA используется модель сетевой защиты целевого NBS1 deployment; TLS для RDMA этим RFC не предлагается.

Frontend сохраняет совместимость с параметрами throttling/QoS NBS1, но не должен создавать второй независимый контур ограничения IOPS или bandwidth. До production необходимо выбрать один слой NBS2, отвечающий за применение каждого ограничения.

Для NBS2 frontend и route resolution требуются:

- отдельный флаг включения; по умолчанию classic NBS service и RDMA target не запускаются;
- ограниченные очереди, память и число одновременно выполняющихся запросов;
- защита других сервисов `ydbd` от неконтролируемой нагрузки frontend;
- публикация только routable gRPC/RDMA endpoints, доступных из разрешённых исходных cells;
- передача и проверка совместимых с classic NBS authentication metadata и client identity для `MountVolume`/`UnmountVolume`; NBS2 принимает session RPC только от разрешённых NBS1 cells;
- метрики и логи в соответствующих NBS1/NBS2-компонентах для discovery, placement resolve, `MountVolume`/`UnmountVolume`, состояния и автоматического завершения неактивных сессий, gRPC/RDMA запросов, stale routes, reconnect, очередей и ошибок;
- возможность поэтапной выкатки, остановки и отката без потери контроля над активными сессиями.

Базовый рассматриваемый вариант регистрирует classic NBS service на общем YDB gRPC server внутри `ydbd`, а запрос маршрутизируется по полному имени gRPC service. RDMA target запускается в том же процессе на отдельном конфигурируемом `RdmaPort`. Окончательный выбор между общим gRPC server и отдельным listener остаётся открытым вопросом 20.6 и определяется сетевой доступностью, совместимостью TLS/authentication, параметрами gRPC-соединений и необходимостью независимо открывать или закрывать classic NBS traffic, но не меняет распределение CPU, памяти или actor-system ресурсов внутри `ydbd`.

## 16. Альтернативы

- **Placement-aware route и strict-affinity NBS2 frontend внутри `ydbd`.** **Предлагается**: NBS1 разрешает и выбирает host текущей partition, а frontend выполняет session operations и I/O только для локальной partition.

- **Произвольный NBS2 frontend host с межузловым forwarding.** Не выбран: добавляет actor-interconnect hop в RDMA hot path и скрывает неактуальный placement.

- **Compatibility sidecar.** Не выбран: добавляет IPC/routing и отдельный deployment surface, не устраняя необходимость доступа к локальной partition.

- **Прямой tablet pipe из NBS1.** Не выбран для `fast_disk`: не использует требуемый gRPC/RDMA data path и переносит YDB routing details в legacy NBS process.

- **Обрабатывать `MountVolume` в NBS1 и передавать в NBS2 отдельные `RegisterSession`/`UnregisterSession`.** Не выбран: добавляет внутренний контракт, разделяет обработку одной session между двумя backend и отличается от существующей модели cells, в которой `TSession` отправляет classic `MountVolume` выбранному service.

- **Обязательный `VolumeDirect` перед partition.** Не выбран для single-partition path: добавляет дополнительный уровень, хотя session/writer validation может выполняться partition. Вариант следует пересмотреть при появлении multi-partition volume-level координации.

- **Local NBS2 vhost socket.** Не выбран: node-local endpoint обходит управляемый NBS1 endpoint и выбранный сетевой session/data contract и не даёт нужного меж-cell доступа.

- **Расширить draft `Ydb.Nbs` вместо classic NBS compatibility surface.** Не выбран: потребует отдельного transport/backend adapter в NBS1 и не позволит переиспользовать существующий cells gRPC/RDMA contract.

Кодовые доказательства и расширенное сравнение после согласования RFC должны быть приведены в соответствие в [architecture.md](architecture.md).

## 17. Проверка решения и последующее обновление планов

До согласования этого RFC существующие MVP/production документы сохраняются без изменений и могут описывать предыдущую gateway-модель. Их этапы, оценки и тестовые матрицы обновляются отдельным явным шагом после принятия архитектурных решений RFC.

Обновлённый план должен последовательно проверить:

1. NBS1 находит локальный NBS2 `fast_disk`, получает placement-aware route, выбирает remote mount semantics несмотря на пустой `CellId`, отправляет `MountVolume` по gRPC и выполняет gRPC I/O через frontend на host локальной partition.
2. Тот же workload использует gRPC `MountVolume` и classic NBS RDMA для I/O на одном NBS2 host без межузлового forwarding.
3. Существующие локальные и меж-cell NBS1-диски продолжают использовать прежние data paths.
4. NBS1 в одной cell находит NBS2-диск в другой cell, получает routable endpoints конкретного NBS2 host и отправляет на него `MountVolume` и I/O.
5. Stale placement приводит к `E_REJECTED` с отдельным stale-route indication, повторному resolve и атомарному переключению NBS2 `Service` и `Storage` на новый host; remount выполняется только после `E_BS_INVALID_SESSION`, а для production восстановление автоматизировано.
6. Restart frontend и migration partition сохраняют зафиксированную в PartitionTablet session, позволяют доставить `UnmountVolume` текущей incarnation и не дают устаревшему writer продолжить запись.
7. gRPC и RDMA используют одинаковые validation, session и error semantics.
8. Полное падение NBS2 host приводит к обновлению маршрута для последующих запросов без автоматического replay write с неопределённым результатом; безопасный read может быть повторён согласно retry contract.
9. Mount нового writer не завершается, пока уже принятые изменяющие операции предыдущей generation не завершены или безопасно отклонены.
10. NBS1 host без поддержки `fast_disk`, `DataRoute` или NBS2 session RPC возвращает ошибку совместимости и не строит смешанный либо legacy backend.
11. Повтор `MountVolume` после потерянного ответа не создаёт вторую независимую session и не увеличивает writer generation повторно; повтор `UnmountVolume` безопасен.
12. Stale route во время mount/unmount приводит к resolve и идемпотентному повтору на текущем NBS2 host.

Performance acceptance thresholds и полная failure matrix определяются в планах соответствующих этапов, а не в RFC.

## 18. Технические риски / возможные проблемы

1. **Placement меняется между discovery, mount и I/O.**

   Снижение риска: атомарно публикуемая и отзываемая пара lifetime-bound session-control target и I/O handle на NBS2 host, отдельный stale-route indication и повторный resolve на стороне NBS1.

2. **Placement resolver или NBS2 host address resolver возвращает устаревший либо недоступный адрес.**

   Снижение риска: version/generation маршрута, health checks, ограниченный cache TTL и fallback к стабильным NBS1 control endpoints cell-владельца.

3. **Session split-brain после restart frontend или migration partition.**

   Снижение риска: authoritative session/writer state транзакционно хранится в PartitionTablet; session-checking I/O path проверяет это состояние для каждого I/O и актуального writer для каждой изменяющей операции.

4. **Изменения discovery ломают существующие NBS1-диски.**

   Снижение риска: optional `DataRoute`, прежняя семантика для NBS1-диска при его отсутствии, fail-closed для `fast_disk` и отдельная regression matrix для всех существующих media kinds.

5. **Неприемлемый dependency graph classic NBS proto/RDMA stack.**

   Снижение риска: ранний dependency spike и первая попытка перенести существующий RDMA target целиком. Минимальный wire-compatible adapter остаётся резервным вариантом при неприемлемом dependency graph.

6. **Один frontend actor/mailbox либо NBS1 vhost executor становится bottleneck.**

   Снижение риска: метрики очередей; при подтверждении — несколько I/O actors и/или vhost queues.

7. **Нарушение ordering overlapping writes.**

   Снижение риска: явно зафиксировать ordering contract и проверить конкурентными тестами.

8. **Дополнительные network/processing hops снижают IOPS.**

   Снижение риска: сравнить производительность прямого NBS2 I/O, gRPC и RDMA и определить вклад каждого участка data path с помощью профилирования.

9. **NBS2 endpoints cell-владельца недоступны из исходной cell или несовместимы с её security policy.**

   Снижение риска: проверять routability, TLS/authentication, передачу mount identity и параметры соединений при конфигурации cell и до включения `fast_disk`.

10. **Нагрузка frontend влияет на другие сервисы `ydbd`.**

    Риск принимается: `ydbd`, в котором включён NBS2 frontend, является dynamic node, предназначенным в том числе для обработки пользовательского I/O. Ограниченные очереди, память и число одновременно выполняющихся запросов обязательны независимо от выбранного listener.

11. **Старая и новая incarnation partition одновременно принимают I/O при migration.**

    Снижение риска: при управляемой миграции — атомарный отзыв lifetime-bound session-control target и I/O handle, запрет новых запросов и безопасное завершение или отклонение уже принятых операций; при аварии или сетевом разделении — crash-safe placement lease/epoch либо доказанная гарантия tablet/DDisk lifecycle до активации новой incarnation.

12. **Одинаковый `E_REJECTED` приводит к небезопасному retry.**

    Снижение риска: отдельный машиночитаемый stale-route indication, обработка route recovery до общей durable retry-логики и запрет повторения write с неопределённым результатом без дедупликации.

13. **Смена writer generation опережает уже принятый write предыдущего writer.**

    Снижение риска: mount нового writer завершается только после запрета новых операций и drain уже принятых изменяющих операций старой generation либо generation проверяется в точке фиксации записи.

14. **Mixed-version NBS1 host направляет `fast_disk` в legacy или смешанный backend.**

    Снижение риска: fail-closed при отсутствии поддерживаемого `DataRoute` или NBS2 session RPC и capability gate перед созданием или экспонированием `fast_disk`.

15. **Повтор `MountVolume` или `UnmountVolume` после route change или потерянного ответа нарушает session state.**

    Снижение риска: идемпотентность по устойчивой client identity и параметрам mount, а не только по transport `RequestId` или `MountSeqNumber`; транзакционная фиксация результата и достаточное хранение terminal result в PartitionTablet.

## 19. Зафиксированные решения

1. NBS1 является единой внешней точкой control plane для NBS1- и NBS2-дисков и всегда создаёт пользовательский endpoint.
2. NBS1 должен находить и направлять session/data path к обоим типам backend как внутри исходной cell, так и между cells.
3. `fast_disk` имеет отдельный согласованный discriminator, создаётся только в NBS2, а его постоянная metadata хранится в SchemeShard cell-владельца.
4. `CellId` определяет cell-владельца. Для NBS1-диска отсутствие `DataRoute` сохраняет существующий path; для `fast_disk` обязательный `DataRoute` задаёт связанные NBS2 gRPC `Service` и gRPC/RDMA `Storage` одного host.
5. NBS2 session/data path использует strict affinity: NBS1 подключается к host текущей partition, а NBS2 frontend не выполняет межузловой forwarding.
6. Для single-partition `fast_disk` I/O направляется в опубликованный partition-owned session-checking I/O path, который передаёт разрешённый запрос в `FastPathService`; обязательный `VolumeDirect` в data path не используется.
7. Session-checking I/O path по `Headers.ClientId` и `SessionId` проверяет принадлежащее partition состояние session и access mode для каждого I/O, а соответствие текущему writer generation — для каждой изменяющей операции; process-local frontend state не является источником истины.
8. NBS2 не инициирует discovery, mount или I/O к NBS1-дискам.
9. Для существующих NBS1-дисков сохраняются прежние local и cells data paths; placement-aware оптимизация NBS1 не обязательна.
10. Существующий classic NBS RDMA target сначала пробуем перенести целиком. Минимальный wire-compatible adapter остаётся резервным вариантом при неприемлемом dependency graph.
11. Local auto-vhost выключен для `fast_disk`, которым управляет NBS1, но остаётся в коде для автономных конфигураций NBS2.
12. Sidecar не требуется: NBS2 frontend работает внутри `ydbd`.
13. `DescribeVolume`, создание диска и `StartEndpoint` остаются в NBS1. Созданная NBS1 `TSession` инициирует `MountVolume`/`UnmountVolume`, но для `fast_disk` отправляет их по gRPC в NBS2 frontend на host из `DataRoute`; NBS2 PartitionTablet транзакционно хранит server-side session state.
14. Уникальность `DiskId` между доступными discovery cells обеспечивается внешним control plane или проверкой конфигурации, а не текущим алгоритмом cells discovery.
15. Для распознанного `fast_disk` `DataRoute` обязателен; отсутствие или непонимание маршрута не включает legacy NBS1 fallback.
16. Дополнительный внутренний `RegisterSession`/`UnregisterSession` между NBS1 и NBS2 не вводится: classic `MountVolume`/`UnmountVolume` являются единственным session contract выбранного NBS2 backend.

## 20. Оставшиеся открытые вопросы

1. Как представить логический `DataRoute` в wire contract: optional поля в `DescribeVolume` или результат отдельного `ResolveVolumeEndpoint` RPC? Представление должно включать обязательный gRPC session endpoint, выбранный gRPC/RDMA data endpoint и признак их принадлежности одному NBS2 host/incarnation.
2. Какой компонент является authoritative источником текущего `PartitionTabletId -> NodeId`, как direct partition ограничивается hosts с включёнными NBS2 service/frontend, как `NodeId` сопоставляется с routable gRPC/RDMA endpoints и как NBS2 host address resolver получает аутентифицированную исходную cell, если выбор адреса зависит от неё?
3. Как реализовать local partition registry, session-проверку и безопасный handoff атомарно публикуемой пары session-control target и I/O handle между incarnations: через actor hop на каждом I/O, обёртку над `FastPathService` или расширение самого `FastPathService`? Какой crash-safe lease/epoch либо существующая tablet/DDisk гарантия запрещает старой incarnation писать после hard failure или сетевого разделения?
4. Какую точную persistent session state machine реализует NBS2 PartitionTablet, включая устойчивую client identity и ключ идемпотентности mount/unmount, срок хранения terminal result, роль `InstanceId`, `IdempotenceId` и `MountSeqNumber`, writer generation, `InactiveClientsTimeout`, drain уже принятых операций предыдущего writer, periodic remount и автоматическое завершение неактивных сессий?
5. Как NBS1 сериализует новые и in-flight session RPC/I/O и атомарно переключает связанные `Service` и `Storage` под существующей `TSession` после stale placement или полного падения host, как кодируется stale-route indication и как route recovery для mount/unmount и I/O взаимодействует с существующей durable retry-логикой? Целевой вариант сохраняет `SessionId`, если новая incarnation подтверждает persisted session; при `E_BS_INVALID_SESSION` выполняется remount через новый `Service`.
6. Можно ли зарегистрировать classic NBS service на существующем YDB gRPC server или для него нужен отдельный gRPC listener внутри `ydbd`?
   - существующий endpoint должен быть доступен из сети NBS1 cells, а открытие YDB-порта для этого трафика должно быть допустимо;
   - TLS- и authentication-настройки должны быть совместимы с требованиями NBS1;
   - параметры gRPC-соединений, задаваемые на уровне server/listener, должны подходить для classic NBS traffic;
   - отдельный listener нужен, если classic NBS traffic требуется независимо открывать или закрывать на сетевом уровне.
7. Должна ли partition упорядочивать одновременно выполняющиеся записи в пересекающиеся диапазоны блоков? Выбранное поведение должно быть одинаковым для gRPC и RDMA.
8. Какой внутренний NBS1 -> NBS2 контракт используется для create/delete/resize `fast_disk`, включая точное значение media discriminator и его трансляцию в direct tablets, при сохранении внешнего control plane в NBS1?
9. На каком единственном слое NBS2 применяются IOPS/bandwidth throttling и QoS, чтобы не дублировать ограничения NBS1?
10. Как control plane обеспечивает уникальность `DiskId` между cells или обнаруживает несколько успешных результатов discovery?
11. Как capability gate подтверждает поддержку `fast_disk`, `DataRoute` и NBS2 `MountVolume`/`UnmountVolume` всеми NBS1 endpoint hosts и целевыми NBS2 hosts до включения нового типа диска?

## 21. Чеклист ревьювера

Нужно подтвердить следующие пункты:

1. NBS1 остаётся единой внешней точкой control plane и владельцем NBD/vhost endpoint.
2. SchemeShard каждой cell хранит описания принадлежащих ей NBS1- и NBS2-дисков; `fast_disk` имеет отдельный согласованный discriminator и создаётся только в NBS2.
3. Поддержаны четыре маршрута: local NBS1, local NBS2, remote NBS1 и remote NBS2.
4. `CellId` определяет cell-владельца; для NBS1-диска отсутствие `DataRoute` сохраняет прежний path, а для `fast_disk` поддерживаемый `DataRoute` обязателен и определяет связанные NBS2 session/data endpoints одного host.
5. Изменения discovery/backend selection обратно совместимы с существующими NBS1-дисками.
6. `MountVolume`/`UnmountVolume` по gRPC и I/O по gRPC/RDMA направляются на один host текущей NBS2 partition без межузлового forwarding.
7. Stale placement обнаруживается по атомарно публикуемой и отзываемой паре lifetime-bound session-control target и I/O handle и приводит к `E_REJECTED` с отличимым stale-route indication и повторному resolve на стороне NBS1.
8. PartitionTablet транзакционно хранит session/access/writer state; session-checking I/O path проверяет `Headers.ClientId`, `SessionId`, access mode для каждого I/O и актуального writer для изменяющих операций, а frontend state не используется как источник корректности.
9. `VolumeDirect` не является обязательным уровнем single-partition data path.
10. NBS2 не инициирует доступ к NBS1-дискам.
11. Classic NBS gRPC и wire-compatible RDMA используют общий frontend core; существующий RDMA target сначала переносится целиком.
12. Local auto-vhost не создаёт обходной путь к управляемому NBS1 `fast_disk`.
13. Подтверждено, что NBS1 `TSession` инициирует `MountVolume`/`UnmountVolume`, но для `fast_disk` отправляет их непосредственно в NBS2 gRPC `Service`; backend route задаёт remote mount semantics независимо от `CellId`, а отдельный `RegisterSession`/`UnregisterSession` не используется.
14. До принятия RFC определены wire-представление `DataRoute`, обязательный gRPC session endpoint, источник placement, eligibility NBS2 hosts, публикация routable endpoints одного host и атомарная публикация/отзыв локальной пары session-control target и I/O handle.
15. До production согласованы идемпотентная session/writer state machine, хранение terminal result и `InactiveClientsTimeout`, отличимый stale-route error, сериализация backend operations, атомарное переключение `Service`/`Storage` под существующей `TSession`, безопасное автоматическое восстановление route и поведение overlapping writes.
16. Для выбранного gRPC listener подтверждены сетевая доступность, совместимость TLS/authentication и параметров соединений.
17. Выбран единственный слой применения NBS2 throttling/QoS.
18. Подтверждены media discriminator `fast_disk`, его create-time трансляция и механизм уникальности `DiskId` между cells.
19. Подтверждены crash-safe защита от старой partition incarnation, drain операций предыдущего writer и recovery после полного падения data host.
20. Mixed-version rollout защищён capability gate для media discriminator, `DataRoute` и NBS2 session RPC, а `fast_disk` без полного поддерживаемого backend завершается fail-closed.
