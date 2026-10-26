# MVP-1/MVP-2: нагрузочный hot path NBS1 → NBS2

## 1. Метаданные и назначение

| Поле | Значение |
|---|---|
| Статус | Актуальный план разработки |
| Создан | 2026-08-20 |
| Архитектурная основа | [RFC интеграции NBS1 с NBS2](rfc3.md) |
| Историческая версия | [Архивный план MVP-1/MVP-2](archive/mvp_benchmark.md), неактуален |

Этот документ превращает архитектуру `rfc3` в проверяемый план двух ранних
этапов разработки:

1. MVP-1 строит и проверяет промежуточный end-to-end путь через штатный NBS1
   cells gRPC transport.
2. MVP-2 заменяет data transport на штатный cells RDMA и проверяет целевой
   RDMA path.

`rfc3.md` является источником архитектурных решений. Этот план может сужать
scope раннего прототипа, но не заменяет и не переопределяет RFC. Временные
допущения MVP перечислены отдельно и не считаются изменением целевой
архитектуры.

## 2. Результат этапа и границы

### 2.1. Целевой путь

Оба MVP используют один пользовательский workflow и один NBS2 backend:

```text
StartEndpoint в NBS1
    -> DescribeVolume и получение DataRoute на стороне NBS1
    -> MountVolume по gRPC на выбранный NBS2 host
    -> benchmark session adapter для одного клиента
    -> vhost endpoint в NBS1
    -> ReadBlocks/WriteBlocks через cells gRPC (MVP-1) или RDMA (MVP-2)
    -> проверка фиксированных DiskId/SessionId
    -> request pre-processing
    -> FastPathService -> PBuffer/DDisk
```

### 2.2. Архитектурные ограничения из RFC

- пользовательский vhost endpoint всегда создаёт NBS1;
- `DescribeVolume` выполняет NBS1; NBS2 frontend его не реализует;
- `MountVolume` и `UnmountVolume` идут по gRPC;
- gRPC и RDMA I/O направляются на тот же host, где локально работает нужная
  NBS2 partition;
- NBS2 frontend не пересылает I/O между hosts;
- gRPC и RDMA используют общий backend, validation и error model;
- write с неопределённым результатом не повторяется автоматически;
- local auto-vhost NBS2 для управляемого NBS1 `fast_disk` выключен;
- multi-partition `fast_disk` не поддерживается.

RFC также задаёт целевую модель, в которой authoritative session, access mode
и writer state хранит PartitionTablet, а каждый I/O проходит
session/access/generation guard. MVP-1/2 не заменяет эту модель; временное
упрощение session path описано в разделе 3.

### 2.3. Границы MVP-1/2

Scope этапов ограничивается:

- одним заранее созданным `fast_disk` с одной partition и block size 4096;
- одним NBS2 host, клиентом и writer;
- операциями `ReadBlocks` и `WriteBlocks`;
- закреплённой конфигурацией на время каждого прогона.

Отдельный benchmark storage stub и упрощённый benchmark-only RDMA protocol не
допускаются: они изменили бы исследуемый data path и не позволили бы проверить
целевой workflow.

## 3. Временные допущения MVP-1/2

Следующие решения используются только для быстрого построения benchmark path.
Они упрощают реализацию MVP, но не предлагают альтернатив целевой архитектуре
из `rfc3`.

| Временное допущение | Реализация в MVP | Почему не противоречит RFC |
|---|---|---|
| Статические disk metadata и `DataRoute` | Один диск и связанная пара gRPC/RDMA endpoints одного host задаются benchmark-конфигурацией на стороне NBS1 | `DescribeVolume` и выбор route остаются ответственностью NBS1; hardcode временно заменяет resolver, а не меняет его архитектуру |
| Frontend внутри `ydbd` | Classic-compatible gRPC facade и RDMA target встраиваются в `ydbd` и выключены по умолчанию | Сохраняется логическая граница NBS2 frontend и strict host affinity; deployment choice ограничен MVP |
| Process-local session | `MountVolume` возвращает один synthetic `SessionId`, который проверяется вместе с `DiskId` | Целевая authoritative session model остаётся в PartitionTablet; adapter не объявляется её заменой |
| Закреплённый placement | Route не обновляется во время прогона, восстановление после migration/restart выполняется вручную | RFC прямо допускает закреплённый placement и ручное восстановление для раннего нагрузочного прототипа |
| Один client/writer | Multi-client, access modes и writer handoff не входят в E2E-сценарий | MVP не меняет соответствующие гарантии RFC, а не реализует и не проверяет их |
| `ZeroBlocks` выключен | Capability не рекламируется, запрос завершается контролируемой ошибкой без обращения к backend | RFC разрешает операцию после готовности внутреннего NBS2 path; MVP проверяет только read/write |

## 4. MVP-1: gRPC hot path

### 4.1. Цель и результат

MVP-1 должен максимально быстро получить корректный read/write путь из
созданного NBS1 endpoint через штатный cells gRPC transport в настоящий NBS2
partition и подтвердить его end-to-end работоспособность.

Результат MVP-1:

- classic NBS-compatible NBS2 frontend;
- NBS1-side metadata/route для одного заранее подготовленного `fast_disk`;
- process-local session для одного заранее настроенного клиента и writer;
- общий для gRPC/RDMA transport backend;
- настоящий path до `FastPathService`, PBuffer и DDisk;
- минимальный E2E correctness-набор для cells gRPC path.

### 4.2. Шаг 0. Зафиксировать контракты MVP

Цель: не начинать реализацию на неоднозначных wire и benchmark contracts.

Действия:

- использовать classic NBS revision
  `953e9966ce94d1724ccdb3e6676ac1f6036f04ae` как source для protobuf, RDMA
  wire protocol и error codes;
- определить регистрацию frontend внутри `ydbd`, gRPC/RDMA ports и
  конфигурацию безопасного включения;
- зафиксировать benchmark config для одного disk, partition и статического
  `DataRoute`;
- зафиксировать временный benchmark session contract: один `DiskId`, один
  `SessionId`, один writer, без persistence и restart recovery;
- определить минимальный error mapping и запрет автоматического retry write
  после передачи запроса backend;
- зафиксировать проверки range, alignment и payload size;
- определить способ выключения local auto-vhost для benchmark `fast_disk`.

Проверка:

- все перечисленные контракты однозначно записаны и проверяемы;
- список classic messages, fields и error codes проверяется compatibility
  tests относительно revision
  `953e9966ce94d1724ccdb3e6676ac1f6036f04ae`;
- benchmark session явно помечена как временное допущение из раздела 3;
- минимальный session contract покрывает mount, unmount и unknown/revoked
  `SessionId` в рамках жизни frontend process.

### 4.3. Шаг 1. Встроить каркас NBS2 frontend

Цель: доказать build/runtime совместимость перенесённого classic gRPC server с
`ydbd` и заранее определить границу зависимостей classic gRPC server и RDMA
target.

```text
classic NBS client/test
    -> NBS2 gRPC endpoint
    -> classic-compatible frontend
    -> direct Ping/error response
```

Действия:

- добавить выключенную по умолчанию конфигурацию frontend и ports;
- перенести classic NBS gRPC server из зафиксированной source revision и
  запустить его внутри `ydbd` на отдельном listener;
- зарегистрировать classic `NCloud.NBlockStore.NProto.TBlockStoreService` и
  `NCloud.NBlockStore.NProto.TBlockStoreDataService`; frontend backend adapter
  реализует только необходимое MVP подмножество методов;
- реализовать `Ping` и каркас `MountVolume`, `UnmountVolume`, `ReadBlocks`,
  `WriteBlocks`, а для `ZeroBlocks` — временную семантику из раздела 3;
- не реализовывать `DescribeVolume` как runtime API NBS2 frontend; если общий
  classic service требует handler, он возвращает контролируемую ошибку, а NBS1
  не направляет в него этот RPC;
- выполнить compile/dependency spike переноса classic gRPC server и classic
  RDMA target;
- не добавлять отдельный backend path или отдельный benchmark protocol.

Проверка:

- сборка и обычный запуск NBS2 не меняются при выключенном frontend;
- отдельный classic gRPC listener открывается только при включённом frontend;
- classic client получает совместимый `Ping` response;
- неподдерживаемые RPC завершаются контролируемой classic error;
- `DescribeVolume` не обслуживается NBS2 frontend как runtime API;
- зафиксирован состав переносимых зависимостей classic gRPC server и RDMA
  target либо конкретный dependency blocker и граница минимального
  wire-compatible adapter.

### 4.4. Шаг 2. Подготовить один диск и закреплённый `DataRoute`

Цель: дать NBS1 достаточно metadata для штатного `StartEndpoint`, не выдавая
статическую benchmark route за production resolver.

Действия:

- создать один `fast_disk` с одной NBS2 partition и block size 4096;
- подготовить на стороне NBS1 полный результат `DescribeVolume` для этого
  диска и явный признак `fast_disk`;
- задать связанную пару gRPC `Service` и gRPC `Storage` одного NBS2 host;
- проверить соответствие route локальной `PartitionTabletId` и выбранному
  NBS2 host;
- для другого `DiskId` возвращать `E_NOT_FOUND`;
- отсутствие/непонимание route завершать ошибкой без fallback в NBS1 storage;
- включить параметры fixture в benchmark config.

Проверка:

- `DescribeVolume` выполняется NBS1, а не NBS2 frontend;
- настроенный диск получает ожидаемую route, другой disk не маршрутизируется;
- `Service` и `Storage` указывают на один NBS2 host;
- route не включает меж-host forwarding и legacy fallback.

### 4.5. Шаг 3. Реализовать временный benchmark session adapter

Цель: дать cells минимальные согласованные mount/unmount ответы для одного
заранее настроенного диска, не реализуя persistent session lifecycle и writer
fencing.

```text
MountVolume(configured DiskId)
    -> process-local benchmark session
    -> classic Volume + fixed/non-empty SessionId

ReadBlocks/WriteBlocks
    -> проверка configured DiskId и active SessionId
    -> common NBS2 backend
```

Действия:

- реализовать `MountVolume` и `UnmountVolume` для одного настроенного диска;
- возвращать полный classic `Volume` и синтетический непустой `SessionId`;
- хранить одну active session в памяти frontend;
- проверять `DiskId` и `SessionId` до вызова common backend;
- после unmount или потери process-local state возвращать
  `E_BS_INVALID_SESSION`;
- исключить multi-client, read-only и multi-writer session-сценарии;
- не реализовывать persistence, writer generation/fencing и восстановление
  session после restart;
- сохранить transport-neutral backend boundary, чтобы на следующем этапе
  заменить adapter целевым PartitionTablet guard без второго data path.

Проверка:

- настроенный disk успешно проходит mount/unmount;
- другой `DiskId` получает `E_NOT_FOUND`;
- unknown или завершённая session получает `E_BS_INVALID_SESSION` и не
  достигает FastPath;
- restart frontend теряет session, что явно является ожидаемым ограничением
  MVP, а не проверкой recovery;
- gRPC и RDMA adapter используют одну benchmark session check и один
  common backend.

### 4.6. Шаг 4. Провести gRPC I/O в настоящий NBS2 backend

Цель: реализовать общий path, который MVP-2 повторно использует без
изменений.

```text
ReadBlocks/WriteBlocks по gRPC
    -> common request adapter
    -> benchmark DiskId/SessionId check
    -> range/alignment/payload validation
    -> FastPathService
    -> PBuffer/DDisk
```

Действия:

- ввести один transport-neutral backend contract для gRPC и RDMA;
- реализовать зафиксированные validation и error mapping до вызова FastPath;
- удерживать request buffers и sglist до terminal completion;
- не повторять write после pipe/transport error с неопределённым результатом;
- исключить из E2E-сценария конкурентные writes в пересекающиеся диапазоны;
- не использовать storage stub, draft `Ydb.Nbs` I/O API или прямые partition
  I/O команды как часть проверяемого path.

Проверка:

- direct classic gRPC pattern write/read даёт одинаковый checksum;
- malformed range, overflow, alignment и payload size возвращают
  контролируемую ошибку до FastPath;
- неправильный write не изменяет данные;
- unknown/revoked session не достигает backend;
- трассировка/счётчики доказывают прохождение настоящего NBS2 partition path.

### 4.7. Шаг 5. Выполнить cells gRPC E2E

Цель: проверить пользовательский workflow с endpoint в NBS1.

Действия:

- настроить peer cell и закреплённый gRPC `DataRoute`;
- выполнить `StartEndpoint` в NBS1;
- подтвердить `DescribeVolume` на NBS1 и `MountVolume` на NBS2 host;
- через созданный NBS1 endpoint выполнить pattern write/read и сравнить
  checksum для нескольких offsets и I/O sizes;
- проверить границы диска и неверный payload;
- подтвердить по трассировке/счётчикам, что запросы прошли через cells gRPC в
  настоящий NBS2 partition.

Проверка:

- `StartEndpoint` завершается и создаёт vhost socket только в NBS1;
- control и data проходят по штатному cells gRPC path;
- checksum записанных и прочитанных данных совпадает;
- local auto-vhost NBS2 не создаётся.

## 5. MVP-2: RDMA hot path

### 5.1. Цель и результат

MVP-2 на том же диске, временной benchmark session model, endpoint workflow и
NBS2 backend меняет только data transport с gRPC на штатный cells RDMA.
Control plane остаётся на gRPC.

### 5.2. Шаг 1. Добавить classic-compatible RDMA target

Цель: принять штатный NBS1 cells RDMA protocol и передать запрос в backend,
проверенный в MVP-1 через gRPC.

```text
NBS1 cells RDMA client
    -> classic NBS RDMA wire protocol
    -> NBS2 RDMA adapter
    -> та же benchmark session check и common backend
    -> FastPathService
```

Действия:

- использовать message IDs, wire layout и errors зафиксированной classic NBS
  revision;
- запустить RDMA endpoint в том же deployment boundary и на том же host, что
  gRPC `Service`;
- адаптировать RDMA read/write к общему backend без второго storage path;
- повторно использовать проверку `DiskId`/`SessionId`, validation и error
  mapping MVP-1;
- применить временную семантику неподдерживаемого `ZeroBlocks` из раздела 3;
- добавить protocol compatibility и malformed-message tests.

Проверка:

- штатный NBS1 RDMA client устанавливает соединение;
- protocol tests подтверждают выбранную classic NBS revision;
- RDMA pattern write/read даёт одинаковый checksum;
- unknown/revoked session и malformed request не достигают FastPath;
- gRPC и RDMA вызывают один и тот же backend и benchmark session check.

### 5.3. Шаг 2. Переключить cells data transport на RDMA

Цель: собрать полный workflow с gRPC control plane и RDMA data plane.

Действия:

- добавить RDMA `Storage` в ту же закреплённую `DataRoute`;
- проверить, что gRPC `Service` и RDMA `Storage` принадлежат одному host;
- включить штатный `CELL_DATA_TRANSPORT_RDMA` и требуемые RDMA workers NBS1;
- повторить `StartEndpoint` без изменения disk, benchmark session model и
  backend;
- доказать по метрикам/логам transport split: mount/unmount по gRPC,
  read/write по RDMA;
- не добавлять молчаливый benchmark fallback на gRPC.

Проверка:

- `MountVolume`/`UnmountVolume` видны на gRPC frontend;
- `ReadBlocks`/`WriteBlocks` проходят через RDMA adapter;
- тот же vhost E2E-сценарий работает без изменений;
- недоступный RDMA target даёт контролируемую transport error и не вызывает
  replay write или скрытый переход на другой path.

### 5.4. Шаг 3. Выполнить cells RDMA E2E

Цель: подтвердить целевой workflow с gRPC control plane и RDMA data plane.

Действия:

- повторить через RDMA минимальный E2E correctness-набор MVP-1;
- использовать тот же disk, benchmark session adapter и common backend;
- подтвердить по трассировке/счётчикам, что mount/unmount идут по gRPC, а
  read/write — по RDMA;
- проверить controlled error при недоступном RDMA target без fallback на gRPC
  и без автоматического replay write.

Проверка:

- RDMA pattern write/read даёт одинаковый checksum;
- запросы проходят в настоящий NBS2 partition через RDMA adapter;
- используется тот же common backend и benchmark session adapter, что в
  промежуточном gRPC path;
- нет скрытого fallback или автоматического replay write.

## 6. E2E-проверки корректности

Под correctness suite здесь понимается минимальный E2E-набор, доказывающий,
что запросы проходят по нужному transport в настоящий NBS2 partition и
сохраняют данные. Набор ограничен перечисленными ниже проверками.

Минимальный набор:

- pattern write/read и checksum для нескольких диапазонов;
- начало/конец диска и запрос за границей;
- misaligned/overflow range и неверный payload size;
- unknown и revoked benchmark session;
- pipe/transport disconnect без автоматического replay write;
- mount, I/O, unmount и последующее отклонение завершённой session;
- подтверждение transport path: gRPC в MVP-1, gRPC control plane и RDMA data
  plane в MVP-2.

## 7. Бинарные критерии успеха

### 7.1. MVP-1 завершён, если

1. NBS1 выполняет `StartEndpoint` и создаёт пользовательский vhost endpoint.
2. NBS1 выполняет `DescribeVolume` и получает закреплённый `DataRoute` для
   одного `fast_disk`; NBS2 frontend не реализует `DescribeVolume`.
3. `MountVolume` возвращает synthetic `SessionId` для одного настроенного
   клиента, а I/O с unknown/revoked session не достигает backend.
4. Control и data идут через штатный cells gRPC transport на один NBS2 host.
5. I/O достигает настоящего `FastPathService`/partition backend и проходит
   pattern write/read с совпадающим checksum.
6. Malformed и stale-session requests дают контролируемые ошибки без crash и
   изменения данных.
7. Synthetic session явно ограничена benchmark scope и не заявлена как
   реализация целевой PartitionTablet session model.
8. Нет storage stub, скрытого write replay, draft I/O API и прямых partition
   I/O команд в проверяемом workflow.

### 7.2. MVP-2 завершён, если

1. Тот же `StartEndpoint`, disk, benchmark session adapter и E2E-сценарий
   работают со штатным `CELL_DATA_TRANSPORT_RDMA`.
2. Mount/unmount идут по gRPC, read/write — по RDMA на том же NBS2 host.
3. RDMA adapter wire-compatible с выбранной classic NBS revision.
4. gRPC и RDMA используют одну проверку `DiskId`/`SessionId`, validation и
   common backend.
5. RDMA проходит E2E correctness-набор с совпадающим checksum записанных и
   прочитанных данных.
6. Нет benchmark-only RDMA protocol, скрытого fallback или
   автоматического replay write.

## 8. За пределами MVP-1/2

- динамический resolver, обновление `DataRoute`, recovery и multi-host;
- persistent session/access/writer state в PartitionTablet и writer fencing;
- `ZeroBlocks`;
- TLS/authentication, observability, QoS и rollout;
- multi-partition disks и произвольный block size.
