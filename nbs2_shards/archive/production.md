# Production-интеграция NBS cells с nbs2

> Навигация: [обзор и решения](README.md), [RFC](rfc.md),
> [архитектура](architecture.md),
> [нагрузочные MVP](mvp_benchmark.md), [функциональный MVP](mvp_functional.md).

## Production

### Цель production-части

Довести работающий MVP-3 до безопасной эксплуатации: перенести session state
и writer generation из памяти gateway в authoritative nbs2-компонент,
реализовать writer fencing, завершить data-plane семантику, проверить отказы
и multi-host работу, добавить observability, security и контролируемый
rollout.

Под authoritative nbs2-компонентом понимается единый отказоустойчивый владелец
mount/session state диска. Он хранит действующие sessions и writer generation,
последовательно разрешает конфликтующие mounts и обеспечивает fencing: I/O
старого writer отклоняется всеми gateway hosts, в том числе после restart.
Authoritative владельцем выбран `VolumeDirect` tablet; рассмотренный отдельный
volume/session tablet не используется. SSProxy/SchemeShard остаётся
источником метаданных диска, но не session ownership.

### Что получим

- отказоустойчивое authoritative mount/session state;
- защиту от двух конфликтующих writers через разные gateway hosts и после
  restart;
- сквозное подключение готового NBS2 `ZeroBlocks`/discard к cells с проверкой диапазонов;
- несколько gateway hosts, обслуживающих любой disk и использующих единое
  authoritative session state для новых mounts;
- определённую error/recovery semantics для resolver, gateway, network и
  partition failures;
- gateway/session/RPC metrics и диагностические логи;
- проверенные performance limits, TLS/authentication, QoS и постепенный
  rollout.

### Что останется за пределами production-части

- multi-partition volumes и произвольный block size, если они не войдут в
  отдельные продуктовые требования;
- placement-aware изменения `GetCellEndpoint` и cells protocol;
- автоматическое переключение уже существующего endpoint на другой gateway
  host без отдельного изменения cells/session switching;
- gateway sidecar, который не входит в принятую архитектуру;
- сложный resolver cache;
- полный create/delete/resize lifecycle nbs2 за пределами поведения gateway
  при изменении или удалении уже смонтированного volume.

Production реализуется следующими пятью шагами с собственной нумерацией.

### Production-шаг 1. Реализовать authoritative session state и writer fencing

Цель:

Заменить process-local session MVP-3-шага 3 на отказоустойчивое authoritative
mount state, не допускающее двух конфликтующих writers после restart,
migration или работы через разные gateway hosts.

Логика:

Classic NBS уже реализует похожую модель в своём volume tablet: учитывает
clients, access mode и `MountSeqNumber`, возвращает `E_BS_MOUNT_CONFLICT` и
отклоняет stale session. Для nbs2 нужна аналогичная server-side семантика,
потому что fencing старого NBS не защищает foreign nbs2 volume автоматически.

```text
MountVolume
    -> gateway
    -> authoritative nbs2 VolumeDirect tablet
    -> persistent session + writer generation

WriteBlocks
    -> проверка актуальной session/generation в VolumeDirect
    -> только затем изменение данных
```

Каждый I/O проходит проверку session и writer generation в `VolumeDirect`.
Рассматривался generation/token, проверяемый непосредственно partition, но
этот вариант не выбран. Проверки только в gateway недостаточно из-за
multi-host и гонки с новым mount.

Действия:

- реализовать authoritative mount state в `VolumeDirect` tablet;
- реализовать `ClientId`, `InstanceId`, access modes, `MountSeqNumber`,
  writer fencing, unmount и inactive client cleanup;
- реализовать создание и хранение `SessionId`/generation и их проверку в
  `VolumeDirect` на каждом I/O;
- определить поведение gateway restart и session recovery;
- постоянно отключить local auto-vhost endpoint в cells deployment, сохранив
  его для конфигураций nbs2 без cells.

Проверка:

- два conflicting writers не получают одновременный доступ;
- более новый допустимый mount fencing-ит старого writer;
- read-only clients работают согласно контракту;
- gateway restart не снимает fencing и не открывает второй writer;
- mount через разные gateway hosts использует одно authoritative state;
- stale session получает `E_BS_INVALID_SESSION` и не изменяет данные.

### Production-шаг 2. Подключить готовый nbs2 `ZeroBlocks`/discard к cells

Цель:

Интегрировать готовый внутренний nbs2 zero/discard path с classic NBS gateway
и только после сквозной проверки рекламировать discard/write-zeroes guest-у.
Реализация самого `ZeroBlocksLocal` и его storage semantics выполняется вне
этого плана.

Логика:

```text
ZeroBlocks
    -> gateway проверяет SessionId и block range
    -> serialized zero event через tablet pipe
    -> готовый partition/FastPathService::ZeroBlocksLocal
    -> обнуление только запрошенного диапазона
```

Шаг предполагает, что внутренний nbs2 handler уже реализован без `Y_ABORT` и
проверен отдельно. Здесь добавляются только gateway/cells transport, session и
error semantics. Capability advertisement включается последним; до успешного
завершения E2E сохраняется поведение MVP-3 с `E_NOT_IMPLEMENTED`.

Действия:

- подключить classic gRPC/RDMA `ZeroBlocks` к готовому внутреннему handler
  через стабильный tablet pipe;
- валидировать session, переполнение, границы диапазона и request limits;
- преобразовать внутренние ошибки в classic NBS error semantics;
- определить error/retry semantics при pipe disconnect;
- включить `VhostDiscardEnabled`/write-zeroes advertisement только после
  прохождения сквозных тестов.

Проверка:

- zero через NBS1 cells работает по gRPC и RDMA и достигает готового nbs2
  handler;
- сквозной запрос очищает только запрошенный диапазон и не меняет соседние
  блоки;
- проверены boundary, overflow и большие ranges;
- malformed request возвращает `E_ARGUMENT` без падения процесса;
- stale session получает `E_BS_INVALID_SESSION`;
- guest видит discard/write-zeroes только при полностью включённом zero path.

### Production-шаг 3. Проверить failure matrix и observability

Цель:

Зафиксировать ожидаемое поведение для отказов resolver, gateway, network и
partition и обеспечить возможность отличить эти случаи по метрикам и логам.

Логика:

Метрики добавляются вместе с соответствующими методами на MVP-этапах и предыдущих
production-шагах, а здесь собираются в законченную систему и проверяются с
fault injection.
Существующая latency monitoring page partition показывает backend latency, но
не видит gateway RPC, sessions, pipe reconnects и преобразование ошибок.

Действия:

- добавить метрики и логи для describe latency/result;
- добавить mount/unmount/session count;
- добавить read/write/zero latency и bytes;
- учитывать pipe reconnects, tablet resolution failures, invalid requests и
  cell ID mismatch;
- определить ожидаемую classic NBS error semantics для каждого сбоя;
- добавить failure tests для resolver, gateway, pipe, partition и cells.

Проверка:

- nbs2 cell полностью недоступна -> `E_REJECTED`;
- диск отсутствует во всех доступных cells -> `E_NOT_FOUND`;
- проверены restart выбранного gateway и restart/migration partition tablet;
- проверен disconnect при in-flight read/write;
- ошибочная конфигурация cell ID выявляется как `E_REJECTED` и видна в
  метриках;
- существующий, но неподдерживаемый MVP-3 volume успешно проходит discovery и
  детерминированно отклоняется на mount;
- restart gateway приводит к ожидаемому `E_BS_INVALID_SESSION`, remount и
  продолжению I/O без пересоздания endpoint;
- оба входных mount mode (`LOCAL` и `REMOTE`) работают через одинаковую remote
  gateway semantics;
- origin NBS с выключенными discard/write-zeroes flags не рекламирует эти
  возможности guest-у;
- удаление volume во время активной session имеет заранее определённое
  поведение и не оставляет молча доступный stale volume;
- существующие cross-cell сценарии `vol-a/vol-b` не сломаны.

Одинаковый `DiskId` в нескольких cells нельзя надёжно выявить на этом слое без
изменения cells: discovery возвращает первый успешный ответ. Для MVP-3 глобальная
уникальность `DiskId` является проверяемым control-plane/operational invariant,
а не обещанием gateway обнаружить дубликат.

### Production-шаг 4. Завершить интеграцию и измерить производительность

Цель:

После функциональной корректности подтвердить пределы обоих уже реализованных
transports в полной production-like конфигурации и подтвердить готовность
интеграции к выкатке.

Логика:

```text
legacy endpoint
    -> control: classic NBS gRPC
    -> data: classic NBS gRPC или RDMA
    -> gateway actor/tablet pipe -> VolumeDirect
    -> проверка session/writer generation
    -> partition/FastPathService
```

Baseline gRPC и RDMA уже получен на MVP-1/MVP-2. Здесь измерения повторяются с
authoritative session state, writer fencing, multi-host, failure handling,
TLS/QoS и production
limits. Placement-aware gateway selection рассматривается только при
подтверждённой необходимости. Affinity может уменьшить interconnect hop, но
не должна отменять способность любого gateway обслужить любой disk через
tablet pipe. Sidecar не входит в принятую архитектуру.

Действия:

- измерить gRPC и RDMA data path с одинаковым production-like workload;
- отделить стоимость transport от actor/interconnect/backend latency;
- проверить большие I/O и message-size limits;
- проверить несколько gateway hosts и stale placement affinity;
- настроить TLS/authentication, QoS и лимиты;
- проверить и настроить выбранный при принятии RFC вариант: существующий YDB gRPC port или отдельный listener внутри `ydbd`, включая сетевую доступность, TLS/authentication, параметры gRPC-соединений и управление доступностью classic NBS traffic;
- добавить compatibility/upgrade tests при смене зафиксированной ревизии
  classic NBS protobuf и error codes.

Проверка:

- нет деградации legacy NBS paths при выключенном gateway;
- multi-host failure не ломает новые mounts;
- migration partition при stale affinity не ломает I/O;
- обновление classic NBS revision не меняет wire/error semantics без явно
  принятого compatibility change;
- нагрузочные тесты не обнаруживают memory leaks, unbounded in-flight state и
  неконтролируемые retries.

### Production-шаг 5. Выполнить поэтапную выкатку

Цель:

Контролируемо включить проверенную интеграцию в production, ограничивая радиус
возможного отказа и сохраняя возможность остановить или откатить rollout.

Логика:

Gateway остаётся выключенным по умолчанию и включается feature flag сначала на
canary hosts/cells и ограниченном наборе дисков. Каждый следующий этап
начинается только после проверки correctness, error rate, latency и resource
usage относительно результатов Production-шага 4.

Действия:

- определить этапы rollout, canary scope, критерии продолжения и остановки;
- подготовить и проверить процедуру rollback, включая поведение активных
  endpoints и sessions;
- включать gateway feature flag постепенно;
- на каждом этапе сравнивать correctness, ошибки и производительность с
  production-like baseline;
- расширять охват hosts/cells/disks только после успешной проверки предыдущего
  этапа.

Проверка:

- gateway по умолчанию выключен и не меняет legacy NBS paths;
- canary проходит gRPC/RDMA E2E и сохраняет fencing guarantees;
- метрики и алерты позволяют остановить rollout по заранее заданным критериям;
- rollback проверен и не оставляет неконтролируемые активные sessions;
- расширение охвата не приводит к необъяснённой деградации latency, IOPS,
  error rate или resource usage.
