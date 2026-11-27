# Интеграция NBS cells с nbs2

Этот каталог — точка входа в архитектуру и план интеграции legacy NBS cells
с nbs2. Подробности разделены по назначению:

- [rfc.md](rfc.md) — архитектурное решение, предлагаемое к согласованию;
- [architecture.md](architecture.md) — исходные факты, ограничения,
  рассмотренные варианты и рекомендуемая архитектура;
- [mvp_benchmark.md](mvp_benchmark.md) — MVP-1 с gRPC hot path и MVP-2 с RDMA
  hot path для нагрузочных тестов;
- [mvp_functional.md](mvp_functional.md) — MVP-3 с основной функциональной
  логикой cells-интеграции;
- [production.md](production.md) — production-доведение, финальные измерения и
  выкатка.

Этапы выполняются последовательно: сначала проверяется gRPC hot path, затем тот
же cells-сценарий переводится на RDMA, после этого добавляется функционально
целостная интеграция и, наконец, production-свойства.

## Цель

Требуется, чтобы NBS endpoint server, работающий в одной из существующих NBS
cells, мог:

1. найти по `DiskId` диск, созданный в nbs2;
2. подключиться к нему;
3. предоставить его через обычный NBS endpoint (NBD/vhost и далее);
4. выполнять чтение и запись;
5. корректно переживать перенос или перезапуск nbs2 partition tablet.

Целевой пользовательский путь должен выглядеть примерно так:

```text
ydb-dstool nbs partition create --disk-id disk1 ...
    -> StartEndpoint отправляется в legacy NBS cell-a
    -> cells находит disk1 в cell nbs2
    -> endpoint создаётся в cell-a
    -> I/O проходит в nbs2 partition
```

Пользовательский I/O должен проходить через стандартный NBS1 endpoint и штатный cells transport, без использования draft `Ydb.Nbs` I/O API и прямых `ydb-dstool nbs partition io` команд.

## Зафиксированные решения и оставшиеся вопросы

1. Gateway встраивается в `ydbd`; отдельный sidecar не требуется.
2. Остаётся открытым выбор между существующим YDB gRPC server и отдельным classic NBS listener внутри `ydbd`. Выбор зависит от сетевой доступности, совместимости TLS/authentication, параметров gRPC-соединений и необходимости независимо открывать или закрывать classic NBS traffic, но не обеспечивает отдельного распределения ресурсов процесса.
3. Для MVP-1/MVP-2 допускается явно непроизводственный in-memory mount под отдельным feature flag; в MVP-3 он заменяется нормальным process-local registry.
4. Локальный auto-vhost nbs2 постоянно отключён в cells deployment, но
   остаётся в коде для конфигураций nbs2 без cells.
5. Политика одинаковых `DiskId` в legacy NBS и nbs2. Рекомендация: глобальная
   уникальность внутри множества настроенных cells как control-plane invariant;
   без изменения cells gateway не может гарантированно обнаружить дубликат.
6. Кто повторяет запрос после tablet pipe disconnect. Рекомендация: gateway
   возвращает `E_REJECTED` для неопределённых in-flight запросов и не повторяет
   writes; retry выполняет существующий durable client origin NBS.
7. Какая ревизия classic NBS API является contract source. Рекомендация:
   зафиксировать commit до переноса proto/service facade и менять его только с
   compatibility tests.
8. Существующий classic NBS RDMA target сначала пробуем перенести целиком.
   Минимальный wire-compatible adapter к nbs2 backend остаётся резервным
   вариантом при неприемлемом dependency graph.
9. Authoritative session state и writer generation хранятся в `VolumeDirect`;
   generation проверяется там на каждом I/O и обеспечивает writer fencing.
   Отдельный session tablet и
   проверка generation/token непосредственно в partition не выбраны.
10. Остаётся открытым ordering contract для одновременно выполняющихся writes
    с пересекающимися диапазонами блоков.

## Краткий контекст для новой сессии

> Legacy NBS cells уже умеет на `StartEndpoint` параллельно искать `DiskId` в
> local service и peer cells, а затем один раз привязывать endpoint к classic
> NBS gRPC/RDMA backend найденной cell. `StartEndpoint` выполняется в legacy
> NBS cell и там же создаёт guest-facing NBD/vhost socket; nbs2 gateway этот
> RPC не реализует. Для направления NBS -> nbs2 переносить сам cells не
> требуется. nbs2 должен предоставить classic NBS compatibility gateway внутри `ydbd`, а не как отдельный бинарь. Для MVP используется существующий YDB gRPC server; окончательный выбор между ним и отдельным listener внутри `ydbd` остаётся открытым.
> Gateway реализует `DescribeVolume` через внутренний
> `INbs2VolumeResolver`; первая реализация использует SSProxy/SchemeShard,
> потому что там сейчас хранится authoritative mapping `DiskId -> VolumeConfig
> + VolumeTabletId + PartitionTabletId`; отдельной регистрации в cells/SSProxy
> нет. Gateway направляет I/O по стабильному tablet pipe в существующий `FastPathService` NBS2. Любой gateway host обязан обслуживать
> любой диск, поскольку cells выбирает data host независимо от host, ответившего
> на discovery. Реализация разделена на четыре milestone. MVP-1 как можно
> раньше измеряет настоящий hot path через NBS1 cells и gRPC, допуская вручную
> заданные disk geometry/`PartitionTabletId` и синтетическую session. MVP-2 на
> том же backend добавляет точный classic NBS RDMA target внутри `ydbd` и
> сравнивает штатные cells gRPC/RDMA transports. MVP-3 убирает benchmark
> hardcodes: добавляет resolver через SSProxy/SchemeShard, process-local
> session registry, restart/migration через stable pipe и функциональный E2E
> обоих transports; local auto-vhost и discard остаются выключенными.
> Production-часть переносит session state и writer generation в
> `VolumeDirect`, который обеспечивает writer fencing, подключает к gateway готовый внутренний NBS2 `ZeroBlocks`, добавляет multi-host safety, failure matrix, observability,
> TLS/QoS и контролируемый rollout.
