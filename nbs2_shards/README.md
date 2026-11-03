# Интеграция NBS1 cells с NBS2

## Кратко для человека

Цель — сделать диски NBS2 доступными через обычный NBS1 workflow:
`StartEndpoint` и пользовательский vhost endpoint остаются в NBS1, а
read/write выполняется через штатный cells transport в настоящий NBS2
partition.

Текущая работа ограничена MVP-1/MVP-2 для одного диска и host. MVP-1 строит
промежуточный gRPC path, необходимый для настройки frontend, cells и общего
backend. MVP-2 переключает data path на целевой RDMA transport. Сравнение
производительности gRPC и RDMA в эти MVP не входит.

```text
StartEndpoint в NBS1
    -> NBS1: DescribeVolume + DataRoute
    -> NBS2 host: MountVolume по gRPC
    -> vhost endpoint в NBS1
    -> ReadBlocks/WriteBlocks по gRPC (MVP-1) или RDMA (MVP-2)
    -> NBS2 partition -> PBuffer/DDisk
```

## Актуальные документы

- [rfc3.md](rfc3.md) — источник целевой архитектуры и обязательных гарантий;
- [mvp_benchmark.md](mvp_benchmark.md) — текущие границы, временные допущения,
  шаги и критерии успеха MVP-1/MVP-2;
- [archive/README.md](archive/README.md) — старая проработка, только для
  истории; её решения не являются актуальными без подтверждения в `rfc3.md`.

При расхождениях между документами приоритет имеет `rfc3.md`.

## Краткий контекст для новой сессии

### Граница системы

- NBS1 остаётся единой внешней точкой control plane, discovery и создания
  пользовательского endpoint. `StartEndpoint` и guest-facing vhost socket
  находятся в NBS1.
- Переиспользуется существующий cells flow: при `StartEndpoint` NBS1 ищет диск
  локально и в peer cells, затем привязывает endpoint к выбранному backend.
  NBS2 frontend не реализует `StartEndpoint`.
- `fast_disk` создаётся в NBS2, имеет явный признак типа, а его постоянная
  metadata хранится в SchemeShard cell-владельца.
- `DescribeVolume` выполняется в NBS1 и не входит в runtime API NBS2 frontend.
- Для `fast_disk` NBS1 получает обязательный `DataRoute`: связанный gRPC
  `Service` и gRPC/RDMA `Storage` одного NBS2 host.
- `MountVolume`/`UnmountVolume` идут по gRPC. Read/write I/O идёт по gRPC или
  RDMA на тот же host. NBS2 frontend обслуживает только локальную partition и
  не пересылает I/O между hosts.
- Оба transport используют общий backend, validation и error model поверх
  существующего NBS2 `FastPathService`/partition path.

### Целевая архитектура

- Authoritative session/access/writer state хранится в PartitionTablet, а
  partition-owned path проверяет её для каждого I/O.
- Write с неопределённым результатом не повторяется автоматически.
- `fast_disk` содержит одну partition; multi-partition вариант не входит в
  текущее решение.
- Local auto-vhost NBS2 выключен для `fast_disk`, которым управляет NBS1, но
  остаётся доступен для автономных конфигураций NBS2.

### Текущий MVP-1/MVP-2

- NBS2 frontend встраивается в `ydbd` и выключен по умолчанию. Точная classic
  NBS source revision, регистрация gRPC/RDMA endpoints и dependency boundary
  RDMA target фиксируются до реализации.
- Disk metadata и `DataRoute` для одного диска задаются статически на стороне
  NBS1; placement закреплён, восстановление после migration/restart ручное.
- Используется одна временная process-local session для одного client/writer.
  Это scaffolding MVP, а не замена целевой PartitionTablet session model.
- MVP-1 проверяет полный cells gRPC E2E и является промежуточным шагом.
- MVP-2 добавляет classic-compatible RDMA target: mount/unmount остаются на
  gRPC, read/write переключаются на штатный cells RDMA.
- E2E-проверка выполняет pattern write/read и сравнивает checksum. Обработка
  конкурентных пересекающихся writes уже реализована в NBS2 и переиспользуется.
- `ZeroBlocks` не рекламируется и возвращает контролируемую ошибку без вызова
  backend.
- Не допускаются отдельный storage stub, benchmark-only RDMA protocol, draft
  `Ydb.Nbs` I/O API и прямые `ydb-dstool nbs partition io` команды.
- MVP-1/MVP-2 не сравнивают производительность gRPC и RDMA.

### За пределами MVP-1/MVP-2

- динамический resolver, обновление `DataRoute`, recovery и multi-host;
- persistent session/access/writer state в PartitionTablet и writer fencing;
- `ZeroBlocks`;
- TLS/authentication, observability, QoS и rollout;
- multi-partition disks и произвольный block size.

### Как продолжать работу

1. Сначала читать `rfc3.md`, затем соответствующий шаг
   `mvp_benchmark.md`; архив использовать только для исторического контекста.
2. Не превращать временные допущения MVP в новые архитектурные решения.
3. Не расширять MVP работами из раздела «За пределами MVP-1/MVP-2», если они
   не нужны для gRPC/RDMA E2E.
4. Перед изменением кода проверить актуальные YDB/NBS revisions и подтвердить
   classic NBS wire contract из source revision, зафиксированной в метаданных
   RFC.
5. Содержимое `archive/` сохранять без изменений.
