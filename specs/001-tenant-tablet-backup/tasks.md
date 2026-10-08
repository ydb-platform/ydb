# Tasks: Бекап тенантных системных таблеток

**Input**: spec.md, plan.md, research.md, data-model.md, contracts/backup.md, quickstart.md.
**Tests**: адресные regression tests и существующий Backup suite.

## Phase 1: Setup

- [x] T001 Подключить Spec Kit и записать подтверждённые правила в .specify/memory/constitution.md.

## Phase 2: Foundational

- [x] T002 Проверить условие допуска, backend и recovery; записать решения в specs/001-tenant-tablet-backup/research.md.

## Phase 3: US1 — tenant backup (P1)

**Goal**: Получить читаемые снимок и журнал для tenant system tablet.
**Independent Test**: snapshot + subsequent writes + restore дают ожидаемые строки.

- [x] T003 [US1] Добавить проверку production NeedBackup для системных типов с tenant ID и без него в ydb/core/tablet_flat/ut/ut_backup.cpp.
- [x] T004 [US1] Добавить tenant snapshot/changelog round trip с согласованным типом таблетки при normal/recovery boot в ydb/core/tablet_flat/ut/ut_backup.cpp.
- [x] T005 [US1] Удалить запрет по TenantPathId из ydb/core/tablet_flat/tablet_flat_executor.cpp.

## Phase 4: US2 — границы допуска (P1)

**Goal**: Сохранить ограничения типов, режимов и конфигурации.
**Independent Test**: Recovery, user types, excluded tablet, missing backend не запускают backup.

- [x] T006 [US2] Проверить Recovery, пользовательские типы, исключённый tenant tablet и отсутствующий backend в ydb/core/tablet_flat/ut/ut_backup.cpp.

## Phase 5: Validation

- [x] T007 Выполнить Backup suite командой из specs/001-tenant-tablet-backup/quickstart.md и записать результат.
- [x] T008 Проверить diff и соответствие FR/SC, записать converge-результат в specs/001-tenant-tablet-backup/validation.md.

## Dependencies & Parallel Opportunities

T001 → T002 → T003 → T004 → T005 → T006 → T007 → T008.
US1 и US2 проверяются независимо, но редактируют один файл; выполнять последовательно.
В US1 можно независимо читать production predicate и тестовые helpers; в US2 — конфигурационный
фильтр и ограничения режимов. Параллельные изменения файлов не нужны.

## Implementation Strategy

Расширить политику допуска к backup и добавить управление recovery через Hive/Local.
Проверить snapshot/changelog, эксплуатационное восстановление настоящего TxAllocator,
ограничения режима и связанные регрессии Hive. Не создавать коммиты или внешние публикации автоматически.


## Phase 6: US3 — эксплуатационное восстановление (P1)

**Goal**: Восстановить выбранную таблетку с прежним ID через Hive, затем явно вернуть normal.
**Independent Test**: Настоящий TxAllocator восстанавливает сохранённый счётчик после изменения
живого состояния; Hive restart сохраняет recovery, а без явной команды normal не запускается.

- [x] T009 [US3] Уточнить сценарий, способ включения и возврат в specs/001-tenant-tablet-backup/spec.md.
- [x] T010 [US3] Пересмотреть research.md, plan.md, data-model.md и contracts/backup.md в specs/001-tenant-tablet-backup/.
- [x] T011 [US3] Добавить persistent RecoveryMode и загрузку в ydb/core/mind/hive/{hive_schema.h,leader_tablet_info.h,tx__load_everything.cpp}; общий предикат типов в ydb/core/base/system_tablet_backup.h.
- [x] T012 [US3] Реализовать POST SetRecoveryMode и audit в ydb/core/mind/hive/{monitoring.cpp,hive_impl.h}, счётчик транзакции в ydb/core/protos/counters_hive.proto.
- [x] T013 [US3] Передать boot/sync режим и capability через ydb/core/protos/local.proto, ydb/core/mind/local.cpp, ydb/core/tablet/tablet_setup.h и уведомление готовности recovery в ydb/core/tablet/tablet_sys.cpp.
- [x] T014 [US3] Обеспечить выбор совместимых узлов, остановку followers, sync и запрет миграции recovery в ydb/core/mind/hive/{node_info.cpp,node_info.h,tablet_info.cpp,tx__start_tablet.cpp,tx__sync_tablets.cpp,tx__seize_tablets.cpp,tx__lock_tablet.cpp,tx__create_tablet.cpp}.
- [x] T015 [US3] Добавить реальные recovery и отрицательные проверки в ydb/core/mind/hive/hive_ut.cpp; выполнить их и записать результат в specs/001-tenant-tablet-backup/validation.md.
- [x] T016 [US3] Описать процедуру и ограничения в ydb/docs/{ru,en}/core/recipes/backup/system-tablet-backup/recovery.md и specs/001-tenant-tablet-backup/quickstart.md.

Зависимости: T009 → T010 → T011 → T012 → T013 → T014 → T015 → T016 → T008.
T016 можно готовить параллельно ожиданию сборки; изменения кода выполняются последовательно.
T003–T007 доказывают только executor backup, готовность всей задачи требует T011–T016.
