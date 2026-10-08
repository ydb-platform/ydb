# Implementation Plan: Бекап тенантных системных таблеток

**Feature**: `001-tenant-tablet-backup` | **Date**: 2026-10-05 | **Spec**: [spec.md](spec.md)

**Input**: `specs/001-tenant-tablet-backup/spec.md`

**Status**: Реализован в рабочей копии; 2026-10-06 сборка и 81 unit/actor-runtime тест прошли.

## Summary

Разрешить существующий системный бекап для таблеток с TenantPathId, сохранив проверку режима
запуска и whitelist типов. Добавить сохраняемый режим восстановления выбранной таблетки в Hive
и его передачу в Local. Оператор применяет бекап через существующий RecoveryShard, затем явно
возвращает таблетку в normal. Проверить путь на настоящем TxAllocator с метаданными тенанта.

## Technical Context

**Language/Version**: C++, не выше C++20.
**Primary Dependencies**: существующий flat executor, actor runtime, unittest.
**Storage**: существующий filesystem backend системного бекапа.
**Testing**: `ydb/core/tablet_flat/ut` (Backup) и `ydb/core/mind/hive/ut` (recovery), через обычный `./ya`.
**Target Platform**: сервер YDB, Linux для локальной проверки.
**Project Type**: компонент существующей распределённой БД.
**Performance Goals**: не вводятся новые; снимается фильтр принадлежности тенанту.
**Constraints**: существующие форматы бекапа, список типов, ограничения режима и исключения.
**Scale/Scope**: допуск к backup, сохраняемый режим Hive, протокол Hive/Local, monitoring и интеграционные тесты.

## Constitution Check

До исследования: PASS. Применим корневой AGENTS.md; вложенных инструкций в tablet_flat нет.
После проектирования: PASS. C++20 или ранее; обычный ya для тестов; без -j и force rebuild;
вывод тестов через `2>&1 | tail`. Сторонние зависимости для production не добавляются.

## Project Structure

### Documentation (this feature)

`spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/backup.md`,
`quickstart.md`, `checklists/requirements.md`, далее `tasks.md` и результаты проверок.

### Source Code (repository root)

- `ydb/core/tablet_flat/tablet_flat_executor.cpp`: убрать отклонение по TenantPathId.
- `ydb/core/tablet_flat/ut/ut_backup.cpp`: проверить реальную политику допуска и tenant backup round trip.
- `ydb/core/tablet_flat/test/libs/exec/dummy.h`: существующий dummy переопределяет NeedBackup;
  production-политику тестировать явно, не полагаясь на флаг dummy.
- `ydb/core/tablet_flat/flat_executor.cpp`: действующие конфигурация, исключения, повторные попытки.
- `ydb/core/tablet_flat/flat_executor_backup.cpp`: формат и пути уже используют тип и ID таблетки.

**Structure Decision**: Сохранить backend и recovery reader; добавить управление recovery через monitoring Hive и штатный Local.

## Implementation and Validation

1. Покрыть поддерживаемые типы с пустым/непустым tenant ID и Normal/Recovery режимами;
   отрицательные примеры DataShard, ColumnShard, PersQueue, Dummy.
2. Проверить снимок и журнал изменений при непустом TenantPathId через реальный NeedBackup.
3. Проверить отсутствие файлов при исключённом ID и ненастроенном backend.
4. Убрать только запрет по TenantPathId.
5. Запустить suite Backup с relwithdebinfo; записать фактический результат.
6. Сопоставить реализацию с FR/SC, зафиксировать оставшиеся ограничения проверки.

## Complexity Tracking

Нарушений конституции и новых архитектурных слоёв нет.

## Operational recovery design

- `ydb/core/mind/hive/hive_schema.h`, `leader_tablet_info.h`, `tx__load_everything.cpp`:
  RecoveryMode, default false, независимый от CreateTablet и обычного stop/resume.
- `ydb/core/mind/hive/monitoring.cpp`, `hive_impl.h`, `ydb/core/protos/counters_hive.proto`:
  POST SetRecoveryMode(tablet, recovery=0|1), валидация ID/типа/состояния, запись флага и audit,
  остановка followers, перезапуск leader после commit. Повтор команды идемпотентен.
- `ydb/core/protos/local.proto`, `ydb/core/mind/local.cpp`: SupportsRecovery, RecoveryMode
  в boot/sync; отдельная фабрика recovery, отсутствие promotion обычного follower.
- `ydb/core/tablet/tablet_setup.h`: конструктор замены фабрики с сохранением mailbox/pools.
- `ydb/core/mind/hive/tx__start_tablet.cpp`, `node_info.cpp`, `node_info.h`: флаг boot и проверка capability.
- `ydb/core/mind/hive/tablet_info.cpp`, `tx__sync_tablets.cpp`: followers не запускаются,
  экземпляры в несовпадающем режиме останавливаются и запускаются заново.
- `ydb/core/mind/hive/tx__seize_tablets.cpp`: не передавать активную recovery-таблетку другому Hive.
- `ydb/core/base/system_tablet_backup.h`, `ydb/core/tablet_flat/tablet_flat_executor.cpp`:
  общий предикат типов системного backup/recovery, без изменения списка.
- `ydb/core/mind/hive/hive_ut.cpp`: настоящий TxAllocator через Hive/Local, restore, restart Hive,
  валидация команд и совместимости узлов. Существующий Backup suite остаётся проверкой формата.
- `ydb/docs/{ru,en}/core/recipes/backup/system-tablet-backup/recovery.md`: процедура Hive recovery,
  выбор файла, проверка результата, отдельный возврат, ограничения аварийного сценария.

Constitution check after revision: PASS. Используются существующие библиотеки и цели unittest,
C++20, обычный ya с relwithdebinfo, без -j/force rebuild. Статус сборки проверять раз в 30 минут.

## Уточнения после проверки реализации

`ydb/core/tablet/tablet_sys.cpp::StartRecovery` должен уведомить Local событием `TEvReady`
до применения бекапа: recovery готов принимать операторские команды до старта executor.
Обычные `TEvBoot`/активация executor происходят только после начала восстановления.

`ydb/core/mind/hive/tx__lock_tablet.cpp` и `tx__create_tablet.cpp` запрещают перевод recovery
в external/locked execution обходным путём. Обычный повтор CreateTablet сохраняет RecoveryMode.

Тестовое окружение Hive требует ResourceBroker для backup scan; helper RebootTablet ждёт
обычный TEvBoot и не подходит для перезапуска recovery-актора. Эти уточнения проверяются
новым интеграционным тестом, а не считаются подтверждёнными прошедшим ранее Backup suite.

Автоматический балансировщик не должен перемещать таблетку во время восстановления;
проверка добавляется в IsGoodForBalancer. Перезапуск из-за отказа узла по-прежнему сохраняет
recovery, но оператор повторно проверяет результат и доступность файлов на новом узле.

Общий предикат находится в отдельном `ydb/core/base/system_tablet_backup.h`,
перечисленном в `ydb/core/base/ya.make`: изменение общего tablet_types.h не требуется.
Sync о несовпадающем режиме останавливает только указанное в сообщении поколение;
устаревший отчёт не сбрасывает состояние более нового recovery-экземпляра.
Интеграционная проверка отправляет старый normal sync после применения бекапа и
проверяет сохранение результата восстановления через App таблетки.
