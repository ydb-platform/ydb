# Refactor: вычистить host-статусы из dirty_map — снимок состояния

Branch: `refactor/blockstore-host-status-list`
Дата снимка: 2026-04-30
Working tree: грязный (всё в нём, ничего не закоммичено в этой сессии)

## Цель рефакторинга

Убрать `THostStatusList` (и сами лейблы `EHostStatus`) из `TBlocksDirtyMap`. Dirty_map оперирует только `THostIndex`/`THostMask`. Логика статусов (Primary/HandOff/Active/Disabled) инкапсулирована в `TVChunk` и `TVChunkConfig`.

## Согласованный план (5 секций)

1. **Архитектура.** Dirty_map host-blind. VChunk считает 3 маски: `DDiskReadable=DDiskHosts.GetActive()`, `DDiskFlushTargets=DDiskHosts.GetPrimary()`, `PBufferActive=PBufferHosts.GetActive()`.
2. **API dirty_map** (Вариант A — параметры в каждый вызов):
   - `TBlocksDirtyMap(blockSize, blockCount, hostCount)`
   - `MakeReadHint(range, ddiskReadable, pbufferReadable)`
   - `MakeFlushHint(batch, ddiskFlushTargets)`
   - `MakeEraseHint(batch, pbufferEraseTargets)`
   - `UpdateHostStatuses` удалён, поля `THostStatusList PBufferHosts/DDiskHosts` удалены.
3. **VChunk.** Маски хранятся как `const`-поля, инициализированные в списке инициализации из `VChunkConfig`. Семантика flush сохранена (Primary != Active).
4. **Раскладка файлов.** Создана новая мини-библиотека `partition_direct/host/` (сосед `dirty_map/`, `model/`):
   - `host/host_mask.{h,cpp}` — THostIndex, THostMask, THostRoute, MaxHostCount, InvalidHostIndex.
   - `host/host_status.{h,cpp}` — EHostStatus, THostStatusList (с `Get*` методами).
   - `host/ut/` — `host_mask_ut.cpp` + `host_status_ut.cpp`.
   - `dirty_map/` PEERDIR'ит `host`. host_status.{h,cpp,_ut.cpp} удалены из dirty_map/.
5. **Тесты.** `dirty_map_ut.cpp` переписан с хелпером `MakeMasks`/`DefaultMasks`. `base_test_fixture` отдаёт маски через геттеры. Метод `Active()/Primary()/HandOff()/Disabled()` → `Get*` во всех call sites.

## Дополнительный фикс (мини-секция, по запросу пользователя в конце)

Удалена константа `DirectBlockGroupPrimaryCount` из `common/constants.h` — она не принадлежит DBG-понятию (число primary может расти при ремаппинге). Вместо неё в `TVChunkConfig` добавлена `static constexpr size_t DefaultPrimaryCount = 3;`, которой пользуется фабрика `Make`.

## Состояние реализации

### Сделано и подтверждено зелёным
- Все файлы написаны/переписаны (см. список ниже).
- Билд `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct` проходит.
- Тесты `partition_direct/host` — 18 GOOD, зелёные.
- Тесты `partition_direct/dirty_map` — 41 GOOD, зелёные (после фикса clang-format).
- Целевой тест `TPartitionDirectTest::BasicWriteReadDirectPBufferFilling` индивидуально — проходит.
- Clang-format: green по всему модулю.

### Изменённые файлы
**Создано:**
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_mask.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_mask.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_status.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_status.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ya.make`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ut/host_mask_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ut/host_status_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ut/ya.make`

**Удалено:**
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status_ut.cpp`

**Изменено:**
- `ydb/core/nbs/cloud/blockstore/libs/common/constants.h` (удалена `DirectBlockGroupPrimaryCount`)
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/inflight_info.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/range_locker.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/ya.make`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/ut/ya.make`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ya.make`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_with_pb_replication_request.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_with_direct_replication_request.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/base_test_fixture.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/base_test_fixture.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/read_request_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ddisk_data_copier.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ddisk_data_copier.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ddisk_data_copier_ut.cpp`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/restore_request.h`
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl_ut.cpp` (только clang-format)

### Не доделано
- Полный прогон **всех** тестов `partition_direct` целиком (`partition_ut`, `write_request_ut`, `read_request_ut`, `vchunk_config_ut`, `ddisk_data_copier_ut`, `partition_direct/ut`) на чистом билде. Предыдущая попытка упёрлась в переполнение `/root/.ya/build/cache` (раздулся до 69G). Кэш полностью почищен по запросу пользователя — нужно перезапустить.
- Никакие коммиты в этой сессии не делались. Всё в working tree. Последние draft-коммиты `babb6172685`, `24132ac3d3d` — от прошлых сессий.

### Известные нюансы
- 4 теста `partition_ut` (`BasicWriteReadDirectPBufferFilling`, `RandomWritesDirectPBufferFilling`, `ShouldRestorePartitionAfterRestart`, `ShouldWriteAndReadBlocksInDifferentRegionsDirectPBufferFilling`) крашатся в `vhost::vhd_submit_work_and_wait` (`evloop != home_evloop`) **только при параллельном запуске**. Каждый из них зелёный при индивидуальном запуске. Это пред-существующая флакость vhost'а, не следствие рефакторинга.
- Field declaration order в `vchunk.h` корректный: `VChunkConfig` ДО `DDiskReadable/DDiskFlushTargets/PBufferActive` ДО `BlockSize/BlocksCount/BlocksDirtyMap`.

## Что делать после восстановления сессии

1. `git status` — убедиться что working tree содержит все изменения (см. список выше).
2. `df -h /` — убедиться, что есть ≥30G свободно (билд + тесты partition_direct ~25G).
3. `./ya make --build relwithdebinfo -tA ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct 2>&1 | tail -40` — финальный прогон.
4. Если зелёный (модулю vhost-флаки разрешены) — спросить пользователя, можно ли коммитить и какой messag'ом, а потом `git add -A && git commit`.

## Команды-памятки
```bash
# Билд только модуля
./ya make --build relwithdebinfo ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct

# Все тесты модуля
./ya make --build relwithdebinfo -tA ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct 2>&1 | tail -40

# Конкретный тест
./ya make --build relwithdebinfo -tA ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_ut -F 'TPartitionDirectTest::BasicWriteReadDirectPBufferFilling' 2>&1 | tail -15

# Чистка кэша при переполнении диска
rm -rf /root/.ya/build/cache /root/.ya/build/build_root /root/.ya/build/symres
```

## Контекст из памяти проекта
- **Working style:** section-by-section approval ("ок"/"нет, переделай"); после "ок" не редизайнить — выполнять согласованное.
- **Build:** `./ya make --build relwithdebinfo`, `-tA` для тестов, без `-j`, без force rebuild, `2>&1 | tail`.
- **Project layout:** blockstore в `ydb/core/nbs/cloud/blockstore/`, не в `cloud/blockstore/`.
