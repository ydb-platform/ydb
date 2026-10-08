# Research: Tenant tablet backup

## Допуск к бекапу

**Decision**: Убрать проверку непустого TenantPathId в ITablet::NeedBackup.
**Rationale**: `ydb/core/tablet_flat/tablet_flat_executor.cpp` сначала проверяет BootType,
затем отвергает tenant ID и только потом проверяет whitelist из девяти системных типов.
Это прямое препятствие запрошенному поведению.
**Alternatives considered**: отдельный переключатель увеличивает конфигурационный контракт;
пользователь подтвердил использование существующей настройки. Расширять whitelist не требуется.

## Формат и конфигурация

**Decision**: Повторно использовать SystemTabletBackupConfig и существующий filesystem backend.
**Rationale**: `flat_executor.cpp::StartNewBackup` уже применяет ExcludeTabletIds и
BackupExclusion; writers создаются только при настроенном backend. `flat_executor_backup.cpp`
разделяет каталоги по типу, ID, поколению и шагу. `flat_executor_recovery.cpp` проверяет тип и ID
из manifest, а принадлежность тенанту не участвует в формате.
**Alternatives considered**: новый формат или tenant-prefixed directory не нужен для разделения
таблеток и создал бы дополнительные требования совместимости.

## Проверки

**Decision**: Добавить regression coverage в `ut/ut_backup.cpp`.
**Rationale**: существующий NFake::TDummy::NeedBackup возвращает флаг теста вместо production
policy. Проверка только существующего suite не обнаружит запрет TenantPathId.
Нужны явные вызовы ITablet::NeedBackup и сценарий snapshot/changelog с tenant metadata.
**Alternatives considered**: один интеграционный stress workload проверяет преимущественно
NodeBroker и существенно тяжелее адресной проверки условия допуска.


### Уточнение тестового окружения

Исследование отдельным агентом подтвердило два ограничения helpers: GetLastBackupPath
жёстко выбирал dummy, а recovery starter сбрасывал тип и tenant metadata к значениям по
умолчанию. Поэтому TEnv параметризуется типом и tenant ID, общий starter передаёт их в обоих
режимах, а TSystemDummy вызывает production policy. Политика проверяется отдельно через
минимальный ITablet без actor runtime. Типы/ID в manifest проверяет существующий reader.

## Эксплуатационное восстановление: пересмотр

**Decision**: Ввести отдельный сохраняемый RecoveryMode выбранной таблетки в Hive. Оператор
управляет им POST-командой SetRecoveryMode через существующий monitoring Hive. Local получает
режим вместе с TEvBootTablet и выбирает RecoveryShard, сохраняя исходные ID, тип и TenantPathId.
**Rationale**: Local сейчас всегда выбирает обычную фабрику; BootType не сериализуется
TabletStorageInfoToProto. Существующая поддержка recovery находится только в
configured_tablet_bootstrapper.cpp. Пользователь выбрал управление через Hive без bootstrap.
**Alternatives considered**: StopTablet + временный bootstrap потребовал бы ручного переноса
каналов; пользователь предпочёл Hive. Использование BootMode вместо отдельного поля позволило
бы обычному повторному CreateTablet случайно сбросить recovery. Поэтому флаг независим.

**Decision**: Остановить followers и перезапустить leader; сохранять флаг при перезапуске Hive,
сверять его в SyncTablets, блокировать обычный boot followers. Во время recovery не передавать
таблетку другому Hive; запретить включение уже захваченной/заблокированной/external таблетки.
**Rationale**: Иначе смена управляющего Hive или принятие старого normal-экземпляра потеряет режим.

**Decision**: Local объявляет SupportsRecovery в доступности типов; Hive размещает recovery
только на совместимых узлах, повторно проверяя способность перед отправкой boot.
**Rationale**: Старый Local игнорировал бы новое protobuf-поле и запускал обычную фабрику.
Во время recovery запрещён downgrade Hive; механизм не обещает совместимость новых операций
со старым управляющим процессом.

**Decision**: Интеграционный тест в hive_ut.cpp использует настоящий TxAllocator, созданный Hive
с ObjectDomain, backup снимка его счётчика, изменение живого состояния, восстановление через
Hive/Local и проверку выдачи диапазона после явного выхода из recovery. Проверить restart Hive,
ошибку восстановления, повторную команду и отсутствие раннего normal boot.
**Rationale**: Старый Dummy round trip остаётся проверкой executor, но не подтверждает FR-007–009.
Попытки запустить research agents завершились ошибкой недоступной модели; исследование выполнено
основным агентом по исходникам. Независимое ревью не выполнено.
