# Data model

Добавляется RecoveryMode таблетки Hive (default false) и поля протокола Local. Формат backup не меняется.

- TTabletStorageInfo: TabletID, TabletType, TenantPathId, BootType. TenantPathId больше не
  исключает таблетку из бекапа. BootType должен быть Normal; тип должен входить в whitelist.
- TSystemTabletBackupConfig: существующие backend, ExcludeTabletIds, лимиты и политика ошибок.
- Backup: `<backend path>/<type>/<tablet ID>/backup_<timestamp>_g<generation>_s<step>/`, snapshot и changelog.
  Имена и содержимое manifest не меняются.
- Жизненный цикл: запуск → снимок и сбор изменений → завершение снимка → продолжение журнала;
  новый бекап/перезапуск/ошибка следуют текущей политике flat executor.

- Hive RecoveryMode: сохраняется в строке таблетки; изменяется только явной командой оператора.
- Local SupportsRecovery: возможность узла запустить recovery вместо обычного актора.
- Boot/Sync RecoveryMode: желаемый/фактический режим; несоответствие требует остановки и нового boot.
- Восстановление: Normal → Recovery (stop followers + restart leader) → применение backup →
  Recovery с результатом → Normal только по явной команде оператора. Ошибка сохраняет Recovery.
