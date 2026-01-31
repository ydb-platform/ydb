# Импорт и восстановление резервных копий

Импорт резервных копий из внешнего хранилища и восстановление данных в базе данных.

## Импорт из файловой системы

Импорт ранее экспортированных резервных копий:

```bash
# Импорт резервной копии в целевую базу данных
ydb tools restore -i /backup/exports/production_backups_export -d .backups/collections/production_backups_restored

# Импорт конкретной резервной копии в коллекцию
ydb tools restore -i /backup/exports/backup_20240315 -d .backups/collections/emergency_restore
```

## Импорт из S3

```bash
ydb import s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket my-backup-bucket \
  --item backups/production_backups,.backups/collections/production_backups
```

## Восстановление данных

После импорта резервных копий в кластер восстановите данные:

```sql
RESTORE production_backups;
```

## Процесс аварийного восстановления

1. **Импорт полной резервной копии**: сначала импортируйте базовую полную резервную копию
2. **Импорт инкрементальных копий**: импортируйте каждую инкрементальную резервную копию по порядку
3. **Выполнение RESTORE**: запустите команду RESTORE для применения цепочки резервных копий

```bash
# Шаг 1: Импорт полной резервной копии
ydb tools restore -i /backup/full_20250821 -d .backups/collections/recovery/20250821141425Z_full

# Шаг 2: Импорт инкрементальных копий по порядку
ydb tools restore -i /backup/inc_20250822 -d .backups/collections/recovery/20250822070000Z_incremental

# Шаг 3: Восстановление
ydb yql -s "RESTORE recovery;"
```

## Следующие шаги

- [Обслуживание и очистка резервных копий](maintenance-and-cleanup.md)
- [Проверка и тестирование резервных копий](validation-and-testing.md)
