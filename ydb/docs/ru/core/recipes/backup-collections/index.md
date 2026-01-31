# Рецепты для коллекций резервных копий

Пошаговые руководства для типовых сценариев работы с коллекциями резервных копий.

## Начало работы

- [Создание первой коллекции резервных копий](getting-started.md) — создание коллекции, резервное копирование и мониторинг операций

## Настройка окружений

- [Настройка резервного копирования для разных сред](multi-environment-setup.md) — конфигурация для разработки и продакшена
- [Стратегия резервного копирования для микросервисов](microservices-backup-strategy.md) — организация резервных копий по границам сервисов

## Внешнее хранилище

- [Экспорт резервных копий во внешнее хранилище](exporting-to-external-storage.md) — экспорт в S3 или файловую систему для аварийного восстановления
- [Импорт и восстановление резервных копий](importing-and-restoring.md) — восстановление из внешнего хранилища

## Обслуживание

- [Обслуживание и очистка резервных копий](maintenance-and-cleanup.md) — управление жизненным циклом и хранилищем
- [Проверка и тестирование резервных копий](validation-and-testing.md) — проверка целостности резервных копий

## См. также

- [Коллекции резервных копий](../../concepts/datamodel/backup-collection.md)
- [Резервное копирование и восстановление](../../devops/backup-and-recovery.md)
- [CREATE BACKUP COLLECTION](../../yql/reference/syntax/create-backup-collection.md)
- [BACKUP](../../yql/reference/syntax/backup.md)
- [RESTORE](../../yql/reference/syntax/restore-backup-collection.md)
- [DROP BACKUP COLLECTION](../../yql/reference/syntax/drop-backup-collection.md)
