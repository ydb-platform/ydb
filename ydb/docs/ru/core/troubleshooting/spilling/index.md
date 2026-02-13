# Устранение неполадок спиллинга

Этот раздел предоставляет информацию по устранению неполадок для распространенных проблем со спиллингом в {{ ydb-short-name }}. Спиллинг — это механизм управления памятью, который временно сохраняет промежуточные вычислительные данные на диск при нехватке оперативной памяти. Эти ошибки могут возникать во время выполнения запросов, когда система пытается использовать функциональность спиллинга, и могут наблюдаться в логах и ответах запросов.

## Частые проблемы

- [Permission denied](permission-denied.md) - Недостаточные права доступа к директории спиллинга
- [Spilling Service not started](service-not-started.md) - Попытка использования спиллинга при выключенном Spilling Service
- [Total size limit exceeded](total-size-limit-exceeded.md) - Превышен максимальный суммарный размер файлов спиллинга
- [Can not run operation](can-not-run-operation.md) - Переполнение очереди операций в пуле потоков I/O

## См. также

- [Конфигурация спиллинга](../../reference/configuration/table_service_config.md)
- [Концепция спиллинга](../../concepts/query_execution/spilling.md)
- [Конфигурация контроллера памяти](../../reference/configuration/memory_controller_config.md)
- [Мониторинг {{ ydb-short-name }}](../../devops/observability/monitoring.md)
- [Диагностика производительности](../performance/index.md)
