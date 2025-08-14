# Устранение неполадок спиллинга

Этот раздел предоставляет информацию по устранению неполадок для распространенных проблем со спиллингом в {{ ydb-short-name }}. Спиллинг — это механизм управления памятью, который временно сохраняет данные на диск при нехватке оперативной памяти.

## Частые проблемы

- [Permission denied](permission-denied.md) - Недостаточные права доступа к директории спиллинга
- [Spilling Service not started](service-not-started.md) - Попытка использования спиллинга при выключенном Spilling Service
- [Total size limit exceeded](total-size-limit-exceeded.md) - Превышен максимальный суммарный размер файлов спиллинга
- [Can not run operation](can-not-run-operation.md) - Переполнение очереди операций в пуле потоков I/O

## См. также

- [Конфигурация спиллинга](../../reference/configuration/spilling.md)
- [Концепция спиллинга](../../concepts/spilling.md)
- [Конфигурация контроллера памяти](../../reference/configuration/index.html#memory-controller)
- [Мониторинг {{ ydb-short-name }}](../../devops/observability/monitoring.md)
- [Диагностика производительности](../performance/index.md)
