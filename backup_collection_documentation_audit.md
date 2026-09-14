# Функциональный аудит backup collections

Дата проверки: 2026-09-12.

Проверяемая версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Область проверки: backup collections только для строковых таблиц, хранилище `cluster`. Баги в трекере не создавались. ASAN по последнему указанию не использовался.

## Итог

Найдены две проблемы реализации и два расхождения документации с CLI:

| ID | Тип | Краткое описание | Regression test |
|---|---|---|---|
| F1 | реализация | Full и incremental backup одной коллекции в пределах одной секунды конфликтуют по имени внутреннего CDC stream | `test_rapid_consecutive_backups_do_not_collide` |
| F2 | реализация/контракт | После restore нельзя продолжить существующую incremental-цепочку без нового full backup | `test_incremental_backup_continues_after_restore` |
| D1 | документация | Для наблюдения за full backup и restore ошибочно указан только тип операций `incbackup` | не нужен |
| D2 | документация | В monitoring-скрипте используется не поддерживаемый CLI-формат `json` | не нужен |

Regression tests намеренно падают на текущей реализации; итог запуска: `2 tests: 2 - FAIL`. Обе ошибки совпадают с описанием ниже.

Тест секундной коллизии дополнительно запущен с `--test-retries 2`: оба запуска упали на `path exist` для внутреннего `*_continuousBackupImpl`, то есть воспроизведение детерминировано в проверенной конфигурации.

## Методика

Сначала сценарии выполнялись вручную через `ydb yql`, `ydb sql`, `ydb operation list` и `ydb scheme describe`. Затем проблемы реализации были перенесены в отдельный functional test.

Проверялись:

- создание коллекции из двух таблиц, включая таблицу со secondary index;
- full backup, изменение данных и incremental backup;
- восстановление полной цепочки full + incremental;
- восстановление таблицы и secondary index;
- конфликт restore с уже существующей таблицей и отсутствие частичного восстановления;
- повторный full backup и продолжение incremental-цепочки;
- быстрый full → incremental;
- продолжение incremental-цепочки непосредственно после restore;
- команды наблюдения из документации и используемые ими форматы вывода.

## Проблемы реализации

### F1. Full и incremental backup конфликтуют в пределах одной секунды

Последовательность:

1. Создать backup collection с `INCREMENTAL_BACKUP_ENABLED = 'true'`.
2. Выполнить `BACKUP collection;` и дождаться появления full snapshot.
3. В ту же секунду изменить строку и выполнить `BACKUP collection INCREMENTAL;`.

Full backup успешно завершает YQL-операцию, но incremental backup падает:

```text
Check failed: path: '/Root/rapid_orders/20260912045003Z_continuousBackupImpl',
error: path exist, request doesn't accept it ... type: EPathTypeCdcStream
```

Имя внутреннего CDC stream содержит timestamp с секундной точностью. Две резервные копии, начатые в одну секунду, получают одинаковое имя. Пользователь не должен подбирать задержку между успешно завершившимися командами.

Воспроизведение: `test_rapid_consecutive_backups_do_not_collide` в `ydb/tests/functional/backup_collection_doc_audit/test_backup_collection_doc_audit.py`.

### F2. После restore нельзя продолжить incremental-цепочку

Последовательность:

1. Создать full backup.
2. Изменить данные и создать incremental backup.
3. Удалить таблицу, выполнить restore и дождаться, пока значение из incremental backup станет доступно.
4. Снова изменить данные и выполнить следующий incremental backup.

Последняя команда падает:

```text
Last continuous backup stream is not found
```

При этом восстановление full + incremental действительно завершено: перед последней записью тест читает значение из первого incremental backup. Новый full backup восстанавливает возможность делать incremental backup, но начинает новую цепочку и требует дополнительного полного снимка.

Нужно явно определить контракт. Если restore должен оставлять коллекцию пригодной для дальнейших incremental backup, это дефект реализации. Если цепочка намеренно становится read-only, это существенное ограничение необходимо описать в документации и диагностировать более явно.

Воспроизведение: `test_incremental_backup_continues_after_restore` в `ydb/tests/functional/backup_collection_doc_audit/test_backup_collection_doc_audit.py`.

## Проблемы документации

### D1. Для разных операций указан только `incbackup`

В руководстве и YQL reference для проверки статуса предлагается команда:

```bash
ydb operation list incbackup
```

Фактически тип зависит от операции:

| Команда | Тип для `ydb operation list` |
|---|---|
| `BACKUP collection` | `fullbackup` |
| `BACKUP collection INCREMENTAL` | `incbackup` |
| `RESTORE collection` | `restore` |

После full backup список `incbackup` пуст, поэтому приведённая команда не позволяет проверить результат операции перед повтором. Следует показывать все три типа либо выбирать тип в зависимости от команды.

Затронутые страницы:

- `ydb/docs/ru/core/recipes/backup/backup-collections/getting-started.md`;
- `ydb/docs/ru/core/recipes/backup/backup-collections/validation-and-testing.md`;
- `ydb/docs/ru/core/yql/reference/syntax/backup.md`;
- `ydb/docs/ru/core/yql/reference/syntax/restore-backup-collection.md`.

### D2. Monitoring-скрипт использует неподдерживаемый `--format json`

В `validation-and-testing.md` приведено:

```bash
ydb operation list incbackup --format json
```

Текущий CLI отклоняет `json`. Для этой команды доступны, в частности, `pretty` и `proto-json-base64`. Скрипт с `jq` в опубликованном виде не запускается; нужно использовать поддерживаемый JSON-формат и согласовать с ним поля фильтра.

## Что прошло

- Full + incremental restore возвращает последнее состояние данных.
- Secondary index восстанавливается вместе с таблицей и пригоден для чтения через `VIEW`.
- Restore при конфликте одной из таблиц завершается ошибкой до создания остальных таблиц коллекции; частичного результата не обнаружено.
- После restore можно сделать новый full backup, а затем снова создавать incremental backups.
- Появление таблицы при restore не означает завершение восстановления всей цепочки. Ожидание конечного значения корректно учитывает документированную асинхронность.

## Открытые вопросы

1. Должен ли restore сохранять возможность продолжить ту же incremental-цепочку, или после него обязателен новый full backup?
2. Должны ли идентификаторы внутренних CDC streams быть уникальными при нескольких успешно принятых backup-командах в одну секунду?
3. Какой формат вывода CLI считается стабильным контрактом для monitoring-скриптов: `proto-json-base64` или отдельный машинный JSON-формат?
4. Стоит ли на всех страницах явно показать соответствие `fullbackup` / `incbackup` / `restore`, чтобы пользователь мог дождаться завершения именно своей операции?
