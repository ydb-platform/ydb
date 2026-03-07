# Логирование TLI

Логирование [инвалидации блокировок транзакций](../../../concepts/glossary.md#tli) (TLI) позволяет установить, у какого запроса были сломаны блокировки (**жертва**) и какой запрос их сломал (**нарушитель**).

## Включение логирования

Для получения детальных логов о конфликтах блокировок установите уровень `INFO` (числовое значение `6`) для компонента `TLI`.

Добавьте в конфигурацию кластера:

```yaml
log_config:
  entry:
    - component: "TLI"
      level: 6  # INFO
```

Параметр `log_config` [поддерживает динамическое обновление](../../../devops/configuration-management/configuration-v1/dynamic-config.md) без перезапуска узлов.

После включения сервер записывает логи при каждом нарушении блокировок, а `VictimQuerySpanId` начинает появляться в сообщении об ошибке SDK.

{% note info %}

Каждое TLI-событие генерирует около 10–20 КБ логов. Если конфликты редки, нагрузка на логирование незначительна. При высокой частоте TLI суммарный объём логов стоит держать под контролем.

Чтобы исключить таблицы с ожидаемыми конфликтами из TLI-диагностики, используйте параметр [`tli_config.ignored_table_regexes`](../../../reference/configuration/tli_config.md).

{% endnote %}

## Структура логов

Когда происходит TLI, сервер записывает четыре записи — по одной от каждого компонента для каждой из сторон конфликта.

**Пример сценария:**

| Время | QuerySpanId | Роль | Запрос |
|:------|:------------|:-----|:-------|
| T1 | `1111111111111111` | Жертва | `SELECT * FROM Orders WHERE OrderId = 42` (устанавливает блокировку) |
| T2 | `2222222222222222` | Нарушитель | `UPDATE Orders SET Status = 'done' WHERE OrderId = 42` (ломает блокировку) |
| T3 | `3333333333333333` | Жертва | `UPDATE Orders SET Amount = 100 WHERE OrderId = 42` (коммит — падает) |

**Лог нарушителя (DataShard):**

```text
Component: DataShard, TabletId: <tablet-id>,
BreakerQuerySpanId: 2222222222222222, VictimQuerySpanIds: [1111111111111111],
Message: Write transaction broke other locks
```

**Лог нарушителя (SessionActor):**

```text
Component: SessionActor, Message: Query had broken other locks,
BreakerQuerySpanId: 2222222222222222,
BreakerQueryText: UPDATE Orders SET Status = 'done' WHERE OrderId = 42,
BreakerQueryTexts: [QuerySpanId=2222222222222222 QueryText=UPDATE Orders SET Status = 'done' WHERE OrderId = 42]
```

**Лог жертвы (DataShard):**

```text
Component: DataShard, TabletId: <tablet-id>,
VictimQuerySpanId: 1111111111111111, CurrentQuerySpanId: 3333333333333333,
Message: Write transaction was a victim of broken locks
```

**Лог жертвы (SessionActor):**

```text
Component: SessionActor, Message: Query was a victim of broken locks,
VictimQuerySpanId: 1111111111111111, CurrentQuerySpanId: 3333333333333333,
VictimQueryText: SELECT * FROM Orders WHERE OrderId = 42,
VictimQueryTexts: [QuerySpanId=1111111111111111 QueryText=SELECT * FROM Orders WHERE OrderId = 42 |
                   QuerySpanId=3333333333333333 QueryText=UPDATE Orders SET Amount = 100 WHERE OrderId = 42]
```

## Поля логов

| Поле | Описание | Где встречается |
|:-----|:---------|:----------------|
| `VictimQuerySpanId` | Идентификатор запроса, чьи блокировки были сняты | Логи жертвы |
| `BreakerQuerySpanId` | Идентификатор запроса, который сломал блокировки | Логи нарушителя |
| `CurrentQuerySpanId` | Идентификатор запроса в момент ошибки (может отличаться от жертвы) | Логи жертвы |
| `VictimQuerySpanIds` | Массив идентификаторов всех запросов-жертв | Логи нарушителя DataShard |
| `VictimQueryText` | SQL запроса-жертвы, который установил блокировки | Логи жертвы SessionActor |
| `BreakerQueryText` | SQL запроса-нарушителя | Логи нарушителя SessionActor |
| `VictimQueryTexts` | Все запросы транзакции-жертвы | Логи жертвы SessionActor |
| `BreakerQueryTexts` | Все запросы транзакции-нарушителя | Логи нарушителя SessionActor |

## Пошаговая корреляция логов

По `VictimQuerySpanId` из сообщения об ошибке SDK можно найти все связанные события:

1. **Найдите запрос-жертву**: ищите `VictimQuerySpanId: <значение из ошибки>` в логах — это покажет, какой SELECT установил сломанные блокировки, и его текст в поле `VictimQueryText`.

2. **Найдите запрос-нарушитель**: в логе нарушителя DataShard поле `VictimQuerySpanIds` содержит то же значение. Из этого лога возьмите `BreakerQuerySpanId` и ищите лог SessionActor нарушителя — он содержит `BreakerQueryText` с полным текстом запроса.

3. **Получите полный контекст транзакций**: `VictimQueryTexts` и `BreakerQueryTexts` содержат все запросы соответствующих транзакций в порядке выполнения.

## Утилита find_tli_chain

Для автоматической корреляции логов TLI в репозитории {{ ydb-short-name }} есть утилита [`find_tli_chain.py`](https://github.com/ydb-platform/ydb/tree/main/ydb/tools/tli_analysis).

Она принимает `VictimQuerySpanId` из сообщения об ошибке и лог-файл, находит все связанные записи и выводит их в удобном виде:

```bash
python3 find_tli_chain.py <VictimQuerySpanId> <logfile>
```

**Пример:**

```bash
python3 find_tli_chain.py 1111111111111111 ydb.log
```

```text
================================================
  TLI Chain
================================================

VictimQuerySpanId: 1111111111111111

VictimQueryText: SELECT * FROM Orders WHERE OrderId = 42

BreakerQuerySpanId: 2222222222222222

BreakerQueryText: UPDATE Orders SET Status = 'done' WHERE OrderId = 42

================================================
  VictimTx
================================================

SELECT * FROM Orders WHERE OrderId = 42

UPDATE Orders SET Amount = 100 WHERE OrderId = 42

================================================
  BreakerTx
================================================

UPDATE Orders SET Status = 'done' WHERE OrderId = 42
```

Утилита выводит:

- **TLI Chain** — `VictimQuerySpanId`, `VictimQueryText`, `BreakerQuerySpanId`, `BreakerQueryText`;
- **VictimTx** — все запросы транзакции-жертвы в порядке выполнения;
- **BreakerTx** — все запросы транзакции-нарушителя.

Дополнительные параметры:

| Параметр | Описание |
|:---------|:---------|
| `--window-sec N` | Временное окно поиска вокруг события (по умолчанию 10 с) |
| `--no-color` | Отключить цветной вывод |

{% note info %}

Утилита корректно обрабатывает несортированные и смешанные лог-файлы: фильтрация основана на значениях временных меток, а не на позициях строк.

{% endnote %}
