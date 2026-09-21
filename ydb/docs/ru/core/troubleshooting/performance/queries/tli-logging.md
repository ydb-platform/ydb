# Логирование TLI

Логирование [инвалидации блокировок транзакций](../../../concepts/glossary.md#tli) (TLI) позволяет установить, у какого запроса были сломаны блокировки **(жертва)** и какой запрос их сломал **(нарушитель)**.

## Включение логирования

Для получения детальных [логов](../../../devops/observability/logging.md) о конфликтах блокировок установите уровень `INFO` (числовое значение `6`) для компонента `TLI`.

Добавьте в конфигурацию кластера:

```yaml
log_config:
  entry:
    - component: "TLI"
      level: 6  # INFO
```

Параметр `log_config` [поддерживает динамическое обновление](../../../devops/configuration-management/configuration-v1/dynamic-config.md) без перезапуска узлов.

После включения сервер записывает логи при каждой сломанной блокировке, а `VictimQuerySpanId` начинает появляться в сообщении об ошибке SDK.

{% note info %}

Базовый размер лога для одного TLI-события составляет 10–20 КБ. Фактический объем зависит от размера текста логируемых запросов (виновника и жертвы) и транзакций, частью которых они являются, поскольку все эти данные записываются в лог.
Если конфликты редки, нагрузка на логирование незначительна. При высокой частоте TLI суммарный объём логов может быть существенным.

Чтобы исключить таблицы с ожидаемыми конфликтами из TLI-диагностики, используйте параметр [`tli_config.ignored_table_regexes`](../../../reference/configuration/tli_config.md).

{% endnote %}

## Структура логов

Когда происходит TLI, сервер записывает четыре записи — по одной от каждого компонента для каждой из сторон конфликта.

**Пример сценария:**

Устанавливается две сессии к СУБД: жертва и нарушитель.

| Время | QuerySpanId | Роль | Запрос |
|:------|:------------|:-----|:-------|
| T1 | `1111111111111111` | Жертва | `SELECT * FROM Orders WHERE OrderId = 42` (устанавливает блокировку) |
| T2 | `2222222222222222` | Нарушитель | `UPDATE Orders SET Status = 'done' WHERE OrderId = 42` (ломает блокировку) |
| T3 | `3333333333333333` | Жертва | `UPDATE Orders SET Amount = 100 WHERE OrderId = 42` (коммит — падает) |

{% note info %}

В приводимых далее примерах содержимого логов порядок следования отдельных полей внутри записей может отличаться.

{% endnote %}

**Лог нарушителя (DataShard):**

```text
component=DataShard tabletId=<tablet-id> message="Write transaction broke other locks" breakerQuerySpanId=2222222222222222 victimQuerySpanId=1111111111111111
```

**Лог нарушителя (SessionActor):**

```text
component=SessionActor message="Query had broken other locks" breakerTxSpanId=2222222222222222 querySpanId=2222222222222222 queryText="UPDATE Orders SET Status = 'done' WHERE OrderId = 42"
```

**Лог жертвы (DataShard):**

```text
component=DataShard tabletId=<tablet-id> message="Write transaction was a victim of broken locks" victimQuerySpanId=1111111111111111
```

**Лог жертвы (SessionActor):**

Для одной транзакции в лог пишется две записи:

```text
component=SessionActor message="Query was a victim of broken locks" victimTxSpanId=1111111111111111 querySpanId=1111111111111111 queryText="SELECT * FROM Orders WHERE OrderId = 42"

component=SessionActor message="Query was a victim of broken locks" victimTxSpanId=1111111111111111 querySpanId=3333333333333333 queryText="UPDATE Orders SET Amount = 100 WHERE OrderId = 42"
```

## Поля логов

| Поле | Описание | Где встречается |
|:-----|:---------|:----------------|
| `component` | Источник записи: `DataShard` или `SessionActor` | Все логи TLI |
| `message` | Тип события (см. примеры выше) | Все логи TLI |
| `tabletId` | Идентификатор таблета DataShard | Логи DataShard |
| `victimQuerySpanId` | Идентификатор запроса, чьи блокировки были сломаны | Логи жертвы DataShard, логи нарушителя DataShard |
| `breakerQuerySpanId` | Идентификатор запроса, который сломал блокировки | Логи нарушителя DataShard |
| `victimTxSpanId` | Идентификатор транзакции-жертвы (запрос, который установил сломанные блокировки) | Логи жертвы SessionActor |
| `breakerTxSpanId` | Идентификатор транзакции-нарушителя | Логи нарушителя SessionActor |
| `querySpanId` | Идентификатор конкретного запроса в записи SessionActor | Логи SessionActor |
| `queryText` | SQL конкретного запроса в записи SessionActor | Логи SessionActor |

## Анализ логов

По `VictimQuerySpanId` из сообщения об ошибке SDK можно найти все связанные события:

1. **Поиск идентификатора запроса-жертвы**: в логе жертвы поле `victimQuerySpanId` содержит идентификатор SELECT-запроса, который установил сломанные блокировки.

2. **Поиск текста запроса-жертвы**: в логе жертвы есть запись, у которой `victimTxSpanId` совпадает с `querySpanId`. Поле `queryText` этой записи содержит текст SELECT-запроса, который установил сломанные блокировки.

3. **Поиск идентификатора запроса-нарушителя**: в логе нарушителя DataShard есть запись, у которое поле `victimQuerySpanId` содержит то же самое значение (идентификатор запроса-жертвы). Из этой записи берётся значение `breakerQuerySpanId`, которое является идентификатором запроса-нарушителя.

4. **Поиск текста запроса-нарушителя**: в логе нарушителя есть запись, у которой `breakerTxSpanId` содержит идентификатор запроса-нарушителя и в то же время совпадает с `querySpanId`. Поле `queryText` этой записи содержит текст запроса-нарушителя.

5. **Получение полного контекста транзакций**: все записи SessionActor с одним и тем же `victimTxSpanId` содержат сведения о запросах транзакции-жертвы (в порядке их выполнения). Аналогично, все записи SessionActor с одним и тем же `breakerTxSpanId` содержат сведения о запросах транзакции-нарушителя.

## Утилита find_tli_chain

Для автоматического анализа логов TLI в репозитории {{ ydb-short-name }} есть утилита [find_tli_chain.py](https://github.com/ydb-platform/ydb/tree/main/ydb/tools/tli_analysis).

Данная утилита принимает на вход параметры:

- `VictimQuerySpanId` - идентификатор сломанного запроса из сообщения об ошибке SDK;
- `LogFile` - имя лог-файла для анализа логов.

Подразумевается, что логирование TLI было предварительно включено в [конфигурации](../../../reference/configuration/log_config.md), лог файлы собраны с узлов базы данных и объединены в один файл.

```bash
python3 find_tli_chain.py <VictimQuerySpanId> <LogFile>
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
