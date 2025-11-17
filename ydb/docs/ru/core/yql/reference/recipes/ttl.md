# Настройка времени жизни строк (TTL) таблицы

В этом разделе приведены примеры настройки TTL строковых и колоночных таблиц при помощи YQL.

## Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON created_at);
```

{% note tip %}

`Interval` создается из строкового литерала в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) с [некоторыми ограничениями](../builtins/basic#data-type-literals).

{% endnote %}

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON modified_at AS SECONDS);
```


## Включение TTL для вновь создаваемой таблицы {#enable-for-new-table}

Для вновь создаваемой таблицы можно передать настройки TTL вместе с ее описанием:

```yql
CREATE TABLE `mytable` (
  id Uint64,
  expire_at Timestamp,
  PRIMARY KEY (id)
) WITH (
  TTL = Interval("PT1H") ON expire_at
);
```

## Выключение TTL {#disable}

```yql
ALTER TABLE `mytable` RESET (TTL);
```

