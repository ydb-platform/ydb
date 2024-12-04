# Настройка времени жизни строк (TTL) таблицы

В этом разделе приведены примеры настройки TTL строковых и колоночных таблиц при помощи {{ ydb-short-name }} CLI.

## Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column created_at --expire-after 3600 mytable
```

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column modified_at --expire-after 3600 --unit seconds mytable
```

## Выключение TTL {#disable}

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl reset mytable
```

## Получение настроек TTL {#describe}

Текущие настройки TTL можно получить из описания таблицы:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> scheme describe mytable
```

