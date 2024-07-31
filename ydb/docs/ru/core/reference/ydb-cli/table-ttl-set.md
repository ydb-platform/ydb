# Установка параметров TTL

С помощью подкоманды `table ttl set` вы можете установить [TTL](../../concepts/ttl.md) для указанной таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table ttl set [options...] <table path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `table path` — путь таблицы.

Посмотрите описание команды установки TTL:

```bash
{{ ydb-cli }} table ttl set --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--column` | Имя колонки, которая будет использована для вычисления времени жизни строк. Колонка должна иметь тип [числовой](../../yql/reference/types/primitive.md#numeric) или [дата и время](../../yql/reference/types/primitive.md#datetime).<br/>В случае числового типа значение будет интерпретироваться как время, прошедшее с начала [эпохи Unix](https://ru.wikipedia.org/wiki/Unix-время). Единицы измерения должны быть заданы в параметре `--unit`.
`--expire-after` | Дополнительное время до удаления, которое должно пройти после истечения времени жизни строки. Указывается в секундах.<br/>Значение по умолчанию — `0`.
`--unit` | Единицы измерения значений колонки, которая указана в параметре `--column`. Обязателен, если колонка имеет [числовой](../../yql/reference/types/primitive.md#numeric) тип.<br/>Возможные значения:<ul><li>`seconds (s, sec)` — секунды;</li><li>`milliseconds (ms, msec)` — миллисекунды;</li><li>`microseconds (us, usec)` — микросекунды;</li><li>`nanoseconds (ns, nsec)` — наносекунды.</li></ul>
`--run-interval` | Интервал запуска операции удаления строк с истекшим TTL. Указывается в секундах. Настройки БД по умолчанию не позволяют задать интервал меньше 15 минут (900 секунд).<br/>Значение по умолчанию — `3600`.

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Установите TTL для таблицы `series`

```bash
{{ ydb-cli }} -p quickstart table ttl set \
  --column createtime \
  --expire-after 3600 \
  --run-interval 1200 \
  series
```
