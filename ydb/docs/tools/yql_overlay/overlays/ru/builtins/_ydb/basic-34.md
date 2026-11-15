### Синтаксис

`<Простой тип>( <строка>[, <дополнительные атрибуты>] )`

В отличие от `CAST("myString" AS MyType)`:

* Проверка на приводимость литерала к требуемому типу происходит на этапе валидации;
* Результат не является optional.

Аргумент `<строка>` должен быть строковым литералом. Выражения, включая переменные, колонки, параметры запроса и результаты функций, не поддерживаются. Для преобразования значения во время выполнения запроса используйте `CAST(выражение AS <Простой тип>)`. Например, вычисляемое значение `String` следует преобразовывать в `Uuid` через `CAST(выражение AS Uuid)`, а не через `Uuid(выражение)`.

```yql
$uuid = Uuid("550e8400-e29b-41d4-a716-446655440000");
$uuidAsString = CAST($uuid AS String);

SELECT CAST($uuidAsString AS Uuid) = $uuid; -- true
```

Для типов данных `Date`, `Datetime`, `Timestamp` и `Interval` поддерживаются литералы только в формате, соответствующем [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601). У `Interval` есть следующие отличия от стандарта:

* поддерживается отрицательный знак для сдвигов в прошлое;
* микросекунды могут быть записаны как дробная часть секунд;
* единицы измерения больше недель не доступны;
* не поддерживаются варианты с началом/концом интервала, а также повторами.

Для типов данных `TzDate`, `TzDatetime`, `TzTimestamp` литералы также задаются в формате, соответствующем [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601), но вместо опционального суффикса Z через запятую указывается [IANA имя временной зоны](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), например, GMT или Europe/Moscow.

{% include [decimal args](../../_includes/decimal_args.md) %}

{% include [x](../../_includes/type_literals_examples.md) %}

{% if feature_webui %}

## Доступ к метаданным текущей операции {#metadata}

При запуске YQL операций через веб-интерфейс или HTTP API, предоставляется доступ к следующей информации:

* `CurrentOperationId()` — приватный идентификатор операции;
* `CurrentOperationSharedId()` — публичный идентификатор операции;
* `CurrentAuthenticatedUser()` — логин текущего пользователя.

### Сигнатуры

```yql
CurrentOperationId()->String
CurrentOperationSharedId()->String
CurrentAuthenticatedUser()->String
```

Аргументов нет.

При отсутствии данной информации, например, при запуске в embedded режиме, возвращают пустую строку.

### Примеры

```yql
SELECT
    CurrentOperationId(),
    CurrentOperationSharedId(),
    CurrentAuthenticatedUser();
```


{% endif %}
