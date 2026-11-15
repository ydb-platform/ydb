### Syntax

`<Primitive type>( <string>[, <additional attributes>] )`

Unlike `CAST("myString" AS MyType)`:

* The check for literal's castability to the desired type occurs at validation.
* The result is non-optional.

The `<string>` argument must be a string literal. Expressions, including variables, columns, query parameters, and function results, are not supported. To convert a value at query execution time, use `CAST(expression AS <Primitive type>)`. For example, convert a computed `String` to `Uuid` using `CAST(expression AS Uuid)`, not `Uuid(expression)`.

```yql
$uuid = Uuid("550e8400-e29b-41d4-a716-446655440000");
$uuidAsString = CAST($uuid AS String);

SELECT CAST($uuidAsString AS Uuid) = $uuid; -- true
```

For the data types `Date`, `Datetime`, `Timestamp`, and `Interval`, literals are supported only in the format corresponding to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). `Interval` has the following differences from the standard:

* It supports the negative sign for shifts to the past.
* Microseconds can be expressed as fractional parts of seconds.
* You can't use units of measurement exceeding one week.
* The options with the beginning/end of the interval and with repetitions, are not supported.

For the data types `TzDate`, `TzDatetime`, `TzTimestamp`, literals are also set in the format meeting [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), but instead of the optional Z suffix, they specify the [IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), separated by comma (for example, GMT or Europe/Moscow).

{% include [decimal args](../../_includes/decimal_args.md) %}

{% include [x](../../_includes/type_literals_examples.md) %}



{% if feature_webui %}

## Access to the metadata of the current operation {#metadata}

When you run YQL operations via the web interface or HTTP API, you get access to the following data:

* `CurrentOperationId()`: The private ID of the operation.
* `CurrentOperationSharedId()`: The public ID of the operation.
* `CurrentAuthenticatedUser()`: The username of the current user.

No arguments.

If this data is missing, for example, when you run operations in the embedded mode, the functions return an empty string.

### Examples

```yql
SELECT
    CurrentOperationId(),
    CurrentOperationSharedId(),
    CurrentAuthenticatedUser();
```



{% endif %}
