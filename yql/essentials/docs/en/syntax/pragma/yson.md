# Yson

Managing the default behavior of Yson UDF, for more information, see the [documentation](../../udf/list/yson.md) and, in particular, [Yson::Options](../../udf/list/yson.md#ysonoptions).

The Yson pragma affects all subsequent expressions up to the end of the module in which it occurs.

## `yson.AutoConvert` {#autoconvert}

| Value type | Default | Allowed in project settings |
| --- | --- | --- |
| Flag | false | Yes |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

## `yson.Strict` {#strict}

| Value type | Default | Allowed in project settings |
| --- | --- | --- |
| Flag | true | Yes |

Strict mode control in all Yson UDF calls, including implicit calls. If the value is omitted or is `"true"`, it enables the strict mode. If the value is `"false"`, it disables the strict mode.

## `yson.DisableStrict`

| Value type | Default | Allowed in project settings |
| --- | --- | --- |
| Flag | false | Yes |

An inverted version of `yson.Strict`. If the value is omitted or is `"true"`, it disables the strict mode. If the value is `"false"`, it enables the strict mode.

## `yson.CastToString` / `yson.DisableCastToString`

| Arguments | Allowed in project settings |
| --- | --- |
| None | Yes |

`yson.CastToString` allows casting `Yson` to `String` with `CAST`. `yson.DisableCastToString` disallows this conversion. Use the form without arguments.
