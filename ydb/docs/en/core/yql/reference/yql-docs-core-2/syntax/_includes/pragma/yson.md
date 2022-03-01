## Yson

Managing the default behavior of Yson UDF, for more information, see the [documentation](../../../udf/list/yson.md) and, in particular, [Yson::Options](../../../udf/list/yson.md#ysonoptions).

### `yson.AutoConvert`

| Value type | Default |
| --- | --- |
| Flag | false |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

### `yson.Strict`

| Value type | Default |
| --- | --- |
| Flag | true |

Strict mode control in all Yson UDF calls, including implicit calls. If the value is omitted or is `"true"`, it enables the strict mode. If the value is `"false"`, it disables the strict mode.

### `yson.DisableStrict`

| Value type | Default |
| --- | --- |
| Flag | false |

An inverted version of `yson.Strict`. If the value is omitted or is `"true"`, it disables the strict mode. If the value is `"false"`, it enables the strict mode.

