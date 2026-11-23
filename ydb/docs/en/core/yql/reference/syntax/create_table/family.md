# Column groups

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Columns of the same table can be grouped to set the following parameters:

* `DATA`: A storage device type for the data in this column group. Acceptable values: `"ssd"`, `"rot"`.
* `COMPRESSION`: A data compression codec. Acceptable values: `"off"`, `"lz4"`, `"zstd"`.
* `COMPRESSION_LEVEL`: A ZSTD compression level from `0` to `22`. This parameter is only supported with `COMPRESSION = "zstd"`. If omitted, the level set by the administrator in the default settings for this column group is used. If no level is configured, `1` is used. Explicitly setting `0` selects the ZSTD library default, which is `3`.
* `CACHE_MODE`: [Caching mode](../../../../concepts/datamodel/table.md#cache-modes). Acceptable values: `"in_memory"`, `"regular"`.

By default, all columns are in the same group named `default`.  If necessary, the parameters of this group can also be redefined, if they are not redefined, then predefined values are applied.

Setting `COMPRESSION` to `"off"` or `"lz4"` clears any inherited compression level. For details, see [Compression in row-oriented tables](../../../../concepts/compression.md#row-oriented-tables).

## Example

In the example below, for the created table, the `family_large` group of columns is added and set for the `series_info` column, and the parameters for the default group, which is set by `default` for all other columns, are also redefined.

```yql
CREATE TABLE series_with_families (
    series_id Uint64,
    title Utf8,
    series_info Utf8 FAMILY family_large,
    release_date Uint64,
    PRIMARY KEY (series_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off",
        CACHE_MODE = "in_memory"
    ),
    FAMILY family_large (
        DATA = "rot",
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 5,
        CACHE_MODE = "regular"
    )
);
```

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}
