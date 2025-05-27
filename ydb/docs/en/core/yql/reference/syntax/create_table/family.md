# Column groups

Columns of the same table can be grouped to set the following parameters:

* `DATA`: A storage device type for the data in this column group. Acceptable values: `ssd`, `rot`.

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

* `COMPRESSION`: A data compression codec. Acceptable values: `off`, `lz4`, `zstd`.

{% if oss == true and backend_name == "YDB" %}

{% include [codec_zstd_allow_for_olap_note](../../../../_includes/codec_zstd_allow_for_olap_note.md) %}

{% endif %}

* `COMPRESSION_LEVEL` — compression level of codec if it supports different compression levels.

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

By default, all columns are in the same group named `default`.  If necessary, the parameters of this group can also be redefined, if they are not redefined, then predefined values are applied.

## Example

In the example below, for the created table, the `family_large` group of columns is added and set for the `series_info` column, and the parameters for the default group, which is set by `default` for all other columns, are also redefined.

{% list tabs %}

- Creating a row-oriented table

    ```sql
    CREATE TABLE series_with_families (
        series_id Uint64,
        title Utf8,
        series_info Utf8 FAMILY family_large,
        release_date Uint64,
        PRIMARY KEY (series_id),
        FAMILY default (
            DATA = "ssd",
            COMPRESSION = "off"
        ),
        FAMILY family_large (
            DATA = "rot",
            COMPRESSION = "lz4"
        )
    );
    ```

- Creating a column-oriented table

    ```sql
    CREATE TABLE series_with_families (
        series_id Uint64,
        title Utf8,
        series_info Utf8 FAMILY family_large,
        release_date Uint64,
        PRIMARY KEY (series_id),
        FAMILY default (
            COMPRESSION = "lz4"
        ),
        FAMILY family_large (
            COMPRESSION = "zstd",
            COMPRESSION_LEVEL = 5
        )
    ) 
    WITH (STORE = COLUMN);
    ```

{% endlist %}

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}
