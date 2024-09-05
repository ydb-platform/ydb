# Column groups

{% if oss == true and backend_name == "YDB" %}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

Columns of the same table can be grouped to set the following parameters:

* `DATA`: A storage device type for the data in this column group. Acceptable values: ```ssd```, ```rot```.
* `COMPRESSION`: A data compression codec. Acceptable values: ```off```, ```lz4```.

By default, all columns are in the same group named ```default```.  If necessary, the parameters of this group can also be redefined.

In the example below, for the created table, the ```family_large``` group of columns is added and set for the ```series_info``` column, and the parameters for the default group, which is set by ```default``` for all other columns, are also redefined.

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

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}