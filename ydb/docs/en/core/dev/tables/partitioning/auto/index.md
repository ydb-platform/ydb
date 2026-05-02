# Automatic partitioning {#automatic-partitioning}

For row tables, {{ ydb-short-name }} can automatically **split** and **merge** partitions to adapt to data volume and query intensity. This is configured with `AUTO_PARTITIONING_*` table settings (see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).

Articles in this subsection:

* [{#T}](by-size.md) — size-based mode (`AUTO_PARTITIONING_BY_SIZE`, partition size threshold);
* [{#T}](by-load.md) — load-based mode (`AUTO_PARTITIONING_BY_LOAD`, CPU thresholds);
* [{#T}](limits.md) — internal constraints, min/max caps, and typical interactions with split/merge.

Typical split/merge duration is on the order of **500 ms**; during that interval data for the affected partition may be briefly unavailable for reads and writes (details in [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).
