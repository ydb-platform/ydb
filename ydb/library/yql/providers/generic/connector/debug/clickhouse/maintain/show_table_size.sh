#!/bin/bash

set -ex

URL='http://admin:password@localhost:8123/?database=dqrun'

#echo "SELECT table, formatReadableSize(sum(bytes)) as size, min(min_date) as min_date, max(max_date) as max_date
#      FROM system.parts WHERE active GROUP BY table" | curl "${URL}" --data-binary @-

echo "SELECT
        database,
        table,
        formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
        formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
        round(usize / size, 2) AS compr_rate,
        sum(rows) AS rows,
        count() AS part_count
    FROM system.parts
    WHERE (active = 1) AND (database LIKE '%') AND (table LIKE '%')
    GROUP BY database, table ORDER BY size DESC;" | curl "${URL}" --data-binary @-
