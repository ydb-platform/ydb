/* syntax version 1 */
select
    DateTime::FromSeconds64(fts64_sec) as ts64_sec,
    DateTime::FromMilliseconds64(fts64_msec) as ts64_msec,
    DateTime::FromMicroseconds64(fts64_usec) as ts64_usec,
from Input
