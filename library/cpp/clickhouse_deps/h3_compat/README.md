ClickHouse uses a not stable version of H3 library, which interface is incompatible with both 3.x and 4.x H3 versions.

To avoid having several versions of H3 library in our monorepository, we implement H3 API used in ClickHouse via H3 3.x version.
