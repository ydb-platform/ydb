`--root-path PATH`: Root directory for exported objects. If not specified, the root directory of the database is used.

`--include PATH`: Data schema objects to include in the export. Directories are traversed recursively. Paths are specified relative to `root-path`. This parameter can be specified multiple times to include multiple objects. If not specified, all non-system objects in `root-path` are exported.

`--exclude STRING`: Pattern ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from the export. Paths are specified relative to `root-path`. This parameter can be specified multiple times for different patterns.
