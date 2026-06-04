`--root-path PATH`: Root directory for the objects being exported. If not specified, the database root is used.

`--include PATH`: Schema objects to include in the export. Directories are traversed recursively. Paths are relative to `root-path`. You may specify this parameter multiple times to include several objects. If not specified, all non-system objects in `root-path` are exported.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. Paths are relative to `root-path`. You may specify this parameter multiple times for different templates.
