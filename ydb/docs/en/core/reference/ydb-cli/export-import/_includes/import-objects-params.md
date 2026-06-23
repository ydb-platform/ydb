`--destination-path PATH`: Destination directory for the objects being imported; defaults to the database root.

`--include PATH`: Schema objects to include in the import. Directories are traversed recursively. You may specify this parameter multiple times to include several objects. If not specified, all objects from the export are imported.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from import. You may specify this parameter multiple times for different templates.
