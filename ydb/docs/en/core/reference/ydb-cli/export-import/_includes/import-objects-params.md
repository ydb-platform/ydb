`--destination-path PATH`: Target directory for imported objects; the default value is the database root.

`--include PATH`: Data schema objects to include in the import. Directories are traversed recursively. To include multiple objects, the parameter can be specified multiple times. If not specified, all exported objects are loaded.

`--exclude STRING`: Pattern ( [PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) for excluding paths from the import. This parameter can be specified multiple times for different patterns.
