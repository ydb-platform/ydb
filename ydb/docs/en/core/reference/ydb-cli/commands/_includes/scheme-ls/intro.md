# Listing objects

To get a listing of objects, use the `scheme ls <Path>` subcommand. If you don't specify the path, a listing of the DB root will be output:

```bash
{{ ydb-cli }} scheme ls
```

Result:

```text
episodes seasons  series .sys
```

To view detailed information about objects, add the `-l` flag:

```bash
{{ ydb-cli }} scheme ls -l
```

