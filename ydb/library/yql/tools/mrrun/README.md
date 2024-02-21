# mrrun - Utility for MR query debugging

`mrrun` is a utility designed for local debugging of MapReduce queries. It allows you to execute YQL programs which involve creation of YT transactions.

## Command-Line Options

- `-p <file>`: _(required)_ specify a file with an SQL query or an s-expression (`-` for /dev/stdin).
- `-s`: if specified, SQL is used; if not specified, the query plan execution specified in s-expression is used.
- `-f <alias>@<path>`: attach a local file to the query
- `--mrjob-bin <path>`: specify the path of mrjob binary to upload (the mrrun itself is used by default)

## Example of Local Usage

```bash
mrrun -s -p query.sql -f file.txt@./some-file.txt
```

In this example, `mrrun` will use SQL from the file `query.sql`, with file `./some-file.txt` attached to the request with alias `file.txt`.

The simple example of `query.sql`:

```sql
USE `hahn`;

SELECT Length(name) FROM `home/yql/tutorial/users`;
SELECT FileContent('file.txt');
```
## mrjob binary

Note that default mrjob binary (mrrun itself) has size of ~2GB, so it will take a long time to upload it to YT.
It is recommended to compile mrjob separately and strip it (final size is about 200MB), then pass it to the dedicated command-line parameter.
```bash``
mrrun -s -p query.sql --mrjob-bin ./ydb/library/yql/tools/mrjob/mrjob
```
