### Running an operation to export data to S3 {#s3_export}

The commands given in the examples below are based on the assumption that access key data is saved to the `~/.aws/credentials` file.

Running the operation for exporting data from the `$YDB_DB_PATH/backup/episodes`, `$YDB_DB_PATH/backup/seasons`, and `$YDB_DB_PATH/backup/series` tables in the YDB `$YDB_DB_PATH` database to files prefixed with `20200601/` in the `testdbbackups` bucket in {{ objstorage-name }}.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH export s3 --s3-endpoint {{ s3-storage-host }}  --bucket testdbbackups\
--item source=$YDB_DB_PATH/backup/episodes,destination=20200601/episodes\
--item source=$YDB_DB_PATH/backup/seasons,destination=20200601/seasons\
--item source=$YDB_DB_PATH/backup/series,destination=20200601/series
```

Once the command is executed, the console client displays information about this operation's status.

```
┌───────────────────────────────────────────┬───────┬─────────┬───────────┬───────────────────┬───────────────┐
| id                                        | ready | status  | progress  | endpoint          | bucket        |
├───────────────────────────────────────────┼───────┼─────────┼───────────┼───────────────────┼───────────────┤
| ydb://export/6?id=846776181822113&kind=s3 | false | SUCCESS | Preparing | s3.mds.yandex.net | testdbbackups |
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┤
| Items:                                                                                                      |
|   - source_path: /some_path/episodes                                                                        |
|     destination_prefix: 20200601/episodes                                                                   |
|   - source_path: /some_path/seasons                                                                         |
|     destination_prefix: 20200601/seasons                                                                    |
|   - source_path: /some_path/series                                                                          |
|     destination_prefix: 20200601/series                                                                     |
| Description:                                                                                                |
| Number of retries: 10                                                                                       |
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

As a result of the successfully completed export operation, `CSV` files containing the table data and schema with the prefixes listed below are saved to the `testdbbackups` bucket.

```
20200601/episodes
20200601/seasons
20200601/series
```

The file format is identical to that of the files created after [backups to the file system](#filesystem_backup).

After running the command below in the AWS CLI, a list of prefixes created after the backup in the `testdbbackup` bucket is displayed.

```
aws --endpoint-url=https://{{ s3-storage-host }} s3 ls testdbbackups/20200601/
                           PRE episodes/
                           PRE seasons/
                           PRE series/
```

{% note info "Работа с директориями" %}

To back up all tables in the YDB directory, specify the path to the directory as the source.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH export s3 \
--s3-endpoint {{ s3-storage-host }} \
--bucket testdbbackups \
--item source=$YDB_DB_PATH/backup,destination=20200601/
```

{% endnote %}

You can output the current status of the export operation that was started earlier using the command below.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH operation get 'ydb://export/6?id=846776181822113&kind=s3'
┌───────────────────────────────────────────┬───────┬─────────┬───────────────┬───────────────────┬───────────────┐
| id                                        | ready | status  | progress      | endpoint          | bucket        |
├───────────────────────────────────────────┼───────┼─────────┼───────────────┼───────────────────┼───────────────┤
| ydb://export/6?id=846776181822113&kind=s3 | false | SUCCESS | TransferData  | s3.mds.yandex.net | testdbbackups |
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┤
| Items:                                                                                                          |
|   - source_path: /some_path/episodes                                                                            |
|     destination_prefix: 20200601/episodes                                                                       |
|   - source_path: /some_path/seasons                                                                             |
|     destination_prefix: 20200601/seasons                                                                        |
|   - source_path: /some_path/series                                                                              |
|     destination_prefix: 20200601/series                                                                         |
| description:                                                                                                    |
| Number of retries: 10                                                                                           |
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

If the export operation is completed successfully, the progress column displays `Done`, the status column — `Success`, and the ready column — `true`.

```
┌───────────────────────────────────────────┬───────┬─────────┬──────────┬───────────────────┬───────────────┐
| id                                        | ready | status  | progress | endpoint          | bucket        |
├───────────────────────────────────────────┼───────┼─────────┼──────────┼───────────────────┼───────────────┤
| ydb://export/6?id=846776181822113&kind=s3 | true  | SUCCESS | Done     | s3.mds.yandex.net | testdbbackups |
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┤
| Items:                                                                                                     |
|   - source_path: /some_path/episodes                                                                       |
|     destination_prefix: 20200601/episodes                                                                  |
|   - source_path: /some_path/seasons                                                                        |
|     destination_prefix: 20200601/seasons                                                                   |
|   - source_path: /some_path/series                                                                         |
|     destination_prefix: 20200601/series                                                                    |
| description:                                                                                               |
| Number of retries: 10                                                                                      |
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

