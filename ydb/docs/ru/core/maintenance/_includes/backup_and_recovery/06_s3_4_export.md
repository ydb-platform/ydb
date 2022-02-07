### Запуск операции экспорта в S3 {#s3_export}

Команды в примерах ниже составлены из расчёта, что данные ключей доступа сохранены в файл `~/.aws/credentials`.

Запуск операции операции экспорта данных из таблиц `$YDB_DB_PATH/backup/episodes`, `$YDB_DB_PATH/backup/seasons`, `$YDB_DB_PATH/backup/series` в YDB в базе `$YDB_DB_PATH` в файлы с префиксом `20200601/` в бакете `testdbbackups` в {{ objstorage-name }}.
```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH export s3 --s3-endpoint {{ s3-storage-host }}  --bucket testdbbackups\
--item source=$YDB_DB_PATH/backup/episodes,destination=20200601/episodes\
--item source=$YDB_DB_PATH/backup/seasons,destination=20200601/seasons\
--item source=$YDB_DB_PATH/backup/series,destination=20200601/series
```

В результате выполнения команды консольный клиент выведет информацию о статусе запущенной операции.

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

После успешного завершения операции экспорта в бакете `testdbbackups` будут сохранены файлы в формате `csv` с данными и схемой таблиц c префиксами, перечисленными ниже.
```
20200601/episodes
20200601/seasons
20200601/series
```

Формат идентичен формату файлов, создаваемых в результате [резервного копирования на файловую систему](#filesystem_backup).

В результате выполнения с помощью AWS CLI приведённой ниже команды на экран будет выведен список префиксов, созданных в результате бекапа в бакете `testdbbackup`.

```
aws --endpoint-url=https://{{ s3-storage-host }} s3 ls testdbbackups/20200601/
                           PRE episodes/
                           PRE seasons/
                           PRE series/
```

{% note info "Работа с директориями" %}

Чтобы сделать резервную копию всех таблиц в директории YDB, следует указать путь до директории в качестве источника.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH export s3 \
--s3-endpoint {{ s3-storage-host }} \
--bucket testdbbackups \
--item source=$YDB_DB_PATH/backup,destination=20200601/
```

{% endnote %}

вывести на экран текущее состояние запущенной ранее операции экспорта можно приведённой ниже командой.
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

В случае успешного завершения операции экспорта в столбце progress будет отображено значение `Done`, в столбце status – `Success`, в столбце ready – `true`.
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

