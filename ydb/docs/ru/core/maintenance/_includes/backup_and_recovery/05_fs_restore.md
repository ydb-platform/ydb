
## Восстановление из резервной копии на файловой системе {#filesystem_restore}

Команда, приведённая ниже, создаст директории и таблицы из резервной копии, сохранённой в директории `my_backup_of_basic_example` и загрузит в них данные.
```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools restore  -p $YDB_DB_PATH/backup/restored -i my_backup_of_basic_example/
```

### Проверка резервной копии

Команда `{{ ydb-cli }} tools restore`, запущенная с опцией `--dry-run`, позволяет проверить, что все таблицы в базе содержатся в резервной копии и что структуры таблиц одинаковы.

Команда, приведённая ниже, проверит, что все таблицы, сохранённые в `my_backup_of_basic_example`, существуют в базе  `$YDB_DB_PATH` и их структура (состав и порядок столбцов, типы данных столбцов, состав первичного ключа) одинаковы.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools restore  -p $YDB_DB_PATH/restored_basic_example -i my_backup_of_basic_example/ --dry-run
```
