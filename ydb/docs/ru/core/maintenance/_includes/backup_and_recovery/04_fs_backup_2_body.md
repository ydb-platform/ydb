### Пример листинга содержимого директории с резервной копией

```
tree my_backup_of_basic_example/
my_backup_of_basic_example/
├── episodes
│   ├── data_00.csv
│   └── scheme.pb
├── seasons
│   ├── data_00.csv
│   └── scheme.pb
└── series
    ├── data_00.csv
    └── scheme.pb

3 directories, 6 files
```

Структура каждой таблицы сохранена в файле с именем `scheme.pb`, например в `episodes/scheme.pb` сохранена схема таблицы `episodes`. Данные каждой таблицы сохранены в одном или нескольких файлах с именами вида `data_хх.csv`, где хх – порядковый номер файла. Имя первого файла – `data_00.csv`. Ограничение на размер одного файла – 100 Мб.

### Сохранение схемы таблиц

Команда `{{ ydb-cli }} tools dump`, запущенная с опцией `--scheme-only`, сохранит только схемы таблиц. Команда, приведённая ниже, сохранит все директории и файлы со структурой таблиц из директории `examples` в базе `$YDB_DB_PATH` в папку `my_backup_of_basic_example`. Файлы с данными таблиц созданы не будут.
```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools dump -p $YDB_DB_PATH/examples -o my_backup_of_basic_example/ --scheme-only
```
