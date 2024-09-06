# device list

С помощью подкоманды `device list` вы можете вывести список устройств хранения, доступных на кластере {{ ydb-short-name }}.

Общий вид команды:

```bash
ydb-dstool [global options ...] device list [list options ...]
```

* `global options` — [глобальные параметры](global-options.md).
* `list options` — [параметры подкоманды](#options).

Посмотрите описание команды вывода списка устройств:

```bash
ydb-dstool device list --help
```

## Параметры подкоманды {#options}

Параметр | Описание
---|---
`-H`, `--human-readable` | Вывести данные в человекочитаемом формате.
`--sort-by` | Колонка сортировки.<br/>Одно из значений: `SerialNumber`, `FQDN`, `Path`, `Type`, `StorageStatus`, `NodeId:PDiskId`.
`--reverse` | Использовать обратный порядок сортировки.
`--format` | Формат вывода.<br/>Одно из значений: `pretty`, `json`, `tsv`, `csv`.
`--no-header` | Не выводить строку с именами колонок.
`--columns` | Список колонок, которые нужно вывести.<br/>Одно или комбинация значений: `SerialNumber`, `FQDN`, `Path`, `Type`, `StorageStatus`, `NodeId:PDiskId`.
`-A`, `--all-columns` | Вывести все колонки.

## Примеры {#examples}

Следующая команда выведет список устройств, доступных на кластере:

```bash
ydb-dstool -e node-5.example.com device list
```

Результат:

```text
┌────────────────────┬────────────────────┬────────────────────────────────┬──────┬───────────────────────────┬────────────────┐
│ SerialNumber       │ FQDN               │ Path                           │ Type │ StorageStatus             │ NodeId:PDiskId │
├────────────────────┼────────────────────┼────────────────────────────────┼──────┼───────────────────────────┼────────────────┤
│ PHLN123301H41P2BGN │ node-1.example.com │ /dev/disk/by-partlabel/nvme_04 │ NVME │ FREE                      │ NULL           │
│ PHLN123301A62P2BGN │ node-6.example.com │ /dev/disk/by-partlabel/nvme_03 │ NVME │ PDISK_ADDED_BY_DEFINE_BOX │ [6:1001]       │
...
```
