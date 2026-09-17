# pdisk populate

С помощью подкоманды `pdisk populate` можно переместить выбранный набор [VDisk'ов](../../concepts/glossary.md#vdisk) на один [PDisk](../../concepts/glossary.md#pdisk). Команда позволяет сохранить список активных VDisk'ов исходного PDisk в файл-snapshot, а затем использовать этот файл для заполнения целевого PDisk.

{% note warning %}

Используйте `pdisk populate` для контролируемого тестирования устройств: команда позволяет разместить на новом устройстве точно такой же набор VDisk'ов и сравнить, как старое и новое устройства справляются с этой нагрузкой.

{% endnote %}

[Blob Storage Controller](../../concepts/glossary.md#ds-controller) проверяет все выбранные VDisk'и и планирует их перемещение в рамках одной атомарной транзакции конфигурации. Если хотя бы один VDisk невозможно переместить, конфигурация не изменяется. Сам перенос данных продолжается асинхронно после применения транзакции.

Общий вид команды:

```bash
ydb-dstool [global options ...] pdisk populate [populate options ...]
```

* `global options` — [глобальные параметры](global-options.md).
* `populate options` — [параметры подкоманды](#options).

Посмотрите описание команды:

```bash
ydb-dstool pdisk populate --help
```

## Параметры подкоманды {#options}

| Параметр | Описание |
| --- | --- |
| `--snapshot-from-pdisk <NodeId:PDiskId>` | Режим создания snapshot. Собрать активные VDisk'и с указанного PDisk. VDisk'и в режиме донора пропускаются. |
| `-d`, `--destination-pdisk <NodeId:PDiskId>` | Режим заполнения. Переместить на указанный PDisk VDisk'и, перечисленные в файле `--snapshot-file`. |
| `--snapshot-file <PATH>` | В режиме создания snapshot записать список VDisk'ов в формате JSON в указанный файл. В режиме заполнения прочитать список VDisk'ов из этого файла. Параметр обязателен в режиме заполнения. |
| `--suppress-donor-mode` | Не оставлять прежние расположения VDisk'ов в режиме донора после перемещения. Параметр доступен только в режиме заполнения. |
| `--format <FORMAT>` | Формат вывода: `pretty` (по умолчанию) или `json`. |

Необходимо указать ровно один из параметров: `--snapshot-from-pdisk` или `--destination-pdisk`.

## Пример {#example}

Сначала сохраните список активных VDisk'ов на PDisk `[1:1000]` в файл-snapshot:

```bash
ydb-dstool -e node-1.example.com pdisk populate \
  --snapshot-from-pdisk '[1:1000]' \
  --snapshot-file /tmp/pdisk-1-1000.json
```

Файл-snapshot имеет формат:

```json
{
  "pdisk_id": "[1:1000]",
  "vdisk_ids": [
    "[80000001:_:0:0:0]",
    "[80000002:_:0:1:0]"
  ]
}
```

Проверьте содержимое snapshot, а затем переместите перечисленные VDisk'и на PDisk `[2:1000]`:

```bash
ydb-dstool -e node-1.example.com pdisk populate \
  --destination-pdisk '[2:1000]' \
  --snapshot-file /tmp/pdisk-1-1000.json
```

Команда пропускает VDisk'и с `GroupId=0`, потому что их нельзя перемещать. Если snapshot содержит только такие VDisk'и, команда завершается с ошибкой и не изменяет конфигурацию.

