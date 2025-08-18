# admin cluster bridge takedown

С помощью команды `admin cluster bridge takedown` можно выполнить [плановое отключение](../../../../concepts/bridge.md#takedown) пайла. Если отключается текущий `PRIMARY`, можно указать новый `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge takedown [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
ydb admin cluster bridge takedown --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя пайла, который нужно аккуратно остановить. ||
|| `--new-primary <pile>` | Имя пайла, который должен стать новым `PRIMARY`, если отключается текущий `PRIMARY`. ||
|#

## Требования {#requirements}

- Если отключается текущий `PRIMARY`, обязательно укажите `--new-primary` и выберите пайл в состоянии `SYNCHRONIZED`.

## Примеры {#examples}

Вывод `SYNCHRONIZED` пайла из кластера:

```bash
ydb admin cluster bridge takedown --pile pile-b
```

Вывод `PRIMARY` пайла из кластера с переключением `PRIMARY` на `SYNCHRONIZED` пайл:

```bash
ydb admin cluster bridge takedown --pile pile-a --new-primary pile-b
```

## Проверка результата {#verify}

Проверьте итоговые состояния пайлов:

```bash
ydb admin cluster bridge list

pile-a: PRIMARY
pile-b: DISCONNECTED
```

Если отключался текущий `PRIMARY` с указанием `--new-primary`, убедитесь, что выбранный пайл стал `PRIMARY`:

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
