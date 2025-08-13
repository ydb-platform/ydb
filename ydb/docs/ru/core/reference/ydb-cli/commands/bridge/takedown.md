# admin cluster bridge takedown

С помощью команды `admin cluster bridge takedown` вы можете выполнить [плановое отключение](../../../../concepts/bridge.md#takedown) пайла. Если выводится текущий PRIMARY, можно указать новый PRIMARY.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge takedown [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите справку по команде:

```bash
ydb admin cluster bridge takedown --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя пайла, который нужно аккуратно остановить. ||
|| `--new-primary <pile>` | Имя пайла, который должен стать новым PRIMARY (если выводимый пайл — текущий PRIMARY). ||
|#

## Требования {#requirements}

- Если выводится текущий `PRIMARY`, обязательно укажите `--new-primary` и выберите пайл в состоянии `SYNCHRONIZED`.

## Примеры {#examples}

Вывести SYNCHRONIZED пайл из кластера:

```bash
ydb admin cluster bridge takedown --pile pile-b
```

Вывести PRIMARY пайл из кластера, переключив PRIMARY на SYNCHRONIZED пайл:

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

Если отключали текущий PRIMARY с указанием `--new-primary`, проверьте, что выбранный пайл стал PRIMARY:

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
