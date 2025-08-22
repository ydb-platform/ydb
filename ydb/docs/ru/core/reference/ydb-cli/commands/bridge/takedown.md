# admin cluster bridge takedown

С помощью команды `admin cluster bridge takedown` можно выполнить [плановое отключение](../../../../concepts/bridge.md#takedown) pile. Если отключается текущий `PRIMARY`, необходимо указать новый `PRIMARY`.

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
|| `--pile <pile>` | Имя pile, который нужно аккуратно остановить. ||
|| `--new-primary <pile>` | Имя pile, который должен стать новым `PRIMARY`, если отключается текущий `PRIMARY`. ||
|#

## Требования {#requirements}

- Если отключается текущий `PRIMARY`, обязательно укажите `--new-primary` и выберите pile в состоянии `SYNCHRONIZED`.

## Примеры {#examples}

Вывод `SYNCHRONIZED` pile `pile-b` из кластера:

```bash
ydb admin cluster bridge takedown --pile pile-b
```

Вывод `PRIMARY` pile `pile-a` из кластера с переключением pile `pile-b` из состояния `SYNCHRONIZED` в состояние `PRIMARY`:

```bash
ydb admin cluster bridge takedown --pile pile-a --new-primary pile-b
```

## Проверка результата {#verify}

Проверьте итоговые состояния pile с помощью команды [list](list.md):

```bash
ydb admin cluster bridge list

pile-a: PRIMARY
pile-b: DISCONNECTED
```

Если отключался текущий `PRIMARY` с указанием `--new-primary`, убедитесь, что выбранный pile стал `PRIMARY`:

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
