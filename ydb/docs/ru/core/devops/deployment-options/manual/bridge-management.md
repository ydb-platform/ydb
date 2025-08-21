## Управление кластером bridge

Ниже приведены типовые операции для кластера в режиме bridge и соответствующие команды {{ ydb-short-name }} CLI. Подробности смотрите в концепции [{#T}](../../../concepts/bridge.md) и в справке по командам bridge [{#T}](../../../reference/ydb-cli/commands/bridge/index.md).

### Посмотреть текущее состояние {#list}

Показывает текущее состояние каждого pile.

```bash
ydb admin cluster bridge list
```

Пример вывода:

```bash
pile-a: PRIMARY
pile-b: SYNCHRONIZED
```

### Плановая смена PRIMARY (switchover) {#switchover}

Используйте для планового переключения роли PRIMARY на другой pile, когда он находится в состоянии `SYNCHRONIZED`. Переключение выполняется плавно: роли проходят через `DEMOTED/PROMOTED` и завершаются как `SYNCHRONIZED/PRIMARY`.

```bash
ydb admin cluster bridge switchover --new-primary <pile>
```

### Плановое отключение pile (takedown) {#takedown}

Используйте для вывода pile на обслуживание. Если отключается текущий `PRIMARY`, укажите новый `PRIMARY`, который должен быть в состоянии `SYNCHRONIZED`. При выполнении операции pile переводится в `SUSPENDED`, затем — в `DISCONNECTED`; дальнейшие операции выполняются без участия отключённого pile.

```bash
ydb admin cluster bridge takedown --pile <pile>
# если отключаете текущий PRIMARY:
ydb admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

### Аварийное отключение недоступного pile (failover) {#failover}

Используйте, когда pile недоступен и требуется продолжить работу кластера. Если недоступен текущий `PRIMARY`, укажите параметр `--new-primary` и выберите pile в состоянии `SYNCHRONIZED`. Недоступный pile будет переведён в `DISCONNECTED`, а при указании нового `PRIMARY` роль переключится.

```bash
ydb admin cluster bridge failover --pile <unavailable-pile>
# если недоступен текущий PRIMARY:
ydb admin cluster bridge failover --pile <unavailable-primary> --new-primary <synchronized-pile>
```

### Вернуть pile в кластер (rejoin) {#rejoin}

Возвращает ранее отключённый pile после обслуживания или восстановления. Сразу после запуска операции pile переходит в `NOT_SYNCHRONIZED`, запускается синхронизация данных; по завершении pile автоматически становится `SYNCHRONIZED`.

```bash
ydb admin cluster bridge rejoin --pile <pile>
```
