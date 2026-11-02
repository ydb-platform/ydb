# Команды управления кластером в режиме bridge

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Команды управления кластером в режиме [bridge](../../../../concepts/bridge.md) позволяют преобразовывать кластер, просматривать состояние [pile](../../../../concepts/glossary.md#pile), добавлять и удалять pile, выполнять плановую и аварийную смену PRIMARY, временно выводить pile на обслуживание и возвращать его в кластер.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий синтаксис вызова команд управления кластером в режиме bridge:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge [command options...] <subcommand>
```

где:

- `{{ ydb-cli }}` — команда запуска {{ ydb-short-name }} CLI из командной строки операционной системы;
- `[global options]` — глобальные параметры, одинаковые для всех команд {{ ydb-short-name }} CLI;
- `admin cluster bridge` — команда управления конфигурацией кластера;
- `[command options]` — параметры команды, специфичные для каждой команды и подкоманды;
- `<subcommand>` — подкоманда.

## Команды {#list}

Ниже представлен список доступных подкоманд для управления кластером в режиме bridge. Любую команду можно вызвать с опцией `--help` для получения справки по ней.

Команда / подкоманда | Краткое описание
--- | ---
[admin cluster bridge convert](./convert.md) | Преобразование обычного кластера в bridge с одним pile
[admin cluster bridge add-pile](./add-pile.md) | Выделение хранилища для неактивного pile
[admin cluster bridge list](./list.md) | Вывод состояния pile и ожидающих операций
[admin cluster bridge switchover](./switchover.md) | Плановая смена `PRIMARY`
[admin cluster bridge failover](./failover.md) | Аварийное переключение
[admin cluster bridge takedown](./takedown.md) | Вывод pile из кластера
[admin cluster bridge rejoin](./rejoin.md) | Возвращение pile в кластер
[admin cluster bridge remove-pile](./remove-pile.md) | Удаление отключённого pile и его данных
