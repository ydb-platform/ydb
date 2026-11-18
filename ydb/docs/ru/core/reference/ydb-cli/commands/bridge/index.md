# Команды управления кластером в режиме bridge

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Команды управления кластером в режиме [bridge](../../../../concepts/bridge.md) позволяют преобразовывать обычный кластер, просматривать состояние [pile](../../../../concepts/glossary.md#pile), добавлять и удалять pile, переключать режим хранения баз данных, а также выполнять плановые и аварийные операции с pile.

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
[admin cluster bridge list](./list.md) | Вывод состояния pile и хода операций
[admin cluster bridge provision-pile](./provision-pile.md) | Подготовка хранилища нового pile
[admin cluster bridge join-pile (rejoin)](./rejoin.md) | Подключение подготовленного pile или возврат после обслуживания
[admin cluster bridge switchover](./switchover.md) | Плановая смена `PRIMARY`
[admin cluster bridge failover](./failover.md) | Аварийное переключение
[admin cluster bridge takedown](./takedown.md) | Вывод pile из кластера
[admin cluster bridge destroy-pile](./destroy-pile.md) | Удаление хранилища и данных отключённого pile
[admin cluster bridge set-database-mode](./set-database-mode.md) | Переключение базы данных между режимом bridge и обычным хранилищем
