# Команды управления кластером в режиме bridge

Команды управления кластером в режиме [bridge](../../../../concepts/bridge.md) позволяют просматривать состояние [пайлов](../../../../concepts/glossary.md#pile), выполнять плановую и аварийную смену PRIMARY, временно выводить пайл на обслуживание и возвращать его в кластер.

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
[admin cluster bridge list](./list.md) | Вывод состояния пайлов
[admin cluster bridge switchover](./switchover.md) | Плановая смена `PRIMARY`
[admin cluster bridge failover](./failover.md) | Аварийное переключение
[admin cluster bridge takedown](./takedown.md) | Вывод пайла из кластера
[admin cluster bridge rejoin](./rejoin.md) | Возвращение пайла в кластер
