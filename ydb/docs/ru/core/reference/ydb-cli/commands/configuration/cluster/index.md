# Команды управления конфигурацией кластера

Команды управления конфигурацией кластера предназначены для работы с конфигурацией на уровне всего кластера {{ ydb-short-name }}. Эти команды позволяют администраторам просматривать, изменять и управлять [настройками](../../../../../reference/configuration/index.md), которые применяются ко всем узлам кластера.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий синтаксис вызова команд управления конфигурацией кластера:

```bash
ydb [global options] admin cluster config [command options] <subcommand>
```

где:

- `ydb` — команда запуска YDB CLI из командной строки операционной системы;
- `[global options]` — глобальные опции, одинаковые для всех команд YDB CLI;
- `admin cluster config` — команда управления конфигурацией кластера;
- `[command options]` — опции команды, специфичные для каждой команды и подкоманды;
- `<subcommand>` — подкоманда.

## Команды {#list}

Ниже представлен список доступных подкоманд для управления конфигурацией кластера. Любую команду можно вызвать с опцией `--help` для получения справки по ней.

Команда / подкоманда | Краткое описание
--- | ---
[admin cluster config fetch](./fetch.md) | Получение текущей конфигурации (псевдонимы: `get`, `dump`)
[admin cluster config replace](./replace.md) | Замена текущей конфигурации
[admin cluster config generate](./generate.md) | Генерация [конфигурации V2](../../../../../devops/configuration-management/configuration-v2/index.md) из [конфигурации V1](../../../../../devops/configuration-management/configuration-v1/index.md)
admin cluster config vesion | Отображение версии конфигурации узлов (V1/V2)
