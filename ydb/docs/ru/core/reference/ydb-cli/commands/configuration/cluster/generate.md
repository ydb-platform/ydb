# admin cluster config generate

С помощью команды `admin cluster config generate` вы можете сгенерировать динамическую конфигурацию на основе статической конфигурации на кластере {{ ydb-short-name }}.

Общий вид команды:

```bash
ydb [global options...] admin cluster config generate
```

* `global options` — глобальные параметры.

Посмотрите описание команды генерации динамической конфигурации:

```bash
ydb admin cluster config generate --help
```

## Примеры {#examples}

Сгенерируйте динамическую конфигурацию на основе статической конфигурации:

```bash
ydb admin cluster config generate > config.yaml
```

После выполнения данный комманды в файле `config.yaml` будет содержаться YAML документ следующего вида:

```yaml
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  <статическая конфигурация кластера>
```

## Использование сгенерированной динамической конфигурации

После генерации динамической конфигурации вы можете:

1. Добавить параметры динамической конфигурации в конфигурационный файл
2. Применить динамическую конфигурацию к кластеру с помощью команды [`admin cluster config replace`](replace.md)
