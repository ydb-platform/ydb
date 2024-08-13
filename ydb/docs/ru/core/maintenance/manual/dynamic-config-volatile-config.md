# Временные конфигурации

Временные конфигурации — это специальный вид конфигураций, которые дополняют динамическую и при этом не являются персистентными. Т.е. данные конфигурации сбрасываются при переезде или рестарте [таблетки](../../concepts/glossary.md#tablet) Console, а так же при обновлении основной конфигурации.

Основные сценарии использования:
- временное изменение конфигурации для отладки или тестирования;
- пробное включение потенциально опасных настроек. В случае падения или рестарта кластера данные настройки будут автоматически отключены.

Данные конфигурации добавляются в конец набора селекторов, синтаксис описания идентичен [синтаксису селекторов](./dynamic-config-selectors.md).

```bash
# Получить все временные конфигурации загруженные на кластер
{{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
# Получить временную конфигурацию с id=1
{{ ydb-cli }} admin volatile-config fetch --id 1
# Применить временную конфигурацию volatile.yaml на кластер
{{ ydb-cli }} admin volatile-config add -f volatile.yaml
# Удалить временные конфигурации с id=1 и id=3 на кластере
{{ ydb-cli }} admin volatile-config drop --id 1 --id 3
# Удалить все временные конфигурации на кластере
{{ ydb-cli }} admin volatile-config drop --all
```

## Пример работы с временной конфигурацией

Временное включение настроек журналирования компонента `blobstorage` в `DEBUG` на узле `host1.example.com`:
```bash
# Запрос текущих метаданных, чтобы сформировать корректный заголовок временной конфигурации
$ {{ ydb-cli }} admin config fetch --all
---
kind: MainConfig
cluster: "example-cluster-name"
version: 2
config:
  # ...
---
kind: VolatileConfig
cluster: "example-cluster-name"
version: 2
id: 1
selector_config:
  # ...
# Загрузка конфигурации с версией 2, именем кластера example-cluster-name и идентификатором 2
$ {{ ydb-cli }} admin volatile-config add -f - <<<EOF
metadata:
  kind: VolatileConfig
  cluster: "example-cluster-name"
  version: 2
  id: 2
selector_config:
- description: Set blobstorage logging level to DEBUG
  selector:
    node_host: host1.example.com
  config:
    log_config: !inherit
      entry: !inherit_key:component
      - component: BLOBSTORAGE
        level: 8
EOF
# ...
# анализ журнала
# ...
# Удаление конфигурации
$ {{ ydb-cli }} admin volatile-config drop --id 2
```
