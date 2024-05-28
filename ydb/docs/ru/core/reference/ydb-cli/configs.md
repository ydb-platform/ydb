## Работа с конфигурацией

### Общие флаги для команд

* `-f, --filename <filename.yaml>` — считать input из файла, `-` для STDIN. Для команд принимающих n файлов (прим. resolve) можно указать несколько раз, тип файла будет определён по полю metadata
* `--output-directory <dir>` — сдампить/порезолвить файлы в директорию
* `--strip-metadata` — выкинуть поле metadata из вывода
* `--all` — расширяет вывод команд до всей конфигурации (см. продвинутое конфигурирование)
* `--allow-unknown-fields` — позволяет игнорировать неизвестные поля в конфигурации


```bash
# Применить конфигурацию dynconfig.yaml на кластер
{{ ydb-cli }} admin config replace -f dynconfig.yaml
# Проверить возможно ли применить конфигурацию dynconfig.yaml на кластер (проверить все валидаторы, совпадение версий и кластера)
{{ ydb-cli }} admin config replace -f dynconfig.yaml --dry-run
# Применить конфигурацию dynconfig.yaml на кластер игнорирую проверку версий и кластера (версия и кластер всё равно будут перезаписаны на корректные)
{{ ydb-cli }} admin config replace -f dynconfig.yaml --force
# Получить основную конфигурацию кластера
{{ ydb-cli }} admin config fetch
# Получить все текущие конфигурационные файлы кластера
{{ ydb-cli }} admin config fetch --all
# Сгенерировать все возможные конечные конфигурации для dynconfig.yaml
{{ ydb-cli }} admin config resolve --all -f dynconfig.yaml
# Сгенерировать конечную конфигурацию для dynconfig.yaml при лейблах tenant=/Root/test и canary=true
{{ ydb-cli }} admin config resolve -f dynconfig.yaml --label tenant=/Root/test --label canary=true
# Сгенерировать конечную конфигурацию для dynconfig.yaml для лейблов с узла 1003
{{ ydb-cli }} admin config resolve -f dynconfig.yaml --node-id 1003
# Получить все временные конфигурации кластера
{{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
# Получить временную конфигурацию с id 1 с кластера
{{ ydb-cli }} admin volatile-config fetch --id 1
# Применить временную конфигурацию volatile.yaml на кластер
{{ ydb-cli }} admin volatile-config add -f volatile.yaml
# Удалить временные конфигурации с id 1 и 3 на кластере
{{ ydb-cli }} admin volatile-config drop --id 1 --id 3
# Удалить все временные конфигурации на кластере
{{ ydb-cli }} admin volatile-config drop --all
```

## Сценарии

### Обновить основную конфигурацию кластера
 ```bash
# Получить конфигурацию кластера
{{ ydb-cli }} admin config fetch > dynconfig.yaml
# Отредактировать конфигурацию вашим любимым редактором
vim dynconfig.yaml
# Применить конфигурацию dynconfig.yaml на кластер
{{ ydb-cli }} admin config replace -f dynconfig.yaml
```
аналогично в одну строчку:
```bash
{{ ydb-cli }} admin config fetch | yq '.config.actor_system_config.scheduler.resolution = 128' | {{ ydb-cli }} admin config replace -f -
```
вывод команды:
```
OK
```
### Посмотреть конфигурацию для определённого набора лейблов
```bash
{{ ydb-cli }} admin config resolve --remote --label tenant=/Root/db1 --label canary=true
```
вывод команды:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```

### Посмотреть конфигурацию для определённого узла
```bash
{{ ydb-cli }} admin config resolve --remote --node-id <node_id>
```
вывод команды:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```

### Сохранить все конфигурации локально
```bash
{{ ydb-cli }} admin config fetch --all --output-directory <configs_dir>
ls <configs_dir>
```
вывод команды:
```
dynconfig.yaml volatile_1.yaml volatile_3.yaml
```

### Посмотреть все конфигурации локально
```bash
{{ ydb-cli }} admin config fetch --all
```
вывод команды:
```yaml
---
metadata:
  kind: main
  cluster: unknown
  version: 1
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
allowed_labels: {}
selector_config: []
---
metadata:
  kind: volatile
  cluster: unknown
  version: 1
  id: 1
# some comment example
selectors:
- description: test
  selector:
    tenant: /Root/db1
  config:
    actor_system_config: !inherit
      use_auto_config: true
      cpu_count: 12
```

### Посмотреть конечную конфигурацию для определённого узла из сохраненной локально исходной конфигурации
```bash
{{ ydb-cli }} admin config resolve -k <configs_dir> --node-id <node_id>
```
вывод команды:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```
