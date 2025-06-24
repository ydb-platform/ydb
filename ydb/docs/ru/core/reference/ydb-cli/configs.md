## Работа с конфигурацией

{% note info %}

До версии YDB CLI 2.20.0 команды `{{ ydb-cli }} admin cluster config` имели формат `{{ ydb-cli }} admin config`.

{% endnote %}

В этом разделе приведены команды для работы с [конфигурацией кластера](../../reference/configuration/index.md) {{ ydb-short-name }}.

- Применение конфигурации `dynconfig.yaml` на кластер:

    ```bash
    {{ ydb-cli }} admin cluster config replace -f dynconfig.yaml
    ```

- Проверка возможности применения конфигурации `dynconfig.yaml` на кластер (проверить все валидаторы, версия конфигурации в yaml-файле должна быть выше на 1, чем версия конфигурации кластера, имя кластера должно совпадать):

    ```bash
    {{ ydb-cli }} admin cluster config replace -f dynconfig.yaml --dry-run
    ```

- Применение конфигурации `dynconfig.yaml` на кластер игнорируя проверку версий и кластера (версия и кластер всё равно будут перезаписаны на корректные):

    ```bash
    {{ ydb-cli }} admin cluster config replace -f dynconfig.yaml --force
    ```

- Получение основной конфигурации кластера:

    ```bash
    {{ ydb-cli }} admin cluster config fetch
    ```

- Генерация всех возможных конечных конфигураций для `dynconfig.yaml`:

    ```bash
    {{ ydb-cli }} admin cluster config resolve --all -f dynconfig.yaml
    ```

- Генерация конечной конфигурации для `dynconfig.yaml` при лейблах `tenant=/Root/test` и `canary=true`:

    ```bash
    {{ ydb-cli }} admin cluster config resolve -f dynconfig.yaml --label tenant=/Root/test --label canary=true
    ```

- Генерация конечной конфигурации для `dynconfig.yaml` для лейблов с узла 1003:

    ```bash
    {{ ydb-cli }} admin cluster config resolve -f dynconfig.yaml --node-id 100
    ```

- Генерация файла динамической конфигурации на основе статической конфигурации на кластере:

    ```bash
    {{ ydb-cli }} admin cluster config genereate
    ```

- Инициализация директории с конфигурацией, используя путь до конфигурационного файла:

    ```bash
    {{ ydb-cli }} admin node config init --config-dir <путь до директории> --from-config <путь до файла конфигурации>
    ```

- Инициализация директории с конфигурацией, используя конфигурацию на кластере:

    ```bash
    {{ ydb-cli }} admin node config init --config-dir <путь до директории> --seed-node <эндпоинт узла кластера>
    ```

## Работа с временной конфигурацией

В этом разделе перечислены команды, которые используются для работы с [временной конфигурацией](../../devops/configuration-management/configuration-v1/dynamic-config-volatile-config.md).

- Получение всех временных конфигураций кластера:

    ```bash
    {{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
    ```

- Получение временной конфигурации с id 1 с кластера:

    ```bash
    {{ ydb-cli }} admin volatile-config fetch --id 1
    ```

- Применение временной конфигурации `volatile.yaml` на кластер:

    ```bash
    {{ ydb-cli }} admin volatile-config add -f volatile.yaml
    ```

- Удаление временной конфигурации с id 1 и 3 на кластере:

    ```bash
    {{ ydb-cli }} admin volatile-config drop --id 1 --id 3
    ```

- Удаление всех временных конфигурации на кластере:

    ```bash
    {{ ydb-cli }} admin volatile-config drop --all
    ```

## Параметры

* `-f, --filename <filename.yaml>` — считать input из файла, `-` для STDIN. Для команд принимающих n файлов (прим. resolve) можно указать несколько раз, тип файла будет определён по полю metadata
* `--output-directory <dir>` — сдампить/порезолвить файлы в директорию
* `--strip-metadata` — выкинуть поле metadata из вывода
* `--all` — расширяет вывод команд до всей конфигурации (см. продвинутое конфигурирование)
* `--allow-unknown-fields` — позволяет игнорировать неизвестные поля в конфигурации

## Сценарии

### Обновить основную конфигурацию кластера

```bash
# Получить конфигурацию кластера
{{ ydb-cli }} admin cluster config fetch > dynconfig.yaml
# Отредактировать конфигурацию вашим любимым редактором
vim dynconfig.yaml
# Применить конфигурацию dynconfig.yaml на кластер
{{ ydb-cli }} admin cluster config replace -f dynconfig.yaml
```

аналогично в одну строчку:

```bash
{{ ydb-cli }} admin cluster config fetch | yq '.config.actor_system_config.scheduler.resolution = 128' | {{ ydb-cli }} admin cluster config replace -f -
```

вывод команды:

```text
OK
```

### Посмотреть конфигурацию для определённого набора лейблов

```bash
{{ ydb-cli }} admin cluster config resolve --remote --label tenant=/Root/db1 --label canary=true
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
{{ ydb-cli }} admin cluster config resolve --remote --node-id <node_id>
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
{{ ydb-cli }} admin cluster config fetch --all --output-directory <configs_dir>
ls <configs_dir>
```

вывод команды:

```text
dynconfig.yaml volatile_1.yaml volatile_3.yaml
```

### Посмотреть все конфигурации локально

```bash
{{ ydb-cli }} admin cluster config fetch --all
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
{{ ydb-cli }} admin cluster config resolve -k <configs_dir> --node-id <node_id>
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

