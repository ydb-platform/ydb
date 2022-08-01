# Конфигурация кластера

Конфигурация кластера задается в YAML-файле, передаваемом в параметре `--yaml-config` при запуске узлов кластера.

В статье приведено описание основных групп конфигурируемых параметров в данном файле.

## host_configs - Типовые конфигурации хостов {#host-configs}

Кластер YDB состоит из множества узлов, для развертывания которых обычно используется одна или несколько типовых конфигураций серверов. Для того чтобы не повторять её описание для каждого узла, в файле конфигурации существует раздел `host_configs`, в котором перечислены используемые конфигурации, и им присвоены идентификаторы.

**Синтаксис**

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: <path_to_device>
    type: <type>
  - path: ...
- host_config_id: 2
  ...  
```

Атрибут `host_config_id` задает числовой идентификатор конфигурации. В атрибуте `drive` содержится коллекция описаний подключенных дисков. Каждое описание состоит из двух атрибутов:

- `path` : Путь к смонтированному блочному устройству, например `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type` : Тип физического носителя устройства: `ssd`, `nvme` или `rot` (rotational - HDD)

**Примеры**

Одна конфигурация с идентификатором 1, с одним диском типа SSD, доступным по пути `/dev/disk/by-partlabel/ydb_disk_ssd_01`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
```

Две конфигурации с идентификаторами 1 (два SSD диска) и 2 (три SSD диска):

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
- host_config_id: 2
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
```

### Особенности Kubernetes {#host-configs-k8s}

YDB Kubernetes operator монтирует NBS диски для Storage узлов на путь `/dev/kikimr_ssd_00`. Для их использования должна быть указана следующая конфигурация `host_configs`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

Файлы с примерами конфигурации, поставляемые в составе YDB Kubernetes operator, уже содержат такую секцию, и её не нужно менять.

## hosts - Статические узлы кластера {#hosts}

В данной группе перечисляются статические узлы кластера, на которых запускаются процессы работы со Storage, и задаются их основные характеристики:

- Числовой индентификатор узла
- DNS-имя хоста и порт, по которым может быть установлено соединение с узлом в IP network
- Идентификатор [типовой конфигурации хоста](#host-configs)
- Размещение в определенной зоне доступности, стойке
- Инвентарный номер сервера (опционально)

**Синтаксис**

``` yaml
hosts:
- host: <DNS-имя хоста>
  host_config_id: <числовой идентификатор типовой конфигурации хоста>
  port: <порт> # 19001 по умолчанию
  location:
    unit: <строка с инвентарным номером сервера>
    data_center: <строка с идентификатором зоны доступности>
    rack: <строка с идентификатором стойки>
- host: <DNS-имя хоста>
  ...
```

**Примеры**

``` yaml
hosts:
- host: hostname1
  host_config_id: 1
  node_id: 1
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
- host: hostname2
  host_config_id: 1
  node_id: 2
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
```

### Особенности Kubernetes {#hosts-k8s}

При развертывании YDB с помощью оператора Kubernetes секция `hosts` полностью генерируется автоматически, заменяя любой указанный пользователем контент в передаваемой оператору конфигурации. Все Storage узлы используют `host_config_id` = `1`, для которого должна быть задана [корректная конфигурация](#host-configs-k8s).

## domains_config - Домен кластера {#domains-config}

Данный раздел содержит конфигурацию корневого домена кластера YDB, включая [конфигурации Blob Storage](#domains-blob) (хранилища бинарных объектов), [State Storage](#domains-state) (хранилища состояний) и [аутентификации](#auth).

**Синтаксис**

``` yaml
domains_config:
  domain:
  - name: <имя корневого домена>
    storage_pool_types: <конфигурация Blob Storage>
  state_storage: <конфигурация State Storage>
  security_config: <конфигурация аутентификации>
```

### Конфигурация Blob Storage {#domains-blob}

Данный раздел определяет один или более типов пулов хранения, доступных в кластере для данных в базах данных, со следующими возможностями конфигурации:

- Имя пула хранения
- Свойства устройств (например, тип дисков)
- Шифрование данных (вкл/выкл)
- Режим отказоустойчивости

Доступны следующие [режимы отказоустойчивости](../../cluster/topology.md):

Режим | Описание
--- | ---
`none` | Избыточность отсутствует. Применяется для тестирования.
`block-4-2` | Избыточность с коэффициентом 1,5, применяется для однодатацентровых кластеров.
`mirror-3-dc` | Избыточность с коэффициентом 3, применяется для мультидатацентровых кластеров.
`mirror-3dc-3-nodes` | Избыточность с коэффициентом 3. Применяется для тестирования.

**Синтаксис**

``` yaml
  storage_pool_types:
  - kind: <имя пула хранения>
    pool_config:
      box_id: 1
      encryption: <опциональный, укажите 1 для шифрования данных на диске>
      erasure_species: <имя режима отказоустойчивости - none, block-4-2, or mirror-3-dc>
      kind: <имя пула хранения - укажите то же значение, что выше>
      pdisk_filter:
      - property:
        - type: <тип устройства для сопоставления с указанным в host_configs.drive.type>
      vdisk_kind: Default
  - kind: <имя пула хранения>
  ...
```

Каждой базе данных в кластере назначается как минимум один из доступных пулов хранения, выбираемый в операции создания базы данных. Имена пулов хранения среди назначенных могут быть использованы в атрибуте `DATA` при определении групп колонок в операторах YQL [`CREATE TABLE`](../../yql/reference/syntax/create_table.md#column-family)/[`ALTER TABLE`](../../yql/reference/syntax/alter_table.md#column-family).

### Конфигурация State Storage {#domains-state}

State Storage (хранилище состояний) -- это независимое хранилище в памяти для изменяемых данных, поддерживающее внутренние процессы YDB. Оно хранит реплики данных на множестве назначенных узлов.

Обычно State Storage не требует масштабирования в целях повышения производительности, поэтому количество узлов в нём следует делать как можно меньшим с учетом необходимого уровня отказоустойчивости.

Доступность State Storage является ключевой для кластера YDB, так как влияет на все базы данных, независимо от того какие пулы хранения в них используются.Для обеспечения отказоустойчивости State Storate его узлы должны быть выбраны таким образом, чтобы гарантировать рабочее большинство в случае ожидаемых отказов. 

Для выбора узлов State Storage можно воспользоваться следующими рекомендациями:

|Тип кластера|Мин количество<br>узлов|Рекомендации по выбору|
|---------|-----------------------|-----------------|
|Без отказоустойчивости|1|Выберите один произвольный узел.|
|В пределах одной зоны доступности|5|Выберите пять узлов в разных доменах отказа пула хранения block-4-2, для гарантии, что большинство в 3 работающих узла (из 5) остается при сбое двух доменов.|
|Геораспределенный|9|Выберите три узла в разных доменах отказа внутри каждой из трех зон доступности пула хранения mirror-3-dc для гарантии, что большинство в 5 работающих узлов (из 9) остается при сбое зоны доступности + домена отказа.|

При развертывании State Storage на кластерах, в которых прменяется несколько пулов хранения с возможным сочетанием режимов отказоустойчивости, рассмотрите возможность увеличения количества узлов с распространением их на разные пулы хранения, так как недоступность State Storage приводит к недоступности всего кластера.

**Syntax**
``` yaml
state_storage:
- ring:
    node: <массив узлов StateStorage>
    nto_select: <количество реплик данных в StateStorage>
  ssid: 1
```

Каждый клиент State Storage (например, таблетка DataShard) использует `nto_select` узлов для записи копий его данных в State Storage. Если State Storage состоит из большего количества узлов чем `nto_select`, то разные узлы могут быть использованы для разных клиентов, поэтому необходимо обеспечить, чтобы любое подмножество из `nto_select` узлов в пределах State Storage отвечало критериям отказоустойчивости.

Для `nto_select` должны использоваться нечетные числа, так как использование четных чисел не улучшает отказоустойчивость по сравнению с ближайшим меньшим нечетным числом.

### Конфигурация аутентификации {#auth}

[Режим аутентификации](../../concepts/auth.md) на кластере {{ ydb-short-name }} задается в разделе `domains_config.security_config`.

Синтаксис:

``` yaml
domains_config:
  ...
  security_config:
    enforce_user_token_requirement: Bool
  ...
```

Ключ | Описание
--- | ---
`enforce_user_token_requirement` | Требовать токен пользователя.</br>Допустимые значения:</br><ul><li>`false` — режим аутентификации анонимный, токен не требуется;</li><li>`true` — режим аутентификации по логину и паролю, для выполнения запроса требуется валидный токен пользователя.</li></ul>

### Примеры {#domains-examples}

{% list tabs %}

- Block-4-2

  ``` yaml
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: block-4-2
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8]
        nto_select: 5
      ssid: 1

- Mirror-3-dc

  ``` yaml
  domains_config:
    domain:
    - name: global
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
        nto_select: 9
      ssid: 1
  ```

- None

  ``` yaml
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: none
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node:
        - 1
        nto_select: 1
      ssid: 1
  ```

- Несколько пулов

  ``` yaml
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: '1'
          erasure_species: block-4-2
          kind: ssd
          pdisk_filter:
          - property:
            - {type: SSD}
          vdisk_kind: Default
      - kind: rot
        pool_config:
          box_id: '1'
          erasure_species: block-4-2
          kind: rot
          pdisk_filter:
          - property:
            - {type: ROT}
          vdisk_kind: Default
      - kind: rotencrypted
        pool_config:
          box_id: '1'
          encryption_mode: 1
          erasure_species: block-4-2
          kind: rotencrypted
          pdisk_filter:
          - property:
            - {type: ROT}
          vdisk_kind: Default
      - kind: ssdencrypted
        pool_config:
          box_id: '1'
          encryption_mode: 1
          erasure_species: block-4-2
          kind: ssdencrypted
          pdisk_filter:
          - property:
            - {type: SSD}
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 16, 31, 46, 61, 76, 91, 106]
        nto_select: 5
      ssid: 1
  ```

{% endlist %}

## actor_system_config - Акторная система {#actor-system}

Создайте конфигурацию акторной системы. Необходимо указать распределение ядер процессора по пулам из числа доступных ядер в системе.

```bash
actor_system_config:
  executor:
  - name: System
    spin_threshold: 0
    threads: 2
    type: BASIC
  - name: User
    spin_threshold: 0
    threads: 3
    type: BASIC
  - name: Batch
    spin_threshold: 0
    threads: 2
    type: BASIC
  - name: IO
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: IO
  - name: IC
    spin_threshold: 10
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: BASIC
  scheduler:
    progress_threshold: 10000
    resolution: 256
    spin_threshold: 0
```

{% note warning %}

Не рекомендуется суммарно назначать в пулы IC, Batch, System, User больше ядер, чем доступно в системе.

{% endnote %}

## blob_storage_config - Статическая группа кластера {#blob-storage-config}

Укажите конфигурацию статической группы кластера. Статическая группа необходима для работы базовых таблеток кластера, в том числе `Hive`, `SchemeShard`, `BlobstorageContoller`.
Обычно данные таблетки не хранят много информации, поэтому мы не рекомендуем создавать более одной статической группы.

Для статической группы необходимо указать информацию о дисках и нодах, на которых будет размещена статическая группа. Например, для модели `erasure: none` конфигурация может быть такой:

```bash
blob_storage_config:
  service_set:
    groups:
    - erasure_species: none
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
            pdisk_category: SSD
....
```

Для конфигурации расположенной в 3 AZ  необходимо указать 3 кольца. Для конфигураций, расположенных в одной AZ, указывается ровно одно кольцо.

## Примеры конфигураций кластеров {#examples}

В [репозитории](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/)  можно найти модельные примеры конфигураций кластеров для самостоятельного развертывания. Ознакомьтесь с ними перед развертыванием кластера.
