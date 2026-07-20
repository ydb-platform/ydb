# host_configs

Кластер {{ ydb-short-name }} состоит из множества узлов, для развертывания которых обычно используется одна или несколько типовых конфигураций серверов. Для того чтобы не повторять её описание для каждого узла, в файле конфигурации существует раздел `host_configs`, в котором перечислены используемые конфигурации, и им присвоены идентификаторы.

## Синтаксис

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: <path_to_device>
    type: <type>
    disk_scope: <disk_scope>  # необязательный атрибут
  - path: ...
- host_config_id: 2
  ...
```

Атрибут `host_config_id` задает числовой идентификатор конфигурации. В атрибуте `drive` содержится коллекция описаний подключенных дисков. Каждое описание состоит из двух обязательных атрибутов:

- `path` : Путь к смонтированному блочному устройству, например `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type` : Тип физического носителя устройства: `ssd`, `nvme` или `rot` (rotational - HDD)

Дополнительно может быть указан необязательный атрибут `disk_scope` — метка зоны отказа устройства внутри узла, см. [Конфигурирование DiskScope](#disk-scope).

## Примеры

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

## Конфигурирование DiskScope {#disk-scope}

`disk_scope` — необязательный строковый атрибут диска, задающий более детальную зону отказа внутри одного узла. Он учитывается при вычислении [доменов отказа](../../concepts/glossary.md#fail-domain) во время выбора дисков для размещения [VDisk'ов](../../concepts/glossary.md#vdisk) [групп хранения](../../concepts/glossary.md#storage-group).

Домен отказа определяется физическим расположением диска (дата-центр, стойка, сервер, физическое устройство), поэтому либо никакие два VDisk'а одной группы не могут быть размещены на одном узле (если домен отказа соответствует серверу или серверной стойке), либо несколько VDisk'ов одной группы может быть размещено на одном узле на разных дисках (если домен отказа соответствует отдельному физическому устройству). В первом случае невозможно создать конфигурацию в режиме `block-4-2` менее чем с 8 узлами хранения, а во втором случае конфигурация может не переживать отказ 1 узла. Чтобы ограничить количество VDisk'ов одной группы, размещаемых на одном узле, физические устройства можно пометить атрибутом `disk_scope` и включить расчёт доменов отказа по уровню `disk_scope`. Это позволит собирать некоторые отказоустойчивые конфигурации в инсталляциях, в которых серверов меньше, чем требуется доменов отказа для выбранного [режима отказоустойчивости](../../concepts/topology.md).

### Пример {#disk-scope-example}

Кластер из 4 серверов, на каждом сервере 4 диска в режиме отказоустойчивости `block-4-2`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
    disk_scope: fail-domain-1
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
    disk_scope: fail-domain-1
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
    disk_scope: fail-domain-2
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_04
    type: SSD
    disk_scope: fail-domain-2
```

Чтобы домены отказа рассчитывались по уровню `disk_scope` для статической группы и других групп хранения, укажите соответствующий тип домена отказа в yaml-конфиге (верхнеуровневый ключ):

``` yaml
fail_domain_type: disk_scope
```

И в конфигурации домена:

```yaml
domains:
- domain_name: <имя домена>
  ...
  storage_pool_kinds:
  - kind: <тип используемых физических устройств>
    fail_domain_type: disk_scope
    ...
```


При такой конфигурации на каждом сервере будут размещены по два VDisk'а каждой группы хранения, и максимальным отказом в такой конфигурации будет отказ одного сервера.

## Особенности Kubernetes {#host-configs-k8s}

{{ ydb-short-name }} Kubernetes operator монтирует NBS диски для Storage узлов на путь `/dev/kikimr_ssd_00`. Для их использования должна быть указана следующая конфигурация `host_configs`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

Файлы с примерами конфигурации, поставляемые в составе {{ ydb-short-name }} Kubernetes operator, уже содержат такую секцию, и её не нужно менять.