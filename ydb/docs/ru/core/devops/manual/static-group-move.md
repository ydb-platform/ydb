# Перемещение статической группы

Если нужно вывести из эксплуатации хост кластера {{ ydb-short-name }}, на котором располагается часть [статической группы](../../deploy/configuration/config.md#blob_storage_config), необходимо переместить ее на другой хост.

{% include [warning-configuration-error](../_includes/warning-configuration-error.md) %}

В качестве примера рассмотрим кластер {{ ydb-short-name }}, в котором на хосте с `node_id:1` сконфигурирован и запущен [статический узел](../../deploy/configuration/config.md#hosts). Этот узел обслуживает часть статической группы.

Фрагмент конфигурации статической группы:

```yaml
...
blob_storage_config:
  ...
  service_set:
    ...
    groups:
      ...
      rings:
        ...
        fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: /dev/vda
            pdisk_category: SSD
        ...
      ...
    ...
  ...
...
```

Для замены `node_id:1` мы [добавили](../../maintenance/manual/cluster_expansion.md#add-host) в кластер новый хост с `node_id:10` и [развернули](../../maintenance/manual/cluster_expansion.md#add-static-node) на нем статический узел.

Чтобы переместить часть статической группы с хоста `node_id:1` на `node_id:10`:

1. Остановите статический узел кластера на хосте с `node_id:1`.

    {% include [fault-tolerance](../_includes/fault-tolerance.md) %}
1. В конфигурационном файле `config.yaml` измените значение `node_id`, заменив идентификатор удаляемого хоста на идентификатор добавляемого:

    ```yaml
    ...
    blob_storage_config:
      ...
      service_set:
        ...
        groups:
          ...
          rings:
            ...
            fail_domains:
            - vdisk_locations:
              - node_id: 10
                path: /dev/vda
                pdisk_category: SSD
            ...
          ...
        ...
      ...
    ...
    ```

    Измените путь `path` и категорию `pdisk_category` диска, если на хосте с `node_id: 10` они отличаются.

1. Обновите конфигурационные файлы `config.yaml` для всех узлов кластера, в том числе и динамических.
1. С помощью процедуры [rolling-restart](../../maintenance/manual/node_restarting.md) перезапустите все статические узлы кластера.
1. Перейдите на страницу мониторинга Embedded UI и убедитесь, что VDisk статической группы появился на целевом физическом диске и реплицируется. Подробнее см. [{#T}](../../reference/embedded-ui/ydb-monitoring.md#static-group).
1. С помощью процедуры [rolling-restart](../../maintenance/manual/node_restarting.md) перезапустите все динамические узлы кластера.
