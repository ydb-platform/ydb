# Перемещение State Storage

Если нужно вывести из эксплуатации хост кластера {{ ydb-short-name }}, на котором располагается часть [State Storage](../../deploy/configuration/config.md#domains-state), необходимо переместить  ее на другой хост.

{% include [warning-configuration-error](../_includes/warning-configuration-error.md) %}

В качестве примера рассмотрим кластер {{ ydb-short-name }} со следующей конфигурацией State Storage:

```yaml
...
domains_config:
  ...
  state_storage:
  - ring:
      node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
      nto_select: 9
    ssid: 1
  ...
...
```

На хосте с `node_id:1` сконфигурирован и запущен [статический узел](../../deploy/configuration/config.md#hosts) кластера, который обслуживает часть State Storage. Предположим, нам нужно вывести из эксплуатации этот хост.

Для замены `node_id:1` мы [добавили](../../maintenance/manual/cluster_expansion.md#add-host) в кластер новый хост с `node_id:10` и [развернули](../../maintenance/manual/cluster_expansion.md#add-static-node) на нем статический узел.

Чтобы переместить State Storage с хоста `node_id:1` на `node_id:10`:

1. Остановите статические узлы кластера на хостах с `node_id:1` и `node_id:10`.

    {% include [fault-tolerance](../_includes/fault-tolerance.md) %}
1. В конфигурационном файле `config.yaml` измените список хостов `node`, заменив идентификатор удаляемого хоста на идентификатор добавляемого:

    ```yaml
    domains_config:
    ...
      state_storage:
      - ring:
          node: [2, 3, 4, 5, 6, 7, 8, 9, 10]
          nto_select: 9
        ssid: 1
    ...
    ```

1. Обновите конфигурационные файлы `config.yaml` для всех узлов кластера, в том числе и динамических.
1. С помощью процедуры [rolling-restart](../../maintenance/manual/node_restarting.md) перезапустите все узлы кластера, включая динамические, кроме статических узлов на хостах с `node_id:1` и `node_id:10`.
1. Запустите статические узлы кластера на хостах `node_id:1` и `node_id:10`.
