# Эксплуатация {{ ydb-short-name }} в {{ k8s }}

## Мониторинг

Для удобства {{ ydb-short-name }} предоставляет стандартные механизмы сбора логов и метрик.

Логирование осуществляется в стандартные каналы `stdout` и `stderr` и может быть перенаправлено при помощи популярных решений. Мы рекомендуем использовать комбинацию из [Fluentd](https://www.fluentd.org/) и [Elastic Stack](https://www.elastic.co/elastic-stack/).

Для сбора метрик `ydb-controller` предоставляет ресурсы типа `ServiceMonitor`, которые могут быть обработаны с помощью [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack).

## Описание ресурсов контроллера {{ ydb-short-name }}

### Ресурс Storage {#storage}

```yaml
apiVersion: ydb.tech/v1alpha1
kind: Storage
metadata:
  # имя будет необходимо указать при создании базы данных
  name: storage-sample
spec:
  # можно указать либо версию {{ ydb-short-name }}, либо имя контейнера 
  # image:
  #   name: "cr.yandex/ydb/ydb:stable-21-4-14"
  version: 21.4.30
  # количество подов кластера
  nodes: 8

# https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
# спецификация дисковых ресурсов для подов кластера
dataStore:
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 80Gi

# https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
# Ограничение ресурсов подов кластера {{ ydb-short-name }}
resources:
  limits:
    cpu: 2
    memory: 8Gi
  requests:
    cpu: 2
    memory: 8Gi
  
# https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector
# selector того, на каких нодах кластера могут запускаться поды {{ ydb-short-name }}
# nodeSelector:
#   network: fast

# https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
# недостатки каких машин кластера могут быть проигнорированы при назначении подов {{ ydb-short-name }}
# tolerations:
#   - key: "example-key"
#     operator: "Exists"
#     effect: "NoSchedule"

# https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity
# указание для планировщика распределять поды {{ ydb-short-name }} по нодам равномерно
# affinity:
#   podAntiAffinity:
#      preferredDuringSchedulingIgnoredDuringExecution:
#      - weight: 100
#        podAffinityTerm:
#          labelSelector:
#            matchExpressions:
#            - key: app.kubernetes.io/instance
#              operator: In
#              values:
#              - ydb
#          topologyKey: kubernetes.io/hostname
```

## Ресурс Database {#database}

```yaml
apiVersion: ydb.tech/v1alpha1
kind: Database
metadata:
  # имя будет использовано при создании базы, => `/root/database-sample`
  name: database-sample
spec:
  # можно указать либо версию YDB, либо имя контейнера 
  # image:
  #   name: "cr.yandex/ydb/ydb:stable-21-4-14"
  version: 21.4.30
  # количество подов базы данных
  nodes: 6
  
  # указатель storage nodes кластера ydb, соответствует имени релиза Helm
  storageClusterRef:
    name: ydb
    namespace: default
    
  # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
  # Ограничение ресурсов динамического пода YDB
  resources:
    limits:
      cpu: 2
      memory: 8Gi
    requests:
      cpu: 2
      memory: 8Gi
    
  # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector
  # selector того, на каких нодах кластера могут запускаться поды YDB
  # nodeSelector:
  #   network: fast
  
  # https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
  # недостатки каких машин кластера могут быть проигнорированы при назначении подов YDB
  # tolerations:
  #   - key: "example-key"
  #     operator: "Exists"
  #     effect: "NoSchedule"
  
  # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity
  # указание для планировщика распределять поды YDB по нодам равномерно
  # affinity:
  #   podAntiAffinity:
  #      preferredDuringSchedulingIgnoredDuringExecution:
  #      - weight: 100
  #        podAffinityTerm:
  #          labelSelector:
  #            matchExpressions:
  #            - key: app.kubernetes.io/instance
  #              operator: In
  #              values:
  #              - ydb
  #          topologyKey: kubernetes.io/hostname
```

## Выделение ресурсов {#resource-allocation}

Каждый под {{ ydb-short-name }} может быть ограничен в потреблении ресурсов. Если оставить значения ограничений пустыми, поду будет доступно все процессорное время и вся память ВМ, что может привести к нежелательным последствиям. Мы рекомендуем всегда явно указывать лимиты ресурсов.

Более детально ознакомиться с принципами распределения и ограничения ресурсов можно в [документации {{ k8s }}](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).
