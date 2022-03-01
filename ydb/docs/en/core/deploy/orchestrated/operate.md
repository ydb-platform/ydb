# Using {{ ydb-short-name }} in {{ k8s }}

## Monitoring

For convenience, {{ ydb-short-name }} provides standard mechanisms for collecting logs and metrics.

Logging is done to standard `stdout` and `stderr` streams and can be redirected using popular solutions. We recommend using a combination of [Fluentd](https://www.fluentd.org/) and [Elastic Stack](https://www.elastic.co/elastic-stack/).

To collect metrics, `ydb-controller` provides resources like `ServiceMonitor`. They can be handled using [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack).

## Description of {{ ydb-short-name }} controller resources

### Storage resource {#storage}

```yaml
apiVersion: ydb.tech/v1alpha1
kind: Storage
metadata:
  # make sure you specify this name when creating a database
  name: storage-sample
spec:
  # you can specify either the {{ ydb-short-name }} version or the container name 
  # image:
  #   name: "cr.yandex/ydb/ydb:stable-21-4-14"
  version: 21.4.30
  # the number of cluster pods
  nodes: 8

# https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
# specifying disk resources for cluster pods
dataStore:
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 80Gi

# https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
# Limiting the {{ ydb-short-name }} cluster pod resources
resources:
  limits:
    cpu: 2
    memory: 8Gi
  requests:
    cpu: 2
    memory: 8Gi
  
# https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector
# selector of the cluster nodes that the {{ ydb-short-name }} pods can run on
# nodeSelector:
#   network: fast

# https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
# indicating which cluster instances' disadvantages can be ignored when assigning {{ ydb-short-name }} pods
# tolerations:
#   - key: "example-key"
#     operator: "Exists"
#     effect: "NoSchedule"

# https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity
# instructing the scheduler to distribute the {{ ydb-short-name }} pods evenly across the nodes
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

## Database resource {#database}

```yaml
apiVersion: ydb.tech/v1alpha1
kind: Database
metadata:
  # the name to be used when creating a database => `/root/database-sample`
  name: database-sample
spec:
  # you can specify either the YDB version or the container name 
  # image:
  #   name: "cr.yandex/ydb/ydb:stable-21-4-14"
  version: 21.4.30
  # the number of database pods
  nodes: 6
  
  # the pointer of the YDB cluster storage nodes, corresponds to the Helm release name
  storageClusterRef:
    name: ydb
    namespace: default
    
  # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
  # Limiting the YDB dynamic pod resources
  resources:
    limits:
      cpu: 2
      memory: 8Gi
    requests:
      cpu: 2
      memory: 8Gi
    
  # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector
  # selector of the cluster nodes that the YDB pods can run on
  # nodeSelector:
  #   network: fast
  
  # https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
  # indicating which cluster instances' disadvantages can be ignored when assigning YDB pods
  # tolerations:
  #   - key: "example-key"
  #     operator: "Exists"
  #     effect: "NoSchedule"
  
  # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity
  # instructing the scheduler to distribute the YDB pods evenly across the nodes
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

## Allocating resources {#resource-allocation}

You can limit resource consumption for each {{ ydb-short-name }} pod. If you leave the limit values empty, a pod can use the entire CPU time and VM RAM. This may cause undesirable effects. We recommend that you always specify the resource limits explicitly.

To learn more about resource allocation and limits, see the [{{ k8s }} documentation](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

