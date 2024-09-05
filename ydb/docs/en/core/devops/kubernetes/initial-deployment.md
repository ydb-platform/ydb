# Getting started with {{ ydb-short-name }} in {{ k8s }}

Deploying {{ ydb-short-name }} in {{ k8s }} is a simple way to set up and run a {{ ydb-short-name }} cluster. {{ k8s }} allows to use an universal approach to managing your application in any cloud service provider. This guide provides instructions on how to deploy {{ ydb-short-name }} in [AWS EKS](https://aws.amazon.com/eks/) or [{{ managed-k8s-full-name }}](https://cloud.yandex.com/services/managed-kubernetes).

## Prerequisites

{{ ydb-short-name }} is delivered as a [Helm](https://helm.sh/) chart that is a package with templates of {{ k8s }} structures. For more information about Helm, see the [documentation](https://helm.sh/docs/). The {{ ydb-short-name }} chart can be deployed in the following environment:

1. A {{ k8s }} cluster with version 1.20 or higher. It needs to support [Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/). Follow the instructions below if you don't have a suitable cluster yet.
2. The `kubectl` command line tool is [installed](https://kubernetes.io/docs/tasks/tools/install-kubectl) and {{ k8s }} cluster access is configured.
3. The Helm package manager with a version higher than 3.1.0 is [installed](https://helm.sh/docs/intro/install/).

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

## Creating a {{ k8s }} cluster

Skip this section if you have already configured a suitable {{ k8s }} cluster.

{% list tabs %}

- AWS EKS

  1. Configure `awscli` and `eksctl` to work with AWS resources according to the [documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).

  2. Configure `kubectl` to work with a {{ k8s }} cluster.

  3. Run the following command:

      ```bash
        eksctl create cluster \
          --name ydb \
          --nodegroup-name standard-workers \
          --node-type c5a.2xlarge \
          --nodes 3 \
          --nodes-min 1 \
          --nodes-max 4
      ```

      This command will create a {{ k8s }} cluster named `ydb`. The `--node-type` flag indicates that the cluster is deployed using `c5a.2xlarge` (8vCPUs, 16 GiB RAM) instances. This meets minimal guidelines for running {{ ydb-short-name }}.

      It takes 10 to 15 minutes on average to create a {{ k8s }} cluster. Wait for the process to complete before proceeding to the next step of {{ ydb-short-name }} deployment. The `kubectl` configuration will be automatically updated to work with the cluster after it is created.


- {{ managed-k8s-full-name }}

  Follow the instructions in the [{{ managed-k8s-full-name }} quick start guide](https://cloud.yandex.com/en/docs/managed-kubernetes/quickstart).

{% endlist %}

## Overview of {{ ydb-short-name }} Helm chart

The Helm chart installs [YDB Kubernetes Operator](https://github.com/ydb-platform/ydb-kubernetes-operator) to the {{ k8s }} cluster. It is a controller that follows the [Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) design pattern. It implements the logic required for deploying and managing {{ ydb-short-name }} components.

A {{ ydb-short-name }} cluster consists of two kinds of nodes:

* **Storage nodes** ([Storage](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/storage-block-4-2.yaml) resource) provide the data persistence layer.
* **Dynamic nodes** ([Database](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/database.yaml) resource) implement data access and processing.

Create both resources with the desired parameters to deploy a {{ ydb-short-name }} cluster in {{ k8s }}. We'll follow this process in more detail below. The schema for these resources is [hosted on GitHub](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy/ydb-operator/crds).

After the chart data is processed by the controller, the following resources are created:

* [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/): A workload controller that assigns stable network IDs and disk resources to each container.
* [Service](https://kubernetes.io/docs/concepts/services-networking/service/): An object that is used to access the created databases from applications.
* [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/): An object that is used to store the cluster configuration.

See the operator's source code [on GitHub](https://github.com/ydb-platform/ydb-kubernetes-operator). The Helm chart is in the [deploy](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy) folder.
{{ ydb-short-name }} containers are deployed using `cr.yandex/yc/ydb` images. Currently, they are only available as prebuilt artifacts.

## Environment preparation

1. Clone the [ydb-kubernetes-operator](https://github.com/ydb-platform/ydb-kubernetes-operator) repository:

    ```bash
    git clone https://github.com/ydb-platform/ydb-kubernetes-operator && cd ydb-kubernetes-operator
    ```

2. Add the {{ ydb-short-name }} repository to Helm:

    Run the command:

    ```bash
    helm repo add ydb https://charts.ydb.tech/
    ```
    * `ydb`: The repository alias.
    * `https://charts.ydb.tech/`: The {{ ydb-short-name }} repository URL.

    Output:

    ```text
    "ydb" has been added to your repositories
    ```

3. Update the Helm chart index:

    Run the command:

    ```bash
    helm repo update
    ```

    Output:

    ```text
    Hang tight while we grab the latest from your chart repositories...
    ...Successfully got an update from the "ydb" chart repository
    Update Complete. ⎈Happy Helming!⎈
    ```

## Deploying a {{ ydb-short-name }} cluster

### Install the {{ ydb-short-name }} {{ k8s }} operator

Use `helm` to deploy the {{ ydb-short-name }} {{ k8s }} operator to the cluster:

```bash
helm install ydb-operator ydb/ydb-operator
```

* `ydb-operator`: The installation name.
* `ydb/ydb-operator`: The name of the chart in the repository you have added earlier.

Result:

```text
NAME: ydb-operator
LAST DEPLOYED: Thu Aug 12 19:32:28 2021
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

### Deploy storage nodes

{{ ydb-short-name }} supports a number of [storage topologies](../../concepts/topology.md). {{ ydb-short-name }} {{ k8s }} operator comes with a few sample configuration files for the most common topologies. This guide uses them as-is, but feel free to adjust them as needed or implement a new configuration file from scratch.

Apply the manifest for creating storage nodes:

{% list tabs %}

- block-4-2

  ```bash
  kubectl apply -f samples/storage-block-4-2.yaml
  ```

  This will create 8 {{ ydb-short-name }} storage nodes that persist data using erasure coding. This takes only 50% of additional storage space to provide fault-tolerance.

- mirror-3-dc

  ```bash
  kubectl apply -f samples/storage-mirror-3-dc.yaml
  ```

  This will create 9 {{ ydb-short-name }} storage nodes that store data with replication factor 3.

{% endlist %}

This command creates a `StatefulSet` object that describes a set of {{ ydb-short-name }} containers with stable network IDs and disks assigned to them, as well as `Service` and `ConfigMap` objects that are required for the cluster to work.

{{ ydb-short-name }} storage nodes take a while to initialize. You can check the initialization progress with `kubectl get storages.ydb.tech` or `kubectl describe storages.ydb.tech`. Wait until the status of the Storage resource changes to `Ready`.

{% note warning %}

The cluster configuration is static. The controller won't process any changes when the manifest is reapplied. You can only update cluster parameters such as version or disk size by creating a new cluster.

{% endnote %}

### Create a database and dynamic nodes

{{ ydb-short-name }} database is a logical entity that is served by a set of dynamic nodes. A sample manifest that comes with {{ ydb-short-name }} {{ k8s }} operator creates a database named `database-sample` with 3 dynamic nodes. As with storage nodes, feel free to adjust the configuration as needed.

Apply the manifest for creating a database and dynamic nodes:

```bash
kubectl apply -f samples/database.yaml
```

{% note info %}

The value referenced by `.spec.storageClusterRef.name` key must match the name of the `Storage` resource with storage nodes.

{% endnote %}

A `StatefulSet` object that describes a set of dynamic nodes is created after processing the manifest. The created database will be accessible from inside the {{ k8s }} cluster by the `database-sample` hostname or the `database-sample.<namespace>.svc.cluster.local` FQDN, where `namespace` indicates the namespace that was used for the installation. You can connect the database via port 2135.

View the status of the created resource:

```bash
kubectl describe database.ydb.tech
```

Result:

```
Name:         database-sample
Namespace:    default
Labels:       <none>
Annotations:  <none>
API Version:  ydb.tech/v1alpha1
Kind:         Database
...
Status:
  State:  Ready
Events:
  Type     Reason              Age    From          Message
  ----     ------              ----   ----          -------
  Normal   Provisioning        8m10s  ydb-operator  Resource sync is in progress
  Normal   Provisioning        8m9s   ydb-operator  Resource sync complete
  Normal   TenantInitialized   8m9s   ydb-operator  Tenant /root/database-sample created
```

`State: Ready` means that the database is ready to be used.


### Test cluster operation

Check how {{ ydb-short-name }} works:


 1. Check that all nodes are in the `Running` status:

    ```bash
    kubectl get pods
    ```

    Result:

    ```
    NAME                READY   STATUS    RESTARTS   AGE
    database-sample-0   1/1     Running   0          1m
    database-sample-1   1/1     Running   0          1m
    database-sample-2   1/1     Running   0          1m
    database-sample-3   1/1     Running   0          1m
    database-sample-4   1/1     Running   0          1m
    database-sample-5   1/1     Running   0          1m
    storage-sample-0    1/1     Running   0          1m
    storage-sample-1    1/1     Running   0          1m
    storage-sample-2    1/1     Running   0          1m
    storage-sample-3    1/1     Running   0          1m
    storage-sample-4    1/1     Running   0          1m
    storage-sample-5    1/1     Running   0          1m
    storage-sample-6    1/1     Running   0          1m
    storage-sample-7    1/1     Running   0          1m
    storage-sample-8    1/1     Running   0          1m
    ```

 2. Start a new pod with [{{ ydb-short-name }} CLI]((../reference/ydb-cli/index.md)):

    ```bash
    kubectl run -it --image=cr.yandex/crptqonuodf51kdj7a7d/ydb:22.4.44 --rm ydb-cli bash
    ```

 3. Query the {{ ydb-short-name }} database:

    ```bash
    ydb \
      --endpoint grpc://database-sample-grpc:2135 \
      --database /root/database-sample \
      table query execute --query 'SELECT 2 + 2;'
    ```

    * `--endpoint`: The database endpoint.
    * `--database`: The name of the created database.
    * `--query`: The query text.

    Result:

    ```text
    ┌─────────┐
    | column0 |
    ├─────────┤
    | 4       |
    └─────────┘
    ```


## Further steps

After you have tested that the created {{ ydb-short-name }} cluster operates fine you can continue using it as you see fit. For example, if you just want to continue experimenting, you can use it to follow the [YQL tutorial](../../dev/yql-tutorial/index.md).

Below are a few more things to consider.

### Monitoring

{{ ydb-short-name }} provides standard mechanisms for collecting logs and metrics. Logging is done to standard `stdout` and `stderr` streams and can be redirected using popular solutions. For example, you can use a combination of [Fluentd](https://www.fluentd.org/) and [Elastic Stack](https://www.elastic.co/elastic-stack/).

To collect metrics, `ydb-controller` provides resources like `ServiceMonitor`. They can be handled using [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack).


### Tuning allocated resources {#resource-allocation}

You can limit resource consumption for each {{ ydb-short-name }} pod. If you leave the limit values empty, a pod can use the entire CPU time and VM RAM. This may cause undesirable effects. We recommend that you always specify the resource limits explicitly.

To learn more about resource allocation and limits, see the [{{ k8s }} documentation](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).


### Release the resources you don't use {#cleanup}

If you no longer need the created {{ ydb-short-name }} cluster, delete it by following these steps:


1. To delete a {{ ydb-short-name }} database and its dynamic nodes, just delete the respective `Database` resource:

   ```bash
   kubectl delete database.ydb.tech database-sample
   ```

2. To delete {{ ydb-short-name }} storage nodes, run the following commands:

   ```bash
   kubectl delete storage.ydb.tech storage-sample
   kubectl delete pvc -l app.kubernetes.io/name=ydb
   ```

3. To remove the {{ ydb-short-name }} {{ k8s }} operator, delete it with Helm:

   ```bash
   helm delete ydb-operator
   ```

   * `ydb-operator`: The name of the release that the controller was installed under.