# Running {{ ydb-short-name }} in Minikube

To create a {{ ydb-short-name }} cluster using [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), follow these steps.

## Before you start {#before-begin}

1. Install the {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).

1. Install and run [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

1. Install the {{ k8s }} [Helm 3](https://helm.sh/docs/intro/install/) package manager.

1. Clone the repository with [ydb-kubernetes-operator](https://github.com/ydb-platform/ydb-kubernetes-operator).

      ```bash
      git clone https://github.com/ydb-platform/ydb-kubernetes-operator && cd ydb-kubernetes-operator
      ```

# Installing a cluster

## Install the {{ ydb-short-name }} controller in the {#install-ydb-controller} cluster

Install {{ ydb-short-name }} in the standard configuration:

{% list tabs %}

- CLI

  Run the command:

  ```bash
  helm upgrade --install ydb-operator deploy/ydb-operator --set metrics.enabled=false
  ```
  * `ydb-operator`: The release name.
  * `ydb-operator`: The name of the chart in the repository you added earlier.

  Output:

  ```text
  Release "ydb-operator" does not exist. Installing it now.
  NAME: ydb-operator
  LAST DEPLOYED: Tue Mar 22 08:54:08 2022
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

{% endlist %}

## Create a {{ ydb-short-name }} cluster {#create-cluster}

Apply the manifest for creating a {{ ydb-short-name }} cluster:

{% list tabs %}

- CLI

  Run the command:

  ```bash
  kubectl apply -f samples/minikube/storage.yaml
  ```

  This command creates a StatefulSet object that describes a set of containers with stable network IDs and disks assigned to them, as well as Service and ConfigMap objects that are required for the cluster to work.

  You can check the progress of {{ ydb-short-name }} cluster creation using the following commands:

  ```bash
  kubectl get storages.ydb.tech
  kubectl describe storages.ydb.tech
  ```

  Wait until the status of the Storage resource changes to `Ready`.

{% endlist %}

{% note warning %}

The cluster configuration is static. The controller won't process any changes when the manifest is reapplied. You can only update cluster parameters such as version or disk size by creating a new cluster.

{% endnote %}

## Create a database {#create-database}

Apply the manifest for creating a database:

{% list tabs %}

- CLI

  Run the command:

  ```bash
  kubectl apply -f samples/minikube/database.yaml
  ```

  After processing the manifest, a StatefulSet object that describes a set of dynamic nodes is created. The created database will be accessible from inside the {{ k8s }} cluster by the `database-minikube-sample` DNS name. The database is connected to through port `2135`.

  View the status of the created resource:

  ```bash
  kubectl describe database.ydb.tech
  
  Name:         database-sample
  Namespace:    default
  Labels:       <none>
  Annotations:  <none>
  API Version:  ydb.tech/v1alpha1
  Kind:         Database
  ...
  Status:
    ...
    State:  Ready
  Events:
    Type    Reason          Age    From          Message
    ----    ------          ----   ----          -------
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.ConfigMap, Namespace: default, Name: database-minikube-sample, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-grpc, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-interconnect, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-status, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.StatefulSet, Namespace: default, Name: database-minikube-sample, changed, result: created
    Normal  Initialized     6m31s  ydb-operator  Tenant /Root/database-minikube-sample created
    Normal  ResourcesReady  6m30s  ydb-operator  Resource are ready and DB is initialized
  ```

  The database is ready to run.

{% endlist %}

## Test the controller {#test-ydb}

Test how {{ ydb-short-name }} works:

{% list tabs %}

- CLI

  1. Check that all nodes are in the `Ready` status:

      ```bash
      kubectl get pods
      NAME                            READY   STATUS    RESTARTS   AGE
      database-minikube-sample-0      1/1     Running   0          9m33s
      storage-minikube-sample-0       1/1     Running   0          11m
      ydb-operator-6fc76b5b68-q269l   1/1     Running   0          12m      
      ```

  1. Forward port 2135:

      ```bash
      kubectl port-forward database-minikube-sample-0 2135
      ```

  1. Install the {{ ydb-short-name }} CLI as described in [Installing the {{ ydb-short-name }} CLI](../../../reference/ydb-cli/install.md).

  1. Query the {{ ydb-short-name }} database:

      ```bash
      ydb \
        --endpoint grpc://localhost:2135 \
        --database /Root/database-minikube-sample \
        table query execute --query 'select 1;'
      ```
      * `--endpoint`: Database endpoint.
      * `--database`: The name of the created database.
      * `--query`: Query text.

      Output:

      ```text
      ┌─────────┐
      | column0 |
      ├─────────┤
      | 1       |
      └─────────┘
      ```

      For more on {{ ydb-short-name }} CLI commands, see the [documentation](../../../reference/ydb-cli/index.md).

{% endlist %}

## Release the resources you don't use {#cleanup}

If you no longer need the created resources, delete them:

{% list tabs %}

- CLI

  1. To delete a {{ ydb-short-name }} database, just delete the `Database` resource mapped to it:

      ```bash
      kubectl delete database.ydb.tech database-minikube-sample
      ```

  1. To delete a {{ ydb-short-name }} cluster, run the following commands:

      ```bash
      kubectl delete storage.ydb.tech storage-minikube-sample
      ```

  1. To remove the {{ ydb-short-name }} controller from the {{ k8s }} cluster, delete the release created by Helm:

      ```bash
      helm delete ydb-operator
      ```
      * `ydb-operator`: The name of the release that the controller was installed under.

{% endlist %}

