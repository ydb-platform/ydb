# Installing a cluster

## Install the {{ ydb-short-name }} controller in the {#install-ydb-controller} cluster

Install {{ ydb-short-name }} in the standard configuration:

{% list tabs %}

- CLI

   Run the command:

   ```bash
   helm install ydb-operator ydb/operator
   ```

   * `ydb-operator`: The release name.
   * `ydb/operator`: The name of the chart in the repository you added earlier.

   Result:

   ```text
   NAME: ydb-operator
   LAST DEPLOYED: Thu Aug 12 19:32:28 2021
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
   kubectl apply -f samples/storage.yaml
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

The standard configuration includes the minimum required 9 storage nodes, 80 GB each.

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

## Create a database {#create-database}

Apply the manifest for creating a database:

{% list tabs %}

- CLI

   Run the command:

   ```bash
   kubectl apply -f samples/database.yaml
   ```


   {% note info %}

   The `.spec.storageClusterRef.name` key value must match the name of the storage resource of the cluster part.

   {% endnote %}

   After processing the manifest, a StatefulSet object that describes a set of dynamic nodes is created. The created database will be accessible from inside the {{ k8s }} cluster by the `database-sample` DNS name or the `database-sample.<namespace>.svc.cluster.local` FQDN, where `namespace` indicates the namespace that the release was installed. The database is connected to through port `2135`.

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
     State:  Ready
   Events:
     Type     Reason              Age    From          Message
     ----     ------              ----   ----          -------
     Normal   Provisioning        8m10s  ydb-operator  Resource sync is in progress
     Normal   Provisioning        8m9s   ydb-operator  Resource sync complete
     Normal   TenantInitialized   8m9s   ydb-operator  Tenant /root/database-sample created
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

   1. Start a new pod using the {{ ydb-short-name }} CLI:

      ```bash
      kubectl run -it --image=cr.yandex/yc/ydb:21.4.30 --rm ydb-cli bash
      ```

   1. Query the {{ ydb-short-name }} database:

      ```bash
      ydb \
        --endpoint grpc://database-sample-grpc:2135 \
        --database /root/database-sample \
        table query execute --query 'select 1;'
      ```

      * `--endpoint`: Database endpoint.
      * `--database`: The name of the created database.
      * `--query`: Query text.

      Result:

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
      kubectl delete database.ydb.tech database-sample
      ```

   1. To delete a {{ ydb-short-name }} cluster, run the following commands:

      ```bash
      kubectl delete storage.ydb.tech storage-sample
      kubectl delete pvc -l app.kubernetes.io/name=ydb
      ```

   1. To remove the {{ ydb-short-name }} controller from the {{ k8s }} cluster, delete the release created by Helm:

      ```bash
      helm delete ydb-operator
      ```

      * `ydb-operator`: The name of the release that the controller was installed under.

{% endlist %}
