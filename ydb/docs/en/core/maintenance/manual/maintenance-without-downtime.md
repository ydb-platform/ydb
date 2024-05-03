# Maintenance without downtime

Periodically, the {{ ydb-short-name }} cluster needs to be maintained, such as upgrading its version or replacing broken disks. Maintenance can cause the cluster or its databases to become unavailable due to:
- Exceeding the failure model of the affected [storage groups](../../concepts/databases.md#storage-groups).
- Exceeding the [State Storage](../../deploy/configuration/config.md#domains-state) failure model.
- Lack of computational resources due to stopping too many [dynamic nodes](../../concepts/cluster/common_scheme_ydb.md#nodes).

To avoid such situations, {{ ydb-short-name }} has a system [tablet](../../concepts/cluster/common_scheme_ydb.md#tablets) that monitors the state of the cluster - the *Cluster Management System (CMS)*. The CMS allows you to answer the question of whether a {{ ydb-short-name }} node or host running {{ ydb-short-name }} nodes can be safely taken out for maintenance. To do this, create a [maintenance task](#maintenance-task) in the CMS and specify in it to take exclusive locks on the nodes or hosts that will be involved in the maintenance. The cluster components on which the locks are taken are considered unavailable from the CMS perspective and can be safely maintained. The CMS will [check](#checking-algorithm) the current state of the cluster and take locks only if the maintenance comply with the [availability mode](#availability-mode) and [unavailable node limits](#unavailable-node-limits).

{% note warning "Faults during maintenance" %}

During maintenance activities whose safety is guaranteed by the CMS, faults unrelated to those activities may occur in the cluster. If the faults threaten the availability of the cluster, urgent completion of the maintenance can help mitigate the risk of loss of availability.

{% endnote %}

## Maintenance task {#maintenance-task}

A *maintenance task* is a set of *actions* that the user asks the CMS to perform for safe maintenance.

Supported actions:
- Taking an exclusive lock on a cluster component - node or host.

In a task, actions are divided into groups. Actions from the same group are performed atomically. Currently, groups can consist of only one action.

If it's not possible to perform an action at the time of the request, the CMS informs you of the reason and the time when it is worth *refreshing* the task, and sets the action status to *pending*. When the task is refreshed, the CMS attempts to perform the pending actions again.

*Performed* actions have a deadline after which they are considered *completed* and stop having an effect on the cluster. For example, an exclusive lock is removed. An action can be completed early.

{% note info "Protracted maintenance" %}

If maintenance continues after the actions that were performed to make it safe have been completed, this is considered a fault in the cluster.

{% endnote %}

Completed actions are automatically removed from the task.

### Availability mode {#availability-mode}

In a maintenance task, you need to specify the availability mode of the cluster to be complied when checking whether actions can be performed. The following modes are supported:
- **Strong** - a mode that minimizes the risk of availability loss.
    - No more than one unavailable [VDisk](../../concepts/cluster/distributed_storage.md#storage-groups) is allowed in each affected storage group.
    - No more than one unavailable State Storage ring is allowed.
- **Weak** - a mode that does not allow exceeding the failure model.
    - No more than two unavailable VDisks are allowed for affected storage groups with the [block-4-2](../../deploy/configuration/config.md#reliability) scheme.
    - No more than four unavailable VDisks, three of which must be in the same data center, are allowed for affected storage groups with the [mirror-3-dc](../../deploy/configuration/config.md#reliability) scheme. 
    - No more than `(nto_select - 1) / 2` unavailable State Storage rings are allowed.
- **Force** - forced mode, the failure model is ignored. Not recommended for use.

### Priority {#priority}

You can specify the priority of a maintenance task. A lower value means a higher priority.

The actions of the task cannot be performed until all conflicting actions from tasks with a higher priority are completed. Tasks with the same priority have no advantage over each other.

## Unavailable node limits {#unavailable-node-limits}

In the CMS configuration, you can configure limits on the number of unavailable nodes for a database (tenant) or for the cluster as a whole. Relative and absolute limits are supported.

By default, no more than 10% of unavailable nodes are allowed for each database and the cluster as a whole.

## Checking algorithm {#checking-algorithm}

To check if the actions of a maintenance task can be performed, the CMS sequentially goes through each action group in the task and checks the action from the group:
- If the object of the action is a host, the CMS checks whether the action can be performed with all nodes running on the host. 
- If the object of the action is a node, the CMS checks:
    - Whether there is a lock on the node.
    - Whether it's possible to lock the node according to the limits of unavailable nodes.
    - Whether it's possible to lock all VDisks of the node according to the availability mode.
    - Whether it's possible to lock the State Storage ring of the node according to the availability mode.
    - Whether it's possible to lock the node according to the limit of unavailable nodes on which cluster system tablets can run.

If the checks are successful, the action can be performed and a temporary locks are taken on the checked nodes. The CMS then considers the next group of actions. Temporary locks help to understand whether the actions requested in different groups conflict with each other. Once the check is complete, the temporary locks are removed.

## Examples {#examples}

The [ydbops](https://github.com/ydb-platform/ydbops) utility tool uses CMS for cluster maintenance without downtime. You can also use the CMS directly through the [gRPC API](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/grpc/draft/ydb_maintenance_v1.proto).

#### Take out a node for maintenance {#node-maintenance}

{% note info "Functionality in development" %}

Functionality is expected in upcoming versions of ydbops.

{% endnote %}

To take out a node for maintenance, you can use the command:
```
$ ydbops node maintenance --host <node_fqdn>
```
When executing this command, ydbops will take an exclusive lock on the node in CMS.

### Rolling restart {##rolling-restart}

To perform a rolling restart of the entire cluster you can use the command:
```
$ ydbops restart --endpoint grpc://<cluster-fqdn> --availability-mode strong
```
The ydbops utility will automatically create a maintenance task to restart the entire cluster using the given availability mode. As it progresses, ydbops will refresh the maintenance task and take exclusive locks on the nodes in the CMS until all nodes are restarted.
