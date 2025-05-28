# Cluster Maintenance Without Downtime

Periodically, a {{ ydb-short-name }} cluster needs maintenance, such as updating its version or replacing failed disks. Maintenance work can lead to cluster or database unavailability due to:

- Exceeding the failure model of affected [storage groups](../../concepts/glossary.md#storage-group).
- Exceeding the failure model of [State Storage](../../reference/configuration/index.md#domains-state).
- Insufficient computational resources due to stopping too many [dynamic nodes](../../concepts/glossary.md#dynamic-node).

To avoid such situations, {{ ydb-short-name }} has a system [tablet](../../concepts/glossary.md#tablet) that monitors cluster state — *Cluster Management System (CMS)*. CMS allows you to understand whether it's safe to take a {{ ydb-short-name }} node or host running {{ ydb-short-name }} nodes into maintenance. To do this, you need to create a [maintenance task](#maintenance-task) in CMS and specify taking exclusive locks on nodes or hosts that will be involved in maintenance. Cluster components that are locked are considered unavailable from CMS perspective and can safely undergo maintenance. CMS will [check](#checking-algorithm) the current cluster state and take locks only if the maintenance work complies with [availability mode](#availability-mode) constraints and [unavailable node limits](#unavailable-node-limits).

{% note warning "Failures during maintenance" %}

During maintenance work whose safety is guaranteed by CMS, failures unrelated to this work may occur in the cluster. If failures threaten cluster availability, urgent completion of maintenance may help reduce the risk of unavailability.

{% endnote %}

## Maintenance Task {#maintenance-task}

A *maintenance task* is a set of *actions* that the user asks CMS to grant permission to perform for safe maintenance.

Supported actions:

- Taking an exclusive lock on a cluster component (node, host, or disk).

In a task, actions are divided into groups. Actions from one group are executed atomically. Currently, groups can consist of only one action.

If an action cannot be performed at the time of request, CMS reports the reason and time when the task should be *updated*, and puts the action in *pending* state. When updating the task, CMS retries pending actions.

*Performed* actions have a deadline after which they are considered *completed* and stop affecting the cluster. For example, the exclusive lock is released. An action can be completed early.

{% note info "Extended maintenance" %}

If cluster maintenance continues after completion of requested actions, this is considered a failure in the cluster.

{% endnote %}

Completed actions are automatically removed from the task.

### Availability Mode {#availability-mode}

In a maintenance task, you must specify the cluster availability mode that should be observed when checking the possibility of performing actions. The following modes are supported:

- **Strong**: mode that minimizes the risk of availability loss.
    - No more than one unavailable [VDisk](../../concepts/glossary.md#vdisk) is allowed in each affected storage group.
    - No more than one unavailable State Storage ring is allowed.
- **Weak**: mode that does not allow exceeding the failure model.
    - No more than two unavailable VDisks are allowed for affected storage groups with [block-4-2](../../reference/configuration/index.md#reliability) scheme.
    - No more than four unavailable VDisks are allowed, three of which must be in one data center, for affected storage groups with [mirror-3-dc](../../reference/configuration/index.md#reliability) scheme.
    - No more than `(nto_select - 1) / 2` unavailable State Storage rings are allowed.
- **Force**: forced mode, failure model is ignored. *Not recommended for use*.

### Priority {#priority}

You can specify a priority for a maintenance task. A lower value means higher priority.

Task actions cannot be performed until all conflicting actions from higher priority tasks are completed. Tasks with the same priority have no advantage over each other.

## Unavailable Node Limits {#unavailable-node-limits}

In CMS configuration, you can set limits on the number of unavailable nodes for a database (tenant) or for the cluster as a whole. Both relative and absolute limits are supported.

By default, no more than 13% of unavailable nodes are allowed for each database and the cluster as a whole.

## Checking Algorithm {#checking-algorithm}

To check whether maintenance task actions can be performed, CMS sequentially goes through each action group in the task and checks the action from the group:

- If the action object is a host, CMS checks whether the action can be performed with all nodes running on the host.
- If the action object is a node, CMS checks:
  - Whether there is a lock on this node.
  - Whether a lock can be taken on this node according to unavailable node limits.
  - Whether locks can be taken on all VDisks of this node according to availability mode.
  - Whether a lock can be taken on the State Storage ring of this node according to availability mode.
  - Whether a lock can be taken on this node according to the limit of unavailable nodes on which cluster system tablets can run.
- If the action object is a disk, CMS checks:
  - Whether there is a lock on this disk.
  - Whether locks can be taken on all VDisks of this disk according to availability mode.

If checks pass successfully, the action can be performed, and temporary locks are taken on the checked nodes, hosts, or disks. After that, CMS considers the next action group. Temporary locks help understand whether actions requested in different groups conflict with each other. After complete verification, temporary locks are removed.

## Practice

* [For manually deployed clusters](../deployment-options/manual/maintenance.md)