# Cluster maintenance without downtime

A {{ ydb-short-name }} cluster periodically needs maintenance, such as upgrading its version or replacing broken disks. Maintenance can cause the cluster or existing databases to become unavailable due to:

- Exceeding the failure model of the affected [storage groups](../../concepts/glossary.md#storage-groups).
- Exceeding the failure model of [State Storage](../../reference/configuration/domains_config.md#domains-state).
- Lack of computational resources due to stopping too many [dynamic nodes](../../concepts/glossary.md#dynamic-node).

To avoid such situations, {{ ydb-short-name }} has a system [tablet](../../concepts/glossary.md#tablet) that monitors the cluster state — the *Cluster Management System (CMS)*. The CMS allows you to determine whether a {{ ydb-short-name }} node or a host running {{ ydb-short-name }} nodes can be safely taken out for maintenance. To do this, create a [maintenance task](#maintenance-task) in the CMS and specify acquiring exclusive locks on the nodes or hosts that will be involved in the maintenance. Cluster components with locks acquired on them are considered unavailable from the CMS perspective and can be safely maintained. The CMS will [check](#checking-algorithm) the current cluster state and acquire locks only if the maintenance complies with the [availability mode](#availability-mode) and [unavailable node limits](#unavailable-node-limits).

{% note warning "Failures during maintenance" %}

During maintenance activities whose safety is guaranteed by the CMS, failures unrelated to those activities may occur in the cluster. If the failures threaten cluster availability, urgently aborting the maintenance can help mitigate the risk of downtime.

{% endnote %}

## Maintenance task {#maintenance-task}

A *maintenance task* is a set of *actions* that the user asks the CMS to perform for safe maintenance.

Supported actions:

- Acquiring an exclusive lock on a cluster component (node, host, or disk).

Actions in a task are divided into groups. Actions from the same group are performed atomically. Currently, groups can consist of only one action.

If an action cannot be performed at the time of the request, the CMS reports the reason and the time when it is worth *refreshing* the task, and sets the action status to *pending*. When the task is refreshed, the CMS retries the pending actions.

*Performed* actions have a deadline after which they are considered *completed* and stop affecting the cluster. For example, an exclusive lock is released. An action can be completed early.

{% note info "Protracted maintenance" %}

If cluster maintenance continues after the actions performed to make it safe have been completed, this is considered a failure in the cluster.

{% endnote %}

Completed actions are automatically removed from the task.

### Availability mode {#availability-mode}

In a maintenance task, you need to specify the cluster availability mode to comply with when checking whether actions can be performed. The following modes are supported:

- **Strong**: a mode that minimizes the risk of availability loss.
    - No more than one unavailable [VDisk](../../concepts/glossary.md#vdisk) is allowed in each affected storage group.
    - No more than one unavailable State Storage ring is allowed.
- **Weak**: a mode that does not allow exceeding the failure model.
    - No more than two unavailable VDisks are allowed for affected storage groups with the [block-4-2](../../reference/configuration/domains_config.md#reliability) scheme.
    - No more than four unavailable VDisks, three of which must be in the same data center, are allowed for affected storage groups with the [mirror-3-dc](../../reference/configuration/domains_config.md#reliability) scheme.
    - No more than `(nto_select - 1) / 2` unavailable State Storage rings are allowed.
- **Smart**: a mode that behaves like **Strong** by default but automatically switches to **Weak** if there are already unavailable VDisks in the affected storage groups or unavailable State Storage rings not related to the current maintenance.
- **Force**: a forced mode, the failure model is ignored. *Not recommended for use*.

### Priority {#priority}

You can specify the priority of a maintenance task. A lower value means a higher priority; negative priorities are supported (similar to `nice` in Linux).

Priority determines the order in which maintenance tasks are executed. Tasks with a higher priority have an advantage over tasks with a lower priority: they block their execution and can run on top of them. Tasks with the same priority are processed equally — none of them has an advantage over another.

## Unavailable node limits {#unavailable-node-limits}

The CMS has absolute and relative limits on the number of unavailable nodes for each database (tenant) and for the cluster as a whole. Unavailable node limits are configured in the [CMS configuration](../../reference/configuration/cms_config.md).

## Checking algorithm {#checking-algorithm}

To check whether the actions of a maintenance task can be performed, the CMS sequentially goes through each action group in the task and checks the action from the group:

- If the action's object is a host, the CMS checks whether the action can be performed with all nodes running on the host.
- If the action's object is a node, the CMS checks:
  - Whether there is a lock on the node.
  - Whether it's possible to lock the node according to the unavailable node limits.
  - Whether it's possible to lock all VDisks of the node according to the availability mode.
  - Whether it's possible to lock the State Storage ring of the node according to the availability mode.
  - Whether it's possible to lock the node according to the limit of unavailable nodes on which cluster system tablets can run.
- If the action's object is a disk, the CMS checks:
  - Whether there is a lock on the disk.
  - Whether it's possible to lock all VDisks of the disk according to the availability mode.

If the checks are successful, the action can be performed, and temporary locks are acquired on the checked nodes, hosts, or disks. The CMS then considers the next group of actions. Temporary locks help to understand whether the actions requested in different groups conflict with each other. Once the check is fully complete, the temporary locks are released.

## Bridge mode {#bridge}

If the cluster runs in [bridge mode](../../concepts/glossary.md#bridge), limits and availability checks are applied independently for each [pile](../../concepts/glossary.md#pile).

## Suspending maintenance {#disable-maintenance}

{% note warning %}

Prolonged suspension of cluster maintenance can lead to availability loss due to hardware failures.

{% endnote %}

In emergency situations, such as infrastructure problems, you can temporarily suspend new cluster maintenance using the `disable_maintenance` parameter in the [CMS configuration](../../reference/configuration/cms_config.md).

During maintenance suspension:

- New cluster maintenance will be suspended.
- Already started cluster maintenance will continue to run.
- It remains possible to perform emergency maintenance manually without using the CMS.

## Practice

* [For manually deployed clusters](../deployment-options/manual/maintenance.md)
