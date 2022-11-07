# Decomission of a part of the cluster

{% note info %}

The material of the article is being supplemented, the description of the command to perform the decomission will be published later.

{% endnote %}

Decommissioning is the procedure for moving a VDisk from a PDisk that needs to be decommissioned.

Data is moved to PDisk clusters where there is enough free space to create new slots. Moving is performed only when there is a possibility of moving with at least partial preservation of the failure model. Strict compliance with the failure model during decomission may be violated, but the system strives to ensure fault tolerance during decomission no worse than during regular operation. For example, when encoding `mirror-3-dc`, there may be a situation when the group is located in 4 data centers instead of 3.

Decomission is performed asynchronously, that is, when processing a command, there is no immediate movement of data. The SelfHeal mechanism in the background, when the failure model allows, moves the slots one by one, achieving the complete release of the specified pdisks.

## Managing devomission {#decommitstatus}

The decommission state is controlled by setting the `DecommitStatus` parameter for PDisk. The parameter can take the following values:

* `DECOMMIT_NONE`: the disk does not participate in the decommission and works normally, according to its condition.
* `DECOMMIT_PENDING`: a disk decomission is planned. Data is not transferred from the disk, but slots for new groups will not be created, and slots for previously created groups will not be moved.
* `DECOMMIT_IMMINENT`: it is necessary to perform a disk decomission. Data is transferred in the background to disks that have the status `DECOMMIT_NONE` and satisfy the failure model.

The values of `DECOMMIT_PENDING` and `DECOMMIT_IMMINENT` in the case of a regular decommission do not need to be removed, because the equipment is removed from the cluster via the `DefineBox` command.

To cancel the decommission, it is enough to change the state of the disks in `DECOMMIT_NONE`. At the same time, BS_CONTROLLER will not take any additional actions, the already transported VDisks will remain in their places. If you need to return them, you can use commands to move slots point by point, depending on the specific situation.

Managing the states of `DECOMMIT_PENDING` and `DECOMMIT_IMMINENT` allows you to decommission the cluster in parts.

>For example, you need to move equipment from data center-1 to data center-2:
>
>1. The data center-2 hosts buffer equipment, to which the first part of data will be moved.
>1. All data center-1 disks are put into the `DECOMMIT_PENDING` state to exclude data movement inside data center-1.
>1. In the data center-1, all disks of equipment equivalent to the buffer are transferred to the `DECOMMIT_IMMINENT` state.
>
>    It is necessary to wait for the release of all disks in the `DECOMMIT_IMMINENT` state.
>1. The released equipment from the data center-1 is moved to the data center-2, its disks are transferred to the `DECOMMIT_NONE` state.
>
>The described steps are repeated for the next set of equipment in the data center-1 until all the equipment is moved.
