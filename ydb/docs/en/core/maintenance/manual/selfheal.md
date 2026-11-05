# Working with SelfHeal

<<<<<<< HEAD
While a clusters are running, entire nodes or individual block devices that {{ ydb-short-name }} runs on can fail.

SelfHeal ensures a cluster's continuous performance and fault tolerance if malfunctioning nodes or devices cannot be repaired quickly.

SelfHeal can:

* Detect faulty system elements.
* Transfer faulty elements carefully without data loss and disintegration of storage groups.
=======
{{ ydb-short-name }} has two automatic recovery (SelfHeal) mechanisms:

1. **Storage SelfHeal** (this article) — for disks and [storage groups](../../concepts/glossary.md#storage-group) that hold data.
2. **State Storage SelfHeal** — for [State Storage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) replicas. See [{#T}](selfheal_statestorage.md).

Both mechanisms restore cluster fault tolerance after prolonged failures. If a faulty node or disk is restored before the timeout expires (about one hour by default for disks), SelfHeal does not start relocation.

{% note info %}
>>>>>>> 95c048edc38 (docs: improve SelfHeal documentation (#50561))

State Storage SelfHeal is available only with [configuration V2](../../devops/configuration-management/configuration-v2/config-overview.md). Storage SelfHeal does not depend on the configuration version.

<<<<<<< HEAD
{{ ydb-short-name }} component responsible for SelfHeal is called "Sentinel".
=======
{% endnote %}

## How storage SelfHeal works {#how-it-works}

Sentinel, a component of [CMS](../../concepts/glossary.md#cms), continuously monitors the state of [PDisks](../../concepts/glossary.md#pdisk) and nodes. If a fault persists long enough (about one hour by default), Sentinel initiates relocation of the affected [VDisks](../../concepts/glossary.md#vdisk) to healthy hardware so that the [failure model](../../concepts/topology.md#cluster-config) is satisfied again.

The [Blob Storage Controller](../../concepts/glossary.md#ds-controller) executes the command: data is replicated in the background. The relocation itself can take from minutes to a day, depending on the data volume and the hardware. Once the command has been accepted, CMS treats the task as issued; distributed storage is responsible for completing replication.

Storage SelfHeal is enabled by default for [dynamic groups](../../concepts/glossary.md#dynamic-group). On clusters with configuration V2, you can also enable [static group SelfHeal](../../devops/configuration-management/configuration-v2/static-group-self-heal.md). With configuration V1, static group SelfHeal cannot be enabled.

The sections below describe how to enable, disable, and configure storage SelfHeal.
>>>>>>> 95c048edc38 (docs: improve SelfHeal documentation (#50561))

## Enabling and disabling SelfHeal {#on-off}

You can enable and disable SelfHeal using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md).

To enable SelfHeal, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-self-heal
```

<<<<<<< HEAD
=======
`<bs_endpoint>` is the endpoint of any [storage node](../../concepts/glossary.md#storage-node) in the cluster.

>>>>>>> 95c048edc38 (docs: improve SelfHeal documentation (#50561))
To disable SelfHeal, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-self-heal
```

<<<<<<< HEAD
=======
### When to disable SelfHeal {#when-to-disable}

SelfHeal is normally left enabled. Temporarily disable it only when automatic relocation is riskier than waiting, for example if:

* an error in SelfHeal has been found that makes relocation create a risk of data loss;
* many nodes have failed at once, the cluster is overloaded, and additional background replication would increase the load and interfere with restoring cluster availability.

{% note warning %}

While SelfHeal is disabled, VDisks from faulty PDisks are not relocated automatically. Monitor the storage state and re-enable SelfHeal as soon as the cluster stabilizes.

{% endnote %}

>>>>>>> 95c048edc38 (docs: improve SelfHeal documentation (#50561))
## SelfHeal settings {#settings}

The parameters below control different stages of Sentinel operation: polling PDisk state, confirming a persistent state, and retries when sending a new status to the [Blob Storage Controller](../../concepts/glossary.md#ds-controller). For each state, the time until confirmation is the product of **State update interval** and the cycle limit for that state. For example, for most failure states the defaults are 60 seconds and 60 cycles, so the transition to `FAULTY` starts after about one hour. Configuration update intervals and status-change retries are not part of that product.

{% note warning %}

Do not change these parameters in normal operation. The defaults are chosen for typical clusters. Change them only if you understand how shifting the delay affects SelfHeal reaction time, for example on the advice of {{ ydb-short-name }} developers.

{% endnote %}

You can configure SelfHeal in **Viewer** → **Cluster Management System** → **CmsConfigItems**.

To create the initial settings, click **Create**. If you want to update the current settings, click ![pencil](../../_assets/pencil.svg).

You can use the following settings:

| **Parameter** | **Description** |
|:---------------------------------------- |:------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Status** | Enabling and disabling SelfHeal in CMS. |
<<<<<<< HEAD
| **Dry run** | Enables/disables the mode in which the CMS doesn't change the BSC setting. |
| **Config update interval (sec.)** | BSC configuration update interval. |
| **Retry interval (sec.)** | Interval of configuration update attempts. |
| **State update interval (sec.)** | PDisk state update interval. |
| **Timeout (sec.)** | PDisk state update timeout. |
| **Change status retries** | Number of retries to change the PDisk status for BSC (`ACTIVE`, `FAULTY`, `BROKEN`, and so on). |
| **Change status retry interval (sec.)** | Delay between retries to update the PDisk status in BSC. CMS monitors the status of the disk with the interval **State update inverval**. If the disk remains in one **Status update interval** state during several cycles, the CMS changes its status to BSC.<br/>Next are the settings for the number of update cycles after which the CMS changes the disk status. If the disk state is `Normal`, the disk status changes to `ACTIVE`. In other states, the disk switches to `FAULTY`.<br/>The `0` value disables status changes for the state (by default, this is set for `Unknown`).<br/>For example, with the default settings, if the CMS detects the `Initial` disk state for five `Status update interval` cycles which are 60 seconds each, the disk status changes to `FAULTY`. |
| **Default state limit** | For states with no setting specified, this value can be used by default. This value is also used for unknown PDisk states that don't have any settings. It's used if no value is set for states such as `Initial`, `InitialFormatRead`, `InitialSysLogRead`, `InitialCommonLogRead`, and `Normal`. |
| **Initial** | PDisk starts initializing. Transition to `FAULTY`. |
| **InitialFormatRead** | PDisk is reading its format. Transition to `FAULTY`. |
| **InitialFormatReadError** | PDisk received an error when reading its format. Transition to `FAULTY`. |
| **InitialSysLogRead** | PDisk is reading the system log. Transition to `FAULTY`. |
| **InitialSysLogReadError** | PDisk received an error when reading the system log. Transition to `FAULTY`. |
| **InitialSysLogParseError** | PDisk received an error when parsing and checking the consistency of the system log. Transition to `FAULTY`. |
| **InitialCommonLogRead** | PDisk is reading the common VDisk log. Transition to `FAULTY`. |
| **InitialCommonLogReadError** | PDisk received an error when reading the common VDisk log. Transition to `FAULTY`. |
| **InitialCommonLogParseError** | PDisk received an error when parsing and checking the consistency of the common log. Transition to `FAULTY`. |
| **CommonLoggerInitError** | PDisk received an error when initializing internal structures to be logged to the common log. Transition to `FAULTY`. |
| **Normal** | PDisk completed initialization and is running normally. Transition to `ACTIVE` will occur after a specified number of cycles (for example, if the disk is `Normal` for 5 minutes, it switches to `ACTIVE`). |
| **OpenFileError** | PDisk received an error when opening a disk file. Transition to `FAULTY`. |
| **Missing** | The node responds, but this PDisk is missing from its list. Transition to `FAULTY`. |
| **Timeout** | The node didn't respond within the specified timeout. Transition to `FAULTY`. |
| **NodeDisconnected** | The node has disconnected. Transition to `FAULTY`. |
| **Stopped** | PDisk has been stopped. Transition to `FAULTY`. |
| **Unknown** | Unexpected response, for example, `TEvUndelivered` to the state request. Transition to `FAULTY`. |
=======
| **Dry run** | Enabling and disabling the mode in which CMS does not change the BSC setting. |
| **Config update interval (sec.)** | Period of configuration updates from BSC. |
| **Retry interval (sec.)** | Period of retries for configuration updates. |
| **State update interval (sec.)** | Period of PDisk state updates. |
| **Timeout (sec.)** | Timeout for PDisk state updates. |
| **Change status retries** | Number of retries to change the PDisk status in BSC (`ACTIVE`, `FAULTY`, `BROKEN`, etc.). |
| **Change status retry interval (sec.)** | Delay between retries when submitting a new PDisk status to BSC. |
| **Default state limit** | For states for which no setting is specified, this "default" value can be used. For unknown PDisk states for which there is no setting, this value is also used. This value is used if the value is not set for states `Initial`, `InitialFormatRead`, `InitialSysLogRead`, `InitialCommonLogRead`, `Normal`. |
| **Initial** | PDisk starts initialization. Transitions to `FAULTY`. |
| **InitialFormatRead** | PDisk reads its format record. Transitions to `FAULTY`. |
| **InitialFormatReadError** | PDisk received an error while reading its format record. Transitions to `FAULTY`. |
| **InitialSysLogRead** | PDisk reads the system log. Transitions to `FAULTY`. |
| **InitialSysLogReadError** | PDisk received an error while reading the system log. Transitions to `FAULTY`. |
| **InitialSysLogParseError** | PDisk received an error while parsing or checking the consistency of the system log. Transitions to `FAULTY`. |
| **InitialCommonLogRead** | PDisk reads the common log of VDisks. Transitions to `FAULTY`. |
| **InitialCommonLogReadError** | PDisk received an error while reading the common log of VDisks. Transitions to `FAULTY`. |
| **InitialCommonLogParseError** | PDisk received an error while parsing or checking the consistency of the common log. Transitions to `FAULTY`. |
| **CommonLoggerInitError** | PDisk received an error while initializing internal structures intended for writing to the common log. Transitions to `FAULTY`. |
| **Normal** | PDisk has completed initialization and is operating normally. Transition to `ACTIVE` will occur after the specified number of cycles (for example, if `Normal` persists for 5 minutes, the disk transitions to state `ACTIVE`). |
| **OpenFileError** | PDisk received an error while opening the disk file. Transitions to `FAULTY`. |
| **Missing** | The node responds, but this PDisk is not in its list. Transitions to `FAULTY`. |
| **Timeout** | The node did not respond within the allotted timeout. Transitions to `FAULTY`. |
| **NodeDisconnected** | Node disconnection. Transitions to `FAULTY`. |
| **Stopped** | PDisk is stopped. Transitions to `FAULTY`. |
| **Unknown** | Unexpected response, for example, response `TEvUndelivered` to a state request. Transitions to `FAULTY`. |
>>>>>>> 95c048edc38 (docs: improve SelfHeal documentation (#50561))

## Working with donor disks {#disks}

The donor disk is the previous VDisk after the data transfer, which continues to store its data and only responds to read requests from the new VDisk. When data is transfered with donor disks enabled, previous VDisks continue to function until the data is fully moved to the new disks. To prevent data loss when moving a VDisk, enable donor disks:

```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-donor-mode
```

To disable donor disks, run the command:

```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-donor-mode
```
