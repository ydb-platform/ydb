# Working with SelfHeal

During cluster operation, entire nodes or individual block devices on which {{ ydb-short-name }} runs may fail.

SelfHeal is used to maintain cluster availability and fault tolerance if failed nodes or devices cannot be quickly restored.

SelfHeal allows you to:

* Detect faulty system components.
* Move faulty components in a gentle manner without data loss or disbanding storage groups.

SelfHeal is enabled by default.

The {{ ydb-short-name }} component responsible for SelfHeal is called Sentinel.

## Enabling and disabling SelfHeal {#on-off}

You can enable and disable SelfHeal using the [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md) utility.

To enable SelfHeal, run the command:


```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-self-heal
```


`<bs_endpoint>` is the endpoint of any [storage node](../../concepts/glossary.md#storage-node) in the cluster.

To disable SelfHeal, run the command:


```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-self-heal
```


## SelfHeal settings {#settings}

You can configure SelfHeal in **Viewer** → **Cluster Management System** → **CmsConfigItems**.

To create settings for the first time, click **Create**. If you need to change existing settings, click the ![pencil](../../_assets/pencil.svg) button.

The following settings are available:

| **Parameter** | **Description** |
| :--- | :--- |
| **Status** | Enabling and disabling SelfHeal in CMS. |
| **Dry run** | Enabling and disabling the mode in which CMS does not change the BSC setting. |
| **Config update interval (sec.)** | Period of configuration updates from BSC. |
| **Retry interval (sec.)** | Period of retries for configuration updates. |
| **State update interval (sec.)** | Period of PDisk state updates. |
| **Timeout (sec.)** | Timeout for PDisk state updates. |
| **Change status retries** | Number of retries to change the PDisk status in BSC (`ACTIVE`, `FAULTY`, `BROKEN`, etc.). |
| **Change status retry interval (sec.)** | Delay between attempts to change the PDisk status in BSC. CMS monitors the disk state at an interval of **State update interval**. If the disk remains in one state for several **Status update interval** cycles, CMS changes its status in BSC.<br/>Next are the settings for the number of update cycles after which CMS will change the disk status. If the disk state is `Normal`, the disk is moved to status `ACTIVE`; in other states, the disk is moved to status `FAULTY`.<br/>The value `0` disables status change for the state (as implemented for `Unknown` by default).<br/>For example, with default settings, if CMS observes disk state `Initial` for 5 `Status update interval` cycles of 60 seconds each, the disk status will be changed to `FAULTY`. |
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

## Working with donor disks {#disks}

A donor disk is a previous VDisk after data migration that continues to store its data and only responds to read requests from the new VDisk. When migrating with donor disks enabled, previous VDisks continue to function until the data is fully migrated to new disks. To prevent data loss during VDisk migration, enable the use of donor disks:


```bash
ydb-dstool -e <bs_endpoint> cluster set --enable-donor-mode
```


To disable donor disks, enter the command:


```bash
ydb-dstool -e <bs_endpoint> cluster set --disable-donor-mode
```
