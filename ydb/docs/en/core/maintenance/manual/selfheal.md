# Enabling/disabling SelfHeal {#selfheal}

During cluster operation, individual block store volumes used by YDB or entire nodes may fail. To maintain the cluster's uptime and fault tolerance when it's impossible to promptly fix the failed nodes or volumes, YDB provides SelfHeal.

SelfHeal includes two parts. Detecting faulty disks and moving them carefully to avoid data loss and disintegration of storage groups.

SelfHeal is enabled by default.
Below are instructions how to enable or disable SelfHeal.

1. Enabling detection

    Open the page

    ```http://localhost:8765/cms#show=config-items-25```

    It can be enabled via the viewer -> Cluster Management System -> CmsConfigItems

    Status field: Enable

    Or via the CLI

    * Go to any node

    * Create a file with modified configurations

        Sample config.txt file

        ```
        Actions {
            AddConfigItem {
                ConfigItem {
                    Config {
                        CmsConfig {
                            SentinelConfig {
                                Enable: true
                            }
                        }
                    }
                }
            }
        }
        ```

    * Update the config on the cluster

        ```bash
        kikimr admin console configs update config.txt
        ```

2. Enable SelfHeal

    ```bash
    kikimr -s <endpoint> admin bs config invoke --proto 'Command{EnableSelfHeal{Enable: true}}'
    ```

Disabled in a similar way by setting the value to false.

### SelfHeal settings

viewer -> Cluster Management System -> CmsConfigItems
If there are no settings yet, click Create, if there are, click the "pencil" icon in the corner.

* **Status**: Enables/disables Self Heal in the CMS.
* **Dry run**: Enables/disables the mode in which the CMS doesn't change the BSC setting.
* **Config update interval (sec.)**: BSC config update interval.
* **Retry interval (sec.)**: Config update retry interval.
* **State update interval (sec.)**: PDisk state update interval, the State is what we're monitoring (through a whiteboard, for example)
* **Timeout (sec.)**: PDisk state update timeout
* **Change status retries**: The number of retries to change the PDisk Status in the BSC, the Status is what is stored in the BSC (ACTIVE, FAULTY, BROKEN, and so on).
* **Change status retry interval (sec.)**: Interval between retries to change the PDisk Status in the BSC. The CMS is monitoring the disk state with the **State update interval**. If the disk remains in the same state for several **Status update interval** cycles, the CMS changes its Status in the BSC.
Next are the settings for the number of update cycles through which the CMS will change the disk Status. If the disk State is Normal, the disk is switched to the ACTIVE Status. Otherwise, the disk is switched to the FAULTY status. The 0 value disables changing the Status for the state (this is done for Unknown by default).
For example, with the default settings, if the CMS is monitoring the state of the Initial disk for 5 Status update interval cycles of 60 seconds each, the disk Status will be changed to FAULTY.
* **Default state limit**: For States with no setting specified, this default value can be used. For unknown PDisk States that have no setting, this value is used, too. This value is used if no value is set for States such as Initial, InitialFormatRead, InitialSysLogRead, InitialCommonLogRead, and Normal.
* **Initial**: PDisk starts initializing. Transition to FAULTY.
* **InitialFormatRead**: PDisk is reading its format. Transition to FAULTY.
* **InitialFormatReadError**: PDisk has received an error when reading its format. Transition to FAULTY.
* **InitialSysLogRead**: PDisk is reading the system log. Transition to FAULTY.
* **InitialSysLogReadError**: PDisk has received an error when reading the system log. Transition to FAULTY.
* **InitialSysLogParseError**: PDisk has received an error when parsing and checking the consistency of the system log. Transition to FAULTY.
* **InitialCommonLogRead**: PDisk is reading the common VDisk log. Transition to FAULTY.
* **InitialCommonLogReadError**: PDisk has received an error when reading the common VDisk log. Transition to FAULTY.
* **InitialCommonLogParseError**: PDisk has received an error when parsing and checking the consistency of the common log. Transition to FAULTY.
* **CommonLoggerInitError**: PDisk has received an error when initializing internal structures to be logged to the common log. Transition to FAULTY.
* **Normal**: PDisk has completed initialization and is running normally. Transition to ACTIVE will occur after this number of Cycles (that is, by default, if the disk is Normal for 5 minutes, it's switched to ACTIVE).
* **OpenFileError**: PDisk has received an error when opening a disk file. Transition to FAULTY.
* **Missing**: The node responds, but this PDisk is missing from its list. Transition to FAULTY.
* **Timeout**: The node didn't respond within the specified timeout. Transition to FAULTY.
* **NodeDisconnected**: The node has disconnected. Transition to FAULTY.
* **Unknown**: Something unexpected, for example, the TEvUndelivered response to the state request. Transition to FAULTY.

## Enabling/disabling donor disks

If donor disks are disabled, when moving the VDisk, its data is lost and has to be restored according to the selected erasure.

The recovery operation is more expensive than regular data transfers. Data loss also occurs, which may lead to data loss when going beyond the failure model.

To prevent the above problems, there are donor disks.

When transferring disks with donor disks enabled, the old VDisk remains alive until the new one transfers all the data from it to itself.

The donor disk is the old VDisk after the transfer, which continues to store its data and only responds to read requests from the new VDisk.

When receiving a request to read data that the new VDisk has not yet transferred, it redirects the request to the donor disk.

To enable the donor disks, run the following command:

`$ kikimr admin bs config invoke --proto 'Command { UpdateSettings { EnableDonorMode: true } }'`

Similarly, when changing the setting to `false`, the command disables the mode.

