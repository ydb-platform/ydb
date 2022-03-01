# Maintaining a cluster's disk subsystem

## Extending a cluster with static nodes {#expand_cluster}

1. Add information about new nodes to NameserviceConfig in the names.txt file.

    ```
    Node {
        NodeId: 1
        Port: <IR port>
        Host: "<old-host>"
        InterconnectHost: "<old-host>"
        Location {
            DataCenter: "DC1"
            Module: "M1"
            Rack: "R1"
            Unit: "U1"
        }
    }
    Node {
        NodeId: 2
        Port: <IR port>
        Host: "<new-host>"
        InterconnectHost: "<new-host>"
        Location {
            DataCenter: "DC1"
            Module: "M2"
            Rack: "R2"
            Unit: "U2"
        }
    }
    ClusterUUID: "<cluster-UUID>"
    AcceptUUID: "<cluster-UUID>"
    ```

1. Update the NameserviceConfig via CMS.

1. Add new nodes to DefineBox.

    Sample proto file for DefineBox.

    ```
    Command {
        DefineHostConfig {
            HostConfigId: 1
            Drive {
                Path: "<device-path>"
                Type: SSD
                PDiskConfig {
                    ExpectedSlotCount: 2
                }
            }
        }
    }
    Command {
        DefineBox {
            BoxId: 1
            Host {
                Key {
                    Fqdn: "<old-host>"
                    IcPort: <IR port>
                }
                HostConfigId: 1
            }
            Host {
                Key {
                    Fqdn: "<new-host>"|
                    IcPort: <IR port>
                }
                HostConfigId: 1
            }
        }
    }
    ```

    Using the command

    ```
    kikimr -s <endpoint> admin bs config invoke --proto-file DefineBox.txt
    ```

## Decommissioning nodes and disks

**WILL BE SOON**

## SelfHeal {#selfheal}

During cluster operation, individual block store volumes used by YDB or entire nodes may fail. To maintain the cluster's uptime and fault tolerance when it's impossible to promptly fix the failed nodes or volumes, YDB provides SelfHeal.

SelfHeal includes two parts. Detecting faulty disks and moving them carefully to avoid data loss and disintegration of storage groups.

SelfHeal is enabled by default.
Below are instructions how to enable or disable SelfHeal.

1. Enabling detection.

    Open the page

    ```http://localhost:8765/cms#show=config-items-25```

    It can be enabled via the viewer -> Cluster Management System -> CmsConfigItems

    Status field: Enable

    Or via the CLI

    * Go to any node.

    * Create a file with modified configs.

        Sample config.txt file.

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

    * Update the config on the cluster.

        ```bash
        kikimr admin console configs update config.txt
        ```

1. Enable SelfHeal.

    ```bash
    kikimr -s <endpoint> admin bs config invoke --proto 'Command{EnableSelfHeal{Enable: true}}'
    ```

### SelfHeal settings

viewer -> Cluster Management System -> CmsConfigItems
If there are no settings yet, click Create, if there are, click the "pencil" icon in the corner.

* **Status**:  Enables/disables Self Heal in the CMS.
* **Dry run**: Enables/disables the mode in which the CMS doesn't change the BSC setting.
* **Config update interval (sec.)**: BSC config update interval.
* **Retry interval (sec.)**: Config update retry interval.
* **State update interval (sec.)**: PDisk state update interval, the State is what we're monitoring (through a whiteboard, for example).
* **Timeout (sec.)**: PDisk state update timeout.
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
* **InitialCommonnLogRead**: PDisk is reading the common VDisk log. Transition to FAULTY.
* **InitialCommonnLogReadError**: PDisk has received an error when reading the common Vdisk log. Transition to FAULTY.
* **InitialCommonnLogParseError**: PDisk has received an error when parsing and checking the consistency of the common log Transition to FAULTY.
* **CommonLoggerInitError**: PDisk has received an error when initializing internal structures to be logged to the common log. Transition to FAULTY.
* **Normal**: PDisk has completed initialization and is running normally. Transition to ACTIVE will occur after this number of Cycles (that is, by default, if the disk is Normal for 5 minutes, it's switched to ACTIVE).
* **OpenFileError**: PDisk has received an error when opening a disk file. Transition to FAULTY.
* **Missing**: The node responds, but this PDisk is missing from its list. Transition to FAULTY.
* **Timeout**: The node didn't respond within the specified timeout. Transition to FAULTY.
* **NodeDisconnected**: The node has disconnected. Transition to FAULTY.
* **Unknown**: Something unexpected, for example, the TEvUndelivered response to the state request. Transition to FAULTY.

## Enabling/disabling donor disks

If donor disks are disabled, when transferring the Vdisk, its data is lost and has to be restored according to the selected erasure.

The recovery operation is more expensive than regular data transfers. Data loss also occurs, which may lead to data loss when going beyond the failure model.

To prevent the above problems, there are donor disks.

When transferring disks with donor disks enabled, the old VDisk remains alive until the new one transfers all the data from it to itself.

The donor disk is the old VDisk after the transfer, which continues to store its data and only responds to read requests from the new VDisk.

When receiving a request to read data that the new VDisk has not yet transferred, it redirects the request to the donor disk.

To enable the donor disks, run the following command:

`$ kikimr admin bs config invoke --proto 'Command { UpdateSettings { EnableDonorMode: true } }'`

Similarly, when changing the setting to `false`, the command to disable the mode.

## Scrubbing

### Enabling/Disabling

**WILL BE SOON**

### Scrubbing settings

The Scrub settings let you adjust the interval from the beginning of the previous disk scrubbing cycle to that of the next one and the maximum number of disks that can be scrubbed simultaneously. The default value is 1 month.
`$ kikimr admin bs config invoke --proto 'Command { UpdateSettings { ScrubPeriodicitySeconds: 86400 MaxScrubbedDisksAtOnce: 1 } }'`

**WILL BE SOON**

## Move one vdisk from a block store volume {#moving_vdisk}

To move a disk from a block store volume, log in to the node via ssh and execute the following command:

```bash
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <Storage group ID> GroupGeneration: <Storage group generation> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Slot number> } }'
```

You can find the parameters for the command in the viewer (link).

## Move vdisks from a broken/missing block store volume {#removal_from_a_broken_device}

If SelfHeal is disabled or fails to move vdisks, you'll have to run this operation manually.

1. Make sure that the disk has actually failed.

    Write down the node's fqdn, ic-port, disk path, and pdiskId.

1. Go to any cluster node.

1. Move the disk.

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<Host>" IcPort: <IC Port>} Path: "<Path to the device part label>" PDiskId: <ID PDisk> Status: BROKEN } }'|||UNTRANSLATED_CONTENT_END|||
    ```

## Enable the disk back after reassignment  {#return_a_device_to_work}

1. In Monitoring, make sure that the disk is actually operable.

    Write down the node's fqdn, ic-port, disk path, and pdiskId.

1. Go to any cluster node.

1. Re-enable the disk.

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<Host>" IcPort: <IC Port>} Path: "<Path to the storage volume part label>" PDiskId: <PDisk ID> Status: ACTIVE } }'
    ```

## Stopping/restarting a YDB process on a node {#restart_process}

To make sure that the process is stoppable, follow these steps.

1. Access the node via ssh.

1. Execute the command

    ```bash
    kikimr cms request restart host {node_id} --user {user} --duration 60 --dry --reason 'some-reason'
    ```

    If the process is stoppable, you'll see `ALLOW`.

1. Stop the process

    ```bash
    sudo service kikimr stop
    ```

1. Restart the process if needed

   ```bash
    sudo service kikimr start
   ```

## Replacing equipment {#replace_hardware}

Before replacing equipment, make sure that the YDB process is [stoppable](#restart_process).
If the replacement is going to take a long time, first move all the vdisks from this node and wait until replication is complete.
After replication is complete, you can safely shut down the node.

To disable a dynamic node, you may also need to drain the tablets to avoid the effect on running queries.

Go to a hive or tenant hive's web monitoring page.
Click "View Nodes" to see a list of all nodes running under this hive.

It provides various information about running tablets and resources used.
On the right of the list, there are buttons with the following actions for each node:

* **Active**: Enables/disables the node to move tablets to this node.
* **Freeze**: Disables tablets to be deployed on other nodes.
* **Kick**: Moves all tablets from the node at once.
* **Drain**: Smoothly moves all tablets from the node.

Before disabling the node, first disable the transfer of tablets through the Active button, then click Drain, and wait for all the tablets to be moved away.

## Adding storage groups

To add storage groups, you need to redefine the config of the pool you want to extend.

Before that, you need to get the config of the desired pool. You can do this with the following command:

```proto
Command {
  ReadStoragePool{
    BoxId: <box-id>
    // StoragePoolId: <storage-pool-id>
    Name: <pool name>
  }
}
```

```
kikimr -s <endpoint> admin bs config invoke --proto-file ReadStoragePool.txt
```

Insert the obtained pool config into the protobuf below and edit the **NumGroups** field value in it.

```proto
Command {
  DefineStoragePool {
    <pool config>
  }
}
```

```
kikimr -s <endpoint> admin bs config invoke --proto-file DefineStoragePool.txt
```

## Changing the number of slots for VDisks on the PDisk

To add storage groups, redefine the host config by increasing the number of disk slots for it.

Before that, you need to get the config to be changed. You can do this with the following command:

```proto
Command {
  TReadHostConfig{
    HostConfigId: <host-config-id>
  }
}
```

```
kikimr -s <endpoint> admin bs config invoke --proto-file ReadHostConfig.txt
```

Insert the obtained config into the protobuf below and edit the **PDiskConfig/ExpectedSlotCount** field value in it.

```proto
Command {
  TDefineHostConfig {
    <host config>
  }
}
```

```
kikimr -s <endpoint> admin bs config invoke --proto-file DefineHostConfig.txt
```

## Cluster health issues {#cluster_liveness_issues}

### No more than 2 disks belonging to the block-4-2 storage group failed {#storage_group_lost_two_disk}

In this kind of failure, no data is lost, the system maintains operability, and read and write queries are executed successfully. Performance might degrade because of the load handover from the failed disks to the operable ones.

If 2 disks are unavailable at the same time, we recommend reviving at least one of them or replacing one disk to start the replication process. This will provide some room for maneuver in the event a third disk fails before replication completes.

### More than 2 disks belonging to the block-4-2 storage group failed{#exceeded_the_failure_modele}

The availability and operability of the system might be lost. Make sure to revive at least one disk without losing the data stored on it.

## Disk subsystem issues {#storage_issues}

When the disk space is used up, the database may start responding to all queries with an error. To keep the database healthy, we recommend deleting a part of the data or adding block store volumes to extend the cluster.

Below are instructions that can help you add or free up disk space.

### Defragment vdisk

As disks run, their data becomes fragmented. You can find out the disk's fragmentation rate on the vdisk monitoring page. We don't recommend defragmenting disks with a fragmentation ratio of 20% or less.

According to the failure model, the cluster survives the loss of two vdisks in the same group without data loss. If all vdisks in the group are up and there are no vdisks with the error or replication status, then deleting data from one vdisk will result in the vdisk recovering it in a compact format. Please keep in mind that data storage redundancy will be decreased until automatic data replication is complete.

During data replication, the load on all the group's vdisks increases, and response times may deteriorate.

1. View the fragmentation coefficient on the vdisk page in the viewer (link).

    If its value is more than 20%, defragmentation can help free up disk space.

1. Check the status of the group that hosts the vdisk. There should be no vdisks that are unavailable or in the error or replication status in the group.

    You can view the status of the group in the viewer (link).

1. Run the wipe command for the vdisk.

    All data stored by the vdisk will be permanently deleted, and after that, the vdisk will begin restoring the data by reading it from the other vdisks in the group.

    ```bash
    kikimr admin blobstorage group reconfigure wipe --domain <Domain number> --node <Node ID> --pdisk <PDisk ID> --vslot <Slot number>
    ```

    You can view the details for the command in the viewer (link).

If the block store volume is running out of space, you can apply defragmentation to the entire block store volume.

1. Check the health of the groups in the cluster. There shouldn't be any problem groups on the same node with the problem block store volume.

1. Log in via SSH to the node hosting this disk.

1. Check if it is possible to restart the process (link to the maintenance file).

1. Stop the process.

    ```bash
    sudo systemctl stop kikimr
    ```

1. Format the disk.

    ```bash
    sudo kikimr admin blobstorage disk obliterate <path to the store volume part label>
    ```

1. Run the process.

    ```bash
    sudo systemctl start kikimr
    ```

### Distribute vdisks evenly across block store volumes

If vdisks aren't evenly distributed across block store volumes, you can [move them](#moving_vdisks) one at a time from overloaded store volumes.

### Distribute the load evenly across groups

At the bottom of the hive web monitoring page, there is a button named "Reassign Groups".
Click it to open the window with parameters for balancing:

* **Storage pool**: Pool of storage groups for balancing.
* **Storage group**: If the previous item is not specified, you can specify only one group separately.
* **Type**: Type of tablets that balancing will be performed for.
* **Channels**: Range of channels that balancing will be performed for.
* **Percent**: Percentage of the total number of tablet channels that will be moved as a result of balancing.
* **Inflight**: The number of tablets being moved to other groups at the same time.

After specifying all the parameters, click "Query" to get the number of channels moved and unlock the "Reassign" button.
Clicking this button starts balancing.

## Changing settings in the CMS

### Get the current settings

The following command will let you get the current settings for a cluster or tenant.

```
./kikimr -s <endpoint> admin console configs load --out-dir <directory-for-configs>
```

```
./kikimr -s <endpoint> admin console configs load --out-dir <directory-for-configs> --tenant <tenant-name>
```

### Update the settings

First, you need to pull the desired config as indicated above and then prepare a protobuf file with an update request.

```
Actions {
  AddConfigItem {
    ConfigItem {
      Cookie: "<cookies>"
      UsageScope {
        TenantAndNodeTypeFilter {
          Tenant: "<tenant-name>"
        }
      }
      Config {
          <config-name> {
              <full config>
          }
      }
    }
  }
}
```

The UsageScope field is optional and is needed to use settings for a specific tenant.

```
./kikimr -s <endpoint> admin console configs update <file-with-settings>
```

## Changing an actor system's configuration

### On static nodes

Static nodes take the configuration of the actor system from a file located at kikimr/cfg/sys.txt.

After changing the configuration, restart the node.

### On dynamic nodes

Dynamic nodes take the configuration from the CMS. To change it, you can use the following command.

```proto
ConfigureRequest {
  Actions {
    AddConfigItem {
      ConfigItem {
        // UsageScope: { ... }
        Config {
          ActorSystemConfig {
            <actor system config>
          }  
        }
        MergeStrategy: 3
      }
    }
  }
}
```

kikimr -s <endpoint> admin console execute --domain=<domain> --retry=10 actorsystem.txt

```
