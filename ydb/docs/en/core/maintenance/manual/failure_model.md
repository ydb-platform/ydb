# Staying within the failure model

## One VDisk in the storage group failed {#storage_group_lost_one_disk}

With SelfHeal enabled, this situation is considered normal. SelfHeal will move the VDisk over the time specified in the settings, then data replication will start on a different PDisk.

If SelfHeal is disabled, you'll have to manually move the VDisk. Before moving it, make sure that **only one** VDisk in the storage group has failed.
Then follow the [instructions](moving_vdisks.md#removal_from_a_broken_device).

## More than one VDisk in the same storage group have failed without going beyond the failure model {#storage_group_lost_more_than_one_disk}

In this kind of failure, no data is lost, the system maintains operability, and read and write queries are executed successfully. Performance might degrade because of the load handover from the failed disks to the operable ones.

If multiple VDisks have failed in the group, SelfHeal stops moving VDisks. If the maximum number of failed VDisks for the failure model has been reached, recover at least one VDisk before you start [moving the VDisks](moving_vdisks.md#removal_from_a_broken_device). You may also need to be more careful when [moving VDisks one by one](moving_vdisks.md#moving_vdisk).

## The number of failed VDisks has exceeded the failure model {#exceeded_the_failure_model}

The availability and operability of the system might be lost. Make sure to revive at least one VDisk without losing the data stored on it.
