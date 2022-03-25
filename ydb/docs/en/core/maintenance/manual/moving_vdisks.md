# Moving VDisks

## Move a VDisk from a block store volume {#moving_vdisk}

To move a VDisk from a block store volume, log in to the node via SSH and run the command:

```bash
kikimr admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <Storage group ID> GroupGeneration: <Storage group generation> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Slot number> } }'
```

You can find the parameters for the command in the viewer (link).

## Move VDisks from a broken/missing block store volume {#removal_from_a_broken_device}

If SelfHeal is disabled or fails to move VDisks, you'll have to run this operation manually.

1. Make sure that the VDisk has actually failed.

    Write down the node's FQDN, ic-port, VDisk path, and pdisk-id

2. Go to any cluster node

3. Move the VDisk

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-port>} Path: "<Path to the storage volume part label>" PDiskId: <pdisk-id> Status: BROKEN } }'
    ```

## Enable the VDisk back after reassignment {#return_a_device_to_work}

1. In Monitoring, make sure that the PDisk is actually operable

    Write down the node's FQDN, ic-port, store path, and pdisk-id

2. Go to any cluster node

3. Enable the PDisk back

    ```bash
    kikimr admin bs config invoke --proto 'Command { UpdateDriveStatus { HostKey: { Fqdn: "<host>" IcPort: <ic-port>} Path: "<Path to the storage volume part label>" PDiskId: <pdisk-id> Status: ACTIVE } }'
    ```

