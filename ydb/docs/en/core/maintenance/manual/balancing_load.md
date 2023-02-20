# Disk load balancing

{{ ydb-short-name }} supports two methods for the disk load distribution:

## Distribute the load evenly across groups

At the bottom of the [Hive web-viewer](../embedded_monitoring/hive.md#reassign_groups) page there is a button named "Reassign Groups".

## Distribute VDisks evenly across block store volumes

If VDisks aren't evenly distributed across block store volumes, you can [move them](moving_vdisks.md#moving_vdisk) one at a time from overloaded store volumes.

## Changing the number of slots for VDisks on PDisks

To add storage groups, redefine the host config by increasing the number of slots on PDisks.

Before that, you need to get the config to be changed. You can do this with the following command:

```proto
Command {
  TReadHostConfig{
    HostConfigId: <host-config-id>
  }
}
```

```bash
ydbd -s <endpoint> admin bs config invoke --proto-file ReadHostConfig.txt
```

Insert the obtained config into the protobuf below and edit the **PDiskConfig/ExpectedSlotCount** field value in it.

```proto
Command {
  TDefineHostConfig {
    <host config>
  }
}
```

```bash
ydbd -s <endpoint> admin bs config invoke --proto-file DefineHostConfig.txt
```

