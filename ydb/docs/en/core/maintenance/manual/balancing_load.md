# Disk load balancing

{{ ydb-short-name }} supports two methods for disk load balancing:

* [Distribute the load evenly across groups](#reassign-groups).
* [Distribute VDisks evenly across block store volumes](#cluster-balance).

## Distribute the load evenly across groups {#reassign-groups}

At the bottom of the [Hive web-viewer](../../reference/embedded-ui/hive.md#reassign_groups) page, there is a button named "Reassign Groups".

## Distribute VDisks evenly across block store volumes {#cluster-balance}

As a result of some operations, such as [decommissioning](../../devops/manual/decommissioning.md), VDisks can be distributed across block store volumes unevenly. You can distribute them more evenly in one of the following ways:

* [Move VDisks](moving_vdisks.md#moving_vdisk) one by one from overloaded block store volumes.
* Use [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md). The command below moves a VDisk from an overloaded block store volume to a less loaded one:

   ```bash
   ydb-dstool -e <bs_endpoint> cluster balance
   ```

   The command moves a single VDisk per run.

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
