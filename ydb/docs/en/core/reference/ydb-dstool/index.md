# {{ ydb-short-name }} DSTool overview

With the {{ ydb-short-name }} DSTool utility, you can manage your {{ ydb-short-name }} cluster's disk subsystem. To install and configure the utility, follow the [instructions](install.md).

{{ ydb-short-name }} DSTool includes the following commands:

| Command | Description |
--- | ---
| [device list](device-list.md) | List storage devices. |
| pdisk add-by-serial | Add a PDisk to a set by serial number. |
| pdisk remove-by-serial | Remove a PDisk from the set by serial number. |
| pdisk set | Set PDisk parameters. |
| pdisk list | List PDisks. |
| vdisk evict | Move VDisks to different PDisks. |
| vdisk remove-donor | Remove a donor VDisk. |
| vdisk wipe | Wipe VDisks. |
| vdisk list | List VDisks. |
| group add | Add storage groups to a pool. |
| group check | Check storage groups. |
| group show blob-info | Display blob information. |
| group show usage-by-tablets | Display information about tablet usage by groups. |
| group state | Show or change a storage group's state. |
| group take-snapshot | Take a snapshot of storage group metadata. |
| group list | List storage groups. |
| pool list | List pools. |
| box list | List sets of PDisks. |
| node list | List nodes. |
| cluster balance | Move VDisks from overloaded PDisks. |
| cluster get | Show cluster parameters. |
| cluster set | Set cluster parameters. |
| cluster workload run | Run a workload to test the failure model. |
| cluster list | Display cluster information. |
