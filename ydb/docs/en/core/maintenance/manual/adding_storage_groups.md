# Adding storage groups

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

