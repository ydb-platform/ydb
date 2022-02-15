# Добавление групп хранения

Для добавления групп хранения требуется переопределить конфиг пула в котором требуется расширить.

Перед этим требуется получить конфиг интересуемого пула, это можно сделать следующей командой:

```proto
Command {
  ReadStoragePool{
    BoxId: <box-id>
    // StoragePoolId: <storage-pool-id>
    Name: <имя пула>
  }
}
```
    
```
kikimr -s <ендпоинт> admin bs config invoke --proto-file ReadStoragePool.txt
```

Требуется вставить полученный конфиг пула в протобуф ниже и поменять в нем поле **NumGroups**.

```proto
Command {
  DefineStoragePool {
    <конфиг пула>
  }
}
```
    
```
kikimr -s <ендпоинт> admin bs config invoke --proto-file DefineStoragePool.txt
```
