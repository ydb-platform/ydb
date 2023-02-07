# Добавление групп хранения

{% if  feature_ydb-tool %}

Добавить новые группы можно с помощью команды ```dstool group add```. Количество новых групп
задается с помощью опции ```--groups```. Например, чтобы добавить 10 групп в пул ```/ydb_pool```,
нужно ввести команду:

```
ydb-dstool.py -e ydb.endpoint group add --pool-name /ydb_pool --groups 10
```

В случае успеха команда вернет нулевой ```exit status```. Иначе каманда вернет ненулевой статус и
выведет сообщение об ошибке в ```stderr```.

Чтобы попробовать добавить группы без фактического добавления, можно воспользоваться глобальной
опцией ```dry-run```. Например, чтобы попробовать добавить 100 групп в пул ```/ydb_pool```,
нужно ввести команду:

```
ydb-dstool.py --dry-run -e ydb.endpoint group add --pool-name /ydb_pool --groups 100
```

Опция ```dry-run``` позволяет, например, оценить какое максимальное число групп можно добавить в пул.

{% else %}

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
kikimr -s <endpoint> admin bs config invoke --proto-file ReadStoragePool.txt
```

Требуется вставить полученный конфиг пула в protobuf ниже и поменять в нем поле **NumGroups**.

```proto
Command {
  DefineStoragePool {
    <конфиг пула>
  }
}
```
    
```
kikimr -s <endpoint> admin bs config invoke --proto-file DefineStoragePool.txt
```

{% endif %}
