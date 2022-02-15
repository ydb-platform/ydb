# Изменение количествa слотов для вдисков на пдисках

Для добавления групп хранения требуется переопределить конфиг хоста, увеличив для него количество слотов на дисках.

Перед этим требуется получить изменяемые конфиг, это можно сделать следующей командой:

```proto
Command {
  TReadHostConfig{
    HostConfigId: <host-config-id>
  }
}
```
    
```
kikimr -s <ендпоинт> admin bs config invoke --proto-file ReadHostConfig.txt
```

Требуется вставить полученный конфиг в протобуф ниже и поменять в нем поле **PDiskConfig/ExpectedSlotCount**.

```proto
Command {
  TDefineHostConfig {
    <хост конфиг>
  }
}
```
    
```
kikimr -s <ендпоинт> admin bs config invoke --proto-file DefineHostConfig.txt
```
