# Расширение кластера

Расширение кластера производится с помощью Ansible и Terraform в несколько шагов:
1. [Изменение конфигурационного файла {{ ydb-short-name }}](#ydb-conf-edit)
2. Перезагрузка нод кластера 
3. Добавление новых нод в кластер

Рекомендуется добавлять ноды в кластер группами (от трех нод), с указанием единого `data_center`.

## Изменение конфигурационного файла {{ ydb-short-name }} {#ydb-conf-edit}

При создании кластера Ansible загружает конфигурационный файл `ydb-ansible-examples/<ansible-examples>/files/config.yaml` на ноды, далее он используется {{ ydb-short-name }} при запуске. О том из каких разделов состоит конфигурационный файл можно узнать из статьи [{#T}](../../deploy/configuration/config.md). Для того, чтобы уже настроенный кластер смог увидеть новые ноды – их нужно добавить в раздел `hosts` по аналогии с уже существующими нодами. 

Например, добавление трех новых нод в раздел `hosts` выглядит так:  
```
hosts:
... 
- host: static-node-10.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 10
    data_center: 'zone-a'
    rack: '10'    
- host: static-node-11.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 11
    data_center: 'zone-a'
    rack: '11'     
- host: static-node-12.ydb-cluster.com
  host_config_id: 1
  walle_location:
    body: 12
    data_center: 'zone-a'
    rack: '12'
...  
```

Новые ноды добавляются в один `data_center` – это обеспечивает их Подробнее 

https://github.com/ydb-platform/ydb-ansible-examples/blob/main/9-nodes-mirror-3-dc/files/config.yaml