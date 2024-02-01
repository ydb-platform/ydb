В инструкции изложен процесс развертывания {{ ydb-short-name }} кластера на группе серверов с помощью Ansible. Минимальное количество серверов в кластере – восемь штук для модели избыточности `block-4-2` и девять серверов для модели избыточности `mirror-3-dc`. В процессе эксплуатации кластер может быть [расширен](../../maintenance/manual/cluster_expansion.md) без приостановки доступа пользователей к базам данных.

{% note info %}

**Серверы должны соответствовать следующим требованиям** (на каждый сервер):
* 16 CPU (рассчитывается исходя из утилизации 8 CPU сторадж нодой и 8 CPU динамической нодой).
* 8 GB RAM (рекомендуемый минимальный объем RAM).
* Дополнительный сетевой SSD-диск размером 120 GB (не может быть меньшего размера – требования инсталляции YDB).
* Доступ по SSH.
* Сетевая связность машин в кластере.
* OS: Ubuntu 18+, Debian 9+.

{% endnote %}

Скачать репозиторий с плейбуками для установки {{ ydb-short-name }} на кластер можно с GitHub – `git clone --single-branch --branch refactor-use-collections https://github.com/ydb-platform/ydb-ansible.git`. Будут скачены: роли, плейбуки, make файлы и докер-файл для запуска Ansible в контейнере, по желанию. 

Для работы с плейбуками на локальной (промежуточной или инсталляционной) машине понадобится:
* Python 3 версии 3.10+.
* Ansible core не ниже версии 2.15.2.
* Менеджер пакетов Python pip3.
* Менеджер виртуальных окружений Python pipenv.

Если Python, Ansible и pipenv уже установлены – можно переходить к шагу [«Настройка Ansible проекта»](#ansible-project-setup), если окружение не соответствует предъявляемым требованиям – выполните следующие шаги:
* Обновите список пакетов apt репозитория командой `sudo apt update`.
* Создайте директорию, куда будет скачен репозиторий с плейбуками Ansible и создано виртуальное окружения – `mkdir <project name>`.
* Создайте виртуальное окружение командой `pipenv --python /usr/bin/python3`. Команда создаст виртуальное окружение с Python3 версии, установленной в системе. Если `pipenv` не установлен, выполните команду `pip3 install pipenv`.
* Скачайте репозиторий командой `git clone https://github.com/ydb-platform/ydb-ansible.git`.
* Перейдите в директорию `/examples/9-nodes-mirror-3-dc` и установите необходимые пакеты внутрь виртуального окружения командой `pipenv install -r requitements.txt`.

## Настройка Ansible проекта { #ansible-project-setup } 

Основная рабочая директория в скаченном репозитории – это `examples/9-nodes-mirror-3-dc`. В ней находятся следующие файлы и поддиректории:
* `ansible.cfg` – конфигурационный файл Ansible, который содержит настройки подключения к серверам и опции структуры проекта;
* `setup_playbook.yaml` – плейбук, который запускает роли установки и настройки {{ ydb-short-name }} на кластере. Роли располагаются в директории `/roles` (верхний уровень скаченного репозитория).
* `inventory/` – директория, в которой находятся два инвентаризационных файла: 
    + `50-inventory.yaml` – основной инвентаризационный файл, содержащий перечень хостов для подключения и переменные для установки и настройки YDB;
    + `99-inventory-vault.yaml` – зашифрованный инвентаризационный файл для безопасной доставки пароля root пользователя YDB;
* `files` – директория, содержащая файлы, которые будут копироваться на хосты и файлы нужные для подключения Ansible:
    + `ydb-ca-update.sh` – скрипт для генерации ключей и сертификатов безопасности;
    + `ydb-ca-nodes.txt` – текстовый файл со списком FQDN нод для генерации сертификатов безопасности; 
    + `config.yaml` – конфигурационный файл YDB;

Настройка Ansible проекта может производиться в следующем порядке:
1. Находясь в директории проекта, активируйте виртуальное окружение командой `pipenv shell`.
2. Выполните команду `ansible-galaxy collection install git+https://github.com/ydb-platform/ydb-ansible.git,refactor-use-collections` для установки `ydb_platform.ydb` и `community.general` коллекций (формат распространения связанного набора плейбуков, ролей, модулей и плагинов). В результате выполнения команды в терминал будет выведен путь установки коллекций:
  ```
  ...
  Starting collection install process
  Installing 'ydb_platform.ydb:0.0.1' to '/Users/pseudolukian/.ansible/collections/ansible_collections/ydb_platform/ydb'
  ...
  ```
3. Скопируйте путь установки коллекций и вставьте его в конфигурационный файл Ansible (`examples/9-nodes-mirror-3-dc/ansible.cfg`):
  ```
  [defaults]
  collections_paths = /home/ubuntu/.ansible/collections/ansible_collections
  ```
4. Скопируйте приватную часть SSH-ключа для доступа к серверам кластера {{ ydb-short-name }} в директорию `examples/9-nodes-mirror-3-dc/files`.
5. Укажите hostname и FQDN серверов кластера YDB в файле `examples/9-nodes-mirror-3-dc/files/ydb-ca-nodes.txt`:
  ```text
  static-node-1 static-node-1.ydb-cluster.com
  static-node-2 static-node-2.ydb-cluster.com
  static-node-3 static-node-3.ydb-cluster.com
  static-node-4 static-node-4.ydb-cluster.com
  static-node-5 static-node-5.ydb-cluster.com
  static-node-6 static-node-6.ydb-cluster.com
  static-node-7 static-node-7.ydb-cluster.com
  static-node-8 static-node-8.ydb-cluster.com
  static-node-9 static-node-9.ydb-cluster.com
  ```
  Запустите скрипт `examples/9-nodes-mirror-3-dc/files/ydb-ca-update.sh`. Скрипт сгенерирует наборы сертификатов для TLS-шифрования трафика между YDB нодами и поместит их в поддиректорию `examples/9-nodes-mirror-3-dc/files/CA/certs/<create date>`.
6. Скачайте архив актуальной версию YDB из раздела [{#T}](../../downloads/index.md#ydb-server) в директорию `/examples/9-nodes-mirror-3-dc/files`.

### Изменения инвентаризационных файлов проекта { #inventory-edit }

В поставку входят два инвентаризационных файла, которые расположены в директории `examples/9-nodes-mirror-3-dc/inventory`. Инвентаризационный файл `50-inventory.yaml` содержит список хостов для подключения и переменные, используемые в ролях установки и настройки YDB, которые располагаются в директории `roles` на верхнем уровне проекта. Измените стандартные значения в инвентаризационном файле `50-inventory.yaml`:
* замените набор дефолтных хостов в секции `all.children.ydb.hosts`на FQDN созданных для установки YDB серверов:
  ```yaml
  all:
    children:
        ydb:
        hosts:
          static-node-1.ydb-cluster.com:
          static-node-2.ydb-cluster.com:
          static-node-3.ydb-cluster.com:
          static-node-4.ydb-cluster.com:
          static-node-5.ydb-cluster.com:
          static-node-6.ydb-cluster.com:
          static-node-7.ydb-cluster.com:
          static-node-8.ydb-cluster.com:
          static-node-9.ydb-cluster.com:
  ```
* измените стандартные значения следующих переменных `vars`:  
  + `ansible_user` – укажите пользователь для подключения Ansible по SSH.
  + `ansible_ssh_common_args` – не используется в данной конфигурации установки YDB, можно удалить или закомментировать (`#`).
  + `ansible_ssh_private_key_file` – измените путь к приватной части SSH-ключа, используемой для подключения к серверам: `{{ ansible_config_file | dirname + '/files/<uploaded ssh private key>' }}`. 
  + `ydb_tls_dir` – укажите актуальную часть пути (`/files/CA/certs/<date_time create certs>`) к сертификатам безопасности после их генерации скриптом `/examples/9-nodes-mirror-3-dc/files/ydb-ca-update.sh`.
  + `ydb_brokers` – укажите список FQDN нод брокеров. Например:
    ```yaml
    ydb_brokers:
          - static-node-1.ydb-cluster.com
          - static-node-2.ydb-cluster.com
          - static-node-3.ydb-cluster.com
    ``` 

Изменения других секций конфигурационного файла `50-inventory.yaml` и настроек не требуется. Инвентаризационный файл `99-inventory-vault.yaml` и файл `/examples/9-nodes-mirror-3-dc/ansible_vault_password_file.txt` содержат пароль для root пользователя YDB. Инвентаризационный файл ``99-inventory-vault.yaml` зашифрован. Для изменения дефолтного пароля – укажите новый пароль в файле `/examples/9-nodes-mirror-3-dc/ansible_vault_password_file.txt`,  продублируйте его в файле `/examples/9-nodes-mirror-3-dc/inventory/99-inventory-vault.yaml` в формате:
  ```yaml
  all:
        children:
          ydb:
            vars:
              ydb_password: <new password>
  ```

Для шифрования `99-inventory-vault.yaml` выполните команду `ansible-vault encrypt inventory/99-inventory-vault.yaml`.

### Подготовка конфигурационного файла YDB { #ydb-config-prepare }

Конфигурационный файл YDB – содержит настройки нод YDB и располагается по пути `/examples/9-nodes-mirror-3-dc/files/config.yaml` и состоит из секций настройки. С подробным описанием секций настройки конфигурационного файла YDB можно ознакомиться в стать [{#T}](../../deploy/configuration/config.md).

Стандартная поставка Ansible проекта для установки YDB требует минимального изменения конфигурационного файла:   
* отредактируйте секцию `hosts` – измените стандартные host имена на полные FQDN созданных ВМ:
  ```yaml
  ...
  hosts:
  - host: static-node-1.ydb-cluster.com #FQDN ВМ
    host_config_id: 1
    walle_location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  ...    
  ```  
* отредактируйте секцию `blob_storage_config` – добавьте все FQDN созданных нод в раздел `fail_domains`:
  ```yaml
  ...
  - fail_domains:
      - vdisk_locations:
        - node_id: static-node-1.ydb-cluster.com #FQDN ВМ
          pdisk_category: SSD
          path: /dev/disk/by-partlabel/ydb_disk_1
  ...        
  ```

Остальные секции и настройки конфигурационного файла остаются без изменений.

## Настройка типа избыточности хранения и количества серверов в кластере { #erasure-setup } 

По умолчанию конфигурационный файл YDB и Ansible проект настроены на создание кластера из девяти серверов с моделью избыточности `mirror-3-dc`. Если требуется создать кластер из восьми серверов с типом избыточности `block-4-2`, то нужно внести следующие изменения:
1. Задать восемь FQDN серверов в файле `/examples/9-nodes-mirror-3-dc/files/ydb-ca-nodes.txt`.
2. Внести изменения в конфигурационный файл YDB (`/examples/9-nodes-mirror-3-dc/files/config.yaml`):
    * Указать тип избыточности `block-4-2` в опции `static_erasure`:
        ```yaml
        static_erasure: mirror-3-dc
        ```
    * Задать восемь `host` блоков в разделе `hosts`.
    * Изменить значение `erasure_species` в блоке `pool_config`:
        ```yaml
        ...
        pool_config:
            box_id: 1
            erasure_species: block-4-2
            kind: ssd
            pdisk_filter:
            - property:
            - type: SSD
            vdisk_kind: Default
        ...
        ```
    * Указать список из восьми ID нод в разделе `state_storage` и задать `nto_select: 8`:
        ```yaml
        state_storage:
        - ring:
            node: [1, 2, 3, 4, 5, 6, 7, 8]
            nto_select: 8
            ssid: 1
        ```
    * Задать значение `erasure_species: block-4-2` в разделе `blob_storage_config`:
        ```yaml
        blob_storage_config:
            service_set:
                groups:
                - erasure_species: block-4-2
        ```
    * Указать количество `fail_domains` равное восьми нодам.
    * Изменить значения всех `erasure_species` на `block-4-2` в разделе `channel_profile_config`:
        ```yaml
        channel_profile_config:
            profile:
            - channel:
                - erasure_species: block-4-2
                pdisk_category: 1   # 0=ROT, 1=SSD, 2=NVME
                storage_pool_kind: ssd
                - erasure_species: block-4-2
                pdisk_category: 1
                storage_pool_kind: ssd
                - erasure_species: block-4-2
                pdisk_category: 1
                storage_pool_kind: ssd
                profile_id: 0
        ```

Режим избыточности задаётся при создании кластера однократно. Поменять его в процессе эксплуатации кластера нельзя. Если требуется изменить режим избыточности, то это сделать можно только через пересоздание кластера с выгрузкой бэкапов и последующем восстановлением из них.

## План выполнения сценария установки YDB { #ydb-playbook-run }

{% include [ansible-ydb-install](./_includes/ansible-ydb-install.md) %}

{% cut "Подробное пошаговое описание установки YDB" %}

{% include [ansible-install-steps](./_includes/ansible-install-steps.md) %}

{% endcut %}

## Свойства созданного кластера { #final-cluster-view }

В результате выполнения плейбука `setup_playbook.yaml` (стандартная поставка) будет создан кластер YDB, обладающий следующими свойствами:
* Девять статических нод, использующих по восемь CPU на сервер.
* Root пользователь YDB с назначением ему пароля доступа к БД.
* Тестовая база `test` в домене `Root`.
* Доступ к мониторингу на 8765 порту.

Для безопасного подключения к мониторингу можно воспользоваться SSH-туннелированием. Для этого на локальной машине выполните команду `ssh -L 8765:localhost:8765 -i <ssh private key> <user>@<first ydb static node ip>`. После успешной установки соединения можно перейти по URL: localhost:8765 – откроется web-панель мониторинга YDB.

Проверить работоспособность созданного кластера тестовой нагрузкой можно с помощью [YDB SDK](../../reference/ydb-sdk/index.md), в котором есть инструменты создания тестовых нагрузок.