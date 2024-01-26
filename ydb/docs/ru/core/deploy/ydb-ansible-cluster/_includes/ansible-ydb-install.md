## Подготовка окружения виртуальной машины Ansible { #prepare-ansible-vm }

Подключитесь по SSH к виртуальной машине, с которой будет осуществляться установка YDB и установите/обновите необходимые пакеты и приложения:
* `sudo apt update` – обновление списка apt репозиториев;
* `python3 --version`и `pip3 --version` – проверка версий Python и пакетного менеджера pip:
  + если `Python` ниже версии 3.10 – выполните команду `sudo apt install python3`;
  + если `pip3` не установлен – выполните команду `sudo apt install python3-pip`;
* `sudo apt install ansible` – установка Ansible.

Выполните команду `ansible --version`, чтобы убедиться в том, что Ansible использует ядро не ниже версии 2.15.2 – это требование исполнения плейбуков YDB:
```
$ ansible --version
  ansible [core 2.16.2]
  ...
```

## Скачивание и настройка Ansible проекта { #ansible-project-download }

Скачайте репозиторий с готовыми плейбуками для установки YDB на кластер – `git clone https://github.com/ydb-platform/ydb-ansible.git`. Будут скачены: роли, плейбуки, make файлы и докер-файл для запуска Ansible в контейнере, по желанию. 

Основная рабочая директория в скаченном репозитории – это `examples/9-nodes-mirror-3-dc`. В ней находятся следующие файлы и поддиректории, необходимые для работы Ansible:
* `ansible.cfg` – конфигурационный файл Ansible, который содержит настройки подключения к ВМ и опции структуры проекта;
* `setup_playbook.yaml` – плейбук, который запускает роли установки и настройки YDB на кластере. Роли располагаются в директории `/roles`.
* `inventory/` – директория, в которой находятся два инвентаризационных файла: 
    + `50-inventory.yaml` – основной инвентаризационный файл, содержащий перечень хостов для подключения и переменные для установки и настройки YDB;
    + `99-inventory-vault.yaml` – зашифрованный инвентаризационный файл для безопасной доставки пароля root пользователя YDB;
* `files` – директория, содержащая файлы, которые будут копироваться на хосты и файлы нужные для подключения Ansible:
    + `ydb-ca-update.sh` – скрипт для генерации ключей и сертификатов безопасности;
    + `ydb-ca-nodes.txt` – текстовый файл со списком FQDN нод для генерации сертификатов безопасности; 
    + `config.yaml` – конфигурационный файл YDB;

 
Для работы плейбуков потребуется выполнить несколько действий:
1. Выполните команду `ansible-galaxy collection install git+https://github.com/ydb-platform/ydb-ansible.git,refactor-use-collections` для установки `ydb_platform.ydb` и `community.general` коллекций в директорию `/home/<user name>/.ansible/collections/ansible_collections` (путь может отличаться в зависимости от ОС).
2. Заменить стандартный путь, указанный в опции `collections_paths` группы `default` в конфигурационном файле `ansible.cfg` на актуальный путь коллекций Ansible из вывода предыдущей команды: `/home/ubuntu/.ansible/collections/ansible_collections`.
3. Скачайте архив YDB последней версии с сайта https://ydb.tech/docs/ru/downloads/ и поместите его в директорию `examples/9-nodes-mirror-3-dc/files`.
4. Скопируйте приватную часть SSH-ключа пользователя, под которым подключается Ansible в директорию `examples/9-nodes-mirror-3-dc/files`.
5. Укажите все FQDN ваших нод в файле `examples/9-nodes-mirror-3-dc/files/ydb-ca-nodes.txt`. В нашем случае список нод выглядит так:
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

### Генерация сертификатов безопасности { #ca-generate }

Трафик между нодами шифруется, поэтому для каждой ноды генерируется свой набор сертификатов безопасности скриптом `examples/9-nodes-mirror-3-dc/files/ydb-ca-update.sh`. Скрипт поместит готовые наборы сертификатов в поддиректории `examples/9-nodes-mirror-3-dc/files/CA/certs/<create date>`. В дальнейшем этот путь используется в инвентаризационном файле `50-inventory.yaml`.

### Скачивание актуальной версии YDB { #ydb-download }

Для поддержания консистентности версий YDB на всех нодах – используется единый источник установки (путь до скачанного архива YDB), который указывается в переменной `ydb_archive` инвентаризационного файла `50-inventory.yaml`. Скачать последнюю версию YDB можно из раздела [{#T}](../../../downloads/index.md) документации ydb.tech. 

Архив необходимо скачать в директорию `/examples/9-nodes-mirror-3-dc/files` и указать названия архива в переменной `ydb_archive` инвентаризационного файла `50-inventory.yaml`.

### Изменения инвентаризационных файлов { #inventory-edit }

В поставку входят два инвентаризационных файла, расположенные в директории `examples/9-nodes-mirror-3-dc/inventory`. Инвентаризационный файл `50-inventory.yaml` содержит список хостов для подключения и переменные, используемые в ролях установки и настройки YDB, которые располагаются в директории `roles` на верхнем уровне проекта. 

Внесите следующие изменения в инвентаризационный файл `50-inventory.yaml`:
* замените набор дефолтных хостов на FQDN собственных хостов:
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
  + `ansible_user` – укажите пользователь для подключения Ansible по SSH (совпадает с пользователем, указанным при создании ВМ через Terraform).
  + `ansible_ssh_common_args` – не используется в данном конфиге установки YDB, можно удалить или закомментировать (`#`).
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

Конфигурационный файл YDB располагается по пути `/examples/9-nodes-mirror-3-dc/files/config.yaml` и состоит из секций настройки. С подробным описанием секций настройки конфигурационного файла YDB можно ознакомиться в стать [{#T}](../../../deploy/configuration/config.md). 

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

## План выполнения сценария установки YDB { #ydb-playbook-run }

Для запуска плейбука установки – выполните команду `ansible-playbook setup_playbook.yaml`, находясь в директории `/examples/9-nodes-mirror-3-dc`. При выполнении плейбука`setup_playbook.yaml` будут воспроизведены следующие роли:
1. Роль `packages` настраивает репозитории, управляет предпочтениями и конфигурациями APT, а также исправляет неконфигурированные пакеты и устанавливает необходимые программные пакеты в зависимости от версии дистрибутива.
2. Роль `system` устанавливает системные настройки, включая конфигурацию часов и временной зоны, синхронизацию времени через NTP с помощью `systemd-timesyncd`, настройку `systemd-journald` для управления журналами, конфигурацию загрузки модулей ядра и оптимизацию параметров ядра через `sysctl`, а также настройку производительности процессора с использованием `cpufrequtils`.
3. Роль `ydb` выполняет задачи по проверке необходимых переменных, установке базовых компонентов и зависимостей, настройке системных пользователей и групп, развертыванию и конфигурированию YDB, включая управление сертификатами TLS и обновление конфигурационных файлов.
4. Роль `ydb-static` отвечает за подготовку и запуск статических нод YDB, включая проверку необходимых переменных и секретов, форматирование и подготовку дисков, создание и запуск `systemd unit` для узла хранения, а также инициализацию хранилища и управление доступом к базе данных.
5. Роль `ydb-dynamic` настраивает и управляет динамическими узлами YDB, включая проверку необходимых переменных, создание конфигурационных файлов и `systemd unit` файлов для каждого динамического узла, запуск этих узлов, получение токена для доступа к YDB, создание базы данных в YDB.
  
## Подключение к web-консоли мониторинга { #monitoring-connect }

Для безопасного подключения к мониторингу можно воспользоваться SSH-туннелированием. Для этого на локальной машине выполните команду `ssh -L 87654:localhost:8654 -i <ssh private key> <user>@<first ydb static node ip>`. После успешной установки соединения можно перейти по URL: localhost:8765 – откроется web-панель мониторинга YDB.