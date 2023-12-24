# Установка {{ ydb-short-name }} с помощью Ansible

В статье изложен процесс установки и настройки YDB с помощью Ansible на одной виртуальной машине (ВМ). В результате выполнения описанного в статье сценария будет установлен и настроен YDB на удаленной машине, и настроен порт мониторинга для быстрого доступа к метрикам и терминалу выполнения SQL/YQL запросов.    

ВМ может быть создана в ручном режиме через web-консоль любого облачного провайдера или с помощью Terraform. Мы рекомендуем ознакомиться со статьёй, в которой описан подход создания ВМ в Яндекс Облаке с помощью Terraform. Особенность подхода в том, что Terraform генерирует инвентаризационный (hosts) файл для подключения Ansible.  

Перед началом установки и настройки Ansible кратко опишем работу YDB, это позволит лучше понять суть задач, которые будет выполнять Ansible на ВМ.  

## Как работает YDB { #how-ydb-works }

В YDB процессы обработки и хранения данных разделены. Это значит, что за обработку данных могут отвечать одни выделенные машины, а за хранения данных другие выделенные машины. Такой подход обеспечивает отказоустойчивость всей системы хранения и обработки данных. В нашем примере мы будем использовать схему совмещения двух процессов на одной ВМ – это позволит продемонстрировать полноценный процесс установки и запуска YDB при минимальных расходах ресурсов.

Процесс отвечающий за обработку данных называется статической нодой, а процесс отвечающий за хранения данных называется динамической нодой. Ноды обмениваются данными по сети, используя протокол gRPC(s). Более детально процесс работы YDB изложен в статье [{#T}](../concepts/index.md). Можно сказать, что статические и динамические ноды – это один и тот же запущенный процесс YDB с разными конфигурационными файлами и инлайн параметрами запуска. 

Приведём фрагмент systemd сервиса для обоих нод, где присутствуют индивидуальные параметры конфигурирования и запуска:

1. Статическая нода:
    ```bash
    ExecStart={{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd server \
        --log-level 3 --syslog --tcp --yaml-config  {{ ydb_dir }}/cfg/static_config.yaml \
        --grpc-port {{ grpc_port }} \
        --ic-port {{ ic_port }} \
        --mon-port {{ mon_port }} \
        --node static
    ```
2. Динамическая нода:
    ```bash
    ExecStart={{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd server \
        --yaml-config  {{ ydb_dir }}/cfg/dynamic_config.yaml \
        --grpc-port {{ grpc_port | int +1 }} \
        --ic-port {{ ic_port | int +1 }}  \
        --mon-port {{ mon_port | int +1 }}  \
        --tenant /{{ ydb_domain }}/{{ ydb_dbname }} \
        --node-broker grpc://{{ inner_net }}:{{ grpc_port }}
    ```

В приведенном примере используются переменные (`{{ grpc_port }}`, `{{ ic_port }}` и т.д.), а не фиксированные значения. Работу этого механизма мы опишем позже. После запуска процессов, взаимодействовать с YDB можно с помощью YDB CLI, API (будет использовано в Ansible), SDK (C++, C#, Go, Java, Node.js, PHP, Python и Rust) и web-консоли, доступной на порту 8765 (monport).

  
## Установка Ansible { #ansible-install }

Для работы Ansible требует Python 3+. Перед установкой Ansible убедитесь, что у вас установлен Python версии 3 и пакетный менеджер pip:

{% list tabs %}

- Windows
    1. Проверка версии Python:
        * Откройте командную строку (Cmd) или PowerShell и введите:
            ```cmd
            python --version
            ```
            Если Python не установлен или его версия ниже 3, установите Python версии 3+ с официального сайта [Python.org](https://www.python.org/downloads/).
    2. Установка и обновление пакетного менеджера `pip`:
        * Если Python установлен, то `pip` обычно устанавливается автоматически. Проверить, что он установлен можно следующей командой:
            ```cmd
            pip --version
            ```
        * Обновить pip до последней версии можно командой:
            ```cmd
            python -m pip install --upgrade pip
            ``` 
    3. Установка Ansible:
        * Устанавливается Ansible командой:
            ```cmd
            pip install ansible
            ```    

- Linux
    1. В дистрибутивах Ubuntu 20-22 идёт предустановленный Python версий 3.6-3.9 и pip3. Их обновлять не надо, можно сразу переходить к шагу установки Ansible. Если у вас Ubuntu 18 или иной дистрибутив, то проверка версию Python можно так:
        * Откройте терминал и введите:
            ```bash
            python3 --version
            ```
        * Если Python не установлен или версии ниже 3.6 – установите более актуальную версию:
            ```bash
            sudo apt-get install software-properties-common
            sudo add-apt-repository ppa:deadsnakes/ppa
            udo apt-get update
            sudo apt-get install python3
            ```
    2. Проверка версии pip3:
        * Проверить версию pip3 можно командой:
            ```bash
            pip3 --version
            ```
        * Обновить pip3 до последней актуальной версии можно командой:
            ```bash
            pip3 install --upgrade pip
            ```   
    3. Установка Ansible:
        * Ansible устанавливается командой:
            ```bash
            pip3 install ansible
            ```     

- macOS
    1. Откройте Lauchpad и введите в поисковую строку terminal.   

{% endlist %}

## Настройка Ansible { #ansible-install }

Для подключения к серверу Ansible использует SSH-ключ и имя пользователя. Есть несколько способов указать ансиблу нужный SSH-ключ и имя пользователя:
1. Добавить строку  `Private_key_file = <path to ssh-key>` в файл `ansible.cfg`, который может располагаться в корне проекта, рядом с плейбуком и указать `ansible_ssh_user=<user name>` в инвентаризационном файле;
2. Добавить параметры `ansible_ssh_private_key=<path to ssh-key>` и `ansible_ssh_user=<user name>` прямо в инвентаризационный файл для каждого хоста.

В нашем случае инвентаризационный файл (hosts) формирует Terraform и в нём уже указаны все нужные параметры для запуска Ansible:
```txt
158.160.99.189 ansible_ssh_user=ubuntu ansible_ssh_private_key_file=~/yandex ansible_ssh_common_args='-o StrictHostKeyChecking=no'
```
В примере мы передаём дополнительный параметр `ansible_ssh_common_args='-o StrictHostKeyChecking=no'`, он позволяет не проверять уникальность ssh-ключ на хосте. Это позволяет не подтверждать `ssh finger print` в ручном режиме, но данный подход можно использовать только в демонстрационных целях.

Сгенерировать пару ssh-ключей можно командой `ssh-keygen`. На одном из шагов будет предложено указать название ключа – задайте его. После генерации пара ключей будет помещена в директорию `.ssh`. Приватная часть ключа (без расширения .pub) остаётся на локальной машине и указывается аргументом `ansible_ssh_private_key=<path to ssh-key>` в инвентаризационном файле, а публичная часть ключа (с расширением .pub) копируется на ВМ. Если вы используете провайдер Terraform для Yandex Cloud, то публичный ключ передаётся через параметр `metadata = { ssh-keys = "ubuntu:${file(<path to ssh pub key>)}" }` ресурса `yandex_compute_instance`, а если вы настраиваете ВМ вручную, то публичную часть ключа можно передать на сервер командой `ssh-copy-id -i <path to pub ssh key> <user>@<your-remote-host>`. 

Указания ssh-ключа, пользователя для подключения и IP-адреса ВМ в инвентаризационном файле нам будет достаточно для наших целей установки YDB на ВМ.

## Задачи Ansible playbook { #playbook }

Вначале плейбука идёт служебная секция с описанием принадлежности плейбука к определенной группе хостов, разрешение на выполнение shell-команд на ВМ с правами суперпользователя и разрешение на сбор фактов о ВМ:
```yaml
- hosts: ydb-server # Группа хостов, для которой будет применён данный playbook. Задаётся в квадратных скобках hosts файла.
  become: true #Разрешение на выполнение shell-команд на ВМ от имени суперпользователя.
  gather_facts: yes #Разрешение на сбор фактов о ВМ. 
```

Далее следует секция `tasks`, где и происходит основная работа. Мы разбили `tasks` на логические группы задач, чтобы было легче ориентироваться:
1. Сбор информации о ВМ:
    * Формирование списка доступных для использования YDB блочных устройств, исключая стартовый диск. Полученный список дисков сохраняется в переменной `disk_info`. Далее при копировании конфигурационного файла для статической ноды Ansible сформирует секцию `drive` из данных переменной `disk_info`.
    ```yaml
    - name: List disk IDs matching 'virtio-ydb-disk-'
      ansible.builtin.shell: ls -l /dev/disk/by-id/ | grep 'virtio-ydb-disk-' | awk '{print $9}'
      register: disk_info
    ```
    * Подсчитывание количества доступных для использования YDB дисков. Эта информация используется далее при создании базы данных.
    ```yaml
    - name: Count disks matching 'virtio-ydb-disk-'
      ansible.builtin.shell: ls -l /dev/disk/by-id/ | grep 'virtio-ydb-disk-' | awk '{print $9}' | wc -l
      register: disk_value  
    ```
    * Подсчитывание количества ядер процессора для их дальнейшего распределения между статической и динамической нодой.
    ```yaml
    - name: Get number of CPU cores
      ansible.builtin.shell: lscpu | grep '^CPU(s):' | awk '{print $2}'
      register: cpu_cores_info
    ```
    * Запрос значение переменной окружения `host`. Оно используется в блоке `host` конфигурационных файлов нод.
    ```yaml
    - name: Get the hostname
      ansible.builtin.command: hostname
      register: hostname
    ```  
    

2.  Создание структуры директорий, скачивание и установка исполняемого файла YDB:
    * Создание группы `ydb`
    ```yaml
    - name: Create the ydb group
      group: name=ydb system=true  
    ```
    * Создание пользователя `ydb`, включенного в группы `ydb` и `disks`
    ```yaml
    - name: Create the ydb user with disk group access
      user: 
      name: ydb
      group: ydb 
      groups: disk 
      system: true 
      create_home: true 
      home: "{{ ydb_dir }}/home"
      comment: "YDB Service Account"
    ```
    * Создание директории для временных файлов пользователя `ydb`
    ```yaml
    - name: Create the Ansible remote_tmp for the ydb user
      file:
        path: "{{ ydb_dir }}/home/.ansible/tmp"
        state: directory
        recurse: true
        group: ydb
        owner: ydb
    ```
    * Создание директорий и присваивание им владельцев и прав доступа
    ```yaml
    - name: Create the YDB release directory
      file: state=directory path={{ ydb_dir }}/release group=bin owner=root mode=755

    - name: Create the YDB configuration directory
      file: state=directory path={{ ydb_dir }}/cfg group=ydb owner=ydb mode=755

    - name: Create the YDB audit directory
      file: state=directory path={{ ydb_dir }}/audit group=ydb owner=ydb mode=700

    - name: Create the YDB certs directory
      file: state=directory path={{ ydb_dir }}/certs group=ydb owner=ydb mode=700

    - name: Create the YDB configuration backup directory
      file: state=directory path={{ ydb_dir }}/reserve group=ydb owner=ydb mode=700

    - name: Create the YDB server binary directory for the specified version
      file: state=directory
            path="{{ ydb_dir }}/release/{{ ydb_version }}"
            recurse=true
            group=bin
            owner=root
    ```
