# Установка {{ ydb-short-name }} с помощью Ansible

В статье изложен процесс установки и настройки YDB с помощью Ansible на одной виртуальной машине (ВМ). В результате выполнения описанного в статье сценария будет установлен и настроен YDB на удаленной машине, и настроен порт мониторинга для быстрого доступа к метрикам и терминалу выполнения SQL/YQL запросов.    

ВМ может быть создана в ручном режиме через web-консоль любого облачного провайдера или с помощью Terraform. Мы рекомендуем ознакомиться со статьёй, в которой описан подход создания ВМ в Яндекс Облаке с помощью Terraform. Особенность подхода в том, что Terraform генерирует инвентаризационный (hosts) файл для подключения Ansible.  

Перед началом установки и настройки Ansible кратко опишем работу YDB, это позволит лучше понять суть задач, которые будет выполнять Ansible на ВМ.  

## Кратко о том как работает YDB { #how-ydb-works }

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
    * Проверка версии Python:
        + Откройте командную строку (Cmd) или PowerShell и введите:
            ```cmd
            python --version
            ```
            Если Python не установлен или его версия ниже 3, установите Python версии 3+ с официального сайта [Python.org](https://www.python.org/downloads/).
    * Установка и обновление пакетного менеджера `pip`:
        + Если Python установлен, то `pip` обычно устанавливается автоматически. Проверить, что он установлен можно следующей командой:
            ```cmd
            pip --version
            ```
        + Обновить pip до последней версии можно командой:
            ```cmd
            python -m pip install --upgrade pip
            ``` 
    * Установка Ansible:
        + Устанавливается Ansible командой:
            ```cmd
            pip install ansible
            ```    

- Linux
    * В дистрибутивах Ubuntu 20-22 идёт предустановленный Python версий 3.6-3.9 и pip3. Их обновлять не надо, можно сразу переходить к шагу установки Ansible. Если у вас Ubuntu 18 или иной дистрибутив, то проверка версию Python можно так:
        + Откройте терминал и введите:
            ```bash
            python3 --version
            ```
        + Если Python не установлен или версии ниже 3.6 – установите более актуальную версию:
            ```bash
            sudo apt-get install software-properties-common
            sudo add-apt-repository ppa:deadsnakes/ppa
            udo apt-get update
            sudo apt-get install python3
            ```
    * Проверка версии pip3:
        + Проверить версию pip3 можно командой:
            ```bash
            pip3 --version
            ```
        + Обновить pip3 до последней актуальной версии можно командой:
            ```bash
            pip3 install --upgrade pip
            ```   
    * Установка Ansible:
        + Ansible устанавливается командой:
            ```bash
            pip3 install ansible
            ```     

- macOS
    * Откройте Lauchpad и введите в поисковую строку terminal.   

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

Далее следует секция `tasks`, где и происходит основная работа. Мы разбили `tasks` на логические группы задач, чтобы было легче ориентироваться.

### Сбор информации о ВМ { #collect-vm-info }

1. Формирование списка доступных для использования YDB блочных устройств, исключая стартовый диск. Полученный список дисков сохраняется в переменной `disk_info`. Далее при копировании конфигурационного файла для статической ноды Ansible сформирует секцию `drive` из данных переменной `disk_info`.
    ```yaml
    - name: List disk IDs matching 'virtio-ydb-disk-'
        ansible.builtin.shell: ls -l /dev/disk/by-id/ | grep 'virtio-ydb-disk-' | awk '{print $9}'
        register: disk_info
    ```
2. Подсчитывание количества доступных для использования YDB дисков. Эта информация используется далее при создании базы данных.
    ```yaml
    - name: Count disks matching 'virtio-ydb-disk-'
        ansible.builtin.shell: ls -l /dev/disk/by-id/ | grep 'virtio-ydb-disk-' | awk '{print $9}' | wc -l
        register: disk_value  
    ```
3. Подсчитывание количества ядер процессора для их дальнейшего распределения между статической и динамической нодой.
    ```yaml
    - name: Get number of CPU cores
        ansible.builtin.shell: lscpu | grep '^CPU(s):' | awk '{print $2}'
        register: cpu_cores_info
    ```
4. Запрос значение переменной окружения `host`. Оно используется в блоке `host` конфигурационных файлов нод.
    ```yaml
    - name: Get the hostname
        ansible.builtin.command: hostname
        register: hostname
    ```  
    

### Создание структуры директорий, скачивание и установка исполняемого файла YDB { #ydb-install }
1. Создание группы `ydb`
    ```yaml
    - name: Create the ydb group
        group: name=ydb system=true  
    ```
2. Создание пользователя `ydb`, включенного в группы `ydb` и `disks`
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
3. Создание директории для временных файлов пользователя `ydb`
    ```yaml
    - name: Create the Ansible remote_tmp for the ydb user
        file:
            path: "{{ ydb_dir }}/home/.ansible/tmp"
            state: directory
            recurse: true
            group: ydb
            owner: ydb
    ```
4. Создание директорий и присваивание им владельцев и прав доступа
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
5. Скачивание архива YDB и назначение пользователя, группы и прав
    ```yaml
    - name: Download the YDB sources archive
        get_url: url={{ ydb_download_url }} dest={{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }} 
    - name: Set owner and group for the YDB sources archive
        file: 
            path="{{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }}"
            group=bin
            owner=root    
    ```
6. Создание директории и распаковка архива YDB с присваиванием пользователя, группы и прав. Параметры модуля `ansible.builtin.unarchive`:
    + `remote_src` – опция работы с файлами на удаленном сервере;
    + `creates` – создаваемая директория;
    + `dest` – путь, где будет создана директория;
    + `src` –     
    ```yaml
    - name: Install the YDB server binary package
        ansible.builtin.unarchive:
        remote_src: yes
        creates: "{{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd"
        dest: "{{ ydb_dir }}/release/{{ ydb_version }}"
        group: bin
        owner: root
        src: "{{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }}"
        extra_opts: "{{ ydb_unpack_options }}"

    - name: Create the YDB CLI default binary directory
        file: state=directory path={{ ydb_dir }}/home/ydb/bin recurse=true group=ydb owner=ydb mode=700      
        ```  
### Генерирование конфигурационных файлов для нод и запуск сервиса YDB { #ydb-gen-config }
1. Генерирование конфигурационного файла для нод. В задачи задаются следующие параметры: 
    + `src` – ссылка на шаблон конфигурационного файла;
    + `dest` – путь расположения сгенерированного конфигурационного файла;   
    + `vars` – переменная, из которой будет браться информация для генерации файла. 
    ```yaml
    - name: Generate YDB static configuration file from template
        ansible.builtin.template:
        src: ./files/static_config.j2 
        dest: "{{ ydb_dir }}/cfg/static_config.yaml"
    vars:
        disk_info: "{{ disk_info }}"
    
    - name: Generate the YDB dynamic node service file from template
        template:
        src: ./files/dynnode-service.j2
        dest: "/etc/systemd/system/ydbd-dynnode.service" 
    ```      
2. Перезапуск systemd. Применяется модуль `ansible.builtin.systemd` с параметром `daemon_reload` для перезагрузки `systemd`.
    ```yaml
    - name: Refresh systemd configuration to recognize new services
        ansible.builtin.systemd:
        daemon_reload: true
    ```    
3. Запуск статической ноды с помощью systemd. Указываются следующие параметры запуска статической ноды:
    + `become` – запуск от имени суперпользователя;
    + `ansible.builtin.systemd.name` – имя сервиса. Нужно для управления сервисом YDB: остановка – `sudo systemctl stop ydbd-storage`, перезапуск – `sudo systemctl restart ydbd-storage`, запуск – `sudo systemctl start ydbd-storage`.
    + `any_errors_fatal` – остановка запуска сервиса при возникновении любых ошибок. 
    ```yaml
    - name: Start the YDB storage node service
        become: true
        ansible.builtin.systemd:
        state: started
        name: ydbd-storage
        any_errors_fatal: true 
    ```    
4. Проверка, что статическая нода запустилась. Проверка осуществляется командой `systemctl is-active ydbd-storage || true`. Если команда возвращается `active`, то значение записывается в переменную `ydbd_storage_status` и выполняются следующие задачи, если возвращается любой другой статус – выполнение плейбука прекращается. Предпринимаются 5 попыток опроса статуса сервиса с интервалом в 10 секунд. Параметры выполнения задачи:
    + `shell` – shell-команда, которая будет выполнена;
    + `register` – переменная, в которую записывается статус ноды;
    + `any_errors_fatal` – остановка задачи при возникновении любых ошибок в логе процесса `ydbd-storage`;
    + `retries` – количество попыток запроса статуса процесса;
    + `delay` – время ожидания между попытками запроса статуса процесса; 
    + `until` – условие успешного завершения задачи. Если в течение 5 попыток значение переменной `ydbd_storage_status` станет `active` – задача будет выполнена, если значение будет иным – задача завершится с ошибкой и выполнение плейбука прервётся.

    ```yaml
    - name: Verify that the YDB storage node service is active
        shell: systemctl is-active ydbd-storage || true
        register: ydbd_storage_status
        any_errors_fatal: true
        retries: 5
        delay: 10
        until: ydbd_storage_status.stdout == "active"
    ```  
5. Создание базы данных и её регистрация. Базу данных можно создать, используя API, CLI и SDK. В приведенном блоке задач база данных создаётся и регистрируется с помощью API в блоке `else` условного оператора `if` (предотвращает ошибки при повторном запуске плейбука) – `... ydbd -s grpc://... admin blobstorage config init --yaml-file ...`. Для каждой задачи передается переменная окружения `LD_LIBRARY_PATH`, которая нужна для корректного запуска YDB. Задачи данного блока выполняются, только если статус статической ноды `active`. Параметры исполняемых задач:
    + `shell` – shell-команда для исполнения;
    + `environment` – переменная окружения, которая будет активирована при выполнении задачи;
    + `when` – условие старта выполнения задачи. Задача будет начата только если значение переменной `ydbd_storage_status` – `active`.
    ```yaml
    - name: Initialize storage if YDB storage node is active and not already done
        shell: >
        if grep -q 'initialize_storage_done' {{ ydb_dir }}/ydb_init_status; then
            echo 'Initialize storage already done';
        else
            {{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd -s grpc://{{ inner_net }}:{{ grpc_port }} admin blobstorage config init --yaml-file {{ ydb_dir }}/cfg/static_config.yaml && echo 'initialize_storage_done' >> {{ ydb_dir }}/ydb_init_status;
        fi
        environment:
        LD_LIBRARY_PATH: "{{ ydb_dir }}/release/{{ ydb_version }}/lib"
        when: ydbd_storage_status.stdout == "active"

    - name: Register database if YDB storage node is active and not already done
        shell: >
        if grep -q 'register_database_done' {{ ydb_dir }}/ydb_init_status; then
            echo 'Register database already done';
        else
            {{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd -s grpc://{{ inner_net }}:{{ grpc_port }} admin database /{{ ydb_domain }}/{{ ydb_dbname }} create ssd:{{ disk_value.stdout }} && echo 'register_database_done' >> {{ ydb_dir }}/ydb_init_status;
        fi
    environment:
        LD_LIBRARY_PATH: "{{ ydb_dir }}/release/{{ ydb_version }}/lib"
    when: ydbd_storage_status.stdout == "active"
    ```
6. Запуск динамической ноды. Логика и условия выполнения задачи запуска динамической ноды идентичны условиям выполнения задачи запуска статической ноды: 
    ```yaml
    - name: Start database processes if YDB storage node is active
        become: true
        ansible.builtin.systemd:
        state: started
        name: "ydbd-dynnode"    
        any_errors_fatal: true 
        retries: 5
        delay: 10
        when: ydbd_storage_status.stdout == "active"
    ```

### Создание SSH-туннеля и настройка доступа к web-консоли YDB
Работа по созданию SSH-туннеля ведется на локальной машине без использования прав суперпользователя. SSH-туннель нужен для проброса порта мониторинга YDB на локальную машину. Это делает доступ к БД безопасным и простым, так как не надо настраивать сертификаты безопасности, firewall и порты.

1. Получение списка доступных оболочек командной строки локальной ОС и сохранение списка оболочек в переменную `shell_output`. Опции выполняемой команды:
    * `delegate_to` – указание на машину, на которой будет выполнена задача. По умолчанию все задачи плейбука выполняются на хостах, указанных в инвентаризационном файле (host), но такое поведение можно изменить. Например, мы можем выполнить задачу на локальной машине, указав значение `localhost`; 
    * `become` – выполнение задачи с правами суперпользователя;
    * `ansible.builtin.command` – модуль выполнения команд в терминале (может быть заменён на модуль `shell`);
    * `register` – создание переменной и сохранение в неё результатов выполнения задачи.      
    ```yaml
    - name: Get list of available shells from /etc/shells
      delegate_to: localhost
      become: false
      ansible.builtin.command: cat /etc/shells
      register: shell_output
    ```
2. Выбор активной оболочки командной строки. Часто бывает, что в ОС установлено несколько командных оболочек, чтобы гарантировать большую совместимость выполняемых команд с командной оболочкой, текущая задача проверяет список доступных оболочек, и если среди них есть `bash` или `zsh`, то выбирается одна из них, если нет – используется любая доуступная оболочка:
    ```yaml
    - name: Set active shell to bash or zsh if available
      set_fact:
        active_shell: "{{ '/bin/bash' if '/bin/bash' in shell_output.stdout else '/bin/zsh' if '/bin/zsh' in shell_output.stdout else omit }}"
    ```
3. Создание SSH-туннеля. Процедура создания SSH-туннеля может занимать длительное время, поэтому текущая задача выполняется асинхронно, чтобы не блокировать терминал. Опции выполняемой задачи:
    * `delegate_to` – указание на машину, на которой будет выполнена задача. 
    * `become` – выполнение задачи с правами суперпользователя;
    * `ansible.builtin.shell.cmd` – команда, которая будет выполнена в командной строке;
    * `ansible.builtin.shell.executable` – выбор активной оболочки командной строки; Значение будет взято из переменной `active_shell`;
    * `async` – опция запуска задачи асинхронно и время ожидания завершения задачи;
    * `poll` – время в секундах, через которое Ansible проверит статус выполнения задачи. Если `poll` равен нулю – Ansible завершит задачу по истечению времени, указанном в опции `async`.
    ```yaml
    - name: Establish an SSH tunnel to the YDB monitoring port
      delegate_to: localhost
      become: false
      ansible.builtin.shell: 
        cmd: "ssh  -f -L {{ mon_port }}:localhost:{{ mon_port }} -N -i {{ ansible_ssh_private_key_file }} -vvv {{ ansible_ssh_user }}@{{ inventory_hostname }}"
        executable: "{{ active_shell }}"
      async: 15
      poll: 0
    ```
4. Вывод в терминал сообщения с URL для подключения к web-консоли YDB. Описание опций выполняемой задачи:
    + `ansible.builtin.debug.msg` – текст сообщения, которое будет выведено в терминал.
    ```yaml
    - name: Display the YDB monitoring connection URL
      ansible.builtin.debug:
        msg: "Now you can connect to YDB monitoring via this URL: http://localhost:{{ mon_port }}"
    ```

## Troubleshooting { #troubleshooting }
Ansible – это мощный инструмент автоматической установки и настройки окружений на ВМ. Однако стабильность работы плейбуков зависит от многих факторов: версии Python, настроек Ansible, версии оболочки командной строки, настроек безопасности на хостах, правильности указания значений переменных, правильности расположения файлов проекта. 

Приведём возможные ошибки, с которыми можно столкнуться при использовании данного плейбука и сценарии их решения:

|#
|| Задача | Ошибка | Причина | Решение || 
||  Установка SSH-соединения | ```Failed to connect to the host via ssh: ssh: connect to host ... port 22: Connection refused``` | SSH-сервер не успел запуститься на удаленном хосте. | Повторить попытку подключения позже ||  
|| Установка SSH-соединения | ```Failed to connect to the host via ssh: Warning: Permanently added '...' (ED25519) to the list of known hosts.\r\nno such identity: ...: No such file or directory\r\...: Permission denied (publickey).``` | Не правильно указан путь к SSH-ключу или его название в опции `ansible_ssh_private_key_file` инвентаризационного файла | Проверить правильность пути и название SSH-ключа. Скорректировать их, если они не верны. ||
|| Установка SSH-соединения | ```Failed to connect to the host via ssh: ...: Permission denied (publickey).``` | Не правильно указан пользователь для подключения в опции `ansible_ssh_user` инвентаризационного файла. | Если вы использовали Terraform-провайдер Yandex Cloud – проверьте данные для подключения по SSH в блоке `metadata` ресурса `yandex_compute_instance`. Если ВМ в Yandex Cloud была создана вручную – параметры подключения по SSH находятся в **Облачная консоль Yandex CLoud** → **Виртуальные машины** → *<Ваша виртуальная машина>* → **Обзор** → **Метаданные**.
Если для создания ВМ вы использовали иного облачного провайдера – обратитесь к документации провайдера. ||
|| Download the YDB sources archive | ```Request failed", "response": "HTTP Error 404: Not Found", "status_code": 404``` | Не правильно указана версия YDB для скачивания в переменной `ydb_version` или ошибка в URL для скачивания YDB в переменной `ydb_download_url`. | Перейдите [по ссылке](../downloads/index.md#ydb-server), скопируйте тег последней доступной версии YDB (например, 23.3.13) и вставьте его в переменную `ydb_version`. ||
|| Generate YDB static configuration file from template | ```Could not find or access './files/...'``` |
1. Директория `files`, содержащая шаблоны файлов в формате `.j2` имеет ошибку в названии или имеет не правильное расположение относительно плейбука.
2. Названия шаблонов файлов в директории `files` не соответствуют путям их вызова в опциях `src` задач генерации конфигурационных файлов. |     
1. Убедитесь, что директория `files` располагается на одном уровне с плейбуком.
2. Убедитесь, что пути к шаблонам файлов, указанные в опциях `src` задач по генерации конфигурационных файлов соответствуют реальному их расположению в файловой системе. ||
|| Verify that the YDB storage node service is active | `FAILED - RETRYING: [...]: Verify that the YDB storage node service is active (5 retries left).` | Сервис `ydbd-storage` не был успешно запущен через systemd из-за ошибки в конфигурационном файле статической ноды. | Подключитесь по SSH к ВМ и выполните команду `journalctl -u ydbd-storage` – будет выведен лог запуска статической ноды. Найдите строку `Caught exception` или блок `uncaught exception`:
```
uncaught exception:
    address -> 0x46e97ed01810
    what() -> "yaml-cpp: error at line 82, column 32: illegal EOF in scalar"
    type -> YAML::ParserException
```
Строка `what() -> "yaml-cpp: error at line 82, column 32: illegal EOF in scalar"` будет содержать информацию об ошибки в конфигурационном файле статической ноды. Внесите правки в исходных шаблон файла и перезапустите плейбук. ||
|| Start database processes if YDB storage node is active | `FAILED - RETRYING: [...]: Start database processes if YDB storage node is active (5 retries left).` | Сервис `ydbd-dynnode` не был успешно запущен через systemd из-за ошибки в конфигурационном файле динамической ноды. | Подключитесь по SSH к ВМ и выполните команду `journalctl -u ydbd-dynnode` – будет выведен лог запуска статической ноды. Найдите блок `uncaught exception` или строку `Caught exception`: ```Caught exception: /opt/buildagent/work/3e574e3efc81dc20/tag/ydb/library/yaml_config/yaml_config_parser.cpp:36: Array field `fail_domains` must be specified.``` В конце сообщения будет указана причина ошибки. В данном случае ошибка в том, что блок настройки `fail_domains` в конфигурационном файле динамической ноды оформлен не верно. ||
|| Display the YDB monitoring connection URL | Ссылка для подключения к мониторингу YDB выдаёт 404 ошибку | SSH-туннель не был настроен или проброс порта мониторинга на локальную машину не был выполнен. | Выполните ручной проброс порта командой ```ssh -L <mon_port>:localhost:<mon_port> -i <path to private SSH key> <user>@<IP VM>```. Порт мониторинга устанавливается переменной `mon_port` в файле `files/all.yml`. ||
#| 
