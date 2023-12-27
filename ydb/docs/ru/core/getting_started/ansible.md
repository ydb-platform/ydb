# Установка {{ ydb-short-name }} с помощью Ansible

В статье изложен процесс установки и настройки YDB с помощью Ansible на одной виртуальной машине (ВМ), а также приведен подробный разбор задач плейбука и описание переменных. ВМ создавалась в Yandex CLoud с помощью Terraform. Ознакомиться со статьёй по созданию ВМ в Yandex Cloud с помощью Terraform можно по ссылке. Текущая статья подходит для установки YDB с помощью Ansible на ВМ любых облачных провайдеров. 

{% note info %}

Виртуальная машина, на которою будет устанавливаться YDB, должна отвечать следующим требованиям: от 4 vCPU и 8 GB RAM, один неформатированный дополнительный подключенный SSD-сетевой диск объёмом от 120 GB, отсутствие firewall и настроенное SSH-подключение по ключу, Ubuntu не ниже 18 (установлен по умолчанию Python 3.6).  

{% endnote %}      

Структура статьи:
1. [Кратко о том как работает YDB](#how-ydb-works)
2. [Установка Ansible](#ansible-install)
3. [Настройка Ansible](#ansible-setup)
4. [Как устроен проект и как он работает](#project-structure)
5. [Troubleshooting](#troubleshooting)

Перед установкой и настройкой Ansible кратко опишем работу YDB. Это позволит лучше понять суть задач, которые будет выполнять Ansible на ВМ.  

## Кратко о том как работает YDB { #how-ydb-works }

В YDB процессы обработки и хранения данных разделены. Это значит, что за обработку данных могут отвечать одни выделенные машины, а за хранения данных другие выделенные машины. Такой подход обеспечивает отказоустойчивость всей системы хранения и обработки данных. В нашем примере мы будем использовать схему совмещения двух процессов на одной ВМ – это позволит продемонстрировать полноценный процесс установки и запуска YDB при минимальных расходах ресурсов.

Процесс отвечающий за обработку данных называется статической нодой, а процесс отвечающий за хранения данных называется динамической нодой. Ноды обмениваются данными по сети, используя протокол gRPC(s). Более детально процесс работы YDB изложен в статье [{#T}](../concepts/index.md). Можно сказать, что статические и динамические ноды – это один и тот же запущенный процесс YDB с разными конфигурационными файлами и инлайн параметрами запуска (они описаны в разделе [Как устроен проект и как он работает](#project-structure)).

Итоговая модель работы YDB на одной ВМ в нашем случае выглядит так: запущены два процесса (`ydbd-static` – статическая нода, `ydbd-dynnode` – динамическая нода), которые обмениваются данными по сети через gRPC-протокол. Взаимодействовать с нодами можно через API и web-интерфейс. Теперь, когда модель работы YDB очерчена, можно приступать к установки Ansible.

## Установка Ansible { #ansible-install }

Для работы Ansible на локальной и удаленной машине (хосте) потребуется Python 3+. Перед установкой Ansible убедитесь, что у вас установлен Python версии 3 и пакетный менеджер pip:

{% list tabs %}

- Windows
  * Проверить версию Python можно командой `python --version` в командной строке (CMD) или в PowerShell. Если Python не установлен – установите его с официального сайта [Python.org](https://www.python.org/downloads/).
  * Проверить версию пакетного менеджера `pip` можно командой `pip --version` или `pip3 --version`. Если Python установлен, то `pip` обычно устанавливается автоматически. 
  * Обновить pip до последней версии можно командой `python -m pip install --upgrade pip`.
  * Ansible устанавливается командой `pip install ansible`.   
  * Проверьте версию Ansible командой `ansible --version`.

- Linux
  * В дистрибутивах Ubuntu 20-22 идёт предустановленный Python версий 3.6-3.9 и pip3. Их обновлять не надо, можно сразу переходить к шагу установки Ansible. Если у вас Ubuntu 18 или иной дистрибутив, то проверить версию Python можно командой `python3 --version`.
  * Если Python не установлен или его версии ниже 3.6 – установите более актуальную версию:
    ```bash
    sudo apt-get install software-properties-common
    sudo add-apt-repository ppa:deadsnakes/ppa
    udo apt-get update
    sudo apt-get install python3
    ```
  * Проверить версию pip3 можно командой `pip3 --version`, а обновляется pip3 до актуальной версии командой `pip3 install --upgrade pip`.
  * Устанавливается Ansible командой `pip3 install ansible`.   
  * Проверьте версию Ansible командой `ansible --version`.

- macOS
    * Откройте Lauchpad и введите в поисковую строку `terminal`. В терминал введите `python --version`. 
    * Если Python ниже версии 3.6 – перейдите на официальный [сайт Python](https://www.python.org/downloads/macos/), скачайте Python последней версии и установите его.
    * После установке проверьте еще раз версию Python – `python --version`.
    * Установите Ansible с помощью пакетного менеджера brew – `brew install ansible`. Если у вас не установлен brew, его можно установить командой `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`.
    * Проверьте версию Ansible командой `ansible --version`.

{% endlist %}

Ansible установлен, можно переходить к его настройке.

## Настройка Ansible { #ansible-setup }

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


## Как устроен проект и как он работает { #project-structure }

Проект состоит из плейбука – `ydb_1VM.yaml`, директории `group_vars`, где расположен файл с переменными `all.yaml`и директории `files`, в которой находятся шаблоны конфигурационных файлов нод и service-файлы для systemd. Скачать архив проекта установки YDB на ВМ можно из репозитория YDB на GitHub. 

Переменные в файле `./files/all.yaml` задают следующие параметры:

{% include [vars](./_includes/ansible/vars.md) %}

Переменные из файла `/group_vars/all.yaml` используются в задачах плейбука и в шаблонах файлов, которые копируются на хост. На хост копируются service файл для запуска статической и динамической ноды через systemd (юниты) и конфигурационные файлы для обоих нод, которые указываются в параметрах запуска юнитов. 

Содержание копируемых файлов с комментариями:
{% list tabs %}
- Юнит статической ноды

  {% include [static-service](./_includes/ansible/static-service.md) %}

- Юнит динамической ноды

  {% include [dynnode-service](./_includes/ansible/dynnode-service.md) %}

- Конфиг статической ноды
  
  Подробное описание параметров конфигурационного файла изложено в статье [{#T}](../deploy/configuration/config.md). 

  {% include [static-config](./_includes/ansible/static-config.md) %}

- Конфиг динамической ноды
  
  Подробное описание параметров конфигурационного файла изложено в статье [{#T}](../deploy/configuration/config.md).

  {% include [dynnode-config](./_includes/ansible/dynnode-config.md) %}  

{% endlist %}

Конфигурационные файлы для статической и динамической ноды очень схожи и имеют большое количество одинаковых настроек. Можно сказать, индивидуальные настройки для каждой ноды задаются в секции `actor_system_config`, которая содержит настройки [акторной системы](../deploy/configuration/config.md#actor-system). Конфигурационные файлы изначально представлены в виде шаблонов в формате `.j2` и находятся в директории `files`. При выполнение задач плейбука с вызовом модуля `ansible.builtin.template` – переменные в шаблоне подменяются реальными значениями, а файлы переносятся на хост с изменением расширения: `static_config.j2`/`dynamic_config.j2` → `static_config.yaml`/`dynamic_config.yaml`, `static-service.j2`/`dynamic-service.j2` → `static-service.service`/`dynamic-service.service`.

## Структура плейбука и разбор задач { #playbook }

Плейбук состоит из двух секций: секции настройки исполнения задач и секции задач. В секции настройки исполнения задач задаётся:
```yaml
- hosts: ydb-server # Группа хостов, для которой будет применён данный playbook. Задаётся в квадратных скобках hosts файла.
  become: true #Разрешение на выполнение shell-команд на ВМ от имени суперпользователя.
  gather_facts: yes #Разрешение на сбор фактов о ВМ. 
```

В секции `tasks` происходит основная работа. Мы разбили `tasks` на логические группы задач, чтобы было легче ориентироваться:
1. [Сбор информации о ВМ](#collect-vm-info)
2. [Создание структуры директорий, скачивание и установка исполняемого файла YDB](#ydb-install)
3. [Генерирование конфигурационных файлов для нод и запуск сервиса YDB](#ydb-gen-config)
4. [Создание SSH-туннеля и настройка доступа к web-консоли YDB](#ssh-tunnel-create)

### Сбор информации о ВМ { #collect-vm-info }

Группа содержит задачи сбора информации о ВМ и фиксации её в переменных для дальнейшей обработки в других задачах и шаблонах файлов, передаваемых на ВМ.

1. Формирование списка доступных для использования YDB блочных устройств, исключая стартовый диск. Полученный список дисков сохраняется в переменной `non_mounted_disks`. Далее при копировании конфигурационного файла для статической ноды Ansible сформирует секцию `drive` из данных переменной `non_mounted_disks`.
    ```yaml
    - name: Identify the root disk
      ansible.builtin.shell: 
        cmd: df / | tail -1 | awk '{print $1}' | xargs -n 1 basename | sed 's/[0-9]*//g'
      register: root_disk

    - name: List all non-mounted disks excluding the root disk
      ansible.builtin.shell: 
        cmd: lsblk -nlpo NAME,TYPE,MOUNTPOINT | grep -v "part\|loop" | awk -v rootdisk="{{ root_disk.stdout }}" '$2=="disk" && $3=="" && $1 !~ rootdisk {print $1}'
      register: non_mounted_disks   
    ```
2. Подсчитывание количества ядер процессора для их дальнейшего распределения между статической и динамической нодой.
    ```yaml
    - name: Get number of CPU cores
      ansible.builtin.shell: lscpu | grep '^CPU(s):' | awk '{print $2}'
      register: cpu_cores_info
    ```
3. Запрос значение переменной окружения `host`. Оно используется в блоке `host` конфигурационных файлов нод.
    ```yaml
    - name: Get the hostname
      ansible.builtin.command: hostname
      register: hostname
    ```  
    

### Создание структуры директорий, скачивание и установка исполняемого файла YDB { #ydb-install }

Группа содержит задачи создания директорий, пользователя, группы и назначения прав. 

1. Создание группы `ydb`:
    ```yaml
    - name: Create the ydb group
      group: name=ydb system=true  # Задаётся название группы и указывается что группа системная 
    ```
2. Создание пользователя `ydb`, включенного в группы `ydb` и `disks`:
    ```yaml
    - name: Create the ydb user with disk group access
      user:
      name: ydb                        # Имя создаваемого пользователя 
      group: ydb                       # Группа, в которую будет включен пользователь
      groups: disk                     # Дополнительные группы, к которым принадлежит пользователь
      system: true                     # Указывает, что это системный пользователь
      create_home: true                # Создать домашний каталог для пользователя
      home: "{{ ydb_dir }}/home"       # Путь к домашнему каталогу пользователя
      comment: "YDB Service Account"   # Комментарий о пользователе

    ```
3. Создание директории для временных файлов пользователя `ydb` (можно использовать для хранения шаблонов или иных файлов):
    ```yaml
    - name: Create the Ansible remote_tmp for the ydb user
      file:
          path: "{{ ydb_dir }}/home/.ansible/tmp" # Путь к каталогу
          state: directory                        # Указывает, что должен быть создан каталог
          recurse: true                           # Рекурсивно применить настройки к подкаталогам
          group: ydb                              # Группа, которой будет принадлежать каталог
          owner: ydb                              # Владелец каталога
    ```
4. Создание директорий и присваивание им владельцев и прав доступа:
    ```yaml
    - name: Create the YDB release directory
      file:
        state: directory                       # Создать каталог
        path: "{{ ydb_dir }}/release"          # Путь к каталогу релизов YDB
        group: bin                             # Группа для каталога
        owner: root                            # Владелец каталога
        mode: '0755'                           # Права доступа к каталогу

    - name: Create the YDB configuration directory
      file:
        state: directory                       # Создать каталог
        path: "{{ ydb_dir }}/cfg"              # Путь к конфигурационному каталогу YDB
        group: ydb                             # Группа для каталога
        owner: ydb                             # Владелец каталога
        mode: '0755'                           # Права доступа к каталогу

    - name: Create the YDB audit directory
      file:
        state: directory                       # Создать каталог
        path: "{{ ydb_dir }}/audit"            # Путь к каталогу аудита YDB
        group: ydb                             # Группа для каталога
        owner: ydb                             # Владелец каталога
        mode: '0700'                           # Права доступа к каталогу

    - name: Create the YDB certs directory
      file:
        state: directory                       # Создать каталог
        path: "{{ ydb_dir }}/certs"            # Путь к каталогу сертификатов YDB
        group: ydb                             # Группа для каталога
        owner: ydb                             # Владелец каталога
        mode: '0700'                           # Права доступа к каталогу

    - name: Create the YDB configuration backup directory
      file:
        state: directory                       # Создать каталог
        path: "{{ ydb_dir }}/reserve"          # Путь к каталогу резервного копирования конфигурации YDB
        group: ydb                             # Группа для каталога
        owner: ydb                             # Владелец каталога
        mode: '0700'                           # Права доступа к каталогу

    - name: Create the YDB server binary directory for the specified version
      file:
        state: directory                                 # Создать каталог
        path: "{{ ydb_dir }}/release/{{ ydb_version }}"  # Путь к каталогу бинарных файлов сервера YDB для указанной версии
        recurse: true                                    # Рекурсивное применение настроек к подкаталогам
        group: bin                                       # Группа для каталога
        owner: root                                      # Владелец каталога
    ```
5. Скачивание архива YDB и назначение пользователя, группы и прав:
    ```yaml
    - name: Download the YDB sources archive
      get_url:
        url: "{{ ydb_download_url }}"                                      # URL для загрузки архива YDB
        dest: "{{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }}"  # Путь для сохранения архива

    - name: Set owner and group for the YDB sources archive
      file:
        path: "{{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }}"  # Путь к архиву YDB
        group: bin                                                         # Группа, которой будет принадлежать архив
        owner: root                                                        # Владелец архива
    ```
6. Создание директории и распаковка архива YDB с присваиванием пользователя, группы и прав:
    ```yaml
    - name: Install the YDB server binary package
      ansible.builtin.unarchive:
        remote_src: yes                                                   # Использовать источник на удалённом хосте
        creates: "{{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd"       # Путь, где будет распакован пакет
        dest: "{{ ydb_dir }}/release/{{ ydb_version }}"                   # Назначение распаковки
        group: bin                                                        # Группа для установленных файлов
        owner: root                                                       # Владелец установленных файлов
        src: "{{ ydb_dir }}/release/{{ ydb_version }}/{{ ydb_archive }}"  # Исходный архив
        extra_opts: "{{ ydb_unpack_options }}"                            # Дополнительные опции распаковки

    - name: Create the YDB CLI default binary directory
      file:
        state: directory                                 # Создать каталог
        path: "{{ ydb_dir }}/home/ydb/bin"               # Путь к каталогу бинарных файлов YDB CLI
        recurse: true                                    # Рекурсивное применение настроек к подкаталогам
        group: ydb                                       # Группа для каталога
        owner: ydb                                       # Владелец каталога
        mode: '0700'                                     # Права доступа к каталогу
        ```  

### Генерирование конфигурационных файлов для нод и запуск сервиса YDB { #ydb-gen-config }

Блок содержит задачи генерирования конфигурационных файлов для статической и динамической ноды, их копирования на ВМ и запуск нод с помощью systemd. 

1. Генерирование конфигурационного файла для нод: 
    ```yaml
    - name: Generate YDB static configuration file from template
      ansible.builtin.template:
        src: ./files/static_config.j2                    # Путь к шаблону Jinja2 для статической конфигурации
        dest: "{{ ydb_dir }}/cfg/static_config.yaml"     # Путь назначения для сгенерированного статического конфигурационного файла
      vars:
        non_mounted_disks: "{{ non_mounted_disks}}"      # Переменные, используемые в шаблоне

    - name: Generate YDB dynamic configuration file from template
      ansible.builtin.template:
        src: ./files/dynamic_config.j2                   # Путь к шаблону Jinja2 для динамической конфигурации
        dest: "{{ ydb_dir }}/cfg/dynamic_config.yaml"    # Путь назначения для сгенерированного динамического конфигурационного файла
      vars:
        non_mounted_disks: "{{ non_mounted_disks }}"     # Переменные, используемые в шаблоне
    ```      

2. Генерация service-файлов для systemd:
    ```yaml
    - name: Generate the YDB static node service file from template
      template:
        src: ./files/static-service.j2                        # Путь к шаблону Jinja2 для сервисного файла статического узла
        dest: "/etc/systemd/system/ydbd-storage.service"      # Место назначения для сгенерированного сервисного файла статического узла

    - name: Generate the YDB dynamic node service file from template
      template:
        src: ./files/dynnode-service.j2                       # Путь к шаблону Jinja2 для сервисного файла динамического узла
        dest: "/etc/systemd/system/ydbd-dynnode.service"      # Место назначения для сгенерированного сервисного файла динамического узла
    ```

3. Перезапуск systemd:
    ```yaml
    - name: Refresh systemd configuration to recognize new services
        ansible.builtin.systemd:
        daemon_reload: true                                   # Перезагрузка конфигурации демона systemd
    ```    
4. Запуск статической ноды с помощью systemd:
    ```yaml
    - name: Start the YDB storage node service
      become: true                          # Запуск задачи от имени суперпользователя
      ansible.builtin.systemd:
        state: started                      # Указывает, что сервис должен быть запущен
        name: ydbd-storage                  # Имя сервиса, который необходимо запустить
        any_errors_fatal: true              # Любые ошибки в этом задании будут считаться фатальными

    ```    
5. Проверка, что статическая нода запустилась. Проверка осуществляется командой `systemctl is-active ydbd-storage || true`. Если команда возвращается `active`, то значение записывается в переменную `ydbd_storage_status` и выполняются следующие задачи, если возвращается любой другой статус – выполнение плейбука прекращается. Предпринимаются 5 попыток опроса статуса сервиса с интервалом в 10 секунд. Параметры выполнения задачи:
    ```yaml
    - name: Verify that the YDB storage node service is active
      shell: systemctl is-active ydbd-storage || true  # Выполнение команды для проверки активности сервиса
      register: ydbd_storage_status                    # Регистрация результата выполнения команды
      any_errors_fatal: true                           # Любые ошибки считаются фатальными
      retries: 5                                       # Количество попыток выполнения
      delay: 10                                        # Задержка между попытками (в секундах)
      until: ydbd_storage_status.stdout == "active"    # Условие для повторения: сервис должен быть активен
    ``` 
6. Создание базы данных и её регистрация. Базу данных можно создать, используя API, CLI и SDK. В приведенном блоке задач база данных создаётся и регистрируется с помощью API в блоке `else` условного оператора `if` (предотвращает ошибки при повторном запуске плейбука) – `... ydbd -s grpc://... admin blobstorage config init --yaml-file ...`. Для каждой задачи передается переменная окружения `LD_LIBRARY_PATH`, которая нужна для корректного запуска YDB. Задачи данного блока выполняются, только если статус статической ноды `active`. Параметры исполняемых задач:
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
            {{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd -s grpc://{{ inner_net }}:{{ grpc_port }} admin database /{{ ydb_domain }}/{{ ydb_dbname }} create ssd:{{ non_mounted_disks.stdout_lines | length }} && echo 'register_database_done' >> {{ ydb_dir }}/ydb_init_status;
        fi
    environment:
        LD_LIBRARY_PATH: "{{ ydb_dir }}/release/{{ ydb_version }}/lib"
    when: ydbd_storage_status.stdout == "active"
    ```
7. Запуск динамической ноды. Логика и условия выполнения задачи запуска динамической ноды идентичны условиям выполнения задачи запуска статической ноды: 
    ```yaml
    - name: Start database processes if YDB storage node is active
      become: true                                  # Использовать повышение привилегий (например, sudo)
      ansible.builtin.systemd:
        state: started                              # Указывает, что сервис должен быть запущен
        name: "ydbd-dynnode"                        # Имя сервиса, который необходимо запустить
        any_errors_fatal: true                      # Любые ошибки в этом задании будут считаться фатальными
      retries: 5                                    # Количество попыток выполнения
      delay: 10                                     # Задержка между попытками (в секундах)
      when: ydbd_storage_status.stdout == "active"  # Условие для выполнения задания: сервис ydbd-storage должен быть активен
    ```

### Создание SSH-туннеля и настройка доступа к web-консоли YDB { #ssh-tunnel-create }
Работа по созданию SSH-туннеля ведется на локальной машине без использования прав суперпользователя. SSH-туннель нужен для проброса порта мониторинга YDB на локальную машину. Это делает доступ к БД безопасным и простым, так как не надо настраивать сертификаты безопасности, firewall и порты.

1. Получение списка доступных оболочек командной строки локальной ОС и сохранение списка оболочек в переменную `shell_output`. Опции выполняемой команды:
    ```yaml
    - name: Get list of available shells from /etc/shells
      delegate_to: localhost                       # Выполнить команду на управляющем узле (localhost)
      become: false                                # Не использовать повышение привилегий
      ansible.builtin.command: cat /etc/shells     # Выполнить команду 'cat /etc/shells'
      register: shell_output                       # Зарегистрировать вывод команды в переменной shell_output
    ```
2. Выбор активной оболочки командной строки. Часто бывает, что в ОС установлено несколько командных оболочек, чтобы гарантировать большую совместимость выполняемых команд с командной оболочкой, текущая задача проверяет список доступных оболочек, и если среди них есть `bash` или `zsh`, то выбирается одна из них, если нет – используется любая доуступная оболочка:
    ```yaml
    - name: Set active shell to bash or zsh if available
      set_fact:
        active_shell: "{{ '/bin/bash' if '/bin/bash' in shell_output.stdout else '/bin/zsh' if '/bin/zsh' in shell_output.stdout else omit }}"
    ```
3. Создание SSH-туннеля. Процедура создания SSH-туннеля может занимать длительное время, поэтому текущая задача выполняется асинхронно, чтобы не блокировать терминал. Опции выполняемой задачи:
    ```yaml
    - name: Establish an SSH tunnel to the YDB monitoring port
      delegate_to: localhost                         # Выполнить команду на управляющем узле (localhost)
      become: false                                  # Не использовать повышение привилегий
      ansible.builtin.shell: 
        cmd: >                                       # Команда для установки SSH туннеля
          ssh  -f -L {{ mon_port }}:localhost:{{ mon_port }} -N -i {{ ansible_ssh_private_key_file }}
          -vvv {{ ansible_ssh_user }}@{{ inventory_hostname }}
        executable: "{{ active_shell }}"             # Использовать оболочку, указанную в переменной active_shell
      async: 15                                      # Запустить задачу асинхронно, продолжая выполнение playbook
      poll: 0                                        # Не опрашивать статус задачи (0 означает "никогда")

    ```
4. Вывод в терминал сообщения с URL для подключения к web-консоли YDB:
    ```yaml
    - name: Display the YDB monitoring connection URL
      ansible.builtin.debug:
        msg: "Now you can connect to YDB monitoring via this URL: http://localhost:{{ mon_port }}" # Сообщение с URL для подключения к мониторингу YDB
    ```

В итоге успешного выполнения плейбука будет установлен YDB на ВМ, запущена статическая и динамическая нода и настроен доступ к порту мониторинга YDB. 

## Troubleshooting { #troubleshooting }
Ansible – это мощный инструмент автоматической установки и настройки окружений на ВМ. Однако стабильность работы плейбуков зависит от многих факторов: версии Python, настроек Ansible, версии оболочки командной строки, настроек безопасности на хостах, правильности указания значений переменных, правильности расположения файлов проекта. 

Приведём возможные ошибки, с которыми можно столкнуться при использовании данного плейбука и сценарии их решения:

#|
|| **Задача** | **Ошибка** | **Причина** | **Решение** || 
||  Установка SSH-соединения | 
```
Failed to connect to the host via ssh: 
ssh: connect to host ... 
port 22: Connection refused
``` | SSH-сервер не успел запуститься на удаленном хосте. | Повторить попытку подключения позже. ||  
|| Установка SSH-соединения | 
```
Failed to connect to the host via ssh: 
Warning: Permanently added '...' 
(ED25519) to the list of known hosts.
\r\nno such identity: ...: 
No such file or directory\r\...: Permission denied (publickey).
``` | Не правильно указан путь к SSH-ключу или его название в опции `ansible_ssh_private_key_file` инвентаризационного файла | Проверить правильность пути и название SSH-ключа. Скорректировать их, если они не верны. ||
|| Установка SSH-соединения | 
```
Failed to connect to the host via ssh: ...: 
Permission denied (publickey).
``` | Не правильно указан пользователь для подключения в опции `ansible_ssh_user` инвентаризационного файла. | Если вы использовали Terraform-провайдер Yandex Cloud – проверьте данные для подключения по SSH в блоке `metadata` ресурса `yandex_compute_instance`. Если ВМ в Yandex Cloud была создана вручную – параметры подключения по SSH находятся в **Облачная консоль Yandex CLoud** → **Виртуальные машины** → *<Ваша виртуальная машина>* → **Обзор** → **Метаданные**.

Если для создания ВМ вы использовали иного облачного провайдера – обратитесь к документации провайдера. ||
|| Download the YDB sources archive | 
```
Request failed", "response": 
"HTTP Error 404: Not Found", 
"status_code": 404
``` | Не правильно указана версия YDB для скачивания в переменной `ydb_version` или ошибка в URL для скачивания YDB в переменной `ydb_download_url`. | Перейдите [по ссылке](../downloads/index.md#ydb-server), скопируйте тег последней доступной версии YDB (например, 23.3.13) и вставьте его в переменную `ydb_version`. ||
|| Generate YDB static configuration file from template | ```Could not find or access './files/...'``` |
* Директория `files`, содержащая шаблоны файлов в формате `.j2` имеет ошибку в названии или имеет не правильное расположение относительно плейбука.
* Названия шаблонов файлов в директории `files` не соответствуют путям их вызова в опциях `src` задач генерации конфигурационных файлов. |     
* Убедитесь, что директория `files` располагается на одном уровне с плейбуком.
* Убедитесь, что пути к шаблонам файлов, указанные в опциях `src` задач по генерации конфигурационных файлов соответствуют реальному их расположению в файловой системе. ||
|| Verify that the YDB storage node service is active | 
```
FAILED - RETRYING: [...]: 
Verify that the YDB storage node service 
is active (5 retries left).
``` | Сервис `ydbd-storage` не был успешно запущен через systemd из-за ошибки в конфигурационном файле статической ноды. | Подключитесь по SSH к ВМ и выполните команду `journalctl -u ydbd-storage` – будет выведен лог запуска статической ноды. Найдите строку `Caught exception` или блок `uncaught exception`:
```
uncaught exception:
    address -> 0x46e97ed01810
    what() -> "yaml-cpp: error at line 82, column 32: illegal EOF in scalar"
    type -> YAML::ParserException
```
Строка `what() -> "yaml-cpp: error at line 82, column 32: illegal EOF in scalar"` будет содержать информацию об ошибки в конфигурационном файле статической ноды. Внесите правки в исходных шаблон файла и перезапустите плейбук. ||
|| Start database processes if YDB storage node is active | 
```
FAILED - RETRYING: [...]: 
Start database processes if 
YDB storage node is active (5 retries left).
``` | Сервис `ydbd-dynnode` не был успешно запущен через systemd из-за ошибки в конфигурационном файле динамической ноды. | Подключитесь по SSH к ВМ и выполните команду `journalctl -u ydbd-dynnode` – будет выведен лог запуска статической ноды. Найдите блок `uncaught exception` или строку `Caught exception`: 
```
Caught exception: 
.../yaml_config/yaml_config_parser.cpp:36: 
Array field `fail_domains` must be specified.
``` 
В конце сообщения будет указана причина ошибки. В данном случае ошибка в том, что блок настройки `fail_domains` в конфигурационном файле динамической ноды оформлен не верно. ||
|| Display the YDB monitoring connection URL | `Ссылка для подключения к мониторингу YDB выдаёт 404 ошибку` | SSH-туннель не был настроен или проброс порта мониторинга на локальную машину не был выполнен. | Выполните ручной проброс порта командой: 
```
ssh -L <mon_port>:localhost:<mon_port> \ 
-i <path to private SSH key> <user>@<IP VM>
```
Порт мониторинга устанавливается переменной `mon_port` в файле `files/all.yml`. ||
|#
