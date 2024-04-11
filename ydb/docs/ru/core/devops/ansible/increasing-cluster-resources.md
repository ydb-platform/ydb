# Увеличение ресурсов кластера {{ ydb-short-name }} созданного с помощью Ansible

В статье описаны способы увелечения вычислительных ресурсов и дискового пространства кластера {{ ydb-short-name }} с помощью связки Terraform, Ansible и утилиты `ydbops` без остановки кластера. Процесс увелечения ресурсов кластера имеет несколько шагов:
1. Изменение конфигурации инфраструктуры с помощью Terraform. Данный шаг не влияет на состояние работающего кластера {{ ydb-short-name }}.
2. Обновление текущего конфигурационного файла кластера с помощью Ansible. (Опциональный шаг для процесса увелечения дискового пространства кластера).
3. Применение конфигурационных изменений без остановки кластера утилитой `ydbops`.

Утилита `ydbops` поставляется в виде исходного кода написанного на Go, который можно скачать из [репозитория](https://github.com/ydb-platform/ydbops), а собрать исполняемый файл можно командой `go build`, находясь в директории с исходным кодом утилиты. После компиляции утилита запускается командой `./ydbops run --<some_parametr>` из каталога, где располагается исполняемый файл `ydbops`. 

Для работы утилиты с кластером потребуются файлы, входящие в готовые [Ansible шаблоны](https://github.com/ydb-platform/ydb-ansible-examples) развертывания кластера {{ ydb-short-name }}:
* Корневой TLS-сертификат (`ca.crt`) кластера – располагается по пути `TLS/CA/certs/`.
* Пароль `root` пользователя БД – находится в файле `ansible_vault_password_file` по пути `ydb-ansible-examples/<example_folder>/ansible_vault_password_file`. 


**Перед использованием утилиты необходимо сделать следующие действия**: 

1. Выполнить команду, которая последовательно открывает SSH-соединения с нодами кластера {{ ydb-short-name }} через `jumphost` и добавляет `fqdn` нод кластера в локальный `khowhosts`:
    ```bash
    for x in $(seq 1 9); \
    do echo $x; \
    ssh -i ~/yandex -o ProxyJump=ubuntu@<public_ip_jumphost> <ssh_user>@<node_name_prefix>$x.<cluster_domain> whoami;  \
    done
    ```

    Описание и дефолтные значения полей команды: 
    * `<public_ip_jumphost>` – публичный IP `jumphost`.
    * `<ssh_user>` – пользователь для подключения по SSH. По умолчанию: `ubuntu`.
    * `<node_name_prefix>` – префикс `hostname` ноды кластера. По умолчанию: `static-node-`.
    * `<cluster_domain>` – доменное имя кластера. По умолчанию: `ydb-cluster.com`.

1. Добавить `fqdn` первой ноды в файл `hosts` (Linux: `/etc/hosts`, Windows: `c:\Windows\System32\Drivers\etc\hosts`, macOS: `/private/etc/hosts`):
    ```txt
    ::1 static-node-1
    ```  

1. Создать SSH-туннель с `jumphost` сервером, чтобы получить защищенный доступ к нодам кластера:
    ```
    ssh -i yandex -L 2135:<node_1_private_ip>:2135 -N -f ubuntu@<jumphost_public_ip>
    ``` 

Общий вид вызова утилиты для безопасной перезагрузки кластера после выполнения пользовательского bash-скрипта выглядит так:
```bash
./ydbops run \
-e grpcs://<fqdn_node_1> \
--password-file <path_to_ansible_vault_password_file> \
--user root \
--ca-file <path_to_ca.crt>  \
--payload <path_to_bash_script>
```

Описание ключей команды:
* `-e` (endpoint) – путь к первой ноде кластера в формате `grpcs://<fqdn_node>`. `fqdn` ноды должен быть внесен в файл `hosts`.
* `--password-file` – путь к файлу с паролем от `root` пользователя БД. Файл находится по пути `ydb-ansible-examples/<example_folder>/ansible_vault_password_file`.
* `--user` – имя пользователя для подключения к БД с правами администратора.
* `--ca-file` – путь к корневому TLS-сертификату кластера. Сертификат располагается по пути `ydb-ansible-examples/TLS/CA/certs/`.
* `--payload` – ссылка на bash-скрипт, который будет исполняться на нодах.


## Увеличение объёма дискового пространства кластера

При использование готовых [Terraform-сценариев](https://github.com/ydb-platform/ydb-terraform) ВМ для нод кластера создаются с одним дополнительным диском (200 GB) и возможностью подключения второго дополнительного диска.

Переменные, регулирующие размер дополнительных дисков и опцию подключения второго дополнительного диска, находятся в файле `<cloud_provider>/variables.tf`:
* `instance_first_attached_disk_size` – размер первого дополнительного диска. Значение по умолчанию: 200 GB.
* `instance_sec_attached_disk_size` – размер второго дополнительного диска. Значение по умолчанию: 50 GB.
* `sec_instance_attached_disk` – присоединение второго дополнительного диска. Значение по умолчанию: false.

{% note info %}

При увеличении дискового пространства кластера нужно помнить, что в Compute Cloud существуют квоты на суммарный объем HDD/SSD-дисков в одном облаке. При достижении квот Terraform выведет ошибку в терминал – ``, а диски не будут расширены или добавлены. Увеличить квоты можно в [консоле](https://console.yandex.cloud/cloud/) управления Yandex Cloud, разделе **Квоты**, Compute Cloud, квота **Общий объём SSD-дисков** (для HDD-дисков – **Общий объём HDD-дисков**). Ознакомиться с актуальными квотами и лимитами в Compute Cloud можно по [ссылке](https://yandex.cloud/ru/docs/compute/concepts/limits).

{% endnote %}

### Увеличение размера первого присоединённого диска

Для увелечения размера первого присоединённого диска нужно выполнить последовательность следующих действий:
* Изменить значение переменной `instance_first_attached_disk_size`. 
* Выполнить команду `terraform plan` для просмотра изменений инфраструктуры. В терминал будет выведена информация о изменение размера первого присоединенного диска (вывод в терминал на примере Yandex Cloud):
    ```txt
    # module.storage.yandex_compute_disk.first-attached-disk[8] will be updated in-place
    ~ resource "yandex_compute_disk" "first-attached-disk" {
            id          = "fv4nlfv4uefls4j0loer"
            name        = "static-node-9-1"
        ~ size        = 200 -> 300
            # (8 unchanged attributes hidden)

            # (1 unchanged block hidden)
        }
    ```
* Выполните команду `terraform apply` для применения новой конфигурации инфраструктуры. Увидеть, что дисковое пространство было увеличено можно в web-консоле облачного провайдера.    
* Запустить утилиту `ydbops` с ключом `--payload`, где указан путь к bash скрипту со следующим содержимом:
    ```bash
    #!/usr/bin/env bash

    JUMPHOST_IP=<jumphost_ip>
    DISK_NAME=<disk_name>
    SSH_PRIVATE_KEY_PATH=<ssh_private_key_path>

    ssh ubuntu@$HOSTNAME -o=ProxyJump=ubuntu@${JUMPHOST_IP} -i ${SSH_PRIVATE_KEY_PATH} \
    "sudo growpart /dev/${DISK_NAME} 1 && \
    sudo systemctl stop ydbd-storage && \
    sudo LD_LIBRARY_PATH=/opt/ydb/lib \
    /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_1 && \
    sudo systemctl start ydbd-storage"

    echo "SUCCESS $HOSTNAME"
    ```

    Описание значение переменных:
    * `JUMPHOST_IP` – публичный IP `jumphost`.
    * `DISK_NAME` – название блочного устройства в ОС. Для Yandex Cloud дефолтное название диска – `vdb`, а для AWS название диска – `nvme1n1`. 
    * `SSH_PRIVATE_KEY_PATH` – путь к приватной части SSH-ключа.

После запуска утилиты в терминал будут выведены сообщения, о начале процесса перезапуска нод – ```<time start working> info Start rolling restart```, а также будут выведены сообщения, об успешном завершении выполнения bash-скрипта: ```SUCCESS <fqdn node>```. Визуально наблюдать процесс расширения дискового пространства кластера можно в разделе [Cluster](http://localhost:8765/monitoring/cluster/storage?name=/Root), вкладке **Storage**. Индикаторы состояния `VDisks` будут меняться с зеленого цвета на желтый и синий, в зависимости от этапа состояния расширения диска. 

Когда цвет индикаторов всех `VDisks` станет зеленым – работу утилиты `ydbops` можно прекратить сочетанием клавиш `Ctr + C`.

### Подключение второго дополнительного диска к ВМ

В процессе подключения второго дополнительного диска задействованы Terraform, Ansible и `ydbops`. Для присоединения второго дополнительного диска к ВМ выполните следующие действия:
1. Измените значение следующих переменных в файле `<cloud_provider>/variables.tf`:
    * `sec_instance_attached_disk` – установить `true`. Значение по умолчанию: `false`.
    * `instance_sec_attached_disk_size` – установить желаемый стартовый размер второго дополнительного диска. Значение по умолчанию: 50 GB.
2. Выполните команды `terraform plan` и `terraform apply` для создания и прикрепления вторых дополнительных дисков к ВМ. Дискам будут присвоены следующие имена: `/dev/vdc` для Yandex Cloud и `/dev/nvme1n2` для AWS. 
3. Отредактируйте следующие части конфигурационного файла {{ ydb-short-name }} (`ydb-ansible-examples/<ansible_example>/files/config.yaml`):
    * Добавьте 
    ```yaml

    ```



## Увеличение вычислительных ресурсов кластера

Увеличение вычислительных ресурсов кластера производится через добавление новых нод. В процессе расширения кластера новыми нодами участвуют Terraform и Ansible. Выполните следующие действия для добавления новых нод в кластер:
1. Измените значение переменной `instance_count` в файле `<cloud_provider>/variables.tf` с текущего количества нод на желаемое. 
1. Выполнить команды `terraform plan` и `terraform apply` для создания новых ВМ.
1. Добавить `fqdn` новых нод в файл `/ydb-ansible-examples/TLS/ydb-ca-nodes.txt` и запустить скрипт генерации TLS-сертификатов `/ydb-ansible-examples/TLS/ydb-ca-update.sh`.
1. Отредактировать инвентаризационный файл `ydb-ansible-examples/<ansible_example>/inventory/50-inventory.yaml`:
    * Добавить в раздел `hosts` `fqdn` новых нод.
    * Указать актуальное значение переменной `ydb_tls_dir`.
1. Отредактировать следующие разделы конфигурационного файла {{ ydb-short-name }}:
    * Добавить новые ноды в раздел `hosts`.
    * Изменить значение переменных `state_storage.ring.node` и `nto_select` на актуальные.
    * Добавить новые ноды в раздел `fail_domains`
1. Запустить повторное исполнение Ansible-плейбука – `/ydb-ansible-examples/<ansible_example>/setup_playbook.yaml`.
1. Добавить `fqdn` новых нод в `know_hosts` командой:
    ```bash
    for x in $(seq <number_of_first_new_node> <number_of_last_new_node>); \
    do echo $x; \
    ssh -i ~/yandex -o ProxyJump=ubuntu@<public_ip_jumphost> <ssh_user>@<node_name_prefix>$x.<cluster_domain> whoami;  \
    done
    ```
1. Запустить утилиту `ydbops` со следующими параметрами: 
    ```bash
    ./ydbops restart  \
    -e grpcs://static-node-1 \
    --password-file /Users/pseudolukian/Pseudolukian_yandex/Ansible/ydb-ansible-examples/9-nodes-mirror-3-dc/ansible_vault_password_file \
    --user root \
    --ca-file /Users/pseudolukian/Pseudolukian_yandex/Ansible/ydb-ansible-examples/TLS/CA/certs/ca.crt  \
    ```
