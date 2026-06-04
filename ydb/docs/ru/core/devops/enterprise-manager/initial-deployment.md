# Первоначальное развёртывание YDB EM

<!-- markdownlint-disable blanks-around-fences -->

В этом руководстве описан процесс первоначального развёртывания {{ ydb-short-name }} Enterprise Manager (далее — YDB EM) с помощью [Ansible](https://www.ansible.com/). По завершении вы получите работающий экземпляр YDB EM, подключённый к существующему кластеру {{ ydb-short-name }}.

<!-- TODO: добавить альтернативные способы развёртывания (вручную, Kubernetes), когда они станут доступны -->

## Перед началом работы {#before-start}

### Требования {#requirements}

Для развёртывания YDB EM необходимо:

1. **Работающий кластер {{ ydb-short-name }}**. YDB EM подключается к существующему кластеру для управления его ресурсами; без кластера работа EM невозможна. В этом кластере будет автоматически создана служебная база данных для хранения метаинформации YDB EM. Подробнее о развёртывании кластера — в разделе [{#T}](../deployment-options/index.md).

1. **Параметры подключения к базе данных {{ ydb-short-name }}**, которая будет использоваться для хранения метаданных YDB EM:
    * Эндпоинт (например, `grpcs://ydb-node01.ru-central1.internal:2135`).
    * Имя пользователя и пароль для получения токена авторизации. Пользователь должен обладать правами на модификацию схемы, запись и чтение данных. Если кластер {{ ydb-short-name }} настроен на аутентификацию через LDAP, используйте учётную запись из соответствующего каталога.

    <!-- TODO: описать вопросы безопасности YDB EM (LDAP, TLS и т.д.) в отдельной статье -->

1. **Хост для компонентов Gateway и Control Plane** — один сервер (физический или виртуальный), на который будут установлены серверные компоненты YDB EM. Это может быть любой из хостов кластера {{ ydb-short-name }} или отдельная машина.

1. **[Ansible](https://www.ansible.com/)** на управляющей машине (с которой выполняется установка). Рекомендуется ansible-core 2.14–2.18, подробнее см. [{#T}](../deployment-options/ansible/initial-deployment/index.md).

1. **SSH-доступ** с управляющей машины ко всем серверам, на которые будут установлены компоненты YDB EM (хост Gateway/CP и хосты кластера для агентов).

### Сетевые требования {#network-requirements}

Сетевая конфигурация должна разрешать TCP-соединения по следующим портам (значения по умолчанию):

Источник | Приёмник | Порт | Протокол | Назначение
--- | --- | --- | --- | ---
Пользователь | Gateway | 8789 | HTTP/HTTPS | Веб-интерфейс и API
Gateway, Agent | Control Plane | 8787 | gRPC | Управляющие команды
Gateway, Control Plane | {{ ydb-short-name }} | 2135 | gRPC | Подключение к базе данных YDB EM DB

## Загрузка пакета {#download}

<!-- TODO: заменить ссылку на страницу Downloads, когда она появится для YDB EM -->

Скачайте пакет YDB EM на управляющую машину (с которой будет выполняться установка через Ansible). Актуальный номер версии и ссылку на архив уточняйте в информации о последнем релизе:

Версия | Ссылка
--- | ---
`<VERSION>` | `https://binaries.ydbem.website.yandexcloud.net/builds/<VERSION>/ydb-em-<VERSION>-stable-linux-amd64.tar.xz`

Распакуйте скачанный архив в рабочую директорию (замените `<VERSION>` на актуальную версию):

```bash
tar -xf ydb-em-<VERSION>-stable-linux-amd64.tar.xz
```

Содержимое пакета:

Файл | Описание
--- | ---
`bin/ydb-em-gateway` | Бинарный файл Gateway
`bin/ydb-em-cp` | Бинарный файл Control Plane
`bin/ydb-em-agent` | Бинарный файл Agent
`collections/ydb_platform-ydb-*.tar.gz` | Ansible-коллекция для {{ ydb-short-name }}
`collections/ydb_platform-ydb_em-*.tar.gz` | Ansible-коллекция для YDB EM
`examples.tar.gz` | Шаблоны конфигурации Ansible (инвентарь и плейбуки)
`install.sh` | Скрипт автоматической установки

## Установка Ansible-коллекций {#install-collections}

Установите Ansible-коллекцию для {{ ydb-short-name }}, если она ещё не установлена:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb-*.tar.gz
```

Установите Ansible-коллекцию для YDB EM:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb_em-*.tar.gz
```

{% note info %}

Версии Ansible-коллекций входят в состав пакета YDB EM. Используйте файлы из директории `collections/` вашего пакета.

{% endnote %}

## Подготовка конфигурации {#prepare-configuration}

### Распаковка шаблонов конфигурации {#unpack-examples}

Пакет YDB EM содержит готовые шаблоны конфигурации Ansible — файлы инвентаря, плейбуки и директории для размещения бинарных файлов и сертификатов. Распакуйте этот архив:

```bash
tar -xf examples.tar.gz
```

В результате появится директория `examples/`, которая будет использоваться как рабочая директория для установки.

### Размещение файлов {#place-files}

1. Скопируйте бинарные файлы из директории `bin/` пакета в директорию `examples/files/`:

    ```bash
    cp bin/ydb-em-gateway bin/ydb-em-cp bin/ydb-em-agent examples/files/
    ```

1. Разместите TLS-сертификаты в директории `examples/files/certs/`. Используйте те же сертификаты, что и для узлов кластера {{ ydb-short-name }} (сертификат CA, сертификаты и ключи узлов):

    ```bash
    cp /path/to/your/certs/* examples/files/certs/
    ```

### Настройка инвентаря Ansible {#configure-inventory}

Ansible использует файлы инвентаря для описания целевых серверов и их параметров. Конфигурация YDB EM разделена на несколько файлов:

* `examples/inventory/50-inventory.yaml` — общие параметры: SSH-подключение, подключение к YDB, настройки Control Plane.
* `examples/inventory/90-inventory.yaml` — описание серверов (какие хосты используются для каких компонентов).
* `examples/inventory/99-inventory-vault.yaml` — конфиденциальные данные (пароли).

#### Настройка списка хостов {#configure-hosts}

Откройте файл `examples/inventory/90-inventory.yaml` и настройте две группы хостов.

**Группа `ydb_em`** — хосты для серверных компонентов YDB EM (Gateway и Control Plane). Достаточно одного хоста — это может быть любой из хостов кластера {{ ydb-short-name }} или отдельный сервер:

```yaml
ydb_em:
  hosts:
    ydb-node01.ru-central1.internal:
```

**Группа `ydbd_dynamic`** — хосты кластера {{ ydb-short-name }}, на которые будет установлен Agent. Agent нужен на каждом хосте, где работают динамические узлы {{ ydb-short-name }}, — именно через агентов YDB EM управляет процессами узлов. Как правило, сюда включаются все хосты кластера:

```yaml
ydb:
  children:
    ydbd_dynamic:
      hosts:
        ydb-node01.ru-central1.internal:
            ydb_em_agent_cpu: 4
            ydb_em_agent_memory: 8
            location: db-dc-1
        ydb-node02.ru-central1.internal:
            ydb_em_agent_cpu: 4
            ydb_em_agent_memory: 16
            location: db-dc-2
        ydb-node03.ru-central1.internal:
            location: db-dc-3
    ydb_em:
      hosts:
        ydb-node01.ru-central1.internal:
```

Для каждого хоста в группе `ydbd_dynamic` можно указать дополнительные параметры. Если параметры не указаны, будут использованы значения по умолчанию. Уточнить настройки ресурсов можно позже через веб-интерфейс YDB EM.

Параметр | Описание
--- | ---
`ydb_em_agent_cpu` | Количество CPU, доступных для узлов на хосте
`ydb_em_agent_memory` | Объём RAM (в гигабайтах), доступный для узлов на хосте
`ydb_em_agent_name` | Имя хоста, используемое агентом
`location` | Расположение хоста (зона доступности)

{% note warning %}

Бинарные файлы {{ ydb-short-name }} должны быть установлены на всех хостах группы `ydbd_dynamic` до начала развёртывания YDB EM. Подробнее — в разделе [{#T}](../deployment-options/ansible/initial-deployment/index.md).

{% endnote %}

#### Настройка SSH-подключения {#configure-ssh}

В файле `examples/inventory/50-inventory.yaml` проверьте параметры SSH-подключения с управляющей машины к целевым серверам:

```yaml
# Удалённый пользователь с правами sudo
ansible_user: ansible

# Настройки для подключения через Bastion/Jump host (JUMP_IP)
# ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

# Общие настройки SSH
ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

# Приватный ключ для подключения
ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
```

{% note warning %}

Параметры `UserKnownHostsFile=/dev/null` и `StrictHostKeyChecking=no` отключают проверку ключей хостов SSH, что делает соединение уязвимым для атак типа «человек посередине» (MITM). Такая конфигурация допустима только для тестовых сред. В production-окружении рекомендуется использовать проверку ключей хостов, указав путь к реальному файлу `known_hosts`:

```yaml
ansible_ssh_common_args: "-o StrictHostKeyChecking=yes"
```

{% endnote %}

{% note info %}

Если для доступа к серверам используется Bastion/Jump host, раскомментируйте и настройте соответствующую строку `ansible_ssh_common_args`.

{% endnote %}

#### Настройка подключения к базе данных {{ ydb-short-name }} {#configure-ydb-connection}

В файле `examples/inventory/50-inventory.yaml` укажите параметры подключения к базе данных {{ ydb-short-name }}, которая будет использоваться для хранения метаданных YDB EM:

```yaml
# Эндпоинт подключения к YDB
ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
# Корневой домен
ydb_em_db_connection_db_root: "/Root"
# Имя базы данных
ydb_em_db_connection_db: "db"
# Пользователь YDB
ydb_user: root
```

Пароль пользователя {{ ydb-short-name }} задаётся в файле `examples/inventory/99-inventory-vault.yaml`:

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <пароль>
```

{% note tip %}

Для защиты конфиденциальных данных рекомендуется шифровать файл `99-inventory-vault.yaml` с помощью [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html):

```bash
ansible-vault encrypt examples/inventory/99-inventory-vault.yaml
```

{% endnote %}

#### Настройка подключения к Control Plane {#configure-cp-connection}

В файле `examples/inventory/50-inventory.yaml` укажите эндпоинт подключения к YDB EM Control Plane:

```yaml
# Эндпоинт должен указывать на хост из группы ydb_em
ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
```

Этот параметр используется в конфигурации **Gateway** и **Agent** для подключения к **Control Plane**.

{% note info %}

При необходимости обеспечения высокой доступности вместо прямого адреса хоста можно указать адрес балансировщика нагрузки (например, L3/L4 балансировщик или DNS-балансировка).

{% endnote %}

## Запуск установки {#run-installation}

После завершения настройки конфигурации перейдите в директорию `examples` и выполните установку одним из способов:

### Автоматическая установка {#auto-install}

Запустите скрипт автоматической установки:

```bash
cd examples
./install.sh
```

Скрипт автоматически выполнит все необходимые шаги по развёртыванию компонентов YDB EM.

### Ручной запуск Ansible {#manual-install}

Также можно выполнить установку вручную, запустив плейбук Ansible:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup
```

{% note info %}

Если файл `99-inventory-vault.yaml` зашифрован с помощью Ansible Vault, добавьте флаг `--ask-vault-pass`:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup --ask-vault-pass
```

{% endnote %}

## Проверка установки {#verify}

После успешного завершения установки откройте веб-интерфейс YDB EM в браузере:

```text
https://<FQDN хоста из группы ydb_em>:8789/ui/clusters
```

Например:

```text
https://ydb-node01.ru-central1.internal:8789/ui/clusters
```

В веб-интерфейсе должна отобразиться страница со списком кластеров {{ ydb-short-name }}.

## Устранение неполадок {#troubleshooting}

### Не удаётся подключиться к веб-интерфейсу {#cannot-connect-ui}

1. Убедитесь, что порт `8789` доступен с вашей машины:

    ```bash
    curl -k https://<FQDN>:8789/ui/clusters
    ```

1. Проверьте статус сервиса YDB EM Gateway на целевом хосте:

    ```bash
    sudo systemctl status ydb-em-gateway
    ```

1. Проверьте логи сервиса:

    ```bash
    sudo journalctl -u ydb-em-gateway -n 100
    ```

### Агент не подключается к Control Plane {#agent-not-connected}

1. Убедитесь, что порт `8787` доступен между узлами кластера и хостом Control Plane.

1. Проверьте статус сервиса YDB EM Agent на узле:

    ```bash
    sudo systemctl status ydb-em-agent
    ```

1. Проверьте корректность TLS-сертификатов и эндпоинта Control Plane в конфигурации агента.

### Ошибки подключения к базе данных {{ ydb-short-name }} {#db-connection-errors}

1. Убедитесь, что эндпоинт, имя базы данных, пользователь и пароль указаны корректно.

1. Проверьте сетевую доступность порта `2135` на узле {{ ydb-short-name }} с хоста YDB EM.

1. Проверьте логи YDB EM Control Plane:

    ```bash
    sudo journalctl -u ydb-em-cp -n 100
    ```
