# Развёртывание {{ ydb-short-name }} Enterprise Manager

<!-- markdownlint-disable blanks-around-fences -->

В этом руководстве описан процесс первоначального развёртывания {{ ydb-short-name }} Enterprise Manager (далее — YDB EM) для управления кластерами {{ ydb-short-name }}. Установка выполняется с помощью [Ansible](https://www.ansible.com/).

## Перед началом работы {#before-start}

### Требования {#requirements}

Для развёртывания YDB EM необходимо выполнение следующих условий:

1. Развёрнут и функционирует кластер {{ ydb-short-name }}. Подробнее о развёртывании кластера — в разделе [{#T}](../ansible/initial-deployment/index.md) или [{#T}](../manual/index.md).

1. Известны параметры подключения к базе данных {{ ydb-short-name }}, которая будет использоваться для хранения метаданных YDB EM:
    * Эндпоинт (например, `grpcs://ydb-node01.ru-central1.internal:2135`).
    * Имя пользователя и пароль для получения токена авторизации. Пользователь должен обладать правами на модификацию схемы, запись и чтение данных.

1. Хосты кластера {{ ydb-short-name }}, на которых будут размещены узлы, подготовлены:
    * Время синхронизировано между узлами (например, с помощью `ntpd` или `chrony`).
    * Имена хостов соответствуют их полным доменным именам (FQDN).
    * TLS-сертификаты подготовлены для каждого узла.
    * Бинарные файлы {{ ydb-short-name }} установлены в директорию `/opt/ydb`.
    * Кластер {{ ydb-short-name }} настроен с [динамической конфигурацией](../../configuration-management/configuration-v2/index.md).

1. На машине, с которой выполняется установка, установлен [Ansible](https://www.ansible.com/) поддерживаемой версии (рекомендуется ansible-core 2.14–2.18, подробнее см. [{#T}](../ansible/initial-deployment/index.md)).

1. Настроен SSH-доступ ко всем серверам, на которые будут установлены компоненты YDB EM.

### Сетевые требования {#network-requirements}

Сетевая конфигурация должна разрешать TCP-соединения по следующим портам (значения по умолчанию):

Порт | Компонент | Назначение
--- | --- | ---
8787 | YDB EM Control Plane | gRPC-взаимодействие (Gateway ↔ CP, Agent ↔ CP)
8789 | YDB EM Gateway | HTTP/HTTPS-интерфейс (веб-интерфейс и API)
2135 | {{ ydb-short-name }} | gRPC-подключение к базе данных YDB EM DB

## Загрузка пакета {#download}

<!-- TODO: добавить ссылку на страницу со списком доступных версий YDB EM -->

Скачайте нужную версию пакета YDB EM. Актуальный номер версии и ссылку на архив уточняйте в информации о последнем релизе:

Версия | Ссылка
--- | ---
`<VERSION>` | `https://binaries.ydbem.website.yandexcloud.net/builds/<VERSION>/ydb-em-<VERSION>-stable-linux-amd64.tar.xz`

Распакуйте скачанный архив (замените `<VERSION>` на актуальную версию):

```bash
tar -xf ydb-em-<VERSION>-stable-linux-amd64.tar.xz
```

Содержимое пакета:

Файл | Описание
--- | ---
`bin/ydb-em-gateway` | Бинарный файл YDB EM Gateway
`bin/ydb-em-cp` | Бинарный файл YDB EM Control Plane
`bin/ydb-em-agent` | Бинарный файл YDB EM Agent
`collections/ydb_platform-ydb-XXX.tar.gz` | Ansible-коллекция для {{ ydb-short-name }}
`collections/ydb_platform-ydb_em-YYY.tar.gz` | Ansible-коллекция для YDB EM
`examples.tar.gz` | Примеры конфигурации для Ansible
`install.sh` | Скрипт автоматической установки

## Установка Ansible-коллекций {#install-collections}

Установите Ansible-коллекцию для {{ ydb-short-name }}, если она ещё не установлена:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb-2.0.0.tar.gz
```

Установите Ansible-коллекцию для YDB EM:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb_em-1.0.4.tar.gz
```

{% note info %}

Версии Ansible-коллекций в командах выше приведены для примера. Используйте версии, входящие в состав вашего пакета YDB EM.

{% endnote %}

## Подготовка конфигурации {#prepare-configuration}

### Распаковка примеров {#unpack-examples}

Распакуйте архив с примерами конфигурации:

```bash
tar -xf examples.tar.gz
```

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

Конфигурация инвентаря выполняется в нескольких файлах:

* `examples/inventory/50-inventory.yaml` — инвентарь кластера {{ ydb-short-name }} (параметры SSH-подключения, подключения к YDB, настройки Control Plane).
* `examples/inventory/90-inventory.yaml` — инвентарь YDB EM (группы хостов для компонентов YDB EM).
* `examples/inventory/99-inventory-vault.yaml` — конфиденциальные данные (пароли).

#### Настройка списка хостов {#configure-hosts}

Откройте файл `examples/inventory/90-inventory.yaml` и настройте группы хостов.

В группу `ydb_em` включите хосты, на которых будут установлены компоненты **Gateway** и **Control Plane**. Достаточно указать один хост — это может быть любой из хостов кластера или отдельный сервер:

```yaml
ydb_em:
  hosts:
    ydb-node01.ru-central1.internal:
```

В группу `ydbd_dynamic` включите все хосты кластера {{ ydb-short-name }}, на которых должен быть установлен **Agent**. Как правило, сюда включаются все хосты, на которых запускаются узлы {{ ydb-short-name }}:

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

Бинарные файлы {{ ydb-short-name }} должны быть установлены на всех хостах группы `ydbd_dynamic` до начала развёртывания YDB EM. Подробнее — в разделе [{#T}](../ansible/initial-deployment/index.md).

{% endnote %}

#### Настройка SSH-подключения {#configure-ssh}

В файле `examples/inventory/50-inventory.yaml` (инвентарь кластера {{ ydb-short-name }}) проверьте параметры SSH-подключения:

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
