# Настройка конфигурации {#configuration}

Перед запуском развёртывания необходимо настроить файлы Ansible-инвентаря. Все файлы конфигурации расположены в директории `examples/inventory`.

## Настройка инвентаря хостов {#inventory}

Отредактируйте файл `examples/inventory/50-inventory.yaml` — он содержит описание хостов кластера.

### Группы хостов {#host-groups}

В инвентаре определены две группы хостов:

* **`ydb_em`** — хосты, на которых будут установлены {{ ydb-short-name }} EM Gateway и {{ ydb-short-name }} EM Control Plane.
* **`ydbd_dynamic`** — хосты, на которых будет установлен {{ ydb-short-name }} EM Agent. На этих хостах предварительно должны быть установлены бинарные файлы {{ ydb-short-name }}.

### Пример инвентаря {#inventory-example}

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
    # Группа ydb_em обязательна
    ydb_em:
      hosts:
        ydb-node01.ru-central1.internal:
```

### Параметры хостов {#host-parameters}

Для хостов группы `ydbd_dynamic` можно задать следующие параметры:

| Параметр | Описание |
|----------|----------|
| `ydb_em_agent_cpu` | Количество CPU, доступных для динамических слотов на хосте |
| `ydb_em_agent_memory` | Объём оперативной памяти (в гигабайтах), доступной для динамических слотов на хосте |
| `ydb_em_agent_name` | Имя хоста, используемое агентом |
| `location` | Расположение хоста (датацентр) |

<!-- TODO: Уточнить значения по умолчанию для ydb_em_agent_cpu и ydb_em_agent_memory, если они не указаны -->
<!-- TODO: Уточнить, обязательны ли эти параметры или опциональны -->

## Настройка пароля {{ ydb-short-name }} {#password}

Файл `examples/inventory/99-inventory-vault.yaml` содержит пароль для подключения к базе данных {{ ydb-short-name }}.

{% note warning %}

Рекомендуется использовать [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html) для шифрования файла с паролем.

{% endnote %}

<!-- TODO: Уточнить формат файла 99-inventory-vault.yaml и пример содержимого -->

## Настройка SSH-подключения {#ssh}

Проверьте настройки SSH-подключения в инвентаре:

```yaml
# Удалённый пользователь с правами sudo
ansible_user: ansible
# Настройки для подключения через Bastion/Jump-хост (JUMP_IP)
# ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
# Общие настройки
ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
# Приватный ключ для удалённого пользователя
ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
```

## Настройка подключения к {{ ydb-short-name }} {#ydb-connection}

Укажите параметры подключения к базе данных {{ ydb-short-name }}:

```yaml
# Адрес базы данных YDB EM
ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
ydb_em_db_connection_db_root: "/Root"
ydb_em_db_connection_db: "db"
ydb_user: root
# Пароль YDB задаётся в 99-inventory-vault.yaml,
# но может быть определён здесь
# ydb_password: password
```

<!-- TODO: Уточнить, нужно ли заранее создавать базу данных или она создаётся автоматически -->
<!-- TODO: Описать параметры ydb_em_db_connection_db_root и ydb_em_db_connection_db подробнее -->

## Настройка подключения к Control Plane {#cp-connection}

Укажите адрес подключения к {{ ydb-short-name }} EM Control Plane:

```yaml
# Должен указывать на хост из группы ydb_em.
# Этот параметр используется в конфигурациях
# YDB EM Gateway и YDB EM Agent.
# Вместо конкретного хоста можно указать адрес прокси
# для решений высокой доступности.
ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
```

<!-- TODO: Уточнить порт по умолчанию для CP (8787) и можно ли его изменить -->
<!-- TODO: Описать варианты настройки высокой доступности для CP -->
