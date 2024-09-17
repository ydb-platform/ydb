# Обновление версии {{ ydb-short-name }} на кластерах, развёрнутых с помощью Ansible

Во время [начального развёртывания](initial-deployment.md) Ansible playbook предоставляет несколько варивантов для выбора какой именно серверный исполняемый файл {{ ydb-short-name }} (`ydbd`) использовать. В этой статье объясняются доступные варианты изменения [версии](../../downloads/index.md) кластера после начального развёртывания.

{% note warning %}

У {{ ydb-short-name }} существуют специфические правила относительно совместимости версий. Важно ознакомиться с [руководством по совместимости версий](../manual/upgrade.md#version-compatability) и [списком изменений](../../changelog-server.md), чтобы правильно выбрать новую версию для обновления и подготовиться к возможным нюансам.

{% endnote %}

## Обновление исполняемых файлов через Ansible playbook

Репозиторий [ydb-ansible](https://github.com/ydb-platform/ydb-ansible) содержит playbook под названием `ydb_platform.ydb.update_executable`, который можно использовать для обновления или понижения версии кластера {{ ydb-short-name }}. Перейдите в ту же директорию, которая использовалась для [начального развёртывания](initial-deployment.md), отредактируйте файл `inventory/50-inventory.yaml`, чтобы указать целевую версию {{ ydb-short-name }} для установки (обычно через переменные `ydb_version` или `ydb_git_version`), а затем выполните этот playbook:

```bash
ansible-playbook ydb_platform.ydb.update_executable
```

Playbook получает новый бинарный файл и затем разворачивает его на кластере с помощью кросс-серверного копирования Ansible. После этого он выполняет [постепенную перезагрузку](restart.md) кластера.

### Фильтрация по типу узла

Задачи в playbook `ydb_platform.ydb.update_executable` помечены типами узлов, поэтому можно использовать функциональность тегов Ansible для фильтрации узлов по их типу.

Эти две команды эквивалентны и изменят конфигурацию всех [узлов хранения](../../concepts/glossary.md#storage-node):

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags storage
ansible-playbook ydb_platform.ydb.update_executable --tags static
```

Эти две команды эквивалентны и изменят конфигурацию всех [узлов баз данных](../../concepts/glossary.md#database-node):

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags database
ansible-playbook ydb_platform.ydb.update_executable --tags dynamic
```

### Пропуск перезагрузки

Существует тег `no_restart`, чтобы только развернуть исполняемые файлы, а перезагрузку кластера пропустить. Это может быть полезно, если кластер будет [перезагружен](restart.md) позже вручную или в рамках других задач по обслуживанию. Пример запуска:

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags no_restart
```