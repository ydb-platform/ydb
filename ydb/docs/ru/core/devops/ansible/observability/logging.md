# Логирование в кластерах, развёрнутых с помощью Ansible

Во время [первоначального развёртывания](../initial-deployment.md) Ansible playbook настраивает несколько [systemd](https://systemd.io/) юнитов, которые управляют узлами {{ ydb-short-name }}. Как правило, на каждом физическом сервере или виртуальной машине работает несколько узлов {{ ydb-short-name }}, каждый из которых имеет свой собственный лог. Существует два основных способа просмотра логов такого кластера: через Ansible playbook или через ssh.

## Просмотр логов с помощью Ansible playbook

Репозиторий [ydb-ansible](https://github.com/ydb-platform/ydb-ansible) содержит playbook под названием `ydb_platform.ydb.logs`, который можно использовать для просмотра логов со всех узлов кластера {{ ydb-short-name }}. Этот playbook собирает логи с узлов и выводит их в `stdout`, что позволяет при необходимости дальше их обработать, например, с помощью команд `grep` или `awk`.

### Все логи всех узлов

По умолчанию playbook `ydb_platform.ydb.logs` извлекает логи всех узлов {{ ydb-short-name }}. Команда для этого:

```bash
ansible-playbook ydb_platform.ydb.logs
```

### Фильтрация по типу узла

В кластере {{ ydb-short-name }} есть два основных типа узлов:

* Storage (также известные как статические)
* Database (также известные как динамические)

Задачи в playbook `ydb_platform.ydb.logs` размечены тегами по типам узлов, благодаря чему можно использовать функциональность тегов Ansible для фильтрации логов по типу узла.

Следующие две команды эквивалентны и будут выводить логи статических узлов:

```bash
ansible-playbook ydb_platform.ydb.logs --tags storage
ansible-playbook ydb_platform.ydb.logs --tags static
```

Эти две команды также эквивалентны и будут выводить логи динамических узлов:

```bash
ansible-playbook ydb_platform.ydb.logs --tags database
ansible-playbook ydb_platform.ydb.logs --tags dynamic
```

### Фильтрация по имени хоста

Чтобы отобразить логи определённого хоста или подмножества хостов, используйте аргумент `--limit`:

```bash
ansible-playbook ydb_platform.ydb.logs --limit='<hostname>'
ansible-playbook ydb_platform.ydb.logs --limit='<hostname-1,hostname-2>'
```

Его также можно использовать вместе с тегами:

```bash
ansible-playbook ydb_platform.ydb.logs --tags database --limit='<hostname>'
```

## Просмотр логов по ssh

Чтобы вручную получить доступ к логам кластера {{ ydb-short-name }} через `ssh`, выполните следующие действия:

1. Сформируйте команду `ssh` для доступа к серверу, на котором запущен узел {{ ydb-short-name }}, для которого вам нужны логи. Базовая версия будет выглядеть как `ssh -i <path-to-ssh-key> <username>@<hostname>`. Возьмите значения для составления этой команды из файла `inventory/50-inventory.yaml`, который вы использовали для развёртывания:

    * `<path-to-ssh-key>` — это `children.ydb.ansible_ssh_private_key_file`;
    * `<username>` — это `children.ydb.ansible_user`;
    * `<hostname>` — это один из `children.ydb.hosts`.

2. Выберите, логи какого systemd юнита вам нужны. Если вам уже известно название юнита, этот шаг можно пропустить. После входа на сервер с помощью команды `ssh`, созданной на предыдущем шаге, получите список связанных с {{ ydb-short-name }} systemd юнитов, используя `systemctl list-units | grep ydb`. Как правило, там будет один статический узел и несколько динамических узлов.

    {% cut "Пример вывода" %}
    ```bash
    $ systemctl list-units | grep ydb
    ydb-transparent-hugepages.service                                              loaded active     exited    Configure Transparent Huge Pages (THP)
    ydbd-database-a.service                                                        loaded active     running   YDB dynamic node / database / a
    ydbd-database-b.service                                                        loaded active     running   YDB dynamic node / database / b
    ydbd-storage.service                                                           loaded active     running   YDB storage node
    ```
    {% endcut %}

3. Возьмите название systemd юнита из предыдущего шага и используйте его в следующей команде `journalctl -u <systemd-unit>`, чтобы фактически отобразить логи. Вы можете указать `-u` несколько раз, чтобы отобразить логи нескольких юнитов, а также использовать любые другие аргументы из `man journalctl` для настройки вывода команды.