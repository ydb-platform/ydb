# Установка

В этом руководстве описано, как подготовить и установить YDB Enterprise Manager.

## Требования

YDB EM использует базу данных YDB в качестве хранилища метаданных для двух своих основных компонентов (Gateway и Control Plane). Поэтому вам необходимо иметь следующую информацию о базе данных YDB:
- **Эндпоинт**: например, `grpcs://ydb-node01.ru-central1.internal:2135`
- **Учетные данные**: Имя пользователя и пароль для получения токена.

Кроме того, убедитесь, что узлы YDB для динамических слотов настроены правильно:
- Время синхронизировано на всех узлах.
- Имена хостов используют свои полные доменные имена (FQDN).
- TLS-сертификаты настроены для каждого узла.
- Бинарные файлы расположены по пути `/opt/ydb`.
- В YDB включена динамическая конфигурация.

<!-- TODO: Если необходимо, добавьте пошаговую проверку этих требований -->

## Загрузка пакета

Загрузите последнюю версию пакета с нашего сайта:

| Версия | URL |
|---|---|
| 1.0.1  | [http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz](http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz) |

<!-- TODO: Обновите URL-адрес сайта на официальную страницу релиза вместо прямой ссылки на скачивание, если доступно -->

## Подготовка и Установка

1. Проверьте кластер YDB и получите [необходимую информацию](#требования).
2. Скачайте пакет на локальный компьютер и распакуйте его. Пакет включает в себя:
   - `ydb-em-gateway`: Бинарный файл (YDB EM Gateway).
   - `ydb-em-cp`: Бинарный файл (YDB EM CP).
   - `ydb-em-agent`: Бинарный файл (YDB EM CP Agent).
   - `ydb_platform-ydb-XXX.tar.gz`: Ansible коллекция для YDB.
   - `ydb_platform-ydb_em-YYY.tar.gz`: Ansible коллекция для YDB EM.
   - `examples.tar.gz`: Примеры конфигурации для Ansible.
   - `prepare.sh`: Скрипт для автоматизации шагов 3-5.
3. Установите коллекцию YDB для Ansible, если она отсутствует:
   ```bash
   ansible-galaxy collection install ydb_platform-ydb-1.2.0.tar.gz
   ```
4. Установите коллекцию YDB EM для Ansible:
   ```bash
   ansible-galaxy collection install ydb_platform-ydb_em-1.0.1.tar.gz
   ```
5. Распакуйте `examples.tar.gz`.
6. Переместите все бинарные файлы из пакета в папку `examples/files`.
7. Поместите TLS-сертификаты в папку `examples/files/certs`.
8. Обновите инвентарные файлы Ansible: `examples/inventory/50-inventory.yaml` и `examples/inventory/99-inventory-vault.yaml`.
   - `99-inventory-vault.yaml`: Содержит пароль для YDB.
   - Обновите список хостов:
     - Группа `ydb_em`: Хосты для YDB EM Gateway и YDB EM CP.
     - Группа `ydbd_dynamic`: Хосты для YDB EM CP Agent (бинарные файлы YDB должны быть установлены до этой установки).

     ```yaml
     ydb:
       children:
         ydbd_dynamic:
           hosts:
             ydb-node01.ru-central1.internal:
             # Пример специальных значений для хоста
             # ydb_em_agent_cpu - CPU, доступный для слотов на хосте
             # ydb_em_agent_memory - RAM в гигабайтах, доступная для слотов на хосте
             # ydb_em_agent_name - имя хоста для агента
                 ydb_em_agent_cpu: 4
                 ydb_em_agent_memory: 8
                 location: db-dc-1
             ydb-node02.ru-central1.internal:
                 ydb_em_agent_cpu: 4
                 ydb_em_agent_memory: 16
                 location: db-dc-2
             ydb-node03.ru-central1.internal:
                 location: db-dc-3
         # Группа `ydb_em` обязательна
         ydb_em:
           hosts:
             ydb-node01.ru-central1.internal:
     ```

9. Проверьте настройки SSH-подключения Ansible:
   ```yaml
   # Удаленный пользователь с правами sudo
   ansible_user: ansible
   # Настройки для подключения через Bastion/Jump хост (JUMP_IP)
   # ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
   # Общие настройки
   ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
   # Приватный ключ для удаленного пользователя
   ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
   ```

10. Проверьте настройки подключения к YDB:
    ```yaml
    # Расположение БД YDB. Это может быть любое расположение
    ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
    ydb_em_db_connection_db_root: "/Root"
    ydb_em_db_connection_db: "db"
    ydb_user: root
    # Пароль YDB находится в файле 99-inventory-vault.yaml
    # но вы можете определить его здесь
    # ydb_password: password
    ```

11. Проверьте настройки подключения к YDB CP:
    ```yaml
    # Должен указывать на хост в группе `ydb_em`
    # Эта настройка используется для конфигураций YDB EM Gateway
    # и YDB EM CP Agent
    # Но вы можете использовать любой прокси-эндпоинт
    # в качестве фронтенда для HA решений
    ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
    ```

12. Запустите `./install.sh` в папке `examples` или выполните плейбук вручную:
    ```bash
    ansible-playbook ydb_platform.ydb_em.initial_setup
    ```
