# Развёртывание {{ ydb-short-name }} кластера с помощью Ansible

В этом руководстве описан процесс развёртывания кластера {{ ydb-short-name }} на группе серверов с использованием [Ansible](https://www.ansible.com/). Такой способ рекомендуется применять в средах с физическими серверами или виртуальными машинами.

Основные материалы:

- [{#T}](deployment-preparation.md)
- [{#T}](deployment-configuration-v1.md)
- [{#T}](deployment-configuration-v2.md)

## Разделы прежней инструкции

Прежняя общая инструкция разделена на статьи, перечисленные выше. Если вы перешли по ссылке на отдельный шаг, продолжите в соответствующем разделе для своей версии конфигурации.

### Настройка проекта Ansible {#ansible-project-setup}

[Настройте проект Ansible](deployment-preparation.md#ansible-project-setup).

### Файл инвентаризации {#inventory-create}

Откройте этот шаг для [Configuration V1](deployment-configuration-v1.md#inventory-create) или [Configuration V2](deployment-configuration-v2.md#inventory-create).

### Пароль пользователя root {#change-password}

Откройте этот шаг для [Configuration V1](deployment-configuration-v1.md#change-password) или [Configuration V2](deployment-configuration-v2.md#change-password).

### Конфигурация YDB {#ydb-config-prepare}

Откройте этот шаг для [Configuration V1](deployment-configuration-v1.md#ydb-config-prepare) или [Configuration V2](deployment-configuration-v2.md#ydb-config-prepare).

### Состояние кластера {#cluster-state}

Откройте этот шаг для [Configuration V1](deployment-configuration-v1.md#cluster-state) или [Configuration V2](deployment-configuration-v2.md#cluster-state).

### Тестирование кластера {#testing}

Откройте этот шаг для [Configuration V1](deployment-configuration-v1.md#testing) или [Configuration V2](deployment-configuration-v2.md#testing).
