# Обзор YDB Enterprise Manager

YDB Enterprise Manager (YDB EM) — это система для управления кластерами YDB, ресурсами, базами данных и динамическими слотами на хостах.

## Компоненты

YDB EM состоит из следующих основных компонентов:

- **Gateway**: Предоставляет пользовательский интерфейс (UI) и API бэкенд для него.
- **Control Plane (CP)**: Отвечает за управление кластером YDB, управление ресурсами и настройку баз данных.
- **Agent**: Система для управления динамическими слотами на хосте.

## Архитектура

На диаграмме ниже показана высокоуровневая архитектура YDB EM и его взаимодействие с браузером и кластерами YDB.

```mermaid
flowchart LR
    User[fa:fa-user Пользователь] --> Browser
    Browser --http/https--> meta[YDB EM Gateway]
    ydb-host01-ydb --metainfo--> meta
    ydb-host01-agent --get dynnode--> CP
    ydb-host02-agent --get dynnode--> CP
    ydb-host03-agent --get dynnode--> CP

    subgraph YDBEM
        meta --grpc/grpcs--> YDB(fa:fa-database БД YDB EM)
        meta --grpc/grpcs--> CP[YDB EM CP]
        CP --grpc/grpcs--> YDB
    end


    subgraph YDBCluster1
        subgraph Node01
            ydb-host01-ydb[YDB Node 1]
            ydb-host01-agent[YDB EM CP agent1]
        end
        subgraph Node02
            ydb-host02-ydb[YDB Node 2]
            ydb-host02-agent[YDB EM CP agent2]
        end
        subgraph Node03
            ydb-host03-ydb[YDB Node 3]
            ydb-host03-agent[YDB EM CP agent3]
        end
    end
```

## Использование

<!-- TODO: Добавьте здесь более подробные инструкции по использованию или обзорную информацию, если необходимо -->

Для доступа к пользовательскому интерфейсу YDB Enterprise Manager откройте следующий URL в вашем браузере:

`https://<fqdn>:8789/ui/clusters`

*Примечание*: Замените `<fqdn>` на полное доменное имя (Fully Qualified Domain Name) любого хоста из группы хостов `ydb_em` (например, `ydb-node01.ru-central1.internal`).
