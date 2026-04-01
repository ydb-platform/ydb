# Развёртывание {{ ydb-short-name }} Enterprise Manager

{{ ydb-short-name }} Enterprise Manager (далее — YDB EM) — это инструмент управления кластерами {{ ydb-short-name }}, предоставляющий веб-интерфейс и API для администрирования баз данных. YDB EM позволяет централизованно управлять ресурсами кластера, базами данных и динамическими узлами.

YDB EM состоит из трёх основных компонентов:

* **Gateway** — веб-интерфейс и API-бэкенд для взаимодействия пользователей с системой.
* **Control Plane (CP)** — непосредственное управление кластером {{ ydb-short-name }}, ресурсами и базами данных.
* **Agent** — служба, устанавливаемая на хосты кластера {{ ydb-short-name }} для управления узлами.

## Архитектура {#architecture}

Общая схема взаимодействия компонентов YDB EM:

```mermaid
flowchart LR
    User[Пользователь] --> Browser[Браузер]
    Browser -- "HTTP/HTTPS" --> Gateway[YDB EM Gateway]

    subgraph YDBEM [YDB EM]
        Gateway -- "gRPC" --> CP[YDB EM CP]
        Gateway -- "gRPC" --> EMDB[(YDB EM DB)]
        CP -- "gRPC" --> EMDB
    end

    subgraph Cluster [Кластер YDB]
        subgraph Host1 [Хост 1]
            Node1[Узел YDB]
            Agent1[YDB EM Agent]
        end
        subgraph Host2 [Хост 2]
            Node2[Узел YDB]
            Agent2[YDB EM Agent]
        end
        subgraph Host3 [Хост 3]
            Node3[Узел YDB]
            Agent3[YDB EM Agent]
        end
    end

    Agent1 -- "gRPC" --> CP
    Agent2 -- "gRPC" --> CP
    Agent3 -- "gRPC" --> CP
    Node1 -. "метаинформация" .-> Gateway
```

## Основные материалы {#materials}

- [{#T}](initial-deployment.md)
