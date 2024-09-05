##  Три доклада о {{ ydb-short-name }} с about:cloud {#2018-conf-about-cloud}

<div style="display:flex; flex-direction: row; justify-content: flex-start; flex-wrap: wrap; column-gap: 10px;">

{% include notitle [overview_tag](../../tags.md#overview) %}

{% include notitle [use_cases_tag](../../tags.md#use_cases) %}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

</div>

1. [{{ ydb-name }}](https://youtu.be/Kr6WIYPts8I?t=8558): платформа распределенных систем хранения данных, критичных к задержкам. Рассказ о {{ ydb-short-name }}, как о платформе, на которой можно строить различные системы хранения и обработки данных. На платформе {{ ydb-short-name }} построены:
* LogBroker (аналог Apache Kafka).
* Real-time Map Reduce (обработка потоков данных).
* Хранение временных рядов в системе мониторинга.

2. [{{ ydb-name }}](https://youtu.be/Kr6WIYPts8I?t=10550): Distributed SQL база данных. Рассказ о {{ ydb-short-name }} как базе данных. Из доклада вы узнаете:
* Что такое "Таблетка" и какими свойствами она обладает.
* Как работают распределенные транзакции в {{ ydb-short-name }} и что такое детерминированные транзакции.
* Что такое YQL и чем он отличается от SQL.

3. [{{ ydb-name }}](https://youtu.be/Kr6WIYPts8I?t=12861): сетевое блочное устройство. Рассказ о Network Block Store — сервисе виртуальных дисков, на которых работают все виртуальные машины Yandex Cloud. В докладе подробно разобрано:
* Устройство File System (Файл/директория, Операции с метаданными, Индекс блоков).
* Устройство Block Device (Хранение блоков данных, интерфейс взаимодействия).
* Работа с QEMU/KVM (Virtio-очереди, Виртуализация процессора, режим паравиртуализации).

@[YouTube](https://www.youtube.com/watch?v=Kr6WIYPts8I)

Доклады будут полезны всем, кто хочет познакомиться с {{ ydb-short-name }}.