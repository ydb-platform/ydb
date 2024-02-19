# Использование Kafka Connect
В разделе приведены примеры настроек популярных коннекторов Kafka Connect для работы c {{ ydb-short-name }}.

Инструмент [Kafka Connect](https://kafka.apache.org/documentation/#connect) предназначен для перемещения данных между Apache Kafka® и другими хранилищами данных. {{ ydb-short-name }} поддерживает работу с топиками по протоколу Kafka, поэтому можно использовать коннекторы Kafka Connect для работы с {{ ydb-short-name }}.

Подробную информацию о Kafka Connect и его настройке см. в документации [Apache Kafka®](https://kafka.apache.org/documentation/#connect).


{% note warning %}

Экземпляры Kafka Connect для работы с {{ ydb-short-name }} нужно разворачивать только в режиме одного процесса-исполнителя (standalone mode). {{ ydb-short-name }} не поддерживает работу Kafka Connect в распределенном режиме (distributed mode).

{% endnote %}