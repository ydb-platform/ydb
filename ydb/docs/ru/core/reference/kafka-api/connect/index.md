
# Использование Kafka Connect
В разделе приведены примеры настроек популярных коннекторов Kafka Connect для работы c YDB.

Инструмент [Kafka Connect](https://kafka.apache.org/documentation/#connect) предназначен для перемещения данных между Apache Kafka® и другими хранилищами данных. YDB поддерживает работу с топиками по протоколу Kafka, поэтому можно использовать коннекторы Kafka Connect для работы с YDB.

Подробную информацию о Kafka Connect и его настройке см. в документации [Apache Kafka®](https://kafka.apache.org/documentation/#connect).


{% note warning %}

Экземпляры Kafka Connect для работы с YDB стоит разворачивать только в режиме одного процесса-исполнителя (standalone mode). YDB не поддерживает работу Kafka Connect в распределенном режиме (distributed mode).

{% endnote %}