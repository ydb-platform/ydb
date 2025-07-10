{% note alert %}

Функционал векторных индексов доступен в тестовом режиме в main. Для включения векторных индексов необходимо установить значение feature flag `EnableVectorIndex` в `true` в [динамической конфигурации кластера](../../core/maintenance/manual/dynamic-config#dinamicheskaya-konfiguraciya-klastera). 
Начиная с версии 25.1.2. функционал векторных индексов будет включен по умолчанию.

https://ydb.tech/docs/ru/maintenance/manual/dynamic-config#dinamicheskaya-konfiguraciya-klastera

В настоящее время не поддерживается:

* изменение строк в таблицах с векторными индексами;
* построение индекса для векторов c [битовым квантованием](../yql/reference/udf/list/knn.md#functions-convert).

Эти ограничения могут быть устранены в будущих версиях.

{% endnote %}
