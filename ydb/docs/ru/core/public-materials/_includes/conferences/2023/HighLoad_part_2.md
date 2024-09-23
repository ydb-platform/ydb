### YDB Topic Service: как мы повышали производительность очереди сообщений {#2023-conf-hl-ydb-topic}

<div class = "multi-tags-container">

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

{% include notitle [testing_tag](../../tags.md#testing) %}

</div>

5 лет назад Яндекс перешел с Kafka на собственную разработку поверх YDB. С тех пор Yandex Topic Service сильно подрос по нагрузке и вышел в Open Source. В этом докладе {{ team.zivaykin.name }} ({{ team.zivaykin.position }}) рассказывает про ускорение YDB Topic Service и приводит сравнение с конкурентами.

@[YouTube](https://www.youtube.com/watch?v=I-6SS6_C1Cw&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=22&pp=iAQB)

Доклад будет интересен разработчикам, лидам разработки, техническим менеджерам. Всем, кто решал, решает или интересуется задачей масштабируемой поставки данных.

[Слайды](https://presentations.ydb.tech/2023/ru/highload/ydb_topic_service/presentation.pdf)


### Особенности шин данных для очень больших инсталляций на примере YDB Topics {#2023-conf-hl-data-bus}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Шины передачи данных используются практически везде, но использование шин данных в очень больших инсталляциях на тысячи серверов накладывают особые требования для работы и приводят к отличиям в работе систем. [{{ team.dmitriev.name }}]({{ team.dmitriev.profile }}) ({{ team.dmitriev.position }}) показывает на примере YDB Topics, в чем заключаются эти отличия, как они влияют на архитектуру и эксплуатацию.

@[YouTube](https://www.youtube.com/watch?v=zKPOAdNOQx4&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=92&pp=iAQB)

Доклад будет интересен разработчикам и командам эксплуатации, особенно в компаниях больших размеров. 

[Слайды](https://presentations.ydb.tech/2023/ru/highload/ydb_topics_data_bus/presentation.pdf)


### Поиск по образцу на последовательностях строк в БД {#2023-conf-hl-search-inline}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Задача поиска по образцу на последовательности строк БД может возникать в различных сферах деятельности. Например, в финансовой аналитике — поиск определённых паттернов изменения цены акций. Для реализации таких запросов к базам данных в стандарте `SQL:2016` была введена конструкция `MATCH_RECOGNIZE`. {{ team.zverev.name }} ({{ team.zverev.position }}) рассказывает о реализации `MATCH_RECOGNIZE` в YDB: о том, как это работает под капотом, какие подходы и алгоритмы реализованы, с какими сложностями столкнулась команда.

@[YouTube](https://www.youtube.com/watch?v=TSFVV0zGSBI&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=130&pp=iAQB)

Отдельная часть выступления посвящена отличиям в обработке аналитических запросов на табличках и обработке на потоках «живых» данных. Доклад будет интересен разработчикам БД, дата-аналитикам, а также всем, кто интересуется поиском по образцу на больших данных. 

[Слайды](https://presentations.ydb.tech/2023/ru/highload/template_search_in_str_seq/presentation.pdf)


### Из pytest в Go. Тестовое окружение на фикстурах {#2023-conf-hl-pytest-go}

{% include notitle [testing_tag](../../tags.md#testing) %}

Фикстуры позволяют писать очень лаконичные тесты и не отвлекаться на подготовку окружения. [{{ team.kulin.name }}]({{ team.kulin.profile }}) ({{ team.kulin.position }}) рассказал, как перенёс идеологию фикстур из pytest в Go и разработал библиотеку `fixenv`, которая сокращает код тестов и улучшает их читаемость.

@[YouTube](https://www.youtube.com/watch?v=Vtg8UmU62OA&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=100&pp=iAQB)

Доклад будет интересен разработчикам на Go и тимлидам. 

[Слайды](https://presentations.ydb.tech/2023/ru/golang_conf/from_pytest_to_go/presentation.pdf)