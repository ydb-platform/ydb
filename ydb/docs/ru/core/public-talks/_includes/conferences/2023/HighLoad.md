### Реализовать OLAP: как мы делали колоночное хранение в {{ ydb-short-name }} {#2023-conf-hl-olap}

<div style="display:flex; flex-direction: row; justify-content: flex-start; flex-wrap: wrap; column-gap: 10px;">

{% include notitle [overview_tag](../../tags.md#overview) %}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

<div>

{{ ydb-short-name }} – это платформа, которая умеет обрабатывать большой поток быстрых транзакций (OLTP, Online Transaction Processing). Команда, в которой работает {{ team.novozhilova.name }} ({{ team.novozhilova.position }}) захотела научить YDB обрабатывать другой тип запросов — аналитические (OLAP, Online Analytical Processing). 

Из доклада вы узнаете ответы на вопросы: 
* Достаточно ли просто поменять систему хранения, упаковать данные по колонкам, чтобы получить профит? 
* Зачем это было нужно и какая польза от таких расширений системе в целом?

@[YouTube](https://www.youtube.com/watch?v=6A7ZfMsHJRM&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=59&pp=iAQB)

Доклад будет интересен разработчикам нагруженных систем и разработчикам платформ различного назначения.

[Слайды доклада](https://disk.yandex.ru/i/BJXSguMIPJgTaQ)

### database/sql: плохой, хороший и злой. Опыт разработки драйвера для распределенной СУБД YDB {#2023-conf-hl-database-sql}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

В Golang есть пакет `database/sql`, который предоставляет универсальный интерфейс общения с базами данных. С течением времени `database/sql` сильно изменился и далеко ушёл от своего первоначального вида, он стал намного удобнее и функциональнее, но так было не всегда. Команда YDB работала с драйвером для YDB, начиная с версии Golang v1.11, и сталкивалась с различными трудностями в процессе эксплуатации в продакшнах пользователей.

В этом ретроспективном докладе [{{ team.myasnikov.name }}]({{ team.myasnikov.profile }}) ({{ team.myasnikov.position }}) расскажет о том, какие недочеты были в пакете `database/sql`, во что это выливалось при эксплуатации и как он становился все лучше от версии к версии Golang.

@[YouTube](https://www.youtube.com/watch?v=82JGONT3AOE&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=73)

Доклад будет интересен разработчикам и пользователям драйверов в стандарте `database/sql`, а также пользователям распределенной базы данных YDB, разрабатывающим свой код на языке Golang.

[Слайды доклада](https://disk.yandex.ru/i/_a0AQJwZIXfOTQ)

### YDB-оптимизации производительности под ARM {#2023-conf-hl-ydb-opt}

{% include notitle [testing_tag](../../tags.md#testing) %}

{{ team.kita.name }} ({{ team.kita.position }}) расскажет о том, с какими проблемами столкнулась команда YDB и как они их решали при оптимизации YDB под архитектуру ARM. В докладе детально рассмотрены основные проблемы оптимизаций высоконагруженных приложений под ARM, методы и инструменты, с помощью которых проводилось тестирование производительности и находились узкие места для оптимизации.

@[YouTube](https://www.youtube.com/watch?v=AJCp-Uyi_ak&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=124&pp=iAQB)

Доклад будет интересен разработчикам баз данных, разработчикам систем хранения данных и разработчикам высоконагруженных приложений.

[Слайды доклада](https://disk.yandex.ru/i/MlV9na2qSBmw1w)

### YDB Topic Service: как мы повышали производительность очереди сообщений {#2023-conf-hl-ydb-topic}

<div style="display:flex; flex-direction: row; justify-content: flex-start; flex-wrap: wrap; column-gap: 10px;">

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

{% include notitle [testing_tag](../../tags.md#testing) %}

<div>

5 лет назад Яндекс перешел с Kafka на собственную разработку поверх YDB. С тех пор Yandex Topic Service сильно подрос по нагрузке и вышел в Open Source. В этом докладе {{ team.zivaykin.name }} ({{ team.zivaykin.position }}) рассказывает про ускорение YDB Topic Service и приводит сравнение с конкурентами.

@[YouTube](https://www.youtube.com/watch?v=I-6SS6_C1Cw&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=22&pp=iAQB)

Доклад будет интересен разработчикам, лидам разработки, техническим менеджерам. Всем, кто решал, решает или интересуется задачей масштабируемой поставки данных.

[Слайды доклада](https://disk.yandex.ru/i/r8M8LsXGfM8lOw)

### Особенности шин данных для очень больших инсталляций на примере YDB Topics {#2023-conf-hl-data-bus}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Шины передачи данных используются практически везде, но использование шин данных в очень больших инсталляциях на тысячи серверов накладывают особые требования для работы и приводят к отличиям в работе систем. [{{ team.dmitriev.name }}]({{ team.dmitriev.profile }}) ({{ team.dmitriev.position }}) показывает на примере YDB Topics, в чем заключаются эти отличия, как они влияют на архитектуру и эксплуатацию.

@[YouTube](https://www.youtube.com/watch?v=zKPOAdNOQx4&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=92&pp=iAQB)

Доклад будет интересен разработчикам и командам эксплуатации, особенно в компаниях больших размеров. 

[Слайды доклада](https://disk.yandex.ru/i/gttNHRfUZ9hF-Q)

### Поиск по образцу на последовательностях строк в БД {#2023-conf-hl-search-inline}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Задача поиска по образцу на последовательности строк БД может возникать в различных сферах деятельности. Например, в финансовой аналитике — поиск определённых паттернов изменения цены акций. Для реализации таких запросов к базам данных в стандарте `SQL:2016` была введена конструкция `MATCH_RECOGNIZE`. {{ team.zverev.name }} ({{ team.zverev.position }}) рассказывает о реализации `MATCH_RECOGNIZE` в YDB: о том, как это работает под капотом, какие подходы и алгоритмы реализованы, с какими сложностями столкнулась команда.

@[YouTube](https://www.youtube.com/watch?v=TSFVV0zGSBI&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=130&pp=iAQB)

Отдельная часть выступления посвящена отличиям в обработке аналитических запросов на табличках и обработке на потоках «живых» данных. Доклад будет интересен разработчикам БД, дата-аналитикам, а также всем, кто интересуется поиском по образцу на больших данных. 

[Слайды доклада](https://disk.yandex.ru/i/Kzsj2mryXkfPxw)

### Из pytest в Go. Тестовое окружение на фикстурах {#2023-conf-hl-pytest-go}

{% include notitle [testing_tag](../../tags.md#testing) %}

Фикстуры позволяют писать очень лаконичные тесты и не отвлекаться на подготовку окружения. [{{ team.kulin.name }}]({{ team.kulin.profile }}) ({{ team.kulin.position }}) рассказал, как перенёс идеологию фикстур из pytest в Go и разработал библиотеку `fixenv`, которая сокращает код тестов и улучшает их читаемость.

@[YouTube](https://www.youtube.com/watch?v=Vtg8UmU62OA&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=100&pp=iAQB)

Доклад будет интересен разработчикам на Go и тимлидам. 

[Слайды доклада](https://disk.yandex.ru/i/hPK4BTpN_22rZw)