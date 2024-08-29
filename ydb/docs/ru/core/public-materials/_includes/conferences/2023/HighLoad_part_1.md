## Реализовать OLAP: как мы делали колоночное хранение в {{ ydb-short-name }} {#2023-conf-hl-olap}

<div class = "multi-tags-container">

{% include notitle [overview_tag](../../tags.md#overview) %}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

</div>

{{ ydb-short-name }} – это платформа, которая умеет обрабатывать большой поток быстрых транзакций (OLTP, Online Transaction Processing). Команда, в которой работает {{ team.novozhilova.name }} ({{ team.novozhilova.position }}) захотела научить YDB обрабатывать другой тип запросов — аналитические (OLAP, Online Analytical Processing). 

Из доклада вы узнаете ответы на вопросы: 
* Достаточно ли просто поменять систему хранения, упаковать данные по колонкам, чтобы получить профит? 
* Зачем это было нужно и какая польза от таких расширений системе в целом?

@[YouTube](https://www.youtube.com/watch?v=6A7ZfMsHJRM&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=59&pp=iAQB)

Доклад будет интересен разработчикам нагруженных систем и разработчикам платформ различного назначения.

[Слайды](https://presentations.ydb.tech/2023/ru/highload/olap/presentation.pdf)


## database/sql: плохой, хороший и злой. Опыт разработки драйвера для распределенной СУБД YDB {#2023-conf-hl-database-sql}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

В Golang есть пакет `database/sql`, который предоставляет универсальный интерфейс общения с базами данных. С течением времени `database/sql` сильно изменился и далеко ушёл от своего первоначального вида, он стал намного удобнее и функциональнее, но так было не всегда. Команда YDB работала с драйвером для YDB, начиная с версии Golang v1.11, и сталкивалась с различными трудностями в процессе эксплуатации в продакшнах пользователей.

В этом ретроспективном докладе [{{ team.myasnikov.name }}]({{ team.myasnikov.profile }}) ({{ team.myasnikov.position }}) расскажет о том, какие недочеты были в пакете `database/sql`, во что это выливалось при эксплуатации и как он становился все лучше от версии к версии Golang.

@[YouTube](https://www.youtube.com/watch?v=82JGONT3AOE&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=73)

Доклад будет интересен разработчикам и пользователям драйверов в стандарте `database/sql`, а также пользователям распределенной базы данных YDB, разрабатывающим свой код на языке Golang.

[Слайды](https://presentations.ydb.tech/2023/ru/golang_conf/database_sql/presentation.pdf)


## YDB-оптимизации производительности под ARM {#2023-conf-hl-ydb-opt}

{% include notitle [testing_tag](../../tags.md#testing) %}

{{ team.kita.name }} ({{ team.kita.position }}) расскажет о том, с какими проблемами столкнулась команда YDB и как они их решали при оптимизации YDB под архитектуру ARM. В докладе детально рассмотрены основные проблемы оптимизаций высоконагруженных приложений под ARM, методы и инструменты, с помощью которых проводилось тестирование производительности и находились узкие места для оптимизации.

@[YouTube](https://www.youtube.com/watch?v=AJCp-Uyi_ak&list=PLH-XmS0lSi_yksBrXBOIgnuW_RmwfLKYn&index=124&pp=iAQB)

Доклад будет интересен разработчикам баз данных, разработчикам систем хранения данных и разработчикам высоконагруженных приложений.

[Слайды](https://presentations.ydb.tech/2023/ru/highload/ydb_optimizations_for_arm/presentation.pdf)