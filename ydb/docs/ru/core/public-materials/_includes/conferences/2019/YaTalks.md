### {{ ydb-name }} — как выжать 10K IOPS из HDD и вставить в таблицу 50K записей на одном ядре {#2019-conf-yatalks-10k-iops}

{% include notitle [use_cases_tag](../../tags.md#use_cases) %}

Из доклада вы узнаете об успешном использовании {{ ydb-name }} в качестве бэкэнда для распределенной трассировки Jaeger от представителей [Auto.ru](https://auto.ru) и [Яндекс.Недвижимости](https://realty.yandex.ru/), а также об архитектуре распределенного сетевого хранилища в {{ ydb-name }}.

@[YouTube](https://www.youtube.com/watch?v=hXH_tRBxFnA&t=11318s)


[Слайды](https://storage.yandexcloud.net/ydb-public-talks/yatalks-ydb.pptx)

### {{ ydb-name }}: Distributed SQL база данных Яндекса {#2019-conf-yatalks-dist-sql}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

[ {{ team.puchin.name }} ]( {{ team.puchin.profile }} ) ( {{ team.puchin.position }} ) рассказал об основных моментах, связанных с выполнением распределенных запросов в {{ ydb-short-name }}:
* Модель транзакций и уровни изоляции.
* Особенности SQL-диалекта Yandex Query Language (YQL).
* Многошаговые транзакции и механизм оптимистичных блокировок.
* Эффективное выполнение запросов к распределенным БД в целом.
* Основные факторы, влияющие на производительность запросов.
* Стандартные практики работы с {{ ydb-short-name }}, в том числе инструменты для разработчика.

@[YouTube](https://youtu.be/tzANIAbc99o?t=3012)

Доклад будет интересен тем, кто хочет глубже погрузиться в процессы работы {{ ydb-name }} и узнать как клиентские приложения взаимодействуют с {{ ydb-name }}, и как работает система распределенных транзакций {{ ydb-short-name }}.   

[Слайды](https://storage.yandexcloud.net/ydb-public-talks/YdbInCloud_2.pptx)