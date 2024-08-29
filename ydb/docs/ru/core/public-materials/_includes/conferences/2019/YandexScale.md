## {{ ydb-name }} — эффективная альтернатива традиционным noSQL-решениям {#2019-conf-yascale-ydb-nosql-alt}

{% include notitle [overview_tag](../../tags.md#overview) %}

[ {{ team.fomichev.name }} ]( {{ team.fomichev.profile }} ) ( {{ team.fomichev.position }} ) рассказал, как и зачем была создана {{ ydb-name }}, чем она отличается от других БД и для каких задач она лучше всего подходит.

@[YouTube](https://youtu.be/MlSdUq5RIN8)

В докладе подробно разобраны следующие свойства {{ ydb-short-name }}:
* Автоматический split/merge шардов.
* Автоматическое восстановление после сбоев за время обнаружения отказа.
* Синхронная репликация данных, в том числе в геораспределенной конфигурации данных.
* Механизм serializable-транзакций между записями базы данных.

[Слайды](https://presentations.ydb.tech/2019/ru/yandex_scale_nosql_alternative/presentation.pdf)

## {{ ydb-name }} at Scale: опыт применения в высоконагруженных сервисах Яндекса {#2019-conf-yascale-ydb-at-scale}

{% include notitle [use_cases_tag](../../tags.md#use_cases) %}

Представители [Auto.ru](https://auto.ru), [Яндекс.Репетитора](https://yandex.ru/tutor/), [Алисы](https://yandex.ru/alice) и [Condé Nast](https://www.condenast.ru/) рассказали, почему они выбрали {{ ydb-name }} и как эта СУБД помогает развивать их продукты.

@[YouTube](https://youtu.be/kubFwIKJjBY)

Вы узнаете:
* Как {{ ydb-short-name }} хранит гигабайты данных умных устройств.
* Почему разработчики Алисы выбрали {{ ydb-short-name }} для хранения логов.
* Какие преимущества есть у {{ ydb-short-name }} перед Cassandra и MongoDB.

[Слайды](https://storage.yandexcloud.net/ydb-public-talks/242-olegbondar.pptx)