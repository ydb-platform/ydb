### Мультиарендный подход Яндекса к построению инфраструктуры работы с данными {#2021-conf-yasub-multirendn}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Времена, когда для экземпляра базы данных выделялся отдельный компьютер, давно прошли. Сейчас повсюду управляемые решения, поднимающие необходимые процессы в виртуальных машинах. Для вычислений применяется еще более прогрессивный подход — «бессерверные вычисления», например AWS Lambda или Yandex Cloud Functions. И уж совсем на острие прогресса находятся бессерверные БД.

@[YouTube](https://www.youtube.com/watch?v=35Q2338ywEw&t=4282s)

[ {{ team.fomichev.name }} ]( {{ team.fomichev.profile }} ) ( {{ team.fomichev.position }} ) рассказал о бессерверных решениях, которые еще до всеобщего хайпа стали популярны в Яндексе и по-прежнему используются для хранения и обработки данных.

[Слайды](https://presentations.ydb.tech/2021/ru/ya_subbotnic_infrastructure/presentation.pdf)