## Распределенная трассировка с помощью Jaeger и {{ ydb-name }}. Опыт Auto.ru и Yandex Cloud. {#2021-conf-devops-jaeger}

{% include notitle [use_cases_tag](../../tags.md#use_cases) %}

Мы активно используем Jaeger как инструмент распределенной трассировки, и при росте нагрузки встал вопрос эффективности хранения и обработки данных. В докладе мы расскажем, как выбирали базу для хранения трейсов Jaeger и про дальнейший опыт эксплуатации Jaeger и {{ ydb-name }} в Auto.ru и Yandex Cloud. Решение стало популярным внутри Яндекса, и мы выпустили Jaeger-драйвер для {{ ydb-name }} в Open Source. Появление {{ ydb-name }} Serverless дало пользователям возможность сэкономить, и мы хотим поделиться результатами тестов Jaeger с {{ ydb-name }} Serverless.

@[YouTube](https://youtu.be/J0OT8Qxbsvc)

[Слайды](https://presentations.ydb.tech/2021/ru/devops_conf/presentation.pdf)