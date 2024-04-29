### Обновление кешей сервисов в реальном времени с помощью YDB CDC на примере Yandex Monitoring {2024-pub-habr-ydb-cdc}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![Yandex Monitoring: обновление кешей сервисов в реальном времени с помощью YDB CDC](./_includes/ydb-cdc.png ={{pub-cover-size}})

Летом 2023 года Егор Литвиненко (старший разработчик Yandex Observability Platform) рассказывал на Saint Highload в Санкт‑Петербурге о пути внедрения YDB CDC для обновления данных в сервисах, чтобы решить проблему инвалидации кешей. В этой [статье](https://habr.com/ru/companies/oleg-bunin/articles/801603/) будет вся история внедрения с теорией, вопросами, ответами, ошибками, о которых он говорил на выступлении. 
