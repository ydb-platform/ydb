### Гарантии доставки сообщений в {{ ydb-short-name }} Topics {#2024-it-picnic}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

[{{ team.zevaykin.name }}]({{ team.zevaykin.profile }})({{ team.zevaykin.position }}) продемонстрировал базовые проблемы ненадежной передачи данных и способы борьбы с ними: повторы и дедупликацию. На примере паттернов микропроцессорной архитектуры он проиллюстрировал гарантии доставки: at-most-once, at-least-once, exactly-once.


В завершение выступления Александр показал детали реализации гарантий доставки на примере двух брокеров очередей сообщений: Kafka и {{ ydb-short-name }} Topics.

[Слайды](https://presentations.ydb.tech/2024/ru/it_picnic/ydb_topics/presentation.pdf)