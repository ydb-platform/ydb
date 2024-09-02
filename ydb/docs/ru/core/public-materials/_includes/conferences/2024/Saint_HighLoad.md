### Гарантии доставки сообщений в {{ ydb-short-name }} Topics {#2024-saint-highload}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

[{{ team.zevaykin.name }}]({{ team.zevaykin.profile }}) ({{ team.zevaykin.position }}) рассмотрел основные проблемы ненадёжной передачи данных и методы борьбы с ними: повторы и дедупликацию. Александр на примере паттернов микропроцессорной архитектуры продемонстрировал гарантии доставки: at-most-once, at-least-once, exactly-once.

@[YouTube](https://youtu.be/6l64n8t8Ivs?si=coC70xmfuaoIzxPA)

В заключение Александр на примере двух брокеров очередей сообщений — Kafka и {{ ydb-short-name }} Topics — показал детали реализации гарантий доставки.

[Слайды](https://presentations.ydb.tech/2024/ru/saint_highload/ydb_topics/presentation.pdf)
