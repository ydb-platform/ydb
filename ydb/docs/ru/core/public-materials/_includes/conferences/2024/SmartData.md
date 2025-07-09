## Шардированный не значит распределенный: что важно знать, когда PostgreSQL мало {#2024-conf-smartdata}

{% include notitle [testing_tag](../../tags.md#testing) %}

[{{ team.ivanov.name }}]({{ team.ivanov.profile }}) ({{ team.ivanov.position }}) и [{{ team.bondar.name }}]({{ team.bondar.profile }}) ({{ team.bondar.position }}) рассказали, чем отличаются распределённые СУБД от шардированных. Особое внимание уделили тому, почему решения, подобные Citus, не являются ACID в случае широких транзакций. В конце выступления на примере бенчмарка TPC-C показали, что в PostgreSQL вертикальное масштабирование ограничено ботлнеком в синхронной репликации, и сравнили производительность PostgreSQL и распределённых СУБД CockroachDB и YDB.

@[YouTube](https://youtu.be/BDpLLmV37hY)

Доклад будет интересен как разработчикам приложений, которым требуется надёжная СУБД, так и людям, интересующимся распределёнными системами и базами данных.

[Слайды](https://presentations.ydb.tech/2024/ru/smartdataconf/sharded_is_not_distributed/presentation.pdf)