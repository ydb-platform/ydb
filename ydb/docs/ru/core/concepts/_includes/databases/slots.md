## Слоты {#slots}

_Слот_ — часть ресурсов сервера, выделенная под запуск одного узла кластера {{ ydb-short-name }}, фиксированного размера 10 CPU/50 ГБ RAM. Слоты применяются в случае, если кластер {{ ydb-short-name }} разворачивается на bare metal машинах, ресурсы которых достаточны для обслуживания нескольких слотов. В случае использования виртуальных машин для развертывания кластера мощность этих машин выбирается таким образом, чтобы применение слотов не требовалось: один узел обслуживает одну БД, одна БД использует множество выделенных ей узлов.

Список слотов, выделенных базе, можно увидеть в результатах выполнения команды `discovery list` консольного клиента {{ ydb-short-name }}.
