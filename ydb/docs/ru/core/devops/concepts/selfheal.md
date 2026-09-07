# SelfHeal

В {{ ydb-short-name }} есть два механизма автоматического восстановления — SelfHeal:

1. **SelfHeal хранилища** — для дисков и [групп хранения](../../concepts/glossary.md#storage-group) с данными.
2. **SelfHeal State Storage** — для реплик [State Storage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board) и [SchemeBoard](../../concepts/glossary.md#scheme-board).

Оба механизма восстанавливают отказоустойчивость кластера после длительных отказов. Если неисправный узел или диск восстановлен до истечения таймаута (для дисков по умолчанию около часа), SelfHeal не начинает перенос.

{% note info %}

SelfHeal State Storage доступен только при [конфигурации V2](../configuration-management/configuration-v2/config-overview.md).

SelfHeal хранилища от версии конфигурации не зависит.

{% endnote %}

Подробнее о механизмах:

- [SelfHeal хранилища](selfheal-storage.md)
- [SelfHeal State Storage](selfheal-state-storage.md)
