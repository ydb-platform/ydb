# SelfHeal

SelfHeal автоматически восстанавливает отказоустойчивость пяти типов объектов кластера {{ ydb-short-name }}:

1. [Динамических групп хранения](../../concepts/glossary.md#dynamic-group).
2. [Статической группы хранения](../../concepts/glossary.md#static-group).
3. Реплик [State Storage](../../concepts/glossary.md#state-storage).
4. Реплик [Board](../../concepts/glossary.md#board).
5. Реплик [SchemeBoard](../../concepts/glossary.md#scheme-board).

В документации настройки этих объектов сгруппированы по двум направлениям: SelfHeal хранилища для динамических и статической групп и SelfHeal подсистем распространения метаданных для State Storage, Board и SchemeBoard.

При отказе узла или диска SelfHeal ожидает, что неисправность сохранится достаточно долго, прежде чем начать перенос. Если узел или диск восстановлен до срабатывания механизма, перенос не начинается. Для дисков время ожидания по умолчанию составляет около часа.

{% note info %}

SelfHeal динамических групп хранения от версии конфигурации не зависит. SelfHeal статической группы и подсистем распространения метаданных доступен только при [конфигурации V2](../configuration-management/configuration-v2/config-overview.md) и включённой распределённой конфигурации. Для статической группы также должно быть разрешено автоматическое управление с помощью параметра `automatic_static_group_management`.

{% endnote %}

Подробнее о механизмах:

- [SelfHeal хранилища](selfheal-storage.md)
- [SelfHeal подсистем распространения метаданных](selfheal-state-storage.md)
