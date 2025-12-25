# Конфигурация V1

В этом разделе документации {{ ydb-short-name }} описана Конфигурация V1, являющаяся основным способом конфигурирования кластеров {{ ydb-short-name }}.

Конфигурация V1 — двухуровневая система конфигурации кластера {{ ydb-short-name }}, состоящая из [cтатической конфигурации](../configuration-v1/static-config.md) и [динамической конфигурации](../configuration-v1/dynamic-config.md):

1. **Статическая конфигурация**: файл в формате YAML, который располагается локально на каждом статическом узле и используется при запуске процесса `ydbd server`. Эта конфигурация содержит, в том числе, настройки [статической группы](../../../concepts/glossary.md#static-group) и [State Storage](../../../concepts/glossary.md#state-storage).

2. **Динамическая конфигурация**: файл в формате YAML, являющийся расширенной версией статической конфигурации. Загружается через [CLI](../../../recipes/ydb-cli/index.md) и надёжно сохраняется в [таблетке Console](../../../concepts/glossary.md#console), которая затем распространяет конфигурацию на все динамические узлы кластера. Использование динамической конфигурации опционально.

Подробнее о Конфигурации V1 можно узнать в разделе [{#T}](config-overview.md).

Основные материалы:

- [{#T}](config-overview.md)
- [Статическая конфигурация](static-config.md)
- [{#T}](dynamic-config.md)
- [{#T}](dynamic-config-volatile-config.md)
- [DSL конфигурация кластера](dynamic-config-selectors.md)
- [{#T}](cms.md)
- [{#T}](change_actorsystem_configs.md)
- [{#T}](cluster-expansion.md)
- [{#T}](state-storage-move.md)
- [{#T}](static-group-move.md)
- [Замена FQDN узла](replacing-nodes.md)
- [Аутентификация и авторизация узлов баз данных](node-authorization.md)
