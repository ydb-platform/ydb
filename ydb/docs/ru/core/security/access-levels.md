# Уровни доступа

Уровни доступа — это один из двух механизмов управления доступом в {{ ydb-short-name }}, который влияет на дополнительные возможности субъекта при работе со [схемными объектами](../concepts/glossary.md#scheme-object), а также на возможности субъекта в контекстах, не связанных со схемными объектами.

{{ ydb-short-name }} использует три основных уровня доступа:

- **Наблюдатель** даёт возможность просмотра состояния системы, закрытого от публичного доступа, без возможности делать изменения. Этот уровень доступа требуется для большей часть страниц [Embedded UI](../reference/embedded-ui/index.md).

- **Оператор** даёт дополнительные возможности просмотра и выполнения действий, изменяющих состояние системы:

    - доступ к страницам **Developer UI**;
    - рестарт или старт/стоп таблеток на странице таблетки в [Embedded UI](../reference/embedded-ui/index.md) или Developer UI;
    - [перемещение VDisk'ов](../maintenance/manual/moving_vdisks.md) на другие PDisk'и (evict) на странице VDisk'а в [Embedded UI](../reference/embedded-ui/index.md) или Developer UI;
    - рестарт или [декомиссия](../devops/deployment-options/manual/decommissioning.md) диска на странице PDisk'а в [Embedded UI](../reference/embedded-ui/index.md) или Developer UI;
    - действия на вкладке с информацией по фоновым операциям — построение индекса, импорт и экспорт ([Embedded UI](../reference/embedded-ui/index.md) > **Diagnostics** > **Operations**);
    - действия на вкладке с информацией по запросам ([Embedded UI](../reference/embedded-ui/index.md) > **Diagnostics** > **Queries**).

- **Администратор** даёт право на выполнение административных действий с базами данных или кластером:

    - управление кластером:
        - [изменение динамической конфигурации кластера](../devops/configuration-management/configuration-v2/update-config.md);
        - [управление пулами ресурсов](../dev/resource-consumption-management.md);

        [//]: # (управление конфигурацией тарификации — metering config;)

        - изменение настроек таблетки [Console](../concepts/glossary.md#console);
        - запросы к сервису [maintenance](../devops/deployment-options/manual/maintenance.md) (получение списка узлов кластера, управление maintenance-задачами);
        - выполнение drain/fill узла {{ ydb-short-name }} в CLI (ydbd) и в Developer UI;
        - изменение статуса таблеток с помощью [minikql](../concepts/glossary.md#minikql)-запросов через CLI (для продвинутой диагностики и решения проблем);

    - управление дисковой подсистемой кластера без проверки на деградацию групп (с нарушением модели отказа):

        - перезапуск PDisk'а в Developer UI;
        - изменение статуса PDisk'а через [YDB DSTool](../reference/ydb-dstool/index.md);
        - [перемещение VDisk'ов](../maintenance/manual/moving_vdisks.md) на другие PDisk'и (evict) через [YDB DSTool](../reference/ydb-dstool/index.md);

    - управление базами данных и таблицами:
        - управление базами данных в [CMS](../concepts/glossary.md#cms);
        - изменение динамической конфигурации базы данных;
        - выполнение `ALTER TABLE`-запросов на индексные таблицы без ограничений консистентности с основной таблицей;
        - прямой split/merge партиций таблицы (для продвинутой диагностики и решения проблем);
        - прямое выполнение схемной операции по protobuf spec через CLI (для продвинутой диагностики и решения проблем).

{% note info %}

Существует еще четвёртый уровень, который даёт разрешение на регистрацию узлов баз данных в кластере {{ ydb-short-name }}. Подробнее о регистрации узлов баз данных в кластере см. в статье [{#T}](../devops/deployment-options/manual/node-authorization.md)

{% endnote %}

## Списки уровней доступа

Уровни доступа субъектов задаются в секции [`security_config` конфигурации YDB](../reference/configuration/security_config.md#security-access-levels) в параметрах `viewer_allowed_sids`, `monitoring_allowed_sids`, `administration_allowed_sids`, и `register_dynamic_node_allowed_sids`.

Каждый параметр соответствует определенному уровню доступа и содержит список SID'ов [пользователей](../concepts/glossary.md#access-user) или [групп пользователей](../concepts/glossary.md#access-group). Принадлежность пользователя к тому или иному уровню доступа определяется по прямому вхождению в соответствующий список уровня доступа или по вхождению в список уровня доступа группы, в которой состоит пользователь (с учетом рекурсивности, которая возникает из-за того, что одна группа может включать несколько других групп).

{% note tip %}

Рекомендуется включать в списки `*_allowed_sids` группы, тогда наделение индивидуальных пользователей соответствующими возможностями не будет требовать изменения общей конфигурации кластера.

{% endnote %}

[//]: # (TODO: добавить ссылку на справку по viewer api и требуемым правам, когда она появится)

Списки `viewer_allowed_sids`, `monitoring_allowed_sids`, `administration_allowed_sids` последовательно расширяют возможности субъекта доступа. Нахождение субъекта доступа во всех трех списках предоставляет максимальные возможности.

Присутствие в списке `monitoring_allowed_sids` без присутствия во `viewer_allowed_sids`, а также присутствие в списке `administration_allowed_sids` без присутствия во `monitoring_allowed_sids` не имеет смысла.

Например:

- оператор должен состоять (прямо или через группы) во `viewer_allowed_sids` и в `monitoring_allowed_sids`;
- администратор должен состоять во `viewer_allowed_sids`, `monitoring_allowed_sids` и в `administration_allowed_sids`.

{% note warning %}

По-умолчанию, все списки пустые.

Пустой список разрешает доступ любому пользователю (включая анонимного пользователя).

Все три пустых списка дают возможности администратора любому пользователю системы.

Для защищённого развёртывания {{ ydb-short-name }} важно заранее определить модель прав и прописать в списках соответствующие группы.

{% endnote %}
