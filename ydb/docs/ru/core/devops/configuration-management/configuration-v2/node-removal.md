# Удаление узла из кластера

{% include [_](../_includes/experimental_v2.md) %}

В этой статье описано удаление [динамического](../../../concepts/glossary.md#dynamic) или [статического](../../../concepts/glossary.md#static-node) узла из кластера {{ ydb-short-name }}, развёрнутого вручную на виртуальных машинах или физических серверах. При развёртывании в Kubernetes удаляйте узлы с помощью оператора {{ ydb-short-name }}.

## Удаление динамического узла

Для удаления динамического узла не требуется изменять конфигурацию кластера.

Чтобы удаление динамического узла не повлияло на выполнение запросов:

1. Выполните [мягкий перенос таблеток](../../../maintenance/manual/node_restarting.md#replace-hardware) с узла и дождитесь его завершения.
1. Остановите процесс динамического узла.

После остановки проверьте на вкладке **Nodes** [страницы мониторинга кластера](../../../reference/embedded-ui/ydb-monitoring.md#node_list_page), что узел больше не отображается среди подключённых.

## Удаление статического узла {#remove-static-node}

Статические узлы обслуживают систему хранения и перечислены в секции `hosts`. На дисках статического узла могут находиться VDisk динамических и статической групп, а на самом узле — реплики State Storage, Board и SchemeBoard. Поэтому сначала необходимо перенести эти ресурсы, а затем удалить узел из конфигурации.

Перед началом процедуры проверьте во [встроенном UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page), что затронутые группы хранения работоспособны. В кластере также должно быть достаточно свободных слотов для переноса VDisk с удаляемого узла с учётом модели отказа. Расчёт необходимого запаса приведён в статье [{#T}](../../concepts/capacity-planning.md#hardware-estimation).

[SelfHeal](../../../maintenance/manual/selfheal.md) динамических групп включён по умолчанию. Если на узле есть VDisk статической группы, [включите SelfHeal статической группы](static-group-self-heal.md#on-off). Если узел содержит реплики State Storage, Board или SchemeBoard, включите [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md#on-off).

Чтобы удалить статический узел:

1. Если на узле работают таблетки, выполните их [мягкий перенос](../../../maintenance/manual/node_restarting.md#replace-hardware).
1. [Проверьте, что процесс можно безопасно остановить](../../../maintenance/manual/node_restarting.md#restart_process), затем остановите его.
1. Дождитесь, пока SelfHeal перенесёт VDisk с узла. С настройками по умолчанию перенос начинается приблизительно через час после остановки узла.
1. Во [встроенном UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page) проверьте, что на удаляемом узле не осталось VDisk, а затронутые группы хранения работоспособны. Если с узла переносились реплики State Storage, Board или SchemeBoard, [проверьте, что перенос завершён](../../../maintenance/manual/selfheal_statestorage.md#verify-result).
1. Получите актуальную конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. Если запись удаляемого узла не последняя в списке [`hosts`](../../../reference/configuration/hosts.md), сохраните идентификаторы следующих за ней узлов: добавьте `node_id` в каждую запись, где он не указан. В качестве значения используйте текущий порядковый номер записи в списке, начиная с `1`. Если удаляется последняя запись, этот шаг не требуется.
1. Удалите запись узла из секции `hosts`.
1. Примените конфигурацию с помощью команды [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

    {% cut "Если команда завершилась с ошибкой" %}

    Если на PDisk удаляемого узла остались VDisk, команда возвращает ошибку следующего вида:

    ```text
    failed to remove PDisk# 1:1 as it has active VSlots
    ```

    В этом случае дождитесь, пока SelfHeal перенесёт оставшиеся VDisk. Продолжительность переноса зависит от объёма данных и производительности дисков. Следите за переносом на вкладке **Storage** удаляемого узла во [встроенном UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page). Когда на узле не останется VDisk, повторите команду `config replace` с тем же файлом.

    Если список VDisk не сокращается и репликация не идёт, [перенесите оставшиеся VDisk вручную](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

    {% endcut %}

После успешного применения конфигурации сервер и его диски можно вывести из эксплуатации.
