# Удаление узла из кластера

{% include [_](../_includes/experimental_v2.md) %}

В этой статье описано удаление [динамического](../../../concepts/glossary.md#dynamic) или [статического](../../../concepts/glossary.md#static-node) узла из кластера {{ ydb-short-name }}, развёрнутого вручную на виртуальных машинах или физических серверах. Удаление узлов из кластера, развёрнутого в Kubernetes, в этой инструкции не рассматривается.

## Удаление динамического узла

Для удаления динамического узла не требуется изменять конфигурацию кластера.

Чтобы удаление динамического узла не повлияло на выполнение запросов:

1. Выполните [мягкий перенос таблеток](../../../maintenance/manual/node_restarting.md#replace-hardware) с узла и дождитесь его завершения.
1. Остановите процесс {{ ydb-short-name }} на узле. [Сначала проверьте, что процесс можно безопасно остановить, а затем остановите его](../../../maintenance/manual/node_restarting.md#restart_process).

После остановки процесса проверьте на вкладке **Nodes** [страницы мониторинга кластера](../../../reference/ydb-ui/ydb-monitoring.md#node_list_page), что удалённый узел больше не отображается активным. Динамический узел может исчезнуть из списка сразу или временно отображаться со статусом **Disconnected** до истечения аренды его идентификатора в [NodeBroker](../../../concepts/glossary.md#node-broker). Если узел снова переходит в активное состояние, убедитесь, что процесс был остановлен именно на нужном хосте и не настроен на автоматический перезапуск, например службой `systemd` или супервизором.

## Удаление статического узла {#remove-static-node}

Статические узлы обслуживают систему хранения и перечислены в секции [`hosts`](../../../reference/configuration/hosts.md). На дисках статического узла могут находиться [VDisk](../../../concepts/glossary.md#vdisk) [динамических](../../../concepts/glossary.md#dynamic-group) и [статических](../../../concepts/glossary.md#static-group) групп, а на самом узле — реплики [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board) и [SchemeBoard](../../../concepts/glossary.md#scheme-board). Поэтому сначала необходимо перенести эти ресурсы, а затем удалить узел из конфигурации.

Перед началом процедуры проверьте во [встроенном UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page), что затронутые группы хранения работоспособны, то есть все VDisk этих групп отображаются в состоянии `Ok` (выделены зелёным цветом), и ни один VDisk не находится в состоянии `Error` или `Degraded`.

На оставшихся узлах должно быть достаточно свободного места и слотов на [PDisk](../../../concepts/glossary.md#pdisk) для всех VDisk с удаляемого узла. Размещение VDisk по [доменам отказа](../../../concepts/glossary.md#fail-domain) и [областям отказа](../../../concepts/glossary.md#fail-realm) должно соответствовать используемой [схеме кодирования](../../../concepts/glossary.md#erasure-coding), чтобы после удаления узла сохранялась отказоустойчивость групп. Расчёт необходимого запаса приведён в статье [{#T}](../../concepts/capacity-planning.md#hardware-estimation).

[SelfHeal](../../../maintenance/manual/selfheal.md) динамических групп включён по умолчанию. Перед удалением узла убедитесь, что он также включён для остальных ресурсов, размещённых на этом узле:

* Если на узле есть VDisk статической группы, [включите SelfHeal статической группы](static-group-self-heal.md#on-off). Как альтернативу можно перенести VDisk статической группы с узла вручную, см. [{#T}](static-group-move.md).
* Если узел содержит реплики State Storage, Board или SchemeBoard, включите [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md#on-off). Как альтернативу можно перенести эти реплики с узла вручную, см. [{#T}](state-storage-reconfiguration.md).

Чтобы удалить статический узел:

1. Если на узле работают таблетки, выполните их [мягкий перенос](../../../maintenance/manual/node_restarting.md#replace-hardware).
1. [Проверьте, что процесс можно безопасно остановить](../../../maintenance/manual/node_restarting.md#restart_process), затем остановите его.
1. Дождитесь, пока SelfHeal перенесёт VDisk с узла. С настройками по умолчанию перенос начинается приблизительно через час после остановки узла. Чтобы запустить перенос немедленно, сначала получите идентификаторы всех PDisk удаляемого узла с помощью [{{ ydb-short-name }} DSTool](../../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk list --columns NodeId:PDiskId FQDN Path
    ```

    `<bs_endpoint>` — точка подключения к любому доступному узлу хранения в формате `[PROTOCOL://]HOST[:PORT]`, например `http://node1.example.com:8765`.

    Выберите все строки, в которых `FQDN` совпадает с именем хоста удаляемого узла. Проверьте пути к дискам в столбце `Path` и сохраните все соответствующие значения `NodeId:PDiskId`.

    Затем переведите все найденные PDisk в статус `BROKEN` одной командой:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status BROKEN --unavail-as-offline --pdisk-ids "<pdisk_id_1>" ... "<pdisk_id_N>"
    ```

    Используйте ту же точку подключения `<bs_endpoint>`. Вместо `"<pdisk_id_1>" ... "<pdisk_id_N>"` перечислите через пробел все сохранённые идентификаторы дисков в формате `[NodeId:PDiskId]`, например `"[3:1]" "[3:2]"` для двух дисков узла с `NodeId`, равным `3`. Флаг `--unavail-as-offline` позволяет считать PDisk, недоступные через службу мониторинга Whiteboard, неработающими.

    Команда выполняется на переднем плане. Дождитесь её успешного завершения, затем проверьте завершение переноса данных на следующем шаге. Подробнее см. в статье [Перенос VDisk с повреждённого или недоступного тома блочного хранилища](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

1. Во [встроенном UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page) проверьте, что на удаляемом узле не осталось VDisk, а затронутые группы хранения работоспособны (все VDisk находятся в состоянии `Ok`). Если с узла переносились реплики State Storage, Board или SchemeBoard, [проверьте, что перенос завершён](../../../maintenance/manual/selfheal_statestorage.md#verify-result).
1. Получите актуальную конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. Если запись удаляемого узла не последняя в списке [`hosts`](../../../reference/configuration/hosts.md), при удалении позиции всех следующих узлов сместятся. Чтобы сохранить их идентификаторы, для каждой следующей записи без явно заданного `node_id` укажите её текущий идентификатор до удаления: для такой записи он равен текущей позиции в списке, начиная с `1`. Если `node_id` уже задан явно, сохраните его существующее значение. Если удаляется последняя запись, этот шаг не требуется.

    {% note warning %}

    Корректная нумерация узлов критически важна для работоспособности кластера. Ошибка при назначении `node_id` может привести к тому, что VDisk, реплики State Storage, Board или SchemeBoard окажутся привязаны не к тому хосту, что может стать причиной необратимой потери данных.

    {% endnote %}

    Например, дана следующая конфигурация, из которой удаляется узел `node3`:

    ```yaml
    hosts:
    - host: node1
    - host: node2
    - host: node3 # удаляется
    - host: node4
    - host: node5
      node_id: 50
    ```

    Перед удалением `node3` явно укажите `node_id: 4` для `node4`, чтобы его идентификатор не изменился на `3`. Для `node5` сохраните уже заданное значение `node_id: 50`. После удаления `node3` список будет выглядеть так:

    ```yaml
    hosts:
    - host: node1
    - host: node2
    - host: node4
      node_id: 4
    - host: node5
      node_id: 50
    ```

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

    В этом случае дождитесь, пока SelfHeal перенесёт оставшиеся VDisk. Продолжительность переноса зависит от объёма данных и производительности дисков. Следите за переносом на вкладке **Storage** удаляемого узла во [встроенном UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page). Когда на узле не останется VDisk, повторите команду `config replace` с тем же файлом.

    Если список VDisk не сокращается и репликация не идёт, [перенесите оставшиеся VDisk вручную](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

    {% endcut %}

После успешного применения конфигурации сервер и его диски можно вывести из эксплуатации.
