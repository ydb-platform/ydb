# Пример диагностики перегруженных шардов

В этой статье рассматривается пример диагностики перегруженных шардов и решения этой проблемы.

Дополнительную информацию о перегруженных шардах и причинах их перегрузки см. в статье [{#T}](../../performance/schemas/overloaded-shards.md).

Статья начинается с [описания возникшей проблемы](#initial-issue). Затем мы проанализируем графики в Grafana и информацию на вкладке **Diagnostics** в [Embedded UI](../../../reference/embedded-ui/index.md), чтобы [найти решение](#solution), и проверим [его эффективность](#aftermath).

В конце статьи приводятся шаги по [воспроизведению проблемы](#testbed).

## Описание проблемы {#initial-issue}

Вас уведомили о задержках при обработке пользовательских запросов в вашей системе.

{% note info %}

Речь идёт о запросах к [строковой таблице](../../../concepts/datamodel/table.md#row-oriented-tables), управляемой [data shard](../../../concepts/glossary.md#data-shard)'ом.

{% endnote %}

Рассмотрим графики **Latency** на панели мониторинга Grafana [DB overview](../../../reference/observability/metrics/grafana-dashboards.md#dboverview) и определим, имеет ли отношение наша проблема к кластеру {{ ydb-short-name }}:

![DB Overview > Latencies > R tx server latency percentiles](_assets/overloaded-shard-simple-case/incident-grafana-latency-percentiles.png)

{% cut "См. описание графика" %}

График отображает процентили задержек транзакций. Примерно в ##10:19:30## эти значения выросли в два-три раза.

{% endcut %}

![DB Overview > Latencies > Read only tx server latency](_assets/overloaded-shard-simple-case/incident-grafana-latencies.png)

{% cut "См. описание графика" %}

График отображает тепловую карту (heatmap) задержек транзакций. Транзакции группируются на основании их задержек, каждая группа (bucket) окрашивается в свой цвет. Таким образом, этот график показывает как количество транзакций, обрабатываемых {{ ydb-short-name }} в секунду (по вертикальной оси), так и распределение задержек среди транзакций (цветовая дифференциация).

К ##10:20:30## доля транзакций с минимальными задержками (`Группа 1`, тёмно-зелёный) упала в четыре-пять раз. `Группа 4` выросла примерно в пять раз, а также выделилась новая группа транзакций с ещё более высокими задержками — `Группа 8`.

{% endcut %}

Таким образом, мы видим, что задержки действительно выросли. Теперь нам необходимо локализовать проблему.

## Диагностика {#diagnostics}

Давайте определим причину роста задержек. Могли ли они увеличиться из-за возросшей нагрузки? Посмотрим на график **Requests** в секции **API details** панели мониторинга Grafana [DB overview](../../../reference/observability/metrics/grafana-dashboards.md#dboverview):

![API details](./_assets/overloaded-shard-simple-case/incident-grafana-api-section-requests.png)

Количество пользовательских запросов выросло приблизительно с 27 000 до 35 000 в ##10:20:00##. Но может ли {{ ydb-short-name }} справиться с увеличившейся нагрузкой без дополнительных аппаратных ресурсов?

Загрузка CPU увеличилась, что видно на графике **CPU by execution pool**.

![CPU](./_assets/overloaded-shard-simple-case/incident-grafana-cpu-by-execution-pool.png)

{% cut "См. графики на панели мониторинга Grafana **CPU**" %}

Графики на панели мониторинга Grafana **CPU** показывают рост нагрузки на CPU [в пуле ресурсов пользователей и интерконнекта](../../../concepts/glossary.md#actor-system-pool):

![CPU](./_assets/overloaded-shard-simple-case/incident-grafana-cpu-dashboard-user-pool-by-actors.png)

![CPU](./_assets/overloaded-shard-simple-case/incident-grafana-cpu-dashboard-ic-pool.png)

![CPU](./_assets/overloaded-shard-simple-case/incident-grafana-cpu-dashboard-ic-pool-by-host.png)

{% endcut %}

Мы также можем взглянуть на общее использование CPU на вкладке **Diagnostics** в [Embedded UI](../../../reference/embedded-ui/index.md):

![CPU diagnostics](./_assets/overloaded-shard-simple-case/incident-ui-cpu-usage.png)

Кластер {{ ydb-short-name }} не использует все ресурсы CPU.

Взглянув на секции **DataShard** и **DataShard details** на панели мониторинга Grafana [DB overview](../../../reference/observability/metrics/grafana-dashboards.md#dboverview), мы увидим, что после роста нагрузки на кластер один из data shard'ов был перегружен.

![Throughput](./_assets/overloaded-shard-simple-case/incident-grafana-throughput-rows.png)

{% cut "См. описание графика" %}

Этот график показывает, что количество читаемых строк в базе данных {{ ydb-short-name }} увеличилось с ~26 000 до ~33 500 строк в секунду.

{% endcut %}

![Shard distribution by load](./_assets/overloaded-shard-simple-case/incident-grafana-shard-distribution-by-workload.png)

{% cut "См. описание графика" %}

Этот график отображает тепловую карту распределения data shard'ов по нагрузке. Каждый data shard потребляет от 0% до 100% ядра CPU. Data shard'ы делятся на десять групп по занимаемой ими доле ядра CPU — 0-10%, 10-20% и т.д. Эта тепловая карта показывает количество data shard'ов в каждой группе.

График показывает только один data shard, нагрузка на который изменилась примерно в ##10:19:30## — data shard перешёл в `Группу 70`, содержащую шарды, нагруженные на 60–70%.

{% endcut %}

![Overloaded shard](./_assets/overloaded-shard-simple-case/incident-grafana-overloaded-shards.png)

{% cut "См. описание графика" %}

По аналогии с предыдущим графиком, **Overloaded shard count** — это тепловая карта распределения data shard'ов по нагрузке. Однако этот график отображает только data shard'ы с нагрузкой, превышающей 60%.

График показывает, что нагрузка на один data shard увеличилась до 70% примерно в ##10:19:30##.

{% endcut %}

Чтобы определить, какую таблицу обслуживает перегруженный data shard, откроем вкладку **Diagnostics > Top shards** во встроенном UI:

![Diagnostics > shards](./_assets/overloaded-shard-simple-case/incident-ui-top-shards.png)

Мы видим, что один из data shard'ов, обслуживающих таблицу `kv_test`, нагружен на 67%.

Далее давайте взглянем на информацию о таблице `kv_test` на вкладке **Info**:

![stock table info](./_assets/overloaded-shard-simple-case/incident-ui-table-info.png)

{% note warning %}

Таблица `kv_test` была создана с отключённым партиционированием по нагрузке и содержит только одну партицию.

Это означает, что все запросы к этой таблице обрабатывает один data shard. Учитывая, что data shard'ы — это однопоточные компоненты, обрабатывающие за раз только один запрос, такой подход неэффективен.

{% endnote %}

## Решение {#solution}

Нам необходимо включить партиционирование по нагрузке для таблицы `kv_test`:

1. Во встроенном UI выберите базу данных.
2. Откройте вкладку **Query**.
3. Выполните следующий запрос:

    ```yql
    ALTER TABLE kv_test SET (
        AUTO_PARTITIONING_BY_LOAD = ENABLED
    );
    ```

## Результат {#aftermath}

После включения автоматического партиционирования для таблицы `kv_test` перегруженный data shard разделился на два.

![Shard distribution by load](./_assets/overloaded-shard-simple-case/aftermath-grafana-shard-distribution-by-workload.png)

{% cut "См. описание графика" %}

График показывает, что количество data shard'ов выросло примерно в ##10:28:00##. Судя по цвету групп, их нагрузка не превышает 40%.

{% endcut %}

![Overloaded shard count](./_assets/overloaded-shard-simple-case/aftermath-grafana-overloaded-shards.png)

{% cut "См. описание графика" %}

Перегруженный шард исчез с графика примерно в ##10:28:00##.

{% endcut %}

Теперь два data shard'а обрабатывают запросы к таблице `kv_test`, и ни один из них не перегружен:

![Overloaded shard count](./_assets/overloaded-shard-simple-case/aftermath-ui-top-shards.png)

Давайте убедимся, что задержки транзакций вернулись к прежним значениям:

![Final latency percentiles](./_assets/overloaded-shard-simple-case/aftermath-grafana-latency-percentiles.png)

{% cut "См. описание графика" %}

Примерно в ##10:28:00## процентили задержек p50, p75 и p95 упали практически до прежних значений. Задержки p99 сократились не настолько значительно, но всё же уменьшились в два раза.

{% endcut %}

![Final latencies](./_assets/overloaded-shard-simple-case/aftermath-grafana-latencies.png)

{% cut "См. описание графика" %}

Транзакции на этом графике теперь распределены по шести группам. Примерно половина транзакций вернулась в `Группу 1`, то есть их задержки не превышают одной миллисекунды. Более трети транзакций находятся в `Группе 2` с задержками от одной до двух миллисекунд. Одна шестая транзакций — в `Группе 4`. Размеры остальных групп незначительны.

{% endcut %}

Задержки практически вернулись к уровню до увеличения нагрузки. При этом мы не увеличили расходы на приобретение дополнительных аппаратных ресурсов, а просто включили автоматическое партиционирование по нагрузке, что позволило более эффективно использовать доступные ресурсы.

#|
|| Имя группы
| Задержки, мс
|
Один перегруженный data shard,
транзакций в секунду
|
Несколько data shard'ов,
транзакций в секунду
||
|| 1
| 0-1
| 2110
| <span style="color:teal">▲</span> 16961
||
|| 2
| 1-2
| 5472
| <span style="color:teal">▲</span> 13147
||
|| 4
| 2-4
| 16437
| <span style="color:navy">▼</span> 6041
||
|| 8
| 4-8
| 9430
| <span style="color:navy">▼</span> 432
||
|| 16
| 8-16
| 98.8
| <span style="color:navy">▼</span> 52.4
||
|| 32
| 16-32
| —
| <span style="color:teal">▲</span> 0.578
||
|#

## Тестовый стенд {#testbed}

### Топология

Для этого примера мы использовали кластер {{ ydb-short-name }} из трёх серверов на Ubuntu 22.04 LTS. На каждом сервере был запущен один [узел хранения](../../../concepts/glossary.md#storage-node) и три [узла баз данных](../../../concepts/glossary.md#database-node), обслуживающих одну и ту же базу данных.

```mermaid
flowchart

subgraph client[Клиент]
    cli(YDB CLI)
end

client-->cluster

subgraph cluster["Кластер YDB"]
    direction TB
    subgraph S1["Сервер 1"]
        node1(Узел базы данных YDB 1)
        node2(Узел базы данных YDB 2)
        node3(Узел базы данных YDB 3)
        node4(Узел хранения YDB 1)
    end
    subgraph S2["Сервер 2"]
        node5(Узел базы данных YDB 1)
        node6(Узел базы данных YDB 2)
        node7(Узел базы данных YDB 3)
        node8(Узел хранения YDB 1)
    end
    subgraph S3["Сервер 3"]
        node9(Узел базы данных YDB 1)
        node10(Узел базы данных YDB 2)
        node11(Узел базы данных YDB 3)
        node12(Узел хранения YDB 1)
    end
end

classDef storage-node fill:#D0FEFE
classDef database-node fill:#98FB98
class node4,node8,node12 storage-node
class node1,node2,node3,node5,node6,node7,node9,node10,node11 database-node
```

### Аппаратная конфигурация

Аппаратные ресурсы серверов (виртуальных машин) приведены ниже:

- Платформа: Intel Broadwell
- Гарантированный уровень производительности vCPU: 100%
- vCPU: 28
- Память: 32 GB
- Диски:
    - 3 × 93 GB SSD на каждом узле {{ ydb-short-name }}
    - 20 GB HDD для операционной системы


### Тест

Нагрузка на кластер {{ ydb-short-name }} была запущена с помощью команды CLI `ydb workload`. Дополнительную информацию см. в статье [{#T}](../../../reference/ydb-cli/commands/workload/index.md).

Чтобы воспроизвести нагрузку, выполните следующие шаги:

1. Проинициализируйте таблицы для нагрузочного тестирования:

    ```shell
    ydb workload kv init --min-partitions 1 --auto-partition 0
    ```

    Мы намеренно отключаем автоматическое партиционирование для создаваемых таблиц используя опции `--min-partitions 1 --auto-partition 0`.

1. Воспроизведите стандартную нагрузку на кластер {{ ydb-short-name }}:

    ```shell
    ydb workload kv run select -s 600 -t 100
    ```

    Мы запустили простую нагрузку, используя базу данных {{ ydb-short-name }} как Key-Value хранилище. Точнее, мы использовали нагрузку `select` для выполнения `SELECT`-запросов, возвращающих строки по точному совпадению primary ключа.

    Параметр `-t 100` используется для запуска нагрузочного тестирования в 100 потоков.

3. Создайте перегрузку на кластере {{ ydb-short-name }}:

    ```shell
    ydb workload kv run select -s 1200 -t 250
    ```

    Как только первый тест завершился, мы немедленно запустили тот же самый тест в 250 потоков, чтобы создать перегрузку.

## Смотрите также

- [{#T}](../../performance/index.md)
- [{#T}](../../performance/schemas/overloaded-shards.md)
- [{#T}](../../../concepts/datamodel/table.md#row-oriented-tables)
