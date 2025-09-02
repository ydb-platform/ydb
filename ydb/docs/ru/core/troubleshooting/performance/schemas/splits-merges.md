# Избыточные разделения и слияния партиций таблиц

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

Каждая партиция [строковой таблицы](../../../concepts/datamodel/table.md#row-oriented-tables) в {{ ydb-short-name }} обрабатывается таблеткой [data shard](../../../concepts/glossary.md#data-shard). {{ ydb-short-name }} поддерживает автоматическое [разделение и слияние](../../../concepts/datamodel/table.md#partitioning) таблеток data shard, что позволяет легко адаптироваться к изменениям в рабочих нагрузках. Однако эти операции не являются бесплатными и могут оказать кратковременное негативное влияние на задержки запросов.

Когда {{ ydb-short-name }} разбивает партицию, исходная партиция заменяется двумя новыми партициями, охватывающими тот же диапазон первичных ключей. Теперь две таблетки data shard обрабатывают диапазон первичных ключей, который ранее обрабатывался одной таблеткой data shard, тем самым добавляя больше вычислительных ресурсов для таблицы.

По умолчанию {{ ydb-short-name }} разделяет партицию таблицы, когда её размер достигает 2 ГБ. Однако рекомендуется также включить разделение по загрузке, что позволит {{ ydb-short-name }} разделять перегруженные партиции, даже если их размер меньше 2 ГБ.

У [scheme shard](../../../concepts/glossary.md#scheme-shard) уходит примерно 15 секунд на принятие решения о разделении таблетки data shard. По умолчанию пороговое значение потребления процессора для разделения таблетки data shard установлено в 50%.

Когда {{ ydb-short-name }} объединяет соседние партиции в строковой таблице, они заменяются одной партицией, которая охватывает их диапазон первичных ключей. Соответствующие таблетки data shard также объединяются в одну таблетку для управления новой партицией.

Для того чтобы произошло слияние, таблетки data shard должны существовать не менее 10 минут, а их загрузка процессора за последний час не должна превышать 35%.

При настройке [партиционирования таблицы](../../../concepts/datamodel/table.md#partitioning) вы можете также установить лимиты на [минимальное](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) и [максимальное количество партиций](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count). Если разница между минимальным и максимальным пределами превышает 20%, а загрузка таблицы значительно меняется с течением времени, [Hive](../../../concepts/glossary.md#hive) может начать разделять перегруженные таблицы, а затем объединять их обратно в периоды низкой загрузки.

## Диагностика

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/splits-merges.md) %}

## Рекомендации

Если пользовательская нагрузка на {{ ydb-short-name }} не изменилась, рассмотрите возможность изменения интервала между минимальным и максимальным лимитами на количество партиций таблицы до рекомендуемой разницы в 20%. Используйте инструкцию YQL [`ALTER TABLE имя_таблицы SET (ключ = значение)`](../../../yql/reference/syntax/alter_table/set.md) для обновления параметров [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) и [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count).

Если вы хотите избежать разделения и слияния таблеток data shard, вы можете выставить одинаковые значения для параметров [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) и [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) или отключить разделение по загрузке.
