# UPSERT INTO

{% if oss == "true" and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

{% include [OLAP_not_allow_text](../../../../_includes/ways_add_data_to_olap.md) %}

{% endnote %}

{% endif %}

UPSERT (расшифровывается как UPDATE or INSERT) обновляет или добавляет множество строк в строковой таблице на основании сравнения по первичному ключу. Отсутствующие строки добавляются. В присутствующих строках обновляются значения заданных столбцов, значения остальных столбцов остаются неизменными.

{% if feature_mapreduce %}  Таблица по имени ищется в базе данных, заданной оператором [USE](../use.md).{% endif %}

{% if feature_replace %}
`UPSERT` и [`REPLACE`](../replace_into.md) являются операциями модификации данных, которые не требует их предварительного чтения, за счет чего работают быстрее и дешевле других операций.
{% else %}
`UPSERT` является единственной операцией модификации данных, которая не требует их предварительного чтения, за счет чего работает быстрее и дешевле других операций.
{% endif %}

Сопоставление столбцов при использовании `UPSERT INTO ... SELECT` производится по именам. Используйте `AS` для получения колонки с нужным именем в `SELECT`.

**Примеры**

``` yql
UPSERT INTO my_table
SELECT pk_column, data_column1, col24 as data_column3 FROM other_table  
```

``` yql
UPSERT INTO my_table ( pk_column1, pk_column2, data_column2, data_column5 )
VALUES ( 1, 10, 'Some text', Date('2021-10-07')),
       ( 2, 10, 'Some text', Date('2021-10-08'))
```
