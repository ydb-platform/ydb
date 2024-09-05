## EVALUATE IF {#evaluate-if}
`EVALUATE IF` — выполнение действия (action) в зависимости от выполнения условия. Далее указывается:

1. Условие;
2. [DO](#do) с именем и параметрами действия или анонимным действием;
3. Опционально `ELSE` и следом второе `DO` для ситуации, когда условие не выполнено.

## EVALUATE FOR {#evaluate-for}
`EVALUATE FOR` — выполнение действия (action) для каждого элемента в списке. Далее указывается:

1. [Именованное выражение](../../expressions.md#named-nodes), в которое будет подставляться каждый очередной элемент списка;
2. Ключевое слово `IN`;
3. Объявленное выше именованное выражение со списком, по которому будет выполняться действие.
4. [DO](#do) с именем и параметрами действия или анонимным действием, в параметрах можно использовать как текущий элемент из первого пункта, так и любые объявленные выше именованные выражения, в том числе сам список.
5. Опционально `ELSE` и следом второе `DO` для ситуации, когда список пуст.

**Примеры**
``` yql
DEFINE ACTION $hello() AS
    SELECT "Hello!";
END DEFINE;

DEFINE ACTION $bye() AS
    SELECT "Bye!";
END DEFINE;

EVALUATE IF RANDOM(0) > 0.5
    DO $hello()
ELSE
    DO $bye();

EVALUATE IF RANDOM(0) > 0.1 DO BEGIN
    SELECT "Hello!";
END DO;

EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
    SELECT $i;
END DO;
```

``` yql
-- скопировать таблицу $input в $count новых таблиц
$count = 3;
$input = "my_input";
$inputs = ListReplicate($input, $count);
$outputs = ListMap(
    ListFromRange(0, $count),
    ($i) -> {
        RETURN "tmp/out_" || CAST($i as String)
    }
);
$pairs = ListZip($inputs, $outputs);

DEFINE ACTION $copy_table($pair) as
    $input = $pair.0;
    $output = $pair.1;
    INSERT INTO $output WITH TRUNCATE
    SELECT * FROM $input;
END DEFINE;

EVALUATE FOR $pair IN $pairs
    DO $copy_table($pair)
ELSE
    DO EMPTY_ACTION(); -- такой ELSE можно было не указывать,
                       -- ничего не делать подразумевается по умолчанию
```

{% note info "Примечание" %}

Стоит учитывать, что `EVALUATE` выполняется до начала работы основного запроса. Также в рамках вычисления предиката в `EVALUATE IF` или списка в `EVALUATE FOR` невозможно использование {% if feature_temp_table %}[анонимных таблиц](../../select/temporary_table.md){% else %} анонимных таблиц{% endif %}.

{% endnote %}
