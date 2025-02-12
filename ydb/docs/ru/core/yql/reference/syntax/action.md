# ACTION

## DEFINE ACTION {#define-action}

Задает именованное действие, которое представляют собой параметризуемый блок из нескольких выражений верхнего уровня.

### Синтаксис

1. `DEFINE ACTION` — объявление действия.
1. [Имя действия](expressions.md#named-nodes), по которому объявляемое действие доступно далее для вызова.
1. В круглых скобках — список имен параметров.
1. Ключевое слово `AS`.
1. Список выражений верхнего уровня.
1. `END DEFINE` — маркер последнего выражения внутри действия.

Один или более последних параметров могут быть помечены знаком вопроса `?` как необязательные. Если они не будут указаны при вызове, то им будет присвоено значение `NULL`.

## DO {#do}

Выполняет `ACTION` с указанными параметрами.

### Синтаксис

1. `DO` — выполнение действия.
1. Именованное выражение, по которому объявлено действие.
1. В круглых скобках — список значений для использования в роли параметров.

`EMPTY_ACTION` — действие, которое ничего не выполняет.

{% if feature_mapreduce %}

{% note info %}

В больших запросах объявление действий можно выносить в отдельные файлы и подключать их в основной запрос с помощью [EXPORT](export_import.md#export) + [IMPORT](export_import.md#import), чтобы вместо одного длинного текста получилось несколько логических частей, в которых проще ориентироваться. Важный нюанс: директива `USE my_cluster;` в импортирующем запросе не влияет на поведение объявленных в других файлах действий.

{% endnote %}

{% endif %}

### Пример

```yql
DEFINE ACTION $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

DO EMPTY_ACTION();
DO $hello_world(NULL);
DO $hello_world("John");
DO $hello_world(NULL, "Earth");
```



## BEGIN .. END DO {#begin}

Выполнение действия без его объявления (анонимное действие).

### Синтаксис

1. `BEGIN`;
1. Список выражений верхнего уровня;
1. `END DO`.

Анонимное действие не может содержать параметров.

### Пример

```yql
DO BEGIN
    SELECT 1;
    SELECT 2  -- здесь и в предыдущем примере ';' перед END можно не ставить
END DO
```

{% if feature_mapreduce %}

## EVALUATE IF {#evaluate-if}

`EVALUATE IF` — выполнение действия (action) в зависимости от выполнения условия. Далее указывается:

1. Условие;
2. [DO](#do) с именем и параметрами действия или анонимным действием;
3. Опционально `ELSE` и следом второе `DO` для ситуации, когда условие не выполнено.

## EVALUATE FOR {#evaluate-for}

`EVALUATE FOR` — выполнение действия (action) для каждого элемента в списке. Далее указывается:

1. [Именованное выражение](expressions.md#named-nodes), в которое будет подставляться каждый очередной элемент списка;
2. Ключевое слово `IN`;
3. Объявленное выше именованное выражение со списком, по которому будет выполняться действие.
4. [DO](#do) с именем и параметрами действия или анонимным действием, в параметрах можно использовать как текущий элемент из первого пункта, так и любые объявленные выше именованные выражения, в том числе сам список.
5. Опционально `ELSE` и следом второе `DO` для ситуации, когда список пуст.

### Примеры

```yql
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

```yql
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

{% note info %}

Стоит учитывать, что `EVALUATE` выполняется до начала работы основного запроса. Также в рамках вычисления предиката в `EVALUATE IF` или списка в `EVALUATE FOR` невозможно использование {% if feature_temp_table %}[анонимных таблиц](select/temporary_table.md){% else %} анонимных таблиц{% endif %}.

{% endnote %}


{% endif %}
