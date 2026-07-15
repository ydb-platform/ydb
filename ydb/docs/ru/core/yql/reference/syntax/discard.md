
# DISCARD

Вычисляет {% if select_command == "SELECT STREAM" %}[`SELECT STREAM`](select_stream.md){% else %}[`SELECT`](select/index.md){% endif %}{% if feature_mapreduce %}{% if reduce_command %}, [`{{ reduce_command }}`](reduce.md){% endif %}  или [`{{ process_command }}`](process.md){% endif %}, но не возвращает результат ни в клиент, ни в таблицу. {% if feature_mapreduce %}Не может быть задано одновременно с [INTO RESULT](into_result.md).{% endif %}

Полезно использовать в сочетании с [`Ensure`](../builtins/basic.md#ensure) для проверки выполнения пользовательских условий на финальный результат вычислений.

{% if backend_name == "YDB" %}

Запрос с `DISCARD` выполняется полностью — со всеми фильтрами, агрегациями и проверками `Ensure`, — но результирующий набор (result set) не возвращается клиенту. Данные при этом не пересылаются по сети: клиент получает только статус выполнения запроса. На больших выборках это заметно экономит трафик и память.

В многостейтментном запросе `DISCARD` действует на отдельный стейтмент:

```yql
SELECT 1;          -- вернётся
DISCARD SELECT 2;  -- выполнится, но в ответ не попадёт
SELECT 3;          -- вернётся
```

Клиент получит два результирующих набора — от первого и третьего стейтмента.

`DISCARD` применяется только к стейтменту целиком, использовать его внутри выражений нельзя. В частности, недопустимо:

* в подзапросах — `SELECT * FROM (DISCARD SELECT 1)`;
* в `WHERE ... IN (...)` — `SELECT * FROM my_table WHERE Key IN (DISCARD SELECT 1)`;
* в операндах `UNION ALL` — `SELECT 1 UNION ALL DISCARD SELECT 2`.

{% note info %}

`DISCARD` поддерживается при выполнении запросов через сервис исполнения запросов (Query Service) начиная с версии {{ ydb-short-name }} 26.2. При выполнении через устаревшие интерфейсы (Table Service, скан-запросы) `DISCARD` игнорируется, и результат возвращается клиенту — поведение сохранено для обратной совместимости.

{% endnote %}

{% endif %}

{% if select_command != true or select_command == "SELECT" %}

## Примеры

```yql
DISCARD SELECT 1;
```

{% if backend_name == "YDB" %}

```yql
DISCARD SELECT Ensure(
    Data,
    Data < 1000000,
    "Value too big"
) FROM `result_table`;
```

Если условие `Ensure` нарушено, запрос завершится ошибкой. Если всё в порядке — запрос отработает успешно, а клиент не получит результирующий набор, который иначе пришлось бы выкачивать целиком.

{% else %}

```yql
INSERT INTO result_table WITH TRUNCATE
SELECT * FROM
my_table
WHERE value % 2 == 0;

COMMIT;

DISCARD SELECT Ensure(
    0, -- will discard result anyway
    COUNT(*) > 1000,
    "Too small result table, got only " || CAST(COUNT(*) AS String) || " rows"
) FROM result_table;

```

{% endif %}

{% endif %}
