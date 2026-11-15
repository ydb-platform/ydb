### Примеры

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```


{% if feature_window_functions %}

  ## SessionStart {#session-start}

Без аргументов. Допускается только при наличии [SessionWindow](../../syntax/select/group-by.md#session-window) в
[GROUP BY](../../syntax/select/group-by.md) / [PARTITION BY](../../syntax/select/window.md#partition).
Возвращает значение ключевой колонки `SessionWindow`. В случае `SessionWindow` с двумя аргументами – минимальное значение первого аргумента внутри группы/раздела.
В случае расширенного варианта `SessionWindow` – значение второго элемента кортежа, возвращаемого `<calculate_lambda>`, при котором первый элемент кортежа равен `True`.


{% endif %}
