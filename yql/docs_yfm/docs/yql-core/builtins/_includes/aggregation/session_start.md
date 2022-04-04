## SessionStart {#session-start}

Без аргументов. Допускается только при наличии [SessionWindow](../../../syntax/group_by.md#session-window) в
[GROUP BY](../../../syntax/group_by.md) / [PARTITION BY](../../../syntax/window.md#partition).
Возвращает значение ключевой колонки `SessionWindow`. В случае `SessionWindow` с двумя аргументами – минимальное значение первого аргумента внутри группы/раздела.
В случае раширенного варианта `SessionWindoow` – значение второго элемента кортежа, возвращаемого `<calculate_lambda>`, при котором первый элемент кортежа равен `True`.
