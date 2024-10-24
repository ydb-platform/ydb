## Процедура выполнения SELECT {#selectexec}

Результат запроса `SELECT` вычисляется следующим образом:

* определяется набор входных таблиц – вычисляются выражения после [FROM](../../select/from.md);
* к входным таблицам применяется [SAMPLE](sample.md) / [TABLESAMPLE](sample.md)
* выполняется [FLATTEN COLUMNS](../../flatten.md#flatten-columns) или [FLATTEN BY](../../flatten.md); алиасы, заданные во `FLATTEN BY`, становятся видны после этой точки;
{% if feature_join %}
* выполняются все [JOIN](../../join.md);
{% endif %}
* к полученным данным добавляются (или заменяются) колонки, заданные в [GROUP BY ... AS ...](../../group_by.md);
* выполняется [WHERE](where.md) &mdash; все данные не удовлетворяющие предикату отфильтровываются;
* выполняется [GROUP BY](../../group_by.md), вычисляются значения агрегатных функций;
* выполняется фильтрация [HAVING](../../group_by.md#having);
{% if feature_window_functions %}
* вычисляются значения [оконных функций](../../window.md);
{% endif %}
* вычисляются выражения в `SELECT`;
* выражениям в `SELECT` назначаются имена заданные алиасами;
* к полученным таким образом колонкам применяется top-level [DISTINCT](distinct.md);
* таким же образом вычисляются все подзапросы в [UNION ALL](union_all.md), выполняется их объединение (см. [PRAGMA AnsiOrderByLimitInUnionAll](../../pragma.md#pragmas));
* выполняется сортировка согласно [ORDER BY](order_by.md);
* к полученному результату применяются [OFFSET и LIMIT](limit_offset.md).