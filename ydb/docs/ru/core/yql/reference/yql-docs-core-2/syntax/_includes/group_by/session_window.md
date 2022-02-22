## GROUP BY ... SessionWindow() {#session-window}

В YQL поддерживаются группировки по сессиям. К обычным выражениям в `GROUP BY` можно добавить специальную функцию `SessionWindow`:

```sql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```

При этом происходит следующее:

1) Входная таблица партиционируется по ключам группировки, указанным в `GROUP BY`, без учета SessionWindow (в данном случае по `user`).
   Если кроме SessionWindow в `GROUP BY` ничего нет, то входная таблица попадает в одну партицию
2) Каждая партиция делится на непересекающие подмножества строк (сессии).
   Для этого партиция сортируется по возрастанию значения выражения `time_expr`.
   Границы сессий проводятся между соседними элементами партиции, разница значений `time_expr` для которых превышает `timeout_expr`
3) Полученные таким образом сессии и являются финальными партициями, на которых вычисляются агрегатные функции.

Ключевая колонка SessionWindow() (в примере `session_start`) имеет значение "минимальный `time_expr` в сессии".
Кроме того, при наличии SessionWindow() в `GROUP BY` может использоваться специальная агрегатная функция
[SessionStart](../../../builtins/aggregation.md#session-start).

Поддерживается также расширенный вариант SessionWindow с четырьмя аргументами:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Здесь:
* `<order_expr>` – выражение по которому сортируется исходная партиция
* `<init_lambda>` – лямбда-функция для инициализации состояния расчета сессий. Имеет сигнатуру `(TableRow())->State`. Вызывается один раз на первом (по порядку сортировки) элементе исходной партиции
* `<update_lambda>` – лямбда-функция для обновления состояния расчета сессий и определения границ сессий. Имеет сигнатуру `(TableRow(), State)->Tuple<Bool, State>`. Вызывается на каждом элементе исходной партиции, кроме первого. Новое значения состояния вычисляется на основе текущей строки таблицы и предыдущего состояния. Если первый элемент возвращенного кортежа имеет значение `True`, то с _текущей_ строки начнется новая сессия. Ключ новой сессии получается путем применения `<calculate_lambda>` ко второму элементу кортежа.
* `<calculate_lambda>` – лямбда-функция для вычисления ключа сессии ("значения" SessionWindow(), которое также доступно через SessionStart()). Функция имеет сигнатуру `(TableRow(), State)->SessionKey`. Вызывается на первом элемента партиции (после `<init_lambda>`) и на тех элементах, для которых `<update_lambda>` вернула `True` в качестве первого элемента кортежа. Стоит отметить, что для начала новой сессии необходимо, чтобы `<calculate_lambda>` вернула значение, которое отличается от предыдущего ключа сессии. При этом сессии с одинаковыми ключами не объединяются. Например, если `<calculate_lambda>` последовательно возвращает `0, 1, 0, 1`, то это будут четыре различные сессии.

С помощью расширенного варианта SessionWindow можно решить, например, такую задачу: разделить партицию на сессии как в варианте SessionWindow с двумя аргументами, но с ограничением максимальной длины сессии некоторой константой:

**Пример**
```sql
$max_len = 1000; -- максимальная длина сессии
$timeout = 100; -- таймаут (timeout_expr в упрощенном варианте SessionWindow)

$init = ($row) -> (AsTuple($row.ts, $row.ts)); -- состояние сессии - тапл из 1) значения временной колонки ts на первой строчке сессии и 2) на текущей строчке
$update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(ts, $init, $update, $calculate) AS session_start
```

SessionWindow может использоваться в GROUP BY только один раз.
