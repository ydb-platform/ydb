# GROUP BY

status: done
commit: 41ffd7dd87c4b8808cee4a292ff543a0bb65138f
id: select-group
depends: select-where-limit

## Сделать

Функции `count`, `sum`, `min`, `max`. Для row-таблицы агрегат считается в сервисе: datashard программу не принимает. Для колоночной таблицы, когда она уже читается, `GroupBy` из `TOlapProgram` уходит в columnshard, если покрыт программой; до колоночного среза колоночная таблица — `0A000`. Без групп — агрегат по всему входу. Другие агрегаты — `0A000`.

## Тест

Тест: `count` и `sum` по группам на row-таблице. `avg` — `0A000`.

## Готово, когда

Группы и четыре функции совпадают с ручным подсчётом.

## Не трогать

- `ydb/core/tx/datashard`, `ydb/core/tx/columnshard` и их протобуфы
- `yql/` и `ydb/core/kqp`: ни `PEERDIR`, ни вызовы
- remote `official`
- код datashard и columnshard в одном читателе

Пока этой возможности нет в коде, она отвечает SQLSTATE `0A000`, текст `not supported`, сессия жива.
## Перед коммитом

```bash
./ya make --build relwithdebinfo -tA ydb/services/sql
```

Коммит только после зелёного прогона. В прогон входят тесты `pg8000`, когда они уже есть.

## После коммита

В этом файле поставить `status: done` и хеш в `commit`. Правка отметки входит в следующий коммит.
