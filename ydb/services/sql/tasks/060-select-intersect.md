# INTERSECT

status: done
commit: a973f5938bca2234a7746a21fa1fc8956db43fa3
id: select-intersect
depends: select-union

## Сделать

Строки, которые есть в обоих входах.

## Тест

Тест: в ответе только общая строка.

## Готово, когда

Пересечение совпадает с ручной проверкой.

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
