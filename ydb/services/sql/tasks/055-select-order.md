# ORDER BY

status: pending
commit:
id: select-order
depends: select-where-limit

## Сделать

Сортировка в сервисе после фильтра. Если ключ сортировки — первичный ключ, чтение идёт с `TEvRead.Reverse` вместо отдельной сортировки, когда направление это позволяет. `ASC` / `DESC`, `NULLS FIRST` / `NULLS LAST`.

## Тест

Тест: порядок строк совпадает с ключом. `DESC` по PK использует обратное чтение.

## Готово, когда

Порядок ответа устойчив и совпадает с заданным.

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
