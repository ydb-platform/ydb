# Подзапрос

status: pending
commit:
id: select-subquery
depends: select-where-limit

## Сделать

Подзапрос в `FROM` и в списке выборки. Вторая фаза чтения берёт один снимок на таблицы оператора. Предикат в `WHERE` (`IN`, `NOT IN`, `EXISTS`, `ANY`, `ALL`, сравнение со скаляром) сюда не входит: это задача `where-subquery`.

## Тест

Тест: скалярный подзапрос и подзапрос в `FROM` возвращают ожидаемые строки.

## Готово, когда

Подзапрос видит те же зафиксированные данные, что внешний запрос.

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
