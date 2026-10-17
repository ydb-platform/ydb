# Extended query

status: pending
commit:
id: extended
depends: insert-returning

## Сделать

Parse, Bind, Describe, Execute, Sync, Close для уже поддержанного подмножества. Порталы, параметры `$n`, форматы text и binary. Кэш подготовленных выражений в сессии может ссылаться на нодовый кеш планов. Ошибка портала не рвёт соединение: Sync возвращает ReadyForQuery.
Функциональные тесты pg8000 с параметрами и `commit()` до этого коммита не писать: они шлют extended. Без параметров и с autocommit pg8000 остаётся на simple query.

## Тест

Тест параметра `$1` в `SELECT` и `INSERT`. Ошибка одного Execute не закрывает соединение.

## Готово, когда

Повторный Execute того же портала использует разобранный оператор.

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
