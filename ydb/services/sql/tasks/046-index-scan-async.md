# IndexScan асинхронного индекса

status: pending
commit:
id: index-scan-async
depends: index-scan-sync

## Сделать

Асинхронный индекс в том же переборе, только `Ready`. Чтение допускает отставание: догон делает datashard через `TAsyncIndexChangeCollector`, не этот запрос.

## Тест

Тест: после применения изменения `SELECT` по асинхронному индексу видит строку. Сразу после `INSERT` отставание допустимо.

## Готово, когда

Индекс выбирается, и чтение после догона возвращает строку.

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
