# ON CONFLICT по первичному ключу

status: pending
commit:
id: insert-on-conflict
depends: insert-row

## Сделать

Прочитать строку по ключу. Нет строки — `INSERT` всей предлагаемой строки. Есть и `DO NOTHING` — не писать и не считать строку. Есть и `DO UPDATE` — `OPERATION_UPDATE` только столбцов из `SET` у найденного PK. `EXCLUDED.столбец` — значение из предлагаемой строки. Ложный `WHERE` — как `DO NOTHING`. Гонка: для `DO NOTHING` повторный `23505` — пропуск; для `DO UPDATE` прочитать снова и обновить. Цель не по PK и присваивание не литералом и не `EXCLUDED` — `0A000`.

## Тест

Тест: `DO NOTHING` не меняет строку и считает 0. `DO UPDATE` меняет только столбцы `SET`. Ложный `WHERE` не меняет строку.

## Готово, когда

Конфликт по PK обработан без UPSERT всей строки.

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
