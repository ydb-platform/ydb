# UPDATE колоночной таблицы

status: done
commit: 0c926cfdd335d7cbee79350114643d6809ca9989
id: column-update
depends: column-insert

## Сделать

`TEvWrite` columnshard, `OPERATION_UPDATE`. Таблица с синхронным индексом — `0A000`, пока запись индекса не входит в ту же транзакцию.

## Тест

Тест: значение столбца меняется и читается обратно.

## Готово, когда

Обновление видно следующим чтением.

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
