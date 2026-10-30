# information_schema.sequences

status: done
commit: 7762f34326734309bb631ea926566bf75a66f7dd
id: ischema-sequences
depends: select-point

## Сделать

Зависит также от умения вернуть DataRow (`select-point`). Столбцы: catalog, schema, name, `data_type`, `numeric_precision`, start, minimum, maximum, increment, `cycle_option`. Текущее значение последовательности не отдавать. Делать после `select-point`, если к этому коммиту DataRow ещё нет.

## Тест

Тест: поля созданной последовательности совпадают с описанием. Столбца текущего значения нет.

## Готово, когда

Ответ совпадает с `TSequenceDescription`, без next value.

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
