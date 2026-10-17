# Точечный SELECT одного шарда

status: pending
commit:
id: select-point
depends: planner

## Сделать

Исполнитель в каталоге `datashard`, не в общем читателе. Точечный `SELECT`: один `TEvDataShard::TEvRead` с ключом, доставка `TEvPipeCache::TEvForward`. В запрос входят только нужные столбцы. Ответ — RowDescription, DataRow, CommandComplete `SELECT n`. Байты локального batch смотреть без копии до исходящего буфера. Одна фаза чтения снимок не берёт.
Повторы только этого шарда: `OVERLOADED` с `ThrottleDelayMs`, `INTERNAL_ERROR` в пределах конфига, `NOT_FOUND` и обрыв pipe — заново resolve. `SCHEME_ERROR` не повторять. `BAD_REQUEST` не повторять.

## Тест

Тест на таблице одного шарда: `SELECT` по полному первичному ключу возвращает строку. Функциональный тест pg8000 — с autocommit и без параметров, пока нет extended.

## Готово, когда

Клиент получает одну строку и `SELECT 1`.

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
