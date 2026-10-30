# Привязка таблицы через scheme cache

status: done
commit: 4d69b9ab68a1d2febbe97146d8d63545aec12d9a
id: analyze-scheme
depends: parse-where-limit

## Сделать

Analyzer резолвит путь через `TEvTxProxySchemeCache::TEvNavigateKeySet`. Имя без схемы — объект в корне базы из параметра `database`. Имя `schema.table` — путь `<база>/<схема>/<таблица>`. Проверяет имена столбцов. Неизвестный столбец — ошибка до похода в шард.

## Тест

Тест на мини-кластере: неизвестный столбец не читает шард. Известный столбец проходит analyzer.

## Готово, когда

Путь и столбцы сверены со scheme cache.

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
