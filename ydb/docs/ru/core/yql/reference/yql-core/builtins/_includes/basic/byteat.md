## ByteAt {#byteat}

Получение значение байта в строке по индексу от её начала. В случае некорректного индекса возвращается `NULL`.

**Сигнатура**
```
ByteAt(String, Uint32)->Uint8
ByteAt(String?, Uint32)->Uint8?

ByteAt(Utf8, Uint32)->Uint8
ByteAt(Utf8?, Uint32)->Uint8?
```

Аргументы:

1. Строка: `String` или `Utf8`;
2. Индекс: `Uint32`.

**Примеры**
``` yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```
