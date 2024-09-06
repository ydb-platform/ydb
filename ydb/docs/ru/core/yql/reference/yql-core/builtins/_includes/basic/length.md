## LENGTH {#length}

Возвращает длину строки в байтах. Также эта функция доступна под именем `LEN`.

**Сигнатура**
```
LENGTH(T)->Uint32
LENGTH(T?)->Uint32?
```

**Примеры**
``` yql
SELECT LENGTH("foo");
```
``` yql
SELECT LEN("bar");
```

{% note info %}

Для вычисления длины строки в unicode символах можно воспользоваться функцией [Unicode::GetLength](../../../udf/list/unicode.md).<br/><br/>Для получения числа элементов в списке нужно использовать функцию [ListLength](../../list.md#listlength).

{% endnote %}
