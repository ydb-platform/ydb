# Python-биндинги к libyson

## Высокоуровневый интерфейс

Функции `dumps`, `loads` для преобразования строк:
```python
>>> from cyson import dumps, loads

>>> print dumps(1234)
1234
>>> print dumps("Hello world! Привет!")
"Hello world! Привет!"
>>> print dumps([1, "foo", None, {'aaa': 'bbb'}])
[1; "foo"; #; {"aaa" = "bbb"}]
>>> dumps([1, "foo", None, {'aaa': 'bbb'}], format='binary')
'[\x02\x02;\x01\x06foo;#;{\x01\x06aaa=\x01\x06bbb}]'
>>> print dumps([1, "foo", None, {'aaa': 'bbb'}], format='pretty')
[
    1;
    "foo";
    #;
        {
        "aaa" = "bbb"
    }
]

>>> loads('1234')
1234
>>> loads('3.14')
3.14
>>> loads('[1; "foo"; #; {"aaa" = "bbb"}]')
[1, 'foo', None, {'aaa': 'bbb'}]
>>> loads('[\x02\x02;\x01\x06foo;#;{\x01\x06aaa=\x01\x06bbb}]')
[1, 'foo', None, {'aaa': 'bbb'}]
```

Функции `list_fragments`, `map_fragments` для удобного чтения из входного
потока.
```python
import cyson

input = cyson.list_fragments(
    cyson.InputStream.from_fd(STDIN_FILENO),
    process_table_index=True,
)

for record in input:
    ...
```

## Низкоуровневый интерфейс

### Адаптеры потоков ввода-вывода

Классы `InputStream`, `OutputStream` не предоставляют никакой функциональности
сами по себе, но позволяют подключить поток ввода/вывода к Reader или Writer.

Конструкторы классов - статические методы с именами `from_*`:

```python
input = cyson.InputStream.from_fd(0)
input = cyson.InputStream.from_string("...")
input = cyson.InputStream.from_iter(iter_chunks)

output = cyson.OutputStream.from_fd(1)
output = cyson.OutputStream.from_file(stringio_file)
```

### Reader/Writer

`Reader` - самый быстрый метод десериализации, и в целом позволяет получать
объекты привычных и ожидаемых типов. При отсутствии атрибутов, порождает
встроенные типы, иначе - `Yson*`. Не позволяет различать `list`/`tuple`, или
получать на входе `set`.

`Writer` позволяет выводить низкоуровневые элементы потока, или сериализовать
объекты. Для сериализации объектов следует использовать метод `write()`.

### StrictReader

`StrictReader` отличается от Reader тем, что всегда создает объекты типа
`Yson*`, независимо от наличия атрибутов.

Никакое специальное поведение при записи в таком случае не требуется, так что
вместе с ним можно использовать обычный `Writer`.

### PyReader/PyWriter

Пара для сериализации-десериализации произвольных python-типов. Тип кодируется
атрибутом `py` у значения.

Поддержка дополнительных типов добавляется с помощью декораторов
`pywriter_handler`, `pyreader_scalar_handler`, `pyreader_list_handler`,
`pyreader_map_handler`.

### UnicodeReader

Декодирует все строки в юникод. Удобен при работе с `python3`, но может 
ухудшить производительность.
