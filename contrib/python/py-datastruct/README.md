# py-datastruct

This is a (relatively) simple, **pure-Python, no dependency** library, aiming to simplify parsing and building binary data structures. It uses **[`dataclasses`](https://docs.python.org/3/library/dataclasses.html)** as its main container type, and **[`struct`](https://docs.python.org/3/library/struct.html)-compatible format specifiers** for writing field definitions.

The way of composing structures is somewhat similar to (and inspired by) [Construct](https://github.com/construct/construct). While probably not as powerful, it should give more flexibility and control over the data, as well as **full IDE type hinting**.

## Installation

```shell
pip install py-datastruct
```

**NOTE:** `pip install datastruct` installs a **different package** by the same name!

## Breaking changes in v2.0.0

In DataStruct v2.0.0, the field type validation methods have been rewritten. They are now stricter, which means that the type hints will more closely represent the actual possible field values.

The new mechanism allows using **union types** (`int | float | bytes`), as well as **optional fields** (`MyStruct | None`) for fields, which wasn't previously possible. This is particularly useful for `cond()` and `switch()` fields.

Due to this new logic, there are a few **breaking changes** in v2.0.0:

<details>

<summary>`cond()` field default `if_not=` value is now `None` (breaking)</summary>

Previously, if the `cond()` field evaluated to `False`, its value was set to the wrapped field's default value (unless otherwise specified using the `if_not=` argument). For `subfield()`, the structure was created using default values.

Starting in v2.0.0, the field's value will be set to `None`. You can still use `if_not=` to change that (which you should do, if you rely on that field's default value in any way). This means, that the `cond()` field's type specification **must** now include `None` as one of its types.

If your structure was:

```python
@dataclass
class MyStruct(DataStruct):
    var: int = cond(lambda ctx: ctx.my_condition)(field("I"))
```

it must now be changed to either:

```python
@dataclass
class MyStruct(DataStruct):
    var: int | None = cond(lambda ctx: ctx.my_condition)(field("I"))
```

or, using `if_not=`:

```python
@dataclass
class MyStruct(DataStruct):
    var: int = cond(lambda ctx: ctx.my_condition, if_not=0)(field("I"))
```

The same change applies to `subfield()` wrapped in `cond()`.

Note that you **cannot** use `Any` for the `cond()` field, unless it wraps a `switch()` field (in which case the `cond()` field's type is transparently proxied to the `switch()` field).

</details>

<details>

<summary>`switch()` field's type must now account for all possible cases (possibly breaking)</summary>

Since union types are now usable with `switch()` fields, it is required to include all possible cases in the union.

The following structure demonstrates various ways of using the `switch()` field correctly:

```python
@dataclass
class MyStruct(DataStruct):
    var1: int = switch(False)(
        false=(int, field("H")),
        true=(int, field("I")),
    )
    var2: int | bool = switch(False)(
        false=(int, field("H")),
        true=(bool, field("B")),
    )
    var3: Any = switch(False)(
        false=(int, field("H")),
        true=(bool, field("B")),
    )
    var4: Any = switch(False)(
        false=(..., padding(4)),
        true=(int, field("I")),
    )
    var5: ... = switch(False)(
        false=(..., padding(4)),
        true=(int, field("I")),
    )
```

Note that the usage of Ellipsis (`...`) is restricted for `switch()` fields that have at least one case using the `...` type.

By the examples above, if you have a `switch()` field that uses union types, but doesn't list all possible cases, you should either add the missing types or change the type to `Any`.

If your `switch()` field uses `subfield()` cases, and you don't want to use the `Any` type, and you don't want to list all possible types, consider using a base class (this is now possible!), like this:

```python
@dataclass
class MyBase(DataStruct):
    # you can optionally add fields here - they will be *before* any subclass' fields
    pass

@dataclass
class MyStruct1(MyBase):  # note - no DataStruct here!
    pass

@dataclass
class MyStruct2(MyBase):
    pass

@dataclass
class MySwitchStruct(MyBase):
    var1: MyBase = switch(False)(
        false=(MyStruct1, subfield()),
        true=(MyStruct2, subfield()),
    )
```

</details>

<details>

<summary>The minimum required Python version is now 3.8</summary>

While it *may* still work on 3.7, it is recommended to use 3.10 at least. It *should* work on 3.8, but I can't reliably test everything on old versions to make sure it's fine.

</details>

## Examples

Before you read this "documentation", be aware that it is by no means complete, and will probably be not enough for you to understand everything you need.

Here are a few projects that are using `datastruct`:

- https://github.com/tuya-cloudcutter/cloudcutter-universal/blob/master/cloudcutter/modules/dhcp/structs.py
- https://github.com/tuya-cloudcutter/bk7231tools/blob/main/bk7231tools/analysis/kvstorage.py
- https://github.com/libretiny-eu/ltchiptool/blob/master/uf2tool/models/partition.py
- https://github.com/libretiny-eu/ltchiptool/blob/master/ltchiptool/soc/ambz2/util/models/images.py

If you want your project on this list, feel free to submit a PR.

## Usage

This simple example illustrates creating a 24-byte long structure, consisting of a 32-bit integer, an 8-byte 0xFF-filled padding, and a 12-byte `bytes` string.

```python
from hexdump import hexdump
from dataclasses import dataclass
from datastruct import DataStruct
from datastruct.fields import field, padding

@dataclass
class MyStruct(DataStruct):
    my_number: int = field("I", default=123)
    _1: ... = padding(8)
    my_binary: bytes = field("12s")

my_object = MyStruct(my_binary=b"Hello Python")
print(my_object)
# MyStruct(my_number=123, my_binary=b'Hello World!')

my_object = MyStruct(my_number=5, my_binary=b"Hello World!")
print(my_object)
# MyStruct(my_number=5, my_binary=b'Hello World!')

packed = my_object.pack()
hexdump(packed)
# 00000000: 05 00 00 00 FF FF FF FF  FF FF FF FF 48 65 6C 6C  ............Hell
# 00000010: 6F 20 57 6F 72 6C 64 21                           o World!

unpacked = MyStruct.unpack(packed)
print(unpacked)
# MyStruct(my_number=5, my_binary=b'Hello World!')
print(my_object == unpacked)
# True
```

You might also pass a stream (file/BytesIO/etc.) to `pack()` and `unpack()`. Otherwise, `pack()` will create a BytesIO stream and return its contents after packing; `unpack()` will accept a `bytes` object as its parameter.

`pack()` and `unpack()` also accept custom, keyword-only arguments, that are available in the Context, throughout the entire operation.

### Context

Upon starting a pack/unpack operation, a `Context` object is created. The context is a container scoped to the currently processed structure. It's composed of the following main elements:

- all values of the current structure - when packing; during unpacking, it contains all values of fields that were already processes (the context "grows")
- all keyword arguments passed to `pack()`/`unpack()` (for the root context only)
- all keyword arguments passed to `subfield()` (for child contexts only)
- `_: Context` - reference to the parent object's context (only when nesting `DataStruct`s)
- `self: Any` - the current datastruct - note that it's a `DataStruct` subclass when packing, and a `Container` when unpacking
- `G` - global context - general-purpose container that is not scoped to the current structure (it's identical for nested structs)
  - `io: IO[bytes]` - the stream being read from/written to
  - `packing: bool` - whether current operation is packing
  - `unpacking: bool` - whether current operation is unpacking
  - `root: Context` - context of the topmost structure
  - `tell: () -> int` - function returning the current position in the stream
  - `seek: (offset: int, whence: int) -> int` - function allowing to seek to an absolute offset
- `P` - local context - general-purpose container that is different for each nested struct
  - `config: Config` - current DataStruct's config 
  - `tell: () -> int` - function returning the current position in the current structure (in bytes)
  - `seek: (offset: int, whence: int) -> int` - function allowing to seek to an offset within the current structure
  - `skip: (length: int) -> int` - function allowing to skip `length` bytes
  - `i: int` - (for `repeat()` fields only) index of the current item of the list
  - `item: Any` - (for `repeat()` fields, in `last=` lambda only) item processed right before evaluation
  - `self: Any` - (packing only) value of the current field

The context is "general-purpose", meaning that the user can write custom values to it. All fields presented above can be accessed by lambda functions - see "Parameter evaluation".

### Parameter evaluation

Most field parameters support pack/unpack-time evaluation (which means they can e.g. depend on previously read fields). Lambda expressions are then given the current context, and expected to return a simple value, that would be statically valid in this parameter.

```python
an_unpredictable_field: int = field(lambda ctx: "I" if randint(1, 10) % 2 == 0 else "H")
```

### Ellipsis - special value

A special value of type `Ellipsis`/`...` is used in the library, to indicate something not having a type or a value. **It's not the same as `None`**. `built()` fields, for example, have `...` as value after creating the struct, but before packing it for the first time.

Special fields (like `padding()`, which don't have any value) must have `...` as their type hint.

### Variable-length fields

This is a simple example of using parameter evaluation to dynamically size a `bytes` string. Binary strings use the `<len>s` specifier, which can be omitted (simple `int` can be used instead). 

```python
@dataclass
class MyStruct(DataStruct):
    data_length: int = field("I")
    data: bytes = field(lambda ctx: ctx.data_length)
```

The user is still responsible for adjusting `data_length` after changing `data`. The `built()` field comes in handy here:

```python
@dataclass
class MyStruct(DataStruct):
    data_length: int = built("I", lambda ctx: len(ctx.data))
    data: bytes = field(lambda ctx: ctx.data_length)
```

When unpacking, the `data_length` field will be used to dynamically size the `data` field. When packing, `data_length` will always be recalculated based on what's in `data`.

### Wrapper fields - storing a list

Lists are also iterables, like `bytes`, but they store a number of items of a specific type. Thus, the `repeat()` field **wrapper** has to be used. 

**Wrapper fields** simply require calling them first with any used parameters, then with the "base" field.

```python
@dataclass
class MyStruct(DataStruct):
    item_count: int = built("H", lambda ctx: len(ctx.items))
    # This creates a list of 16-bit integers.
    # The list is empty by default.
    items: List[int] = repeat(lambda ctx: ctx.item_count)(field("H"))

my_object = MyStruct()
my_object.items = [0x5555, 0x4444, 0x3333, 0x2222]
my_object.item_count = 1  # this doesn't matter, as the field is rebuilt
packed = my_object.pack()
hexdump(packed)
# 00000000: 04 00 55 55 44 44 33 33  22 22
```

### Conditional fields

They're also wrapper fields - if the condition is not met, they act like as if the field didn't exist at all.

```python
@dataclass
class MyStruct(DataStruct):
    has_text: bool = field("?")
    text: str = cond(lambda ctx: ctx.has_text)(field("8s", default=""))

my_object = MyStruct.unpack(b"\x01HELOWRLD")
print(my_object)
# MyStruct(has_text=True, text='HELOWRLD')

my_object = MyStruct.unpack(b"\x00")
print(my_object)
# MyStruct(has_text=False, text='')
```

### Switch fields

Switch fields are like more powerful conditional fields. The following example reads an 8/16/32-bit number, depending on the prefixing length byte. If the length is not supported, it reads the value as `bytes` instead.

```python
number_length: int = field("B", default=1)
number: Union[int, bytes] = switch(lambda ctx: ctx.number_length)(
    _1=(int, field("B")),
    _2=(int, field("H")),
    _4=(int, field("I")),
    default=(bytes, field(lambda ctx: ctx.number_length)),
)
```

The values on the left (`_1`, `_2`, `_4`) are the **keys**. The key is picked depending on the key-lambda result (`ctx.number_length`). The value on the right is a tuple of the expected field type, and a `field()` specifier.

Since it's not possible to pass just `1` as a keyword argument, integers are looked up prefixed with an underscore as well. Enums are additionally looked up by their name and value, and booleans are looked up by **lowercase** `true`/`false`.

Note that you can pass (probably) any kind of field to the switch list.

## To be continued

## License

```
MIT License

Copyright (c) 2023 Kuba Szczodrzyński

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
