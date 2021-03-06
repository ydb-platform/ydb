# BinaryJson Design Doc

## Introduction

BinaryJson is on-disk binary format for JSON. Its main characteristics are the following:
  - Access to values inside JSON document without document parsing;
  - Minimal effort to value deserialization.

## Main Idea

Let's separate storing values of JSON document and document's structure.
Document's structure would be represented as sequence of fixed size entries, each entry describes a node in the JSON document.
Simple type values would be stored inside these entries, complex type values would be stored in special indexes.
We build a dictionary of document's string values to operate string indexes instead of strings themselves.

## Data Structures

BinaryJson contains the following parts:

```
+--------+------+--------------+--------------+
| Header | Tree | String index | Number index |
+--------+------+--------------+--------------+
```

- `Header` - metadata about BinaryJson
- `Tree` - store documents structure
- `String index` - a place to store all string values
- `Number index` - a place to store all numbers

### Header

`Header ` хранит метаинформацию о BinaryJson документе.

Структура:

```
+----------------+-----------------------------+
| Version, 5 бит | String index offset, 27 бит |
+----------------+-----------------------------+
```

- `Version` - номер версии BinaryJson. Всегда равен `1`
- `String index offset` - сдвиг на начало `String index`

### Tree

Дерево JSON документа, где каждый узел представлен структурой `Entry`, `KeyEntry` или `Meta`.

#### Entry

`Entry` - это `uint32_t`, представляющий узел в дереве JSON. `Entry` может в зависимости от типа:
- Хранить значение в себе (для простых типов вроде `boolean` и `null`)
- Указывать на другой узел дерева (для массивов и объектов)
- Указывать на элемент в `String index` или `Number index` (для строк и чисел)

`Entry` имеет следующую структуру:
```
+-------------------+---------------+
| Entry type, 5 бит | Value, 27 бит |
+-------------------+---------------+
```

- `Entry type`. Задает тип значения:
  - `0` - bool значение `false`
  - `1` - bool значение `true`
  - `2` - значение `null`
  - `3` - строка
  - `4` - число
  - `5` - массив или объект
  - Остальные типы зарезервированы на будущее
- `Value`. В зависимости от типа:
  - Сдвиг указывающий на начало `SEntry` в `String index` (для строк)
  - Сдвиг указывающий на начало числа в `Number index` (для чисел)
  - Сдвиг указывающий на начало структуры `Meta` (для массивов и объектов)
  - Для остальных типов не определено

#### KeyEntry

`KeyEntry` - это `uint32_t`, являющийся сдвигом на начало `SEntry` в `String index`. Указывает на строку которая хранит ключ объекта

#### Meta

`Meta` - это `uint32_t` хранящий в себе тип контейнера (массив или объект) и его размер (длину массива или количество ключей в объекте)

`Meta` имеет следующую структуру:
```
+-----------------------+--------------+
| Container type, 5 бит | Size, 27 бит |
+-----------------------+--------------+
```

- `Container type`
  - `0` если `Meta` описывает массив
  - `1` если `Meta` описывает объект
  - `2` если `Meta` описывает top-level скалярное значение (см раздел "Сериализация")
  - Остальные типы зарезервированы на будущее
- `Size`. В зависимости от типа:
  - Количество элементов в массиве (для массивов)
  - Количество ключей в объекте (для объектов)
  - Для остальных типов не определено

#### Массивы

Массивы хранятся как последовательность `Entry` для каждого элемента массива.

Массивы имеют следующую структуру:
```
+------+---------+-----+------------+
| Meta | Entry 1 | ... | Entry Size |
+------+---------+-----+------------+
```

- `Meta`. Хранит количество элементов в массиве `Size`, имеет `Container type` равный `0` или `2`.
- Последовательность `Entry`, `Size` штук. `Entry` с номером `i` описывает `i`ый элемент массива.

#### Объекты

Объект хранится как массив ключей, сопровождаемый массивом значений. Пары ключ-значение (где ключ берется из первого массива, а значение из второго) отсортированы по ключу в возрастающем лексикографическом порядке

Объекты имеют следующую структуру:
```
+------+------------+-----+---------------+---------+-----+------------+
| Meta | KeyEntry 1 | ... | KeyEntry Size | Entry 1 | ... | Entry Size |
+------+------------+-----+---------------+---------+-----+------------+
```

- `Meta`. Хранит количество пар ключ-значение в объекте `Size`, имеет `Container type` равный `1`.
- Последовательность `KeyEntry`, `Size` штук. Это `KeyEntry` для ключа из каждой пары ключ-значение в объекте
- Последовательность `Entry`, `Size` штук. Это `Entry` для значения из каждой пары ключ-значение в объекте

### String index

`String index` - это место хранения всех строк (и ключей объектов и значений) из JSON документа. Все строки внутри `String index` уникальны.

Каждая строка описывается двумя структурами
- `SEntry` хранит местоположение строки в индексе
- `SData` хранит содержание строки

`String index` имеет следующую структуру:
```
+----------------+----------+-----+--------------+---------+-----+-------------+
| Count, 32 бита | SEntry 1 | ... | SEntry Count | SData 1 | ... | SData Count |
+----------------+----------+-----+--------------+---------+-----+-------------+
```

- `Count`. Это `uint32_t` хранящий количество строк в индексе
- `SEntry`, `Count` штук. `SEntry` для каждой строки в индексе
- `SData`, `Count` штук. `SData` для каждой строки в индексе

#### SEntry

`SEntry` - это `uint32_t`, хранящий сдвиг, который указывает на символ сразу после соответствующего `SData` для строки.

`SEntry` имеет следующую структуру:

```
+--------------------+-----------------------+
| String type, 5 бит | String offset, 27 бит |
+--------------------+-----------------------+
```

- `String type` зарезервирован на будущее
- `String offset` - сдвиг, указывающий на байт сразу после соответствующего `SData` для строки

#### SData

`SData` - это содержимое строки, включая символ `\0` в конце

### Number index

`Number index` - это место хранения всех чисел из JSON документа. Числа в BinaryJson представлены как double, поэтому это просто последовательность double.

## Сериализация

1. `Entry` элементов массива записываются в том порядке как они идут в JSON массиве.
2. `KeyEntry` и `Entry` для пар ключ-значение объектов записываются в возрастающем лексикографическом порядке ключей. Если есть несколько одинаковых ключей, берется значение первого из них.
4. Для представления JSON, состоящего из одного top-level скалярного (не массив и не объект) значения записывается массив из одного элемента. При этом в `Meta` устанавливается `Container type` равный `2`.
5. Все строки в `String index` должны быть уникальны и записываться в возрастающем лексикографическом порядке. Если несколько узлов JSON документа содержат равные строки, соответствующие им `Entry` должны указывать на один и тот же `SEntry`.

## Поиск значений

### Поиск в массиве по индексу

Дано:
- Сдвиг `start` на начало структуры `Meta` массива
- Индекс элемента массива `i`

Найти: Сдвиг на начало `Entry` для элемента массива с индексом `i`

Способ: `start + sizeof(Meta) + i * sizeof(Entry)`

Сложность: `O(1)`

### Поиск в объекте по ключу

Дано:
- Сдвиг `start` на начало структуры `Meta` массива
- Ключ `key`

Найти: Сдвиг на начало `Entry` для значения которое соответствует ключу `key` в объекте

Способ: С помощью бинарного поиска находим в объекте пару ключ-значение для строки `key`

Сложность: `O(log2(Size) + log2(Total count of strings in JSON))`

## Идеи

- Использовать NaN tagging.
  В double есть значение NaN. Оно устроено так, что умеет хранить 53 бита информации.
  Я предлагаю хранить все Entry как double.
Если значение NaN - читаем эти 53 бита информации, там храним тип ноды, сдвиг если нужен. Поскольку бита теперь 53, можем хранить большие сдвиги, большие JSONы.
Если значение не NaN - это нода с числом.
  Данный подход используется в [LuaJIT](http://lua-users.org/lists/lua-l/2009-11/msg00089.html). Статья с [подробностями](https://nikic.github.io/2012/02/02/Pointer-magic-for-efficient-dynamic-value-representations.html).
- Использовать perfect hashing для хранения объектов. Сейчас чтобы произвести lookup в JSON объекте по ключу необходимо сделать бинарный поиск в последовательности KeyEntry. Поскольку объекты в BinaryJson не изменяемы, можно было бы применить perfect hashing чтобы сразу вычислить сдвиг по которому находится значение

## Что нужно обсудить

- Структуры `Header`, `Entry`, `Meta` и `SEntry` резервируют 27 бит на хранение сдвигов. Это вводит ограничение на длину хранимого JSON значения: `2^27 = 128 Mb`. Мы не уверены достаточно ли это для всех пользовательских кейсов. Возможно, стоит рассмотреть увеличение размера этих структур (например, использовать `uint64_t`).
- Структуры `Entry`, `Meta` и `SEntry` резервируют по 5 бит на хранение типа, что даем нам 32 варианта типов. Мы не уверены будет ли этого достаточно для наших целей учитывая что некоторые типы могут иметь параметры (например что-то вроде Decimal). С учетом этого может не хватить расширения структур даже до `uint64_t`. Решением может быть хранить дополнительные `Entry` для некоторых типов, которые будут содержать необходимое описание. К сожалению, сейчас так сделать не получится так как формат полагается на то что все `Entry` имеют фиксированный размер. Возможно, нужно вводить отдельный индекс для сложных типов.
