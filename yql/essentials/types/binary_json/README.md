# BinaryJson Design Doc

## Introduction

BinaryJson is an on-disk binary format for JSON. Its main characteristics are the following:
  - Access to values inside a JSON document without document parsing;
  - Minimal effort for value deserialization.

## Main Idea

Let's separate storing values of the JSON document and the document's structure.
The document's structure would be represented as a sequence of fixed size entries, each entry describes a node in the JSON document.
Simple type values would be stored inside these entries, complex type values would be stored in special indexes.
We build a dictionary of the document's string values to operate string indexes instead of strings themselves.

## Data Structures

BinaryJson contains the following parts:

```
+--------+------+--------------+--------------+
| Header | Tree | String index | Number index |
+--------+------+--------------+--------------+
```

- `Header` - metadata about BinaryJson
- `Tree` - stores the document's structure
- `String index` - a place to store all string values
- `Number index` - a place to store all numbers

### Header

`Header` stores metadata about the BinaryJson document.

Structure:

```
+----------------+-----------------------------+
| Version, 5 bit | String index offset, 27 bit |
+----------------+-----------------------------+
```

- `Version` - BinaryJson version number. Always equal to `1`
- `String index offset` - offset to the beginning of `String index`

### Tree

The JSON document tree, where each node is represented by the structure `Entry`, `KeyEntry`, or `Meta`.

#### Entry

`Entry` is a `uint32_t` representing a node in the JSON tree. Depending on the type, `Entry` can:
- Store the value itself (for simple types like `boolean` and `null`)
- Point to another node in the tree (for arrays and objects)
- Point to an element in `String index` or `Number index` (for strings and numbers)

`Entry` has the following structure:
```
+-------------------+---------------+
| Entry type, 5 bit | Value, 27 bit |
+-------------------+---------------+
```

- `Entry type`. Specifies the value type:
  - `0` - bool value `false`
  - `1` - bool value `true`
  - `2` - value `null`
  - `3` - string
  - `4` - number
  - `5` - array or object
  - Other types are reserved for future use
- `Value`. Depending on the type:
  - Offset pointing to the start of `SEntry` in `String index` (for strings)
  - Offset pointing to the start of a number in `Number index` (for numbers)
  - Offset pointing to the start of the `Meta` structure (for arrays and objects)
  - Not defined for other types

#### KeyEntry

`KeyEntry` is a `uint32_t` which is an offset to the start of `SEntry` in `String index`. It points to the string which holds the object key.

#### Meta

`Meta` is a `uint32_t` storing the container type (array or object) and its size (the length of the array or the number of keys in the object).

`Meta` has the following structure:
```
+-----------------------+--------------+
| Container type, 5 bit | Size, 27 bit |
+-----------------------+--------------+
```

- `Container type`
  - `0` if `Meta` describes an array
  - `1` if `Meta` describes an object
  - `2` if `Meta` describes a top-level scalar value (see the section "Serialization")
  - Other types are reserved for future use
- `Size`. Depending on the type:
  - Number of elements in the array (for arrays)
  - Number of keys in the object (for objects)
  - Not defined for other types

#### Arrays

Arrays are stored as a sequence of `Entry` for each element.

Arrays have the following structure:
```
+------+---------+-----+------------+
| Meta | Entry 1 | ... | Entry Size |
+------+---------+-----+------------+
```

- `Meta`. Stores the number of elements in the array in `Size`, and has a `Container type` equal to `0` or `2`.
- Sequence of `Entry`, `Size` items. The `Entry` with index `i` describes the i-th element of the array.

#### Objects

An object is stored as an array of keys, accompanied by an array of values. Key-value pairs (where the key is taken from the first array and the value from the second) are sorted by key in ascending lexicographic order.

Objects have the following structure:
```
+------+------------+-----+---------------+---------+-----+------------+
| Meta | KeyEntry 1 | ... | KeyEntry Size | Entry 1 | ... | Entry Size |
+------+------------+-----+---------------+---------+-----+------------+
```

- `Meta`. Stores the number of key-value pairs in the object in `Size`, with `Container type` equal to `1`.
- Sequence of `KeyEntry`, `Size` items. These are `KeyEntry` for the key of each key-value pair in the object
- Sequence of `Entry`, `Size` items. These are `Entry` for the value of each key-value pair in the object

### String index

`String index` is where all strings (both object keys and values) from the JSON document are stored. All strings within the `String index` are unique.

Each string is described by two structures:
- `SEntry` stores the location of the string in the index
- `SData` stores the content of the string

`String index` has the following structure:
```
+----------------+----------+-----+--------------+---------+-----+-------------+
| Count, 32 bit  | SEntry 1 | ... | SEntry Count | SData 1 | ... | SData Count |
+----------------+----------+-----+--------------+---------+-----+-------------+
```

- `Count`. This is a `uint32_t` storing the number of strings in the index
- `SEntry`, `Count` items. `SEntry` for each string in the index
- `SData`, `Count` items. `SData` for each string in the index

#### SEntry

`SEntry` is a `uint32_t`, storing an offset which points to the character immediately after the corresponding `SData` for the string.

`SEntry` has the following structure:

```
+--------------------+-----------------------+
| String type, 5 bit | String offset, 27 bit |
+--------------------+-----------------------+
```

- `String type` is reserved for future use
- `String offset` - offset pointing to the byte immediately after the corresponding `SData` for the string

#### SData

`SData` is the contents of the string, including the `\0` character at the end

### Number index

`Number index` is the place where all numbers from the JSON document are stored. Numbers in BinaryJson are represented as double, so this is simply a sequence of doubles.

## Serialization

1. `Entry` elements of arrays are written in the same order as they appear in the JSON array.
2. `KeyEntry` and `Entry` for key-value pairs of objects are written in ascending lexicographical order of keys. If there are multiple identical keys, the value of the first is taken.
4. To represent a JSON consisting of a single top-level scalar (not array or object) value, an array of one element is written. In this case, the `Meta` `Container type` is set to `2`.
5. All strings in the `String index` must be unique and written in ascending lexicographical order. If multiple nodes of the JSON document contain equal strings, the corresponding `Entry` should point to the same `SEntry`.

## Value Lookup

### Array element search by index

Given:
- Offset `start` to the beginning of the array's `Meta` structure
- Index of the array element `i`

Find: Offset to the beginning of the `Entry` for the array element by index `i`

Method: `start + sizeof(Meta) + i * sizeof(Entry)`

Complexity: `O(1)`

### Object lookup by key

Given:
- Offset `start` to the beginning of the object's `Meta` structure
- Key `key`

Find: Offset to the beginning of the `Entry` for the value that corresponds to the key `key` in the object

Method: Using binary search, find the key-value pair for the string `key` in the object

Complexity: `O(log2(Size) + log2(Total count of strings in JSON))`

## Ideas

- Use NaN tagging.
  Double has a NaN value. It is organized in such a way that it can store 53 bits of information.
  I propose to store all Entry as double.
If the value is NaN - we read these 53 bits of information, storing node type, offset if needed. Since there are now 53 bits, we can store large offsets, large JSONs.
If the value is not NaN - it's a node with a number.
  This approach is used in [LuaJIT](http://lua-users.org/lists/lua-l/2009-11/msg00089.html). Article with [details](https://nikic.github.io/2012/02/02/Pointer-magic-for-efficient-dynamic-value-representations.html).
- Use perfect hashing for storing objects. Currently, to perform a lookup in a JSON object by key, you need to do a binary search in the KeyEntry sequence. Since objects in BinaryJson are immutable, perfect hashing could be applied to immediately calculate the offset at which the value is located.

## What needs to be discussed

- Structures `Header`, `Entry`, `Meta`, and `SEntry` reserve 27 bits for storing offsets. This imposes a limitation on the length of a stored JSON value: `2^27 = 128 Mb`. We are not sure if this is sufficient for all user cases. Perhaps, it makes sense to consider increasing the size of these structures (for example, using `uint64_t`).
- Structures `Entry`, `Meta`, and `SEntry` reserve 5 bits each for storing the type, giving us 32 type variants. We are not sure whether this will be enough for our purposes, considering that some types may have parameters (e.g., something like Decimal). Taking this into account, even expanding the structures to `uint64_t` may not be sufficient. A solution may be to store additional `Entry` for some types, which will contain the necessary description. Unfortunately, this cannot currently be done because the format relies on all `Entry` having a fixed size. Perhaps a separate index for complex types should be introduced.
