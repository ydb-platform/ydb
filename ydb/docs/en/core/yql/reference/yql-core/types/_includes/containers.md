# Containers

YQL supports container types to define complex data structures organized in various ways.
Values of container types can be passed to YQL queries as input parameters or returned from YQL queries as columns of the set of results.
Container types can't be used as column data types for {{ ydb-short-name }} tables.

| Type | Declaration,<br/>example | Description |
| ------------ | ---------------- | ------------- |
| List | `List<Type>`,<br/>`List<Int32>` | A variable-length sequence consisting of same-type elements. |
| Dictionary | `Dict<KeyType, ValueType>`,<br/>`Dict<String,Int32>` | Set of key-value pairs with a fixed type of keys and values. |
| Set | `Set<KeyType>`,<br/>`Set<String>` | A set of elements with a fixed type is a special case of a dictionary with the `Void` value type. |
| Tuple | `Tuple<Type1, ..., TypeN>`,<br/>`Tuple<Int32,Double>` | Set of unnamed fixed-length elements with types specified for all elements. |
| Structure | `Struct<Name1:Type1, ..., NameN:TypeN>`,<br/> `Struct<Name:String,Age:Int32>` | A set of named fields with specified value types, fixed at query start (must be data-independent). |
| Stream | `Stream<Type>`,<br/> `Stream<Int32>` | Single-pass iterator by same-type values, not serializable |
| Variant on tuple | `Variant<Type1, Type2>`,<br/> `Variant<Int32,String>` | A tuple known to have exactly one element filled |
| Variant on structure | `Variant<Name1:Type1, Name2:Type2>`,<br/>`Variant<value:Int32,error:String>` | A structure known to have exactly one element filled |
| Enumeration | `Enum<Name1, Name2>`,<br/>`Enum<value,error>` | A container with exactly one enumeration element selected and defined only by its name. |

If necessary, you can nest containers in any combination, for example, `List<Tuple<Int32,Int32>>`.

In certain contexts, [optional values](../optional.md) can also be considered a container type (`Optional<Type>`) that behaves like a list of length 0 or 1.

To create literals of list containers, dictionary containers, set containers, tuple containers, or structure containers, you can use the [operator notation](../../builtins/basic.md#containerliteral).
To create a variant literal over a tuple or structure, use the function [Variant](../../builtins/basic.md#variant).
To create an enumeration literal, use the function [Enum](../../builtins/basic.md#enum).

To access the container elements, use a [dot or square brackets](../../syntax/expressions.md#items-access), depending on the container type.
