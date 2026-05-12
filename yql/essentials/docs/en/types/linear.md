## Linear Types

Most types in YQL are immutable, meaning expressions return new values ​​rather than modify existing ones.
This approach, common in functional programming languages, allows for more aggressive optimizations (such as removing common subexpressions or caching results).
However, in some scenarios, this can lead to slower query execution. In particular, when attempting to modify a single value in a list/dictionary, it is necessary to either return a full copy or use persistent data structures, which also incurs additional overhead.

Linear types offer a different approach: instead of reusing the immutable results of expression evaluation, reuse of a linear type value is prohibited. It is passed, as if in a relay race, from the point of creation to the point of consumption, where it is converted to a regular immutable value.

Linear types are available starting with version [2025.04](../changelog/2025.04.md).

Linear types are described by a single type parameter T and come in two varieties: statically verified Linear<T> and runtime-verified DynamicLinear<T>.
Statically verified types are more efficient but have composition limitations.

Typically, the Resource type is used in the T parameter of a linear type. In this case, user-defined functions (UDFs) can pass such data between each other with a guarantee of protection against reuse in the query, allowing for a more efficient implementation.

Linear types are not serializable—they cannot be read or written to tables, meaning they can only be used in the middle of expressions.

Functions that accept or return linear types are divided into three classes:
* If the linear type is contained in the result but not in the arguments, it is a generating function;
* If the linear type is contained in both the arguments and the result, it is a transforming function;
* If a linear type is contained in the arguments but not in the result, it is an consuming function.

A generating function must accept a dependent expression in at least one argument, since multiple independent values ​​of linear types can be constructed from the input arguments.
It is recommended to create and consume linear types within the [`Block`](../builtins/basic.md#block) function, which allows an anonymous dependent expression to be passed as a `lambda` argument.

### Rules for checking static linear types `Linear<T>`

* `Linear` types are checked as the last stage of optimization, and if the optimizer eliminates expression reuse, no error is raised.
* `Linear` cannot be an argument to `lambda`.
* `Linear` cannot be returned from `lambda`.
* `Linear` can be used either by itself or as a field in a `Struct/Tuple` (without nesting). Whenever possible, individual uses of `Struct/Tuple` fields are tracked using the field access operator (dot).

If you need to use values ​​of linear types within other container types (for example, in lists), you should use the `DynamicLinear` type.

### Rules for checking dynamic linear types `DynamicLinear<T>`

* The `FromDynamicLinear` function or user-defined functions (UDFs) may retrieve a value of this type, but only once, otherwise a query execution error will occur.
