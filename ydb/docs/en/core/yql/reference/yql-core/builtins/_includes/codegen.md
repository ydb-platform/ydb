# Functions for code generation

When running calculations, you can generate the code including [S-expressions](/docs/s_expressions) nodes. This uses a mechanism for packing the code in the [resource](../../types/special.md). After building the code, you can insert it into the main program using the [EvaluateCode](#evaluatecode) function. For debugging purposes, you can convert the code to a string using the [FormatCode](#formatcode) function.

Possible node types in S-expressions that can be used for code generation:

* An atom is an untyped string of zero or more characters.
* A list is a sequence of zero or more nodes. It corresponds to the `tuple` type in SQL.
* A call of a built-in function consists of a name expressed by an atom and a sequence of zero or more nodes that are arguments to this function.
* Lambda function declaration consists of declaring the names of arguments and a node that is the root of the body for this lambda function.
* The lambda function argument is a node that can only be used inside the body of the lambda function.
* World is a special node that labels I/O operations.

The S-expressions nodes form a directed graph. Atoms are always leaf nodes, because they cannot contain child nodes.

In the text representation, S-expressions have the following format:

* Atom: ```'"foo"```. The apostrophe character (') denotes quoting of the next line that is usually enclosed in quotation marks.
* List: ```'("foo" "bar")```. The apostrophe character (') denotes that there will be no function call in parentheses.
* Calling the built-in function: ```(foo "bar")```. The first item inside the brackets is the mandatory name of the function followed by the function arguments.
* Declaring a lambda function: ```(lambda '(x y) (+ x y))```. The `lambda` keyword is followed by a list of argument names and then by the body of the lambda function.
* The lambda function argument is ```x```. Unlike an atom, a string without an apostrophe character (') references a name in the current scope. When declaring a lambda function, the names of arguments are added to the body's visibility scope, and, if needed, the name is hidden from the global scope.
* The ```world```.

## FormatCode

Serializing the code as [S-expressions](/docs/s_expressions). The code must not contain free arguments of functions, hence, to serialize the lambda function code, you must pass it completely, avoiding passing individual expressions that might contain lambda function arguments.

**Examples:**

```yql
SELECT FormatCode(AtomCode("foo"));
-- (
-- (return '"foo")
-- )
```

## WorldCode

Build a code node with the `world` type.

**Examples:**

```yql
SELECT FormatCode(WorldCode());
-- (
-- (return world)
-- )
```

## AtomCode

Build a code node with the `atom` type from a string passed to the argument.

**Examples:**

```yql
SELECT FormatCode(AtomCode("foo"));
-- (
-- (return '"foo")
-- )
```

## ListCode

Build a code node with the `list` type from a set of nodes or lists of code nodes passed to arguments. In this case, lists of arguments are built in as separately listed code nodes.

**Examples:**

```yql
SELECT FormatCode(ListCode(
    AtomCode("foo"),
    AtomCode("bar")));
-- (
-- (return '('"foo" '"bar"))
-- );

SELECT FormatCode(ListCode(AsList(
    AtomCode("foo"),
    AtomCode("bar"))));
-- (
-- (return '('"foo" '"bar"))
-- )
```

## FuncCode

Build a code node with the `built-in function call` from a string with the function name and a set of nodes or lists of code nodes passed to arguments. In this case, lists of arguments are built in as separately listed code nodes.

**Examples:**

```yql
SELECT FormatCode(FuncCode(
    "Baz",
    AtomCode("foo"),
    AtomCode("bar")));
-- (
-- (return (Baz '"foo" '"bar"))
-- )

SELECT FormatCode(FuncCode(
    "Baz",
    AsList(
        AtomCode("foo"),
        AtomCode("bar"))));
-- (
-- (return (Baz '"foo" '"bar"))
-- )
```

## LambdaCode

You can build a code node with the `lambda function declaration` type from:

* [a Lambda function](../../syntax/expressions.md#lambda), if you know the number of arguments in advance. In this case, the nodes of the `argument` type will be passed as arguments to this lambda function.
* The number of arguments and a [lambda function](../../syntax/expressions.md#lambda) with one argument. In this case, a list of nodes of the `argument`type will be passed as an argument to this lambda function.

**Examples:**

```yql
SELECT FormatCode(LambdaCode(($x, $y) -> {
    RETURN FuncCode("+", $x, $y);
}));
-- (
-- (return (lambda '($1 $2) (+ $1 $2)))
-- )

SELECT FormatCode(LambdaCode(2, ($args) -> {
    RETURN FuncCode("*", Unwrap($args[0]), Unwrap($args[1]));
}));
-- (
-- (return (lambda '($1 $2) (* $1 $2)))
-- )
```

## EvaluateCode

Substituting the code node passed in the argument, into the main program code.

**Examples:**

```yql
SELECT EvaluateCode(FuncCode("Int32", AtomCode("1"))); -- 1

$lambda = EvaluateCode(LambdaCode(($x, $y) -> {
    RETURN FuncCode("+", $x, $y);
}));
SELECT $lambda(1, 2); -- 3
```

## ReprCode

Substituting the code node representing the result of evaluating an expression passed in the argument, into the main program.

**Examples:**

```yql
$add3 = EvaluateCode(LambdaCode(($x) -> {
    RETURN FuncCode("+", $x, ReprCode(1 + 2));
}));
SELECT $add3(1); -- 4
```

## QuoteCode

Substituting into the main program the code node that represents an expression or a [lambda function](../../syntax/expressions.md#lambda) passed in the argument. If free arguments of lambda functions were found during the substitution, they are calculated and substituted into the code as in the [ReprCode](#reprcode) function.

**Examples:**

```yql
$lambda = ($x, $y) -> { RETURN $x + $y };
$makeClosure = ($y) -> {
    RETURN EvaluateCode(LambdaCode(($x) -> {
        RETURN FuncCode("Apply", QuoteCode($lambda), $x, ReprCode($y))
    }))
};

$closure = $makeClosure(2);
SELECT $closure(1); -- 3
```

