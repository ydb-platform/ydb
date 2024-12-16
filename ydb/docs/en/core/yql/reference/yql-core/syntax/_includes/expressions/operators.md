## Operators

### Arithmetic operators {#math-operators}

The operators `+`, `-`, `*`, `/`, `%` are defined for [primitive data types](../../../types/primitive.md) that are variations of numbers.

For the Decimal data type, bankers rounding is used (to the nearest even integer).

#### Examples

```yql
SELECT 2 + 2;
```

```yql
SELECT 0.0 / 0.0;
```

### Comparison operators {#comparison-operators}

The operators `=`, `==`, `!=`, `<>`, `>`, `<` are defined for:

* Primitive data types except Yson and Json.
* Tuples and structures with the same set of fields. No order is defined for structures, but you can check for (non-)equality. Tuples are compared element-by-element left to right.

#### Examples

```yql
SELECT 2 > 1;
```

### Logical operators {#logic-operators}

Use the operators `AND`, `OR`, `XOR` for logical operations on Boolean values (`Bool`).

#### Examples*

```yql
SELECT 3 > 0 AND false;
```

### Bitwise operators {#bit-operators}

Bitwise operations on numbers:

* `&`, `|`, `^`: AND, OR, and XOR, respectively. Don't confuse bitwise operations with the related keywords. The keywords `AND`, `OR`, and `XOR` are used *for Boolean values only*, but not for numbers.
* ` ~ `: A negation.
* `<`, `>`: Left or right shifts.
* `|<`, `>|`: Circular left or right shifts.

#### Examples

```yql
SELECT
    key << 10 AS key,
    ~value AS value
FROM my_table;
```

### Precedence and associativity of operators {#operator-priority}

Operator precedence determines the order of evaluation of an expression that contains different operators.
For example, the expression `1 + 2 * 3` is evaluated as `1 + (2 * 3)` because the multiplication operator has a higher precedence than the addition operator.

Associativity determines the order of evaluating expressions containing operators of the same type.
For example, the expression `1 + 2 + 3` is evaluated as `(1 + 2) + 3` because the addition operator is left-associative.
On the other hand, the expression `a ?? b ?? c` is evaluated as `a ?? (b ?? c)` because the `??` operator is right-associative

The table below shows precedence and associativity of YQL operators.
The operators in the table are listed in descending order of precedence.

| Priority | Operator | Description | Associativity |
| --- | --- | --- | --- |
| 1 | `a[], a.foo, a()` | Accessing a container item, calling a function | Left |
| 2 | `+a, -a, ~a, NOT a` | Unary operators: plus, minus, bitwise and logical negation | Right |
| 3 | `a\|\|b` | [String concatenation](../../../syntax/expressions.md#concatenation) | Left |
| 4 | `a*b, a/b, a%b` | Multiplication, division, remainder of division | Left |
| 5 | `a+b, a-b` | Addition/Subtraction | Left |
| 6 | `a ?? b` | Operator notation for [NVL/COALESCE](../../../builtins/basic.md#coalesce) | Right |
| 7 | `a<b, a>b, a\|<b, a>\|b,` `a\|b, a^b, a&b` | Shift operators and logical bit operators | Left |
| 8 | `a<b, a=b, a=b, a>b` | Comparison | Left |
| 9 | `a IN b` | Occurrence of an element in a set | Left |
| 9 | `a==b, a=b, a!=b, a<>b,` `a is (not) distinct from b` | Comparison for (non-)equality | Left |
| 10 | `a XOR b` | Logical XOR | Left |
| 11 | `a AND b` | Logical AND | Left |
| 12 | `a OR b` | Logical OR | Left |

