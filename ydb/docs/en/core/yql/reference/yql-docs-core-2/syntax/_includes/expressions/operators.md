## Operators

### Arithmetic operators {#math-operators}

The operators `+`, `-`, `*`, `/`, `%` are defined for [primitive data types](../../../types/primitive.md) that are variations of numbers.

For the Decimal data type, bankers rounding is used (to the nearest even integer).

**Examples**

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

**Examples**

```yql
SELECT 2 > 1;
```

### Logical operators {#logic-operators}

Use the operators `AND`, `OR`, `XOR` for logical operations on Boolean values (`Bool`).

**Examples**

```yql
SELECT 3 > 0 AND false;
```

### Bitwise operators {#bit-operators}

Bitwise operations on numbers:

* `&`, `|`, `^`: AND, OR, and XOR, respectively. Don't confuse bitwise operations with the related keywords. The keywords `AND`, `OR`, and `XOR` are used * for Boolean values only*, but not for numbers.
* ` ~ `: A negation.
* `<`, `>`: Left or right shifts.
* `|<`, `>|`: Circular left or right shifts.

**Examples**

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
| 1 | <code>a[], a.foo, a()</code> | Accessing a container item, calling a function | Left |
| 2 | <code>+a, -a, ~a, NOT a</code> | Unary operators: plus, minus, bitwise and logical negation | Right |
| 3 | <code>a &#124;&#124; b</code> | [String concatenation](#concatenation) | Left |
| 4 | <code>a*b, a/b, a%b</code> | Multiplication, division, remainder of division | Left |
| 5 | <code>a+b, a-b</code> | Addition/Subtraction | Left |
| 6 | <code>a ?? b</code> | Operator notation for [NVL/COALESCE](../../../builtins/basic.md#coalesce) | Right |
| 7 | <code>a<b, a>b, a&#124;<b, a>&#124;b,</code> <code>a&#124;b, a^b, a&b</code> | Shift operators and logical bit operators | Left |
| 8 | <code>a<b, a=b, a=b, a>b</code> | Comparison | Left |
| 9 | <code>a IN b</code> | Occurrence of an element in a set | Left |
| 9 | <code>a==b, a=b, a!=b, a<>b,</code> <code>a is (not) distinct from b</code> | Comparison for (non-)equality | Left |
| 10 | <code>a XOR b</code> | Logical XOR | Left |
| 11 | <code>a AND b</code> | Logical AND | Left |
| 12 | <code>a OR b</code> | Logical OR | Left |

