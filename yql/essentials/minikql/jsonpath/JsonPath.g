grammar JsonPath;

options {
    language = Cpp;
    memoize = true;
}

// Root rule. Input is a mode followed by jsonpath expression
jsonpath: (STRICT | LAX)? expr EOF;

// Generic jsonpath expression
expr: or_expr;

// Arithmetic and boolean operations
// Operator precedence:
// 1. Unary plus, minus and logical not
// 2. Multiplication, division, modulus
// 3. Addition, substraction
// 4. Compare operators (<, <=, >, >=)
// 5. Equality operators (==, !=, <>)
// 6. Logical and
// 7. Logical or
// NOTE: We execute JsonPath using bottom up approach. Thus
// operations with higher precedence must be located "deeper" inside AST
or_expr: and_expr (OR and_expr)*;
and_expr: equal_expr (AND equal_expr)*;
equal_expr: compare_expr ((EQUAL | NOT_EQUAL | NOT_EQUAL_SQL) compare_expr)?;
compare_expr: add_expr ((LESS | LESS_EQUAL | GREATER | GREATER_EQUAL) add_expr)?;
add_expr: mul_expr ((PLUS | MINUS) mul_expr)*;
mul_expr: unary_expr ((ASTERISK | SLASH | PERCENT) unary_expr)*;
unary_expr: (PLUS | MINUS | NOT)? predicate_expr;

// Predicates, `"string" starts with "str"`
// NOTE: `is unknown` predicate is defined separately in primary rule. This is done
// because if we add it as an alternative to predicate_expr, ANTLR would need backtacking.
// For example it would not be possible to tell if expression like `( ... ) is unknown` is
// related to `starts with` (and braces are part of plain_expr rule) or it is related to
// `is unknown` rule (and braces are not included in plain_expr).
predicate_expr:
    (plain_expr (starts_with_expr | like_regex_expr)?)
    | (EXISTS LBRACE expr RBRACE);

starts_with_expr: STARTS WITH plain_expr;
like_regex_expr: LIKE_REGEX STRING_VALUE (FLAG STRING_VALUE)?;

// Plain expression serves as an argument to binary and unary operators
plain_expr: accessor_expr;

accessor_expr: primary accessor_op*;
accessor_op: member_accessor | wildcard_member_accessor | array_accessor | wildcard_array_accessor | filter | method;

// Member acceccors, `$.key` and `$.*`
member_accessor: DOT (identifier | STRING_VALUE);
wildcard_member_accessor: DOT ASTERISK;

// Array accessors, `$[0, 1 to 3, last]` and `$[*]`
array_subscript: expr (TO expr)?;
array_accessor: LBRACE_SQUARE array_subscript (COMMA array_subscript)* RBRACE_SQUARE;
wildcard_array_accessor: LBRACE_SQUARE ASTERISK RBRACE_SQUARE;

// Filters, `$ ? (@.age >= 18)`
filter: QUESTION LBRACE expr RBRACE;

// Methods, `$.abs().ceiling()`
method: DOT (ABS_METHOD | FLOOR_METHOD | CEILING_METHOD | DOUBLE_METHOD | TYPE_METHOD | SIZE_METHOD | KEYVALUE_METHOD) LBRACE RBRACE;

// Primaries are objects to perform operations on:
// 1. All literals:
//   - Numbers, `1.23e-5`
//   - Bool, `false` and `true`
//   - Null, `null`
//   - Strings, `"привет"`, `\r\n\t`
// 2. Current object, `$`
// 3. Current filtering object, `@`
// 4. Variables, `$my_cool_variable`
// 5. Last array index, `last`
// 6. Parenthesized jsonpath expression, `($.key + $[0])`
primary:
    NUMBER
    | DOLLAR
    | LAST
    | (LBRACE expr RBRACE (IS UNKNOWN)?)
    | VARIABLE
    | TRUE
    | FALSE
    | NULL
    | STRING_VALUE
    | AT;

// Identifier for member accessors and variable names, `$.key` and `$variable_name`
// JsonPath supports using keywords as identifiers. We need to mention keywords in
// identifer rule because otherwise ANTLR will treat them as a separate token.
// For instance input `$.to` without this modification will be treated as
// `DOLLAR DOT TO`, not `DOLLAR DOT IDENTIFIER`
identifier: IDENTIFIER | keyword;

keyword:
    ABS_METHOD
    | CEILING_METHOD
    | DOUBLE_METHOD
    | EXISTS
    | FALSE
    | FLAG
    | FLOOR_METHOD
    | IS
    | KEYVALUE_METHOD
    | LAST
    | LAX
    | LIKE_REGEX
    | NULL
    | SIZE_METHOD
    | STARTS
    | STRICT
    | TO
    | TRUE
    | TYPE_METHOD
    | UNKNOWN
    | WITH;

//
// Lexer
//

AND:           '&&';
ASTERISK:      '*';
AT:            '@';
BACKSLASH:     '\\';
COMMA:         ',';
DOLLAR:        '$';
DOT:           '.';
EQUAL:         '==';
GREATER_EQUAL: '>=';
GREATER:       '>';
LBRACE_SQUARE: '[';
LBRACE:        '(';
LESS_EQUAL:    '<=';
LESS:          '<';
MINUS:         '-';
NOT_EQUAL_SQL: '<>';
NOT_EQUAL:     '!=';
NOT:           '!';
OR:            '||';
PERCENT:       '%';
PLUS:          '+';
QUESTION:      '?';
QUOTE_DOUBLE:  '"';
QUOTE_SINGLE:  '\'';
RBRACE_SQUARE: ']';
RBRACE:        ')';
SLASH:         '/';
UNDERSCORE:    '_';

// Keywords
ABS_METHOD: 'abs';
CEILING_METHOD: 'ceiling';
DOUBLE_METHOD: 'double';
EXISTS: 'exists';
FALSE: 'false';
FLAG: 'flag';
FLOOR_METHOD: 'floor';
IS: 'is';
KEYVALUE_METHOD: 'keyvalue';
LAST: 'last';
LAX: 'lax';
LIKE_REGEX: 'like_regex';
NULL: 'null';
SIZE_METHOD: 'size';
STARTS: 'starts';
STRICT: 'strict';
TO: 'to';
TRUE: 'true';
TYPE_METHOD: 'type';
UNKNOWN: 'unknown';
WITH: 'with';

// String literal
fragment STRING_CORE_SINGLE: ( ~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .) )*;
fragment STRING_CORE_DOUBLE: ( ~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .) )*;
fragment STRING_SINGLE: (QUOTE_SINGLE STRING_CORE_SINGLE QUOTE_SINGLE);
fragment STRING_DOUBLE: (QUOTE_DOUBLE STRING_CORE_DOUBLE QUOTE_DOUBLE);

STRING_VALUE: (STRING_SINGLE | STRING_DOUBLE);

// Number literal
fragment DIGIT: '0'..'9';
fragment DIGITS: DIGIT+;
fragment REAL_PART: DOT DIGITS;
fragment EXP_PART: ('e' | 'E') (PLUS | MINUS)? DIGITS;

NUMBER: DIGITS REAL_PART? EXP_PART?;

// Javascript identifier
fragment ID_START: ('a'..'z' | 'A'..'Z' | UNDERSCORE);
fragment ID_CORE: (ID_START | DIGIT | DOLLAR);

IDENTIFIER: ID_START (ID_CORE)*;

// Jsonpath variable
VARIABLE: DOLLAR (ID_CORE)*;

WS: (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;};
// FIXME: WS and COMMENT tokens are currently required.
// FIXME: Since there are no comments in JSONPATH, we split whitespace characters between WS and COMMENT
COMMENT: ('\u000C') {$channel=HIDDEN;};
