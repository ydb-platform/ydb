## 9.1. Logical Operators {#logical-operators}

The usual logical operators are available:

* boolean AND boolean → boolean
* boolean OR boolean → boolean
* NOT boolean → boolean

## 9.2. Comparison Functions and Operators {#comparison-functions}


The usual comparison operators are available, as shown in Table 9.1.

Table 9.1. Comparison Operators

#|
|| **Operator**| **Description** ||
|| datatype < datatype → boolean | Less than ||
|| datatype > datatype → boolean | Greater than ||
|| datatype <= datatype → boolean | Less than or equal to ||
|| datatype >= datatype → boolean | Greater than or equal to ||
|| datatype = datatype → boolean | Equal ||
|| datatype <> datatype → boolean | Not equal ||
|| datatype != datatype → boolean | Not equal ||
|#

There are also some comparison predicates, as shown in Table 9.2. These behave much like operators, but have special syntax mandated by the SQL standard.

#|
|| Predicate | Description | Example(s) ||
|| datatype BETWEEN datatype AND datatype → boolean | Between (inclusive of the range endpoints). |
```sql
2 BETWEEN 1 AND 3 → true
2 BETWEEN 3 AND 1 → false
```||
|| datatype NOT BETWEEN datatype AND datatype → boolean | Not between (the negation of BETWEEN). | 
```sql
2 NOT BETWEEN 1 AND 3 → false
```||
|| datatype BETWEEN SYMMETRIC datatype AND datatype → boolean | Between, after sorting the two endpoint values. |
```sql
2 BETWEEN SYMMETRIC 3 AND 1 → true
```||
||datatype NOT BETWEEN SYMMETRIC datatype AND datatype → boolean | Not between, after sorting the two endpoint values. | 
```sql
2 NOT BETWEEN SYMMETRIC 3 AND 1 → false
```||
||datatype IS DISTINCT FROM datatype → boolean | Not equal, treating null as a comparable value. (NOT SUPPORTED) |
```sql
#1 IS DISTINCT FROM NULL → true
#NULL IS DISTINCT FROM NULL → false
```||
||datatype IS NOT DISTINCT FROM datatype → boolean | Equal, treating null as a comparable value. (NOT SUPPORTED)|
```sql
#1 IS NOT DISTINCT FROM NULL → false
#NULL IS NOT DISTINCT FROM NULL → true
```||
||datatype IS NULL → boolean | Test whether value is null. |
```sql
1.5 IS NULL → false
```||
||datatype IS NOT NULL → boolean | Test whether value is not null. |
```sql
'null' IS NOT NULL → true
```||
||datatype ISNULL → boolean | Test whether value is null (nonstandard syntax) |
```sql
1.5 ISNULL → false
```||
||datatype NOTNULL → boolean | Test whether value is not null (nonstandard syntax) |
```sql
1.5 NOTNULL → true
```||
||boolean IS TRUE → boolean | Test whether boolean expression yields true. (NOT SUPPORTED)|
```sql
true IS TRUE → true
NULL::boolean IS TRUE → false
```||
|#

## 9.3. Mathematical Functions and Operators {#mathematical-functions}

Mathematical operators are provided for many PostgreSQL types. For types without standard mathematical conventions (e.g., date/time types) we describe the actual behavior in subsequent sections.

Table 9.4 shows the mathematical operators that are available for the standard numeric types. Unless otherwise noted, operators shown as accepting numeric_type are available for all the types smallint, integer, bigint, numeric, real, and double precision. Operators shown as accepting integral_type are available for the types smallint, integer, and bigint. Except where noted, each form of an operator returns the same data type as its argument(s). Calls involving multiple argument data types, such as integer + numeric, are resolved by using the type appearing later in these lists.

Table 9.4. Mathematical Operators

#|
||Operator|Description|Example(s)||
||numeric_type + numeric_type → numeric_type | Addition |
```sql
2 + 3 → 5
```||
|| + numeric_type → numeric_type | Unary plus (no operation) |
```sql
+ 3.5 → 3.5
```||
||numeric_type - numeric_type → numeric_type | Subtraction |
```sql
2 - 3 → -1
```||
|| - numeric_type → numeric_type | Negation |
```sql
- (-4) → 4
```||
||numeric_type * numeric_type → numeric_type | Multiplication |
```sql
2 * 3 → 6
```||
||numeric_type / numeric_type → numeric_type|Division (for integral types, division truncates the result towards zero)|
```sql
5.0 / 2 → 2.5000000000000000
5 / 2 → 2
(-5) / 2 → -2
```||
||numeric_type % numeric_type → numeric_type|Modulo (remainder); available for smallint, integer, bigint, and numeric|
```sql
5 % 4 → 1
```||
||numeric ^ numeric → numeric  
double precision ^ double precision → double precision|

Exponentiation.  
Unlike typical mathematical practice, multiple uses of ^ will associate left to right by default.|
```sql
2 ^ 3 → 8
2 ^ 3 ^ 3 → 512
2 ^ (3 ^ 3) → 134217728
```||
|| \|\/ double precision → double precision|Square root|
```sql
|/ 25.0 → 5
```||
|| \|\|\/ double precision → double precision|Cube root|
```sql
||/ 64.0 → 4
```||
||@ numeric_type → numeric_type | Absolute value |
```sql
@ -5.0 → 5.0
```||
||integral_type & integral_type → integral_type | Bitwise AND |
```sql
91 & 15 → 11
```||
||integral_type \| integral_type → integral_type | Bitwise OR |
```sql
32 | 3 → 35
```||
||integral_type # integral_type → integral_type | Bitwise exclusive OR |
```sql
17 # 5 → 20
```||
||~ integral_type → integral_type | Bitwise NOT |
```sql
~1 → -2
```||
||integral_type << integer → integral_type | Bitwise shift left |
```sql
1 << 4 → 16
```||
||integral_type >> integer → integral_type | Bitwise shift right |
```sql
8 >> 2 → 2
```||
|#

Table 9.5 shows the available mathematical functions. Many of these functions are provided in multiple forms with different argument types. Except where noted, any given form of a function returns the same data type as its argument(s); cross-type cases are resolved in the same way as explained above for operators. The functions working with double precision data are mostly implemented on top of the host system's C library; accuracy and behavior in boundary cases can therefore vary depending on the host system.

Table 9.5. Mathematical Functions

#|
||Function|Description|Example(s)||
||abs ( numeric_type ) → numeric_type | Absolute value |
```sql
abs(-17.4) → 17.4
```||
||cbrt ( double precision ) → double precision | Cube root |
```sql
cbrt(64.0) → 4
```||
||ceil ( numeric ) → numeric  
ceil ( double precision ) → double precision|Nearest integer greater than or equal to argument|
```sql
ceil(42.2) → 43
ceil(-42.8) → -42
```||
||ceiling ( numeric ) → numeric  
ceiling ( double precision ) → double precision|Nearest integer greater than or equal to argument(same as ceil)|
```sql
ceiling(95.3) → 96
```||
||degrees ( double precision ) → double precision|Converts radians to degrees|
```sql
degrees(0.5) → 28.64788975654116
```||
||div ( y numeric, x numeric ) → numeric|Integer quotient of y/x (truncates towards zero)|
```sql
div(9, 4) → 2
```||
||exp ( numeric ) → numeric  
exp ( double precision ) → double precision|Exponential (e raised to the given power)|
```sql
exp(1.0) → 2.7182818284590452
```||
||factorial ( bigint ) → numeric|Factorial|
```sql
factorial(5) → 120
```||
||floor ( numeric ) → numeric  
floor ( double precision ) → double precision|Nearest integer less than or equal to argument|
```sql
floor(42.8) → 42
floor(-42.8) → -43
```||
||gcd ( numeric_type, numeric_type ) → numeric_type|
Greatest common divisor (the largest positive number that divides both inputs with no remainder); returns 0 if both inputs are zero; available for integer, bigint, and numeric|
```sql
gcd(1071, 462) → 21
```||
||lcm ( numeric_type, numeric_type ) → numeric_type|
Least common multiple (the smallest strictly positive number that is an integral multiple of both inputs); returns 0 if either input is zero; available for integer, bigint, and numeric|
```sql
lcm(1071, 462) → 23562
```||
||ln ( numeric ) → numeric  
ln ( double precision ) → double precision|Natural logarithm|
```sql
ln(2.0) → 0.6931471805599453
```||
||log ( numeric ) → numeric  
log ( double precision ) → double precision|Base 10 logarithm|
```sql
log(100) → 2
```||
||log10 ( numeric ) → numeric  
log10 ( double precision ) → double precision|Base 10 logarithm (same as log)|
```sql
log10(1000) → 3
```||
||log ( b numeric, x numeric ) → numeric|Logarithm of x to base b|
```sql
log(2.0, 64.0) → 6.0000000000000000
```||
||min_scale ( numeric ) → integer|Minimum scale (number of fractional decimal digits) needed to represent the supplied value precisely|
```sql
min_scale(8.4100) → 2
```||
||mod ( y numeric_type, x numeric_type ) → numeric_type|
Remainder of y/x; available for smallint, integer, bigint, and numeric|
```sql
mod(9, 4) → 1
```||
||pi ( ) → double precision|Approximate value of π|
```sql
pi() → 3.141592653589793
```||
||power ( a numeric, b numeric ) → numeric  
power ( a double precision, b double precision ) → double precision|a raised to the power of b|
```sql
power(9, 3) → 729
```||
||radians ( double precision ) → double precision|Converts degrees to radians|
```sql
radians(45.0) → 0.7853981633974483
```||
||round ( numeric ) → numeric  
round ( double precision ) → double precision|Rounds to nearest integer. For numeric, ties are broken by rounding away from zero. For double precision, the tie-breaking behavior is platform dependent, but “round to nearest even” is the most common rule.|
```sql
round(42.4) → 42
```||
||round ( v numeric, s integer ) → numeric|
Rounds v to s decimal places. Ties are broken by rounding away from zero.|
```sql
round(42.4382, 2) → 42.44
round(1234.56, -1) → 1230
```||
||scale ( numeric ) → integer|Scale of the argument (the number of decimal digits in the fractional part)|
```sql
scale(8.4100) → 4
```||
||sign ( numeric ) → numeric  
sign ( double precision ) → double precision|
Sign of the argument (-1, 0, or +1)|
```sql
sign(-8.4) → -1
```||
||sqrt ( numeric ) → numeric  
sqrt ( double precision ) → double precision|
Square root|
```sql
sqrt(2) → 1.4142135623730951
```||
||trim_scale ( numeric ) → numeric|
Reduces the value's scale (number of fractional decimal digits) by removing trailing zeroes|
```sql
trim_scale(8.4100) → 8.41
```||
||trunc ( numeric ) → numeric  
trunc ( double precision ) → double precision|
Truncates to integer (towards zero)|
```sql
trunc(42.8) → 42
trunc(-42.8) → -42
```||
||trunc ( v numeric, s integer ) → numeric|Truncates v to s decimal places|
```sql
trunc(42.4382, 2) → 42.43
```||
||width_bucket ( operand numeric, low numeric, high numeric, count integer ) → integer  
width_bucket ( operand double precision, low double precision, high double precision, count integer ) → integer|
Returns the number of the bucket in which operand falls in a histogram having count equal-width buckets spanning the range low to high. Returns 0 or count+1 for an input outside that range.|
```sql
width_bucket(5.35, 0.024, 10.06, 5) → 3
```||
||width_bucket ( operand anycompatible, thresholds anycompatiblearray ) → integer (NOT SUPPORTED)|
Returns the number of the bucket in which operand falls given an array listing the lower bounds of the buckets. Returns 0 for an input less than the first lower bound. operand and the array elements can be of any type having standard comparison operators. The thresholds array must be sorted, smallest first, or unexpected results will be obtained.|
```sql
#width_bucket(now(), array['yesterday', 'today', 'tomorrow']::timestamptz[]) → 2
```||
|#

Table 9.6. Random Functions

#|
||Function|Description|Example(s)||
||random ( ) → double precision|Returns a random value in the range 0.0 <= x < 1.0| ||
||setseed ( double precision ) → void|
Sets the seed for subsequent random() calls; argument must be between -1.0 and 1.0, inclusive
(NOT SUPPORTED)|
|#

Table 9.7 shows the available trigonometric functions. Each of these functions comes in two variants, one that measures angles in radians and one that measures angles in degrees.

Table 9.7. Trigonometric Functions

#|
||Function|Description|Example(s)||
||acos ( double precision ) → double precision|Inverse cosine, result in radians|
```sql
acos(1) → 0
```||
||acosd ( double precision ) → double precision|Inverse cosine, result in degrees|
```sql
acosd(0.5) → 60
```||
||asin ( double precision ) → double precision|Inverse sine, result in radians|
```sql
asin(1) → 1.5707963267948966
```||
||asind ( double precision ) → double precision|Inverse sine, result in degrees|
```sql
asind(0.5) → 30
```||
||atan ( double precision ) → double precision|Inverse tangent, result in radians|
```sql
atan(1) → 0.7853981633974483
```||
||atand ( double precision ) → double precision|Inverse tangent, result in degrees|
```sql
atand(1) → 45
```||
||atan2 ( y double precision, x double precision ) → double precision|Inverse tangent of y/x, result in radians|
```sql
atan2(1, 0) → 1.5707963267948966
```||
||atan2d ( y double precision, x double precision ) → double precision|Inverse tangent of y/x, result in degrees|
```sql
atan2d(1, 0) → 90
```||
||cos ( double precision ) → double precision|Cosine, argument in radians|
```sql
cos(0) → 1
```||
||cosd ( double precision ) → double precision|Cosine, argument in degrees|
```sql
cosd(60) → 0.5
```||
||cot ( double precision ) → double precision|Cotangent, argument in radians|
```sql
cot(0.5) → 1.830487721712452
```||
||cotd ( double precision ) → double precision|Cotangent, argument in degrees|
```sql
cotd(45) → 1
```||
||sin ( double precision ) → double precision|Sine, argument in radians|
```sql
sin(1) → 0.8414709848078965
```||
||sind ( double precision ) → double precision|Sine, argument in degrees|
```sql
sind(30) → 0.5
```||
||tan ( double precision ) → double precision|Tangent, argument in radians|
```sql
tan(1) → 1.5574077246549023
```||
||tand ( double precision ) → double precision|Tangent, argument in degrees|
```sql
tand(45) → 1
```||
|#

Table 9.8 shows the available hyperbolic functions.

Table 9.8. Hyperbolic Functions

#|
||Function|Description|Example(s)||
||sinh ( double precision ) → double precision|Hyperbolic sine|
```sql
sinh(1) → 1.1752011936438014
```||
||cosh ( double precision ) → double precision|Hyperbolic cosine|
```sql
cosh(0) → 1
```||
||tanh ( double precision ) → double precision|Hyperbolic tangent|
```sql
tanh(1) → 0.7615941559557649
```||
||asinh ( double precision ) → double precision|Inverse hyperbolic sine|
```sql
asinh(1) → 0.881373587019543
```||
||acosh ( double precision ) → double precision|Inverse hyperbolic cosine|
```sql
acosh(1) → 0
```||
||atanh ( double precision ) → double precision|Inverse hyperbolic tangent|
```sql
atanh(0.5) → 0.5493061443340548
```||
|#

## 9.4. String Functions and Operators {#string-functions}

This section describes functions and operators for examining and manipulating string values. Strings in this context include values of the types character, character varying, and text. Except where noted, these functions and operators are declared to accept and return type text. They will interchangeably accept character varying arguments. Values of type character will be converted to text before the function or operator is applied, resulting in stripping any trailing spaces in the character value.

SQL defines some string functions that use key words, rather than commas, to separate arguments. Details are in Table 9.9. PostgreSQL also provides versions of these functions that use the regular function invocation syntax (see Table 9.10).

Table 9.9. SQL String Functions and Operators

#|
||Function/Operator|Description|Example(s)||
|| text \|\| text → text | Concatenates the two strings |
```sql
'Post' || 'greSQL' → PostgreSQL
```||
||text \|\| anynonarray → text  
anynonarray \|\| text → text|
Converts the non-string input to text, then concatenates the two strings. (The non-string input cannot be of an array type, because that would create ambiguity with the array \|\| operators. If you want to concatenate an array's text equivalent, cast it to text explicitly.) (UNSUPPORTED)|
```sql
#'Value: ' || 42 → Value: 42
```||
||text IS [NOT] [form] NORMALIZED → boolean|

Checks whether the string is in the specified Unicode normalization form. The optional form key word specifies the form: NFC (the default), NFD, NFKC, or NFKD. This expression can only be used when the server encoding is UTF8. Note that checking for normalization using this expression is often faster than normalizing possibly already normalized strings.|
```sql
U&'\0061\0308bc' IS NFD NORMALIZED → true
```||
||bit_length ( text ) → integer|Returns number of bits in the string (8 times the octet_length) (UNSUPPORTED)|
```sql
#bit_length('jose') → 32
```||
||char_length ( text ) → integer  
character_length ( text ) → integer|
Returns number of characters in the string.|
```sql
char_length('josé') → 4
```||
||lower ( text ) → text|Converts the string to all lower case, according to the rules of the database's locale.|
```sql
lower('TOM') → tom
```||
||normalize ( text [, form ] ) → text|
Converts the string to the specified Unicode normalization form. The optional form key word specifies the form: NFC (the default), NFD, NFKC, or NFKD. This function can only be used when the server encoding is UTF8.|
```sql
normalize(U&'\0061\0308bc', NFC) → äbc
```||
||octet_length ( text ) → integer|
Returns number of bytes in the string.|
```sql
octet_length('josé') → 5
```||
||octet_length ( character ) → integer|
Returns number of bytes in the string. Since this version of the function accepts type character directly, it will not strip trailing spaces.|
```sql
octet_length('abc '::character(4)) → 4
```||
||overlay ( string text PLACING newsubstring text FROM start integer [ FOR count integer ] ) → text|
Replaces the substring of string that starts at the start'th character and extends for count characters with newsubstring. If count is omitted, it defaults to the length of newsubstring.|
```sql
overlay('Txxxxas' placing 'hom' from 2 for 4) → Thomas
```||
||position ( substring text IN string text ) → integer|
Returns first starting index of the specified substring within string, or zero if it's not present.|
```sql
position('om' in 'Thomas') → 3
```||
||substring ( string text [ FROM start integer ] [ FOR count integer ] ) → text|
Extracts the substring of string starting at the start'th character if that is specified, and stopping after count characters if that is specified. Provide at least one of start and count.|
```sql
substring('Thomas' from 2 for 3) → hom
substring('Thomas' from 3) → omas
substring('Thomas' for 2) → Th
```||
||substring ( string text FROM pattern text ) → text|
Extracts the first substring matching POSIX regular expression; see Section 9.7.3.|
```sql
substring('Thomas' from '...$') → mas
```||
||substring ( string text SIMILAR pattern text ESCAPE escape text ) → text  
substring ( string text FROM pattern text FOR escape text ) → text|
Extracts the first substring matching SQL regular expression; see Section 9.7.2. The first form has been specified since SQL:2003; the second form was only in SQL:1999 and should be considered obsolete. (NOT SUPPORTED)|
```sql
#substring('Thomas' similar '%#"o_a#"_' escape '#') → oma
```||
||trim ( [ LEADING \| TRAILING \| BOTH ] [ characters text ] FROM string text ) → text|
Removes the longest string containing only characters in characters (a space by default) from the start, end, or both ends (BOTH is the default) of string.|
```sql
trim(both 'xyz' from 'yxTomxx') → Tom
```||
||trim ( [ LEADING \| TRAILING \| BOTH ] [ FROM ] string text [, characters text ] ) → text|
This is a non-standard syntax for trim().|
```sql
trim(both from 'yxTomxx', 'xyz') → Tom
```||
||upper ( text ) → text|
Converts the string to all upper case, according to the rules of the database's locale.|
```sql
upper('tom') → TOM
```||
|#

Additional string manipulation functions are available and are listed in Table 9.10. Some of them are used internally to implement the SQL-standard string functions listed in Table 9.9.

Table 9.10. Other String Functions

#|
||Function|Description|Example(s)||
||ascii ( text ) → integer|
Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.|
```sql
ascii('x') → 120
```||
||btrim ( string text [, characters text ] ) → text|
Removes the longest string containing only characters in characters (a space by default) from the start and end of string.|
```sql
btrim('xyxtrimyyx', 'xyz') → trim
```||
||chr ( integer ) → text|
Returns the character with the given code. In UTF8 encoding the argument is treated as a Unicode code point. In other multibyte encodings the argument must designate an ASCII character. chr(0) is disallowed because text data types cannot store that character.|
```sql
chr(65) → A
```||
||concat ( val1 "any" [, val2 "any" [, ...] ] ) → text|
Concatenates the text representations of all the arguments. NULL arguments are ignored. (NOT SUPPORTED)|
```sql
concat('abcde', 2, NULL, 22) → abcde222
```||
||concat_ws ( sep text, val1 "any" [, val2 "any" [, ...] ] ) → text|
Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored. (NOT SUPPORTED)|
```sql
concat_ws(',', 'abcde', 2, NULL, 22) → abcde,2,22
```||
||format ( formatstr text [, formatarg "any" [, ...] ] ) → text|
Formats arguments according to a format string; see Section 9.4.1. This function is similar to the C function sprintf. (NOT SUPPORTED)|
```sql
format('Hello %s, %1$s', 'World') → Hello World, World
```||
||initcap ( text ) → text|
Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.|
```sql
initcap('hi THOMAS') → Hi Thomas
```||
||left ( string text, n integer ) → text|
Returns first n characters in the string, or when n is negative, returns all but last \|n\| characters.|
```sql
left('abcde', 2) → ab
```||
||length ( text ) → integer|
Returns the number of characters in the string.|
```sql
length('jose') → 4
```||
||lpad ( string text, length integer [, fill text ] ) → text|
Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).|
```sql
lpad('hi', 5, 'xy') → xyxhi
```||
||ltrim ( string text [, characters text ] ) → text|
Removes the longest string containing only characters in characters (a space by default) from the start of string.|
```sql
ltrim('zzzytest', 'xyz') → test
```||
||md5 ( text ) → text|
Computes the MD5 hash of the argument, with the result written in hexadecimal.|
```sql
md5('abc') → 900150983cd24fb0d6963f7d28e17f72
```||
||parse_ident ( qualified_identifier text [, strict_mode boolean DEFAULT true ] ) → text[]|
Splits qualified_identifier into an array of identifiers, removing any quoting of individual identifiers. By default, extra characters after the last identifier are considered an error; but if the second parameter is false, then such extra characters are ignored. (This behavior is useful for parsing names for objects like functions.) Note that this function does not truncate over-length identifiers. If you want truncation you can cast the result to name[]. (NOT SUPPORTED)|
```sql
#parse_ident('"SomeSchema".someTable') → {SomeSchema,sometable}
```||
||pg_client_encoding ( ) → name|
Returns current client encoding name.|
```sql
pg_client_encoding() → UTF8
```||
||quote_ident ( text ) → text|
Returns the given string suitably quoted to be used as an identifier in an SQL statement string. Quotes are added only if necessary (i.e., if the string contains non-identifier characters or would be case-folded). Embedded quotes are properly doubled. See also Example 43.1.|
```sql
quote_ident('Foo bar') → "Foo bar"
```||
||quote_literal ( text ) → text|
Returns the given string suitably quoted to be used as a string literal in an SQL statement string. Embedded single-quotes and backslashes are properly doubled. Note that quote_literal returns null on null input; if the argument might be null, quote_nullable is often more suitable. See also Example 43.1.|
```sql
quote_literal(E'O\'Reilly') → ''O''Reilly''
```||
||quote_literal ( anyelement ) → text|
Converts the given value to text and then quotes it as a literal. Embedded single-quotes and backslashes are properly doubled. (NOT SUPPORTED)|
```sql
#quote_literal(42.5) → '42.5'
```||
||quote_nullable ( text ) → text|
Returns the given string suitably quoted to be used as a string literal in an SQL statement string; or, if the argument is null, returns NULL. Embedded single-quotes and backslashes are properly doubled. See also Example 43.1.|
```sql
quote_nullable(NULL) → NULL
```||
||quote_nullable ( anyelement ) → text|
Converts the given value to text and then quotes it as a literal; or, if the argument is null, returns NULL. Embedded single-quotes and backslashes are properly doubled. (NOT SUPPORTED)|
```sql
#quote_nullable(42.5) → '42.5'
```||
||regexp_match ( string text, pattern text [, flags text ] ) → text[]|
Returns captured substrings resulting from the first match of a POSIX regular expression to the string; see Section 9.7.3.|
```sql
regexp_match('foobarbequebaz', '(bar)(beque)') → {bar,beque}
```||
||regexp_matches ( string text, pattern text [, flags text ] ) → setof text[]|
Returns captured substrings resulting from the first match of a POSIX regular expression to the string, or multiple matches if the g flag is used; see Section 9.7.3. (NOT SUPPORTED)|
```sql
#regexp_matches('foobarbequebaz', 'ba.', 'g') → {bar},{baz}
```||
||regexp_replace ( string text, pattern text, replacement text [, flags text ] ) → text|
Replaces substrings resulting from the first match of a POSIX regular expression, or multiple substring matches if the g flag is used; see Section 9.7.3.|
```sql
regexp_replace('Thomas', '.[mN]a.', 'M') → ThM
```||
||regexp_split_to_array ( string text, pattern text [, flags text ] ) → text[]|
Splits string using a POSIX regular expression as the delimiter, producing an array of results; see Section 9.7.3.|
```sql
regexp_split_to_array('hello world', '\s+') → {hello,world}
```||
||regexp_split_to_table ( string text, pattern text [, flags text ] ) → setof text|
Splits string using a POSIX regular expression as the delimiter, producing a set of results; see Section 9.7.3. (NOT SUPPORTED)|
```sql
#regexp_split_to_table('hello world', '\s+') → hello,world
```||
||repeat ( string text, number integer ) → text|
Repeats string the specified number of times.|
```sql
repeat('Pg', 4) → PgPgPgPg
```||
||replace ( string text, from text, to text ) → text|
Replaces all occurrences in string of substring from with substring to.|
```sql
replace('abcdefabcdef', 'cd', 'XX') → abXXefabXXef
```||
||reverse ( text ) → text|
Reverses the order of the characters in the string.|
```sql
reverse('abcde') → edcba
```||
||right ( string text, n integer ) → text|
Returns last n characters in the string, or when n is negative, returns all but first \|n\| characters.|
```sql
right('abcde', 2) → de
```||
||rpad ( string text, length integer [, fill text ] ) → text|
Extends the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.|
```sql
rpad('hi', 5, 'xy') → hixyx
```||
||rtrim ( string text [, characters text ] ) → text|
Removes the longest string containing only characters in characters (a space by default) from the end of string.|
```sql
rtrim('testxxzx', 'xyz') → test
```||
||split_part ( string text, delimiter text, n integer ) → text|
Splits string at occurrences of delimiter and returns the n'th field (counting from one), or when n is negative, returns the \|n\|'th-from-last field.|
```sql
split_part('abc~@~def~@~ghi', '~@~', 2) → def
split_part('abc,def,ghi,jkl', ',', -2) → ghi
```||
||strpos ( string text, substring text ) → integer|
Returns first starting index of the specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)|
```sql
strpos('high', 'ig') → 2
```||
||substr ( string text, start integer [, count integer ] ) → text|
Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)|
```sql
substr('alphabet', 3) → phabet
substr('alphabet', 3, 2) → ph
```||
||starts_with ( string text, prefix text ) → boolean|
Returns true if string starts with prefix.|
```sql
starts_with('alphabet', 'alph') → true
```||
||string_to_array ( string text, delimiter text [, null_string text ] ) → text[]|
Splits the string at occurrences of delimiter and forms the resulting fields into a text array. If delimiter is NULL, each character in the string will become a separate element in the array. If delimiter is an empty string, then the string is treated as a single field. If null_string is supplied and is not NULL, fields matching that string are replaced by NULL.|
```sql
string_to_array('xx~~yy~~zz', '~~', 'yy') → {xx,NULL,zz}
```||
||string_to_table ( string text, delimiter text [, null_string text ] ) → setof text|
Splits the string at occurrences of delimiter and returns the resulting fields as a set of text rows. If delimiter is NULL, each character in the string will become a separate row of the result. If delimiter is an empty string, then the string is treated as a single field. If null_string is supplied and is not NULL, fields matching that string are replaced by NULL. (NOT SUPPORTED)|
```sql
#string_to_table('xx~^~yy~^~zz', '~^~', 'yy') → [xx,NULL,zz]
```||
||to_ascii ( string text ) → text  
to_ascii ( string text, encoding name ) → text  
to_ascii ( string text, encoding integer ) → text|
Converts string to ASCII from another encoding, which may be identified by name or number. If encoding is omitted the database encoding is assumed (which in practice is the only useful case). The conversion consists primarily of dropping accents. Conversion is only supported from LATIN1, LATIN2, LATIN9, and WIN1250 encodings. (See the unaccent module for another, more flexible solution.) (NOT SUPPORTED)|
```sql
#to_ascii('Karél') → Karel
```||
||to_hex ( integer ) → text  
to_hex ( bigint ) → text|
Converts the number to its equivalent hexadecimal representation.|
```sql
to_hex(2147483647) → 7fffffff
```||
||translate ( string text, from text, to text ) → text|
Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.|
```sql
translate('12345', '143', 'ax') → a2x5
```||
||unistr ( text ) → text|
Evaluate escaped Unicode characters in the argument. Unicode characters can be specified as \XXXX (4 hexadecimal digits), \+XXXXXX (6 hexadecimal digits), \uXXXX (4 hexadecimal digits), or \UXXXXXXXX (8 hexadecimal digits). To specify a backslash, write two backslashes. All other characters are taken literally.

If the server encoding is not UTF-8, the Unicode code point identified by one of these escape sequences is converted to the actual server encoding; an error is reported if that's not possible.

This function provides a (non-standard) alternative to string constants with Unicode escapes (see Section 4.1.2.3).|
```sql
unistr('d\0061t\+000061') → data
unistr('d\u0061t\U00000061') → data
```||
|#

## 9.5. Binary String Functions and Operators {#binary-string-functions}
This section describes functions and operators for examining and manipulating binary strings, that is values of type bytea. Many of these are equivalent, in purpose and syntax, to the text-string functions described in the previous section.

SQL defines some string functions that use key words, rather than commas, to separate arguments. Details are in Table 9.11. PostgreSQL also provides versions of these functions that use the regular function invocation syntax (see Table 9.12).

Table 9.11. SQL Binary String Functions and Operators

#|
||Function/Operator|Description|Example(s)||
||bytea \|\| bytea → bytea|
Concatenates the two binary strings.|
```sql
'\x123456'::bytea || '\x789a00bcde'::bytea → \x123456789a00bcde
```||
||bit_length ( bytea ) → integer|
Returns number of bits in the binary string (8 times the octet_length). (NOT SUPPORTED)|
```sql
#bit_length('\x123456'::bytea) → 24
```||
||octet_length ( bytea ) → integer|
Returns number of bytes in the binary string.|
```sql
octet_length('\x123456'::bytea) → 3
```||
||overlay ( bytes bytea PLACING newsubstring bytea FROM start integer [ FOR count integer ] ) → bytea|
Replaces the substring of bytes that starts at the start'th byte and extends for count bytes with newsubstring. If count is omitted, it defaults to the length of newsubstring.|
```sql
overlay('\x1234567890'::bytea placing '\002\003'::bytea from 2 for 3) → \x12020390
```||
||position ( substring bytea IN bytes bytea ) → integer|
Returns first starting index of the specified substring within bytes, or zero if it's not present.|
```sql
position('\x5678'::bytea in '\x1234567890'::bytea) → 3
```||
||substring ( bytes bytea [ FROM start integer ] [ FOR count integer ] ) → bytea|
Extracts the substring of bytes starting at the start'th byte if that is specified, and stopping after count bytes if that is specified. Provide at least one of start and count.|
```sql
substring('\x1234567890'::bytea from 3 for 2) → \x5678
```||
||trim ( [ LEADING \| TRAILING \| BOTH ] bytesremoved bytea FROM bytes bytea ) → bytea|
Removes the longest string containing only bytes appearing in bytesremoved from the start, end, or both ends (BOTH is the default) of bytes.|
```sql
trim('\x9012'::bytea from '\x1234567890'::bytea) → \x345678
```||
||trim ( [ LEADING \| TRAILING \| BOTH ] [ FROM ] bytes bytea, bytesremoved bytea ) → bytea|
This is a non-standard syntax for trim().|
```sql
trim(both from '\x1234567890'::bytea, '\x9012'::bytea) → \x345678
```||
|#

Additional binary string manipulation functions are available and are listed in Table 9.12. Some of them are used internally to implement the SQL-standard string functions listed in Table 9.11.

Table 9.12. Other Binary String Functions

#|
||Function|Description|Example(s)||
||bit_count ( bytes bytea ) → bigint|
Returns the number of bits set in the binary string (also known as “popcount”).|
```sql
bit_count('\x1234567890'::bytea) → 15
```||
||btrim ( bytes bytea, bytesremoved bytea ) → bytea|
Removes the longest string containing only bytes appearing in bytesremoved from the start and end of bytes.|
```sql
btrim('\x1234567890'::bytea, '\x9012'::bytea) → \x345678
```||
||get_bit ( bytes bytea, n bigint ) → integer|
Extracts n'th bit from binary string.|
```sql
get_bit('\x1234567890'::bytea, 30) → 1
```||
||get_byte ( bytes bytea, n integer ) → integer|
Extracts n'th byte from binary string.|
```sql
get_byte('\x1234567890'::bytea, 4) → 144
```||
||length ( bytea ) → integer|
Returns the number of bytes in the binary string.|
```sql
length('\x1234567890'::bytea) → 5
```||
||length ( bytes bytea, encoding name ) → integer|
Returns the number of characters in the binary string, assuming that it is text in the given encoding.|
```sql
length('jose'::bytea, 'UTF8') → 4
```||
||ltrim ( bytes bytea, bytesremoved bytea ) → bytea|
Removes the longest string containing only bytes appearing in bytesremoved from the start of bytes.|
```sql
ltrim('\x1234567890'::bytea, '\x9012'::bytea) → \x34567890
```||
||md5 ( bytea ) → text|
Computes the MD5 hash of the binary string, with the result written in hexadecimal.|
```sql
md5('Th\000omas'::bytea) → 8ab2d3c9689aaf18b4958c334c82d8b1
```||
||rtrim ( bytes bytea, bytesremoved bytea ) → bytea|
Removes the longest string containing only bytes appearing in bytesremoved from the end of bytes.|
```sql
rtrim('\x1234567890'::bytea, '\x9012'::bytea) → \x12345678
```||
||set_bit ( bytes bytea, n bigint, newvalue integer ) → bytea|
Sets n'th bit in binary string to newvalue.|
```sql
set_bit('\x1234567890'::bytea, 30, 0) → \x1234563890
```||
||set_byte ( bytes bytea, n integer, newvalue integer ) → bytea|
Sets n'th byte in binary string to newvalue.|
```sql
set_byte('\x1234567890'::bytea, 4, 64) → \x1234567840
```||
||sha224 ( bytea ) → bytea|
Computes the SHA-224 hash of the binary string.|
```sql
sha224('abc'::bytea) → \x23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7
```||
||sha256 ( bytea ) → bytea|
Computes the SHA-256 hash of the binary string.|
```sql
sha256('abc'::bytea) → \xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
```||
||sha384 ( bytea ) → bytea|
Computes the SHA-384 hash of the binary string.|
```sql
sha384('abc'::bytea) → \xcb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7
```||
||sha512 ( bytea ) → bytea|
Computes the SHA-512 hash of the binary string.|
```sql
sha512('abc'::bytea) → \xddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f
```||
||substr ( bytes bytea, start integer [, count integer ] ) → bytea|
Extracts the substring of bytes starting at the start'th byte, and extending for count bytes if that is specified. (Same as substring(bytes from start for count).)|
```sql
substr('\x1234567890'::bytea, 3, 2) → \x5678
```||
|#

Functions get_byte and set_byte number the first byte of a binary string as byte 0. Functions get_bit and set_bit number bits from the right within each byte; for example bit 0 is the least significant bit of the first byte, and bit 15 is the most significant bit of the second byte.

For historical reasons, the function md5 returns a hex-encoded value of type text whereas the SHA-2 functions return type bytea. Use the functions encode and decode to convert between the two. For example write encode(sha256('abc'), 'hex') to get a hex-encoded text representation, or decode(md5('abc'), 'hex') to get a bytea value.

Functions for converting strings between different character sets (encodings), and for representing arbitrary binary data in textual form, are shown in Table 9.13. For these functions, an argument or result of type text is expressed in the database's default encoding, while arguments or results of type bytea are in an encoding named by another argument.

Table 9.13. Text/Binary String Conversion Functions

#|
||Function|Description|Example(s)||
||convert ( bytes bytea, src_encoding name, dest_encoding name ) → bytea|
Converts a binary string representing text in encoding src_encoding to a binary string in encoding dest_encoding (see Section 24.3.4 for available conversions). (NOT SUPPORTED)|
```sql
#convert('text_in_utf8', 'UTF8', 'LATIN1') → \x746578745f696e5f75746638
```||
||convert_from ( bytes bytea, src_encoding name ) → text|
Converts a binary string representing text in encoding src_encoding to text in the database encoding (see Section 24.3.4 for available conversions).|
```sql
convert_from('text_in_utf8', 'UTF8') → text_in_utf8
```||
||convert_to ( string text, dest_encoding name ) → bytea|
Converts a text string (in the database encoding) to a binary string encoded in encoding dest_encoding (see Section 24.3.4 for available conversions).|
```sql
convert_to('some_text', 'UTF8') → \x736f6d655f74657874
```||
||encode ( bytes bytea, format text ) → text|
Encodes binary data into a textual representation; supported format values are: base64, escape, hex.|
```sql
encode('123\000\001', 'base64') → MTIzAAE=
```||
||decode ( string text, format text ) → bytea|
Decodes binary data from a textual representation; supported format values are the same as for encode.|
```sql
decode('MTIzAAE=', 'base64') → \x3132330001
```||
|#

## 9.6. Bit String Functions and Operators {#bit-string-functions}
This section describes functions and operators for examining and manipulating bit strings, that is values of the types bit and bit varying. (While only type bit is mentioned in these tables, values of type bit varying can be used interchangeably.) Bit strings support the usual comparison operators shown in Table 9.1, as well as the operators shown in Table 9.14.

Table 9.14. Bit String Operators

#|
||Operator|Description|Example(s)||
bit \|\| bit → bit |
Concatenation |
```sql
B'10001' || B'011' → 10001011
```||
||bit & bit → bit|
Bitwise AND (inputs must be of equal length)|
```sql
B'10001' & B'01101' → 00001
```||
||bit \| bit → bit|
Bitwise OR (inputs must be of equal length)|
```sql
B'10001' | B'01101' → 11101
```||
||bit # bit → bit|
Bitwise exclusive OR (inputs must be of equal length)|
```sql
B'10001' # B'01101' → 11100
```||
||~ bit → bit|
Bitwise NOT|
```sql
~ B'10001' → 01110
```||
||bit << integer → bit|
Bitwise shift left (string length is preserved)|
```sql
B'10001' << 3 → 01000
```||
||bit >> integer → bit|
Bitwise shift right (string length is preserved)|
```sql
B'10001' >> 2 → 00100
```||
|#

Some of the functions available for binary strings are also available for bit strings, as shown in Table 9.15.

Table 9.15. Bit String Functions

#|
||Function|Description|Example(s)||
||bit_count ( bit ) → bigint|
Returns the number of bits set in the bit string (also known as “popcount”).|
```sql
bit_count(B'10111') → 4
```||
||bit_length ( bit ) → integer|
Returns number of bits in the bit string. (NOT SUPPORTED)|
```sql
#bit_length(B'10111') → 5
```||
||length ( bit ) → integer|
Returns number of bits in the bit string.|
```sql
length(B'10111') → 5
```||
||octet_length ( bit ) → integer|
Returns number of bytes in the bit string.|
```sql
octet_length(B'1011111011') → 2
```||
||overlay ( bits bit PLACING newsubstring bit FROM start integer [ FOR count integer ] ) → bit|
Replaces the substring of bits that starts at the start'th bit and extends for count bits with newsubstring. If count is omitted, it defaults to the length of newsubstring.|
```sql
overlay(B'01010101010101010' placing B'11111' from 2 for 3) → 0111110101010101010
```||
||position ( substring bit IN bits bit ) → integer|
Returns first starting index of the specified substring within bits, or zero if it's not present.|
```sql
position(B'010' in B'000001101011') → 8
```||
||substring ( bits bit [ FROM start integer ] [ FOR count integer ] ) → bit|
Extracts the substring of bits starting at the start'th bit if that is specified, and stopping after count bits if that is specified. Provide at least one of start and count.|
```sql
substring(B'110010111111' from 3 for 2) → 00
```||
||get_bit ( bits bit, n integer ) → integer|
Extracts n'th bit from bit string; the first (leftmost) bit is bit 0.|
```sql
get_bit(B'101010101010101010', 6) → 1
```||
||set_bit ( bits bit, n integer, newvalue integer ) → bit|
Sets n'th bit in bit string to newvalue; the first (leftmost) bit is bit 0.|
```sql
set_bit(B'101010101010101010', 6, 0) → 101010001010101010
```||
|#

In addition, it is possible to cast integral values to and from type bit. Casting an integer to bit(n) copies the rightmost n bits. Casting an integer to a bit string width wider than the integer itself will sign-extend on the left. Some examples:

```sql
44::bit(10)                    → 0000101100
44::bit(3)                     → 100
cast(-44 as bit(12))           → 111111010100
'1110'::bit(4)::integer        → 14
```
Note that casting to just “bit” means casting to bit(1), and so will deliver only the least significant bit of the integer.

## 9.7. Pattern Matching {#pattern-matching}

9.7.1. LIKE

```sql
string LIKE pattern [ESCAPE escape-character]
string NOT LIKE pattern [ESCAPE escape-character]
```
The LIKE expression returns true if the string matches the supplied pattern. (As expected, the NOT LIKE expression returns false if LIKE returns true, and vice versa. An equivalent expression is NOT (string LIKE pattern).)

If pattern does not contain percent signs or underscores, then the pattern only represents the string itself; in that case LIKE acts like the equals operator. An underscore (_) in pattern stands for (matches) any single character; a percent sign (%) matches any sequence of zero or more characters.

Some examples:

```sql
'abc' LIKE 'abc'    → true
'abc' LIKE 'a%'     → true
'abc' LIKE '_b_'    → true
'abc' LIKE 'c'      → false
```

LIKE pattern matching always covers the entire string. Therefore, if it's desired to match a sequence anywhere within a string, the pattern must start and end with a percent sign.

To match a literal underscore or percent sign without matching other characters, the respective character in pattern must be preceded by the escape character. The default escape character is the backslash but a different one can be selected by using the ESCAPE clause. To match the escape character itself, write two escape characters.

Note
If you have standard_conforming_strings turned off, any backslashes you write in literal string constants will need to be doubled. See Section 4.1.2.1 for more information.

It's also possible to select no escape character by writing ESCAPE ''. This effectively disables the escape mechanism, which makes it impossible to turn off the special meaning of underscore and percent signs in the pattern.

According to the SQL standard, omitting ESCAPE means there is no escape character (rather than defaulting to a backslash), and a zero-length ESCAPE value is disallowed. PostgreSQL's behavior in this regard is therefore slightly nonstandard.

The key word ILIKE can be used instead of LIKE to make the match case-insensitive according to the active locale. This is not in the SQL standard but is a PostgreSQL extension.

The operator \~\~ is equivalent to LIKE, and \~\~* corresponds to ILIKE. There are also !\~\~ and !\~\~* operators that represent NOT LIKE and NOT ILIKE, respectively. All of these operators are PostgreSQL-specific. You may see these operator names in EXPLAIN output and similar places, since the parser actually translates LIKE et al. to these operators.

The phrases LIKE, ILIKE, NOT LIKE, and NOT ILIKE are generally treated as operators in PostgreSQL syntax; for example they can be used in expression operator ANY (subquery) constructs, although an ESCAPE clause cannot be included there. In some obscure cases it may be necessary to use the underlying operator names instead.

Also see the prefix operator ^@ and corresponding starts_with function, which are useful in cases where simply matching the beginning of a string is needed.

9.7.2. SIMILAR TO Regular Expressions

```sql
string SIMILAR TO pattern [ESCAPE escape-character] (NOT SUPPORTED)
string NOT SIMILAR TO pattern [ESCAPE escape-character] (NOT SUPPORTED)
```

The SIMILAR TO operator returns true or false depending on whether its pattern matches the given string. It is similar to LIKE, except that it interprets the pattern using the SQL standard's definition of a regular expression. SQL regular expressions are a curious cross between LIKE notation and common (POSIX) regular expression notation.

Like LIKE, the SIMILAR TO operator succeeds only if its pattern matches the entire string; this is unlike common regular expression behavior where the pattern can match any part of the string. Also like LIKE, SIMILAR TO uses _ and % as wildcard characters denoting any single character and any string, respectively (these are comparable to . and .* in POSIX regular expressions).

In addition to these facilities borrowed from LIKE, SIMILAR TO supports these pattern-matching metacharacters borrowed from POSIX regular expressions:

| denotes alternation (either of two alternatives).

* denotes repetition of the previous item zero or more times.

+ denotes repetition of the previous item one or more times.

? denotes repetition of the previous item zero or one time.

{m} denotes repetition of the previous item exactly m times.

{m,} denotes repetition of the previous item m or more times.

{m,n} denotes repetition of the previous item at least m and not more than n times.

Parentheses () can be used to group items into a single logical item.

A bracket expression [...] specifies a character class, just as in POSIX regular expressions.

Notice that the period (.) is not a metacharacter for SIMILAR TO.

As with LIKE, a backslash disables the special meaning of any of these metacharacters. A different escape character can be specified with ESCAPE, or the escape capability can be disabled by writing ESCAPE ''.

According to the SQL standard, omitting ESCAPE means there is no escape character (rather than defaulting to a backslash), and a zero-length ESCAPE value is disallowed. PostgreSQL's behavior in this regard is therefore slightly nonstandard.

Another nonstandard extension is that following the escape character with a letter or digit provides access to the escape sequences defined for POSIX regular expressions; see Table 9.20, Table 9.21, and Table 9.22 below.

Some examples:

```sql
#'abc' SIMILAR TO 'abc'          → true
#'abc' SIMILAR TO 'a'            → false
#'abc' SIMILAR TO '%(b|d)%'      → true
#'abc' SIMILAR TO '(b|c)%'       → false
#'-abc-' SIMILAR TO '%\mabc\M%'  → true
#'xabcy' SIMILAR TO '%\mabc\M%'  → false
```

The substring function with three parameters provides extraction of a substring that matches an SQL regular expression pattern. The function can be written according to standard SQL syntax:

```sql
substring(string similar pattern escape escape-character)
```
or using the now obsolete SQL:1999 syntax:
```sql
substring(string from pattern for escape-character)
```

or as a plain three-argument function:
```sql
substring(string, pattern, escape-character)
```

As with SIMILAR TO, the specified pattern must match the entire data string, or else the function fails and returns null. To indicate the part of the pattern for which the matching data sub-string is of interest, the pattern should contain two occurrences of the escape character followed by a double quote ("). The text matching the portion of the pattern between these separators is returned when the match is successful.

The escape-double-quote separators actually divide substring's pattern into three independent regular expressions; for example, a vertical bar (|) in any of the three sections affects only that section. Also, the first and third of these regular expressions are defined to match the smallest possible amount of text, not the largest, when there is any ambiguity about how much of the data string matches which pattern. (In POSIX parlance, the first and third regular expressions are forced to be non-greedy.)

As an extension to the SQL standard, PostgreSQL allows there to be just one escape-double-quote separator, in which case the third regular expression is taken as empty; or no separators, in which case the first and third regular expressions are taken as empty. (NOT SUPPORTED)

Some examples, with #" delimiting the return string:

```sql
#substring('foobar' similar '%#"o_b#"%' escape '#')   → oob
#substring('foobar' similar '#"o_b#"%' escape '#')    → NULL
```

9.7.3. POSIX Regular Expressions
Table 9.16 lists the available operators for pattern matching using POSIX regular expressions.

Table 9.16. Regular Expression Match Operators

#|
||Operator|Description|Example(s)||
||ext ~ text → boolean|
String matches regular expression, case sensitively|
```sql
'thomas' ~ 't.*ma' → true
```||
||text ~* text → boolean|
String matches regular expression, case insensitively|
```sql
'thomas' ~* 'T.*ma' → true
```||
||text !~ text → boolean|
String does not match regular expression, case sensitively|
```sql
'thomas' !~ 't.*max' → true
```||
||text !~* text → boolean|
String does not match regular expression, case insensitively|
```sql
'thomas' !~* 'T.*ma' → false
```||
|#

POSIX regular expressions provide a more powerful means for pattern matching than the LIKE and SIMILAR TO operators. Many Unix tools such as egrep, sed, or awk use a pattern matching language that is similar to the one described here.

A regular expression is a character sequence that is an abbreviated definition of a set of strings (a regular set). A string is said to match a regular expression if it is a member of the regular set described by the regular expression. As with LIKE, pattern characters match string characters exactly unless they are special characters in the regular expression language — but regular expressions use different special characters than LIKE does. Unlike LIKE patterns, a regular expression is allowed to match anywhere within a string, unless the regular expression is explicitly anchored to the beginning or end of the string.

Some examples:

```sql
'abcd' ~ 'bc'     → true
'abcd' ~ 'a.c'    → true /* dot matches any character */
'abcd' ~ 'a.*d'   → true /* * repeats the preceding pattern item */
'abcd' ~ '(b|x)'  → true /* | means OR, parentheses group */
'abcd' ~ '^a'     → true /* ^ anchors to start of string */
'abcd' ~ '^(b|c)' → false /* would match except for anchoring */
```
The POSIX pattern language is described in much greater detail below.

The substring function with two parameters, substring(string from pattern), provides extraction of a substring that matches a POSIX regular expression pattern. It returns null if there is no match, otherwise the first portion of the text that matched the pattern. But if the pattern contains any parentheses, the portion of the text that matched the first parenthesized subexpression (the one whose left parenthesis comes first) is returned. You can put parentheses around the whole expression if you want to use parentheses within it without triggering this exception. If you need parentheses in the pattern before the subexpression you want to extract, see the non-capturing parentheses described below.

Some examples:

```sql
substring('foobar' from 'o.b')      → oob
substring('foobar' from 'o(.)b')    → o
```

The regexp_replace function provides substitution of new text for substrings that match POSIX regular expression patterns. It has the syntax regexp_replace(source, pattern, replacement [, flags ]). The source string is returned unchanged if there is no match to the pattern. If there is a match, the source string is returned with the replacement string substituted for the matching substring. The replacement string can contain \n, where n is 1 through 9, to indicate that the source substring matching the n'th parenthesized subexpression of the pattern should be inserted, and it can contain \& to indicate that the substring matching the entire pattern should be inserted. Write \\ if you need to put a literal backslash in the replacement text. The flags parameter is an optional text string containing zero or more single-letter flags that change the function's behavior. Flag i specifies case-insensitive matching, while flag g specifies replacement of each matching substring rather than only the first one. Supported flags (though not g) are described in Table 9.24.

Some examples:

```sql
regexp_replace('foobarbaz', 'b..', 'X') → fooXbaz
regexp_replace('foobarbaz', 'b..', 'X', 'g') → fooXX
regexp_replace('foobarbaz', 'b(..)', 'X\1Y', 'g') → fooXarYXazY
```

The regexp_match function returns a text array of captured substring(s) resulting from the first match of a POSIX regular expression pattern to a string. It has the syntax regexp_match(string, pattern [, flags ]). If there is no match, the result is NULL. If a match is found, and the pattern contains no parenthesized subexpressions, then the result is a single-element text array containing the substring matching the whole pattern. If a match is found, and the pattern contains parenthesized subexpressions, then the result is a text array whose n'th element is the substring matching the n'th parenthesized subexpression of the pattern (not counting “non-capturing” parentheses; see below for details). The flags parameter is an optional text string containing zero or more single-letter flags that change the function's behavior. Supported flags are described in Table 9.24.

Some examples:

```sql
regexp_match('foobarbequebaz', 'bar.*que') → {barbeque}
regexp_match('foobarbequebaz', '(bar)(beque)') → {bar,beque}
```

In the common case where you just want the whole matching substring or NULL for no match, write something like

```sql
#(regexp_match('foobarbequebaz', 'bar.*que'))[1] → barbeque
```

The regexp_matches function returns a set of text arrays of captured substring(s) resulting from matching a POSIX regular expression pattern to a string. It has the same syntax as regexp_match. This function returns no rows if there is no match, one row if there is a match and the g flag is not given, or N rows if there are N matches and the g flag is given. Each returned row is a text array containing the whole matched substring or the substrings matching parenthesized subexpressions of the pattern, just as described above for regexp_match. regexp_matches accepts all the flags shown in Table 9.24, plus the g flag which commands it to return all matches, not just the first one.

Some examples:

```sql
SELECT * FROM regexp_matches('foo', 'not there') a → [
]
```

```sql
SELECT * FROM regexp_matches('foobarbequebazilbarfbonk', '(b[^b]+)(b[^b]+)', 'g') a → [
{bar,beque}
{bazil,barf}
]
```

Tip
In most cases regexp_matches() should be used with the g flag, since if you only want the first match, it's easier and more efficient to use regexp_match(). However, regexp_match() only exists in PostgreSQL version 10 and up. When working in older versions, a common trick is to place a regexp_matches() call in a sub-select, for example:

```sql
SELECT col1, (SELECT regexp_matches(col2, '(bar)(beque)')) FROM tab;
```

This produces a text array if there's a match, or NULL if not, the same as regexp_match() would do. Without the sub-select, this query would produce no output at all for table rows without a match, which is typically not the desired behavior.

The regexp_split_to_table function splits a string using a POSIX regular expression pattern as a delimiter. It has the syntax regexp_split_to_table(string, pattern [, flags ]). If there is no match to the pattern, the function returns the string. If there is at least one match, for each match it returns the text from the end of the last match (or the beginning of the string) to the beginning of the match. When there are no more matches, it returns the text from the end of the last match to the end of the string. The flags parameter is an optional text string containing zero or more single-letter flags that change the function's behavior. regexp_split_to_table supports the flags described in Table 9.24.

The regexp_split_to_array function behaves the same as regexp_split_to_table, except that regexp_split_to_array returns its result as an array of text. It has the syntax regexp_split_to_array(string, pattern [, flags ]). The parameters are the same as for regexp_split_to_table.

Some examples:

```sql
SELECT foo FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', '\s+') AS foo → [
the
quick
brown
fox
jumps
over
the
lazy
dog
]
```

```sql
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', '\s+') → {the,quick,brown,fox,jumps,over,the,lazy,dog}
```

```sql
SELECT foo FROM regexp_split_to_table('the quick brown fox', '\s*') AS foo → [
t
h
e
q
u
i
c
k
b
r
o
w
n
f
o
x
]
```

As the last example demonstrates, the regexp split functions ignore zero-length matches that occur at the start or end of the string or immediately after a previous match. This is contrary to the strict definition of regexp matching that is implemented by regexp_match and regexp_matches, but is usually the most convenient behavior in practice. Other software systems such as Perl use similar definitions.

9.7.3.5. Regular Expression Matching Rules
In the event that an RE could match more than one substring of a given string, the RE matches the one starting earliest in the string. If the RE could match more than one substring starting at that point, either the longest possible match or the shortest possible match will be taken, depending on whether the RE is greedy or non-greedy.

Whether an RE is greedy or not is determined by the following rules:

Most atoms, and all constraints, have no greediness attribute (because they cannot match variable amounts of text anyway).

Adding parentheses around an RE does not change its greediness.

A quantified atom with a fixed-repetition quantifier ({m} or {m}?) has the same greediness (possibly none) as the atom itself.

A quantified atom with other normal quantifiers (including {m,n} with m equal to n) is greedy (prefers longest match).

A quantified atom with a non-greedy quantifier (including {m,n}? with m equal to n) is non-greedy (prefers shortest match).

A branch — that is, an RE that has no top-level | operator — has the same greediness as the first quantified atom in it that has a greediness attribute.

An RE consisting of two or more branches connected by the | operator is always greedy.

The above rules associate greediness attributes not only with individual quantified atoms, but with branches and entire REs that contain quantified atoms. What that means is that the matching is done in such a way that the branch, or whole RE, matches the longest or shortest possible substring as a whole. Once the length of the entire match is determined, the part of it that matches any particular subexpression is determined on the basis of the greediness attribute of that subexpression, with subexpressions starting earlier in the RE taking priority over ones starting later.

An example of what this means:

```sql
SUBSTRING('XY1234Z', 'Y*([0-9]{1,3})')  → 123
SUBSTRING('XY1234Z', 'Y*?([0-9]{1,3})') → 1
```

In the first case, the RE as a whole is greedy because Y* is greedy. It can match beginning at the Y, and it matches the longest possible string starting there, i.e., Y123. The output is the parenthesized part of that, or 123. In the second case, the RE as a whole is non-greedy because Y*? is non-greedy. It can match beginning at the Y, and it matches the shortest possible string starting there, i.e., Y1. The subexpression [0-9]{1,3} is greedy but it cannot change the decision as to the overall match length; so it is forced to match just 1.

In short, when an RE contains both greedy and non-greedy subexpressions, the total match length is either as long as possible or as short as possible, according to the attribute assigned to the whole RE. The attributes assigned to the subexpressions only affect how much of that match they are allowed to “eat” relative to each other.

The quantifiers {1,1} and {1,1}? can be used to force greediness or non-greediness, respectively, on a subexpression or a whole RE. This is useful when you need the whole RE to have a greediness attribute different from what's deduced from its elements. As an example, suppose that we are trying to separate a string containing some digits into the digits and the parts before and after them. We might try to do that like this:

```sql
regexp_match('abc01234xyz', '(.*)(\d+)(.*)') → {abc0123,4,xyz}
```

That didn't work: the first .* is greedy so it “eats” as much as it can, leaving the \d+ to match at the last possible place, the last digit. We might try to fix that by making it non-greedy:

```sql
regexp_match('abc01234xyz', '(.*?)(\d+)(.*)') → {abc,0,""}
```

That didn't work either, because now the RE as a whole is non-greedy and so it ends the overall match as soon as possible. We can get what we want by forcing the RE as a whole to be greedy:

```sql
regexp_match('abc01234xyz', '(?:(.*?)(\d+)(.*)){1,1}') → {abc,01234,xyz}
```

Controlling the RE's overall greediness separately from its components' greediness allows great flexibility in handling variable-length patterns.

When deciding what is a longer or shorter match, match lengths are measured in characters, not collating elements. An empty string is considered longer than no match at all. For example: bb* matches the three middle characters of abbbc; (week|wee)(night|knights) matches all ten characters of weeknights; when (.*).* is matched against abc the parenthesized subexpression matches all three characters; and when (a*)* is matched against bc both the whole RE and the parenthesized subexpression match an empty string.

If case-independent matching is specified, the effect is much as if all case distinctions had vanished from the alphabet. When an alphabetic that exists in multiple cases appears as an ordinary character outside a bracket expression, it is effectively transformed into a bracket expression containing both cases, e.g., x becomes [xX]. When it appears inside a bracket expression, all case counterparts of it are added to the bracket expression, e.g., [x] becomes [xX] and [^x] becomes [^xX].

If newline-sensitive matching is specified, . and bracket expressions using ^ will never match the newline character (so that matches will not cross lines unless the RE explicitly includes a newline) and ^ and $ will match the empty string after and before a newline respectively, in addition to matching at beginning and end of string respectively. But the ARE escapes \A and \Z continue to match beginning or end of string only. Also, the character class shorthands \D and \W will match a newline regardless of this mode. (Before PostgreSQL 14, they did not match newlines when in newline-sensitive mode. Write [^[:digit:]] or [^[:word:]] to get the old behavior.)

If partial newline-sensitive matching is specified, this affects . and bracket expressions as with newline-sensitive matching, but not ^ and $.

If inverse partial newline-sensitive matching is specified, this affects ^ and $ as with newline-sensitive matching, but not . and bracket expressions. This isn't very useful but is provided for symmetry

## 9.8. Data Type Formatting Functions {#data-type-formatting-functions}

The PostgreSQL formatting functions provide a powerful set of tools for converting various data types (date/time, integer, floating point, numeric) to formatted strings and for converting from formatted strings to specific data types. Table 9.25 lists them. These functions all follow a common calling convention: the first argument is the value to be formatted and the second argument is a template that defines the output or input format.

Table 9.25. Formatting Functions

#|
||Function|Description|Example(s)||
||to_char ( timestamp with time zone, text ) → text|
Converts time stamp to string according to the given format.|
```sql
to_char(timestamp '2002-04-20 17:31:12.66', 'HH12:MI:SS') → 05:31:12
```||
||to_char ( interval, text ) → text|
Converts interval to string according to the given format.|
```sql
to_char(interval '15h 2m 12s', 'HH24:MI:SS') → 15:02:12
```||
||to_char ( numeric_type, text ) → text|
Converts number to string according to the given format; available for integer, bigint, numeric, real, double precision.|
```sql
to_char(125, '999') → ' 125'
to_char(125.8::real, '999D9') → ' 125.8'
to_char(-125.8, '999D99S') → '125.80-'
```||
||to_date ( text, text ) → date|
Converts string to date according to the given format.|
```sql
to_date('05 Dec 2000', 'DD Mon YYYY') → '2000-12-05'
```||
||to_number ( text, text ) → numeric|
Converts string to numeric according to the given format.|
```sql
to_number('12,454.8-', '99G999D9S') → '-12454.8'
```||
||to_timestamp ( text, text ) → timestamp with time zone|
Converts string to time stamp according to the given format. (See also to_timestamp(double precision) in Table 9.32.)|
```sql
cast(to_timestamp('05 Dec 2000', 'DD Mon YYYY') as timestamp) → '2000-12-05 00:00:00'
```||
|#

Table 9.30 shows some examples of the use of the to_char function.

Table 9.30. to_char Examples

```sql
to_char('2000-06-06 05:39:18'::timestamp, 'Day, DD  HH12:MI:SS') → 'Tuesday  , 06  05:39:18'
to_char('2000-06-06 05:39:18'::timestamp, 'FMDay, FMDD  HH12:MI:SS') → 'Tuesday, 6  05:39:18'
to_char(-0.1, '99.99') → '  -.10'
to_char(-0.1, 'FM9.99') → '-.1'
to_char(-0.1, 'FM90.99') → '-0.1'
to_char(0.1, '0.9') → ' 0.1'
to_char(12, '9990999.9') → '    0012.0'
to_char(12, 'FM9990999.9') → '0012.'
to_char(485, '999') → ' 485'
to_char(-485, '999') → '-485'
to_char(485, '9 9 9') → ' 4 8 5'
to_char(1485, '9,999') → ' 1,485'
to_char(1485, '9G999') → ' 1,485'
to_char(148.5, '999.999') → ' 148.500'
to_char(148.5, 'FM999.999') → '148.5'
to_char(148.5, 'FM999.990') → '148.500'
to_char(148.5, '999D999') → ' 148.500'
to_char(3148.5, '9G999D999') → ' 3,148.500'
to_char(-485, '999S') → '485-'
to_char(-485, '999MI') → '485-'
to_char(485, '999MI') → '485 '
to_char(485, 'FM999MI') → '485'
to_char(485, 'PL999') → '+ 485'
to_char(485, 'SG999') → '+485'
to_char(-485, 'SG999') → '-485'
to_char(-485, '9SG99') → '4-85'
to_char(-485, '999PR') → '<485>'
to_char(485, 'L999') → '  485'
to_char(485, 'RN') → '        CDLXXXV'
to_char(485, 'FMRN') → 'CDLXXXV'
to_char(5.2, 'FMRN') → 'V'
to_char(482, '999th') → ' 482nd'
to_char(485, '"Good number:"999') → 'Good number: 485'
to_char(485.8, '"Pre:"999" Post:" .999') → 'Pre: 485 Post: .800'
to_char(12, '99V999') → ' 12000'
to_char(12.4, '99V999') → ' 12400'
to_char(12.45, '99V9') → ' 125'
to_char(0.0004859, '9.99EEEE') → ' 4.86e-04'
```

## 9.9. Date/Time Functions and Operators {#date-time-functions}

Table 9.32 shows the available functions for date/time value processing, with details appearing in the following subsections. Table 9.31 illustrates the behaviors of the basic arithmetic operators (+, *, etc.). For formatting functions, refer to Section 9.8. You should be familiar with the background information on date/time data types from Section 8.5.

In addition, the usual comparison operators shown in Table 9.1 are available for the date/time types. Dates and timestamps (with or without time zone) are all comparable, while times (with or without time zone) and intervals can only be compared to other values of the same data type. When comparing a timestamp without time zone to a timestamp with time zone, the former value is assumed to be given in the time zone specified by the TimeZone configuration parameter, and is rotated to UTC for comparison to the latter value (which is already in UTC internally). Similarly, a date value is assumed to represent midnight in the TimeZone zone when comparing it to a timestamp.

All the functions and operators described below that take time or timestamp inputs actually come in two variants: one that takes time with time zone or timestamp with time zone, and one that takes time without time zone or timestamp without time zone. For brevity, these variants are not shown separately. Also, the + and * operators come in commutative pairs (for example both date + integer and integer + date); we show only one of each such pair.

Table 9.31. Date/Time Operators

#|
||Operator|Description|Example(s)||
||date + integer → date|
Add a number of days to a date|
```sql
date '2001-09-28' + 7 → 2001-10-05
```||
||date + interval → timestamp|
Add an interval to a date|
```sql
date '2001-09-28' + interval '1 hour' → 2001-09-28 01:00:00
```||
||date + time → timestamp|
Add a time-of-day to a date|
```sql
date '2001-09-28' + time '03:00' → 2001-09-28 03:00:00
```||
||interval + interval → interval|
Add intervals|
```sql
interval '1 day' + interval '1 hour' → 1 day 01:00:00
```||
||timestamp + interval → timestamp|
Add an interval to a timestamp|
```sql
timestamp '2001-09-28 01:00' + interval '23 hours' → 2001-09-29 00:00:00
```||
||time + interval → time|
Add an interval to a time|
```sql
time '01:00' + interval '3 hours' → 04:00:00
```||
||- interval → interval|
Negate an interval|
```sql
- interval '23 hours' → -23:00:00
```||
||date - date → integer|
Subtract dates, producing the number of days elapsed|
```sql
date '2001-10-01' - date '2001-09-28' → 3
```||
||date - integer → date|
Subtract a number of days from a date|
```sql
date '2001-10-01' - 7 → 2001-09-24
```||
||date - interval → timestamp|
Subtract an interval from a date|
```sql
date '2001-09-28' - interval '1 hour' → 2001-09-27 23:00:00
```||
||time - time → interval|
Subtract times|
```sql
time '05:00' - time '03:00' → 02:00:00
```||
||time - interval → time|
Subtract an interval from a time|
```sql
time '05:00' - interval '2 hours' → 03:00:00
```||
||timestamp - interval → timestamp|
Subtract an interval from a timestamp|
```sql
timestamp '2001-09-28 23:00' - interval '23 hours' → 2001-09-28 00:00:00
```||
||interval - interval → interval|
Subtract intervals|
```sql
interval '1 day' - interval '1 hour' → 1 day -01:00:00
```||
||timestamp - timestamp → interval|
Subtract timestamps (converting 24-hour intervals into days, similarly to justify_hours())|
```sql
timestamp '2001-09-29 03:00' - timestamp '2001-07-27 12:00' → 63 days 15:00:00
```||
||interval * double precision → interval|
Multiply an interval by a scalar|
```sql
interval '1 second' * 900 → 00:15:00
interval '1 day' * 21 → 21 days
interval '1 hour' * 3.5 → 03:30:00
```||
||interval / double precision → interval|
Divide an interval by a scalar|
```sql
interval '1 hour' / 1.5 → 00:40:00
```||
|#

Table 9.32. Date/Time Functions

#|
||Function|Description|Example(s)||
||age ( timestamp, timestamp ) → interval|
Subtract arguments, producing a “symbolic” result that uses years and months, rather than just days|
```sql
age(timestamp '2001-04-10', timestamp '1957-06-13') → 43 years 9 mons 27 days
```||
||age ( timestamp ) → interval|
Subtract argument from current_date (at midnight) (NOT SUPPORTED)|
```sql
#age(timestamp '1957-06-13') → 62 years 6 mons 10 days
```||
||clock_timestamp ( ) → timestamp with time zone|
Current date and time (changes during statement execution); see Section 9.9.5|
```sql
clock_timestamp() ~→ 2019-12-23 14:39:53.662522-05
```||
||current_date → date|
Current date; see Section 9.9.5|
```sql
current_date ~→ 2019-12-23
```||
||current_time → time with time zone|
Current time of day; see Section 9.9.5|
```sql
current_time ~→ 14:39:53.662522-05
```||
||current_time ( integer ) → time with time zone|
Current time of day, with limited precision; see Section 9.9.5|
```sql
current_time(2) ~→ 14:39:53.66-05
```||
||current_timestamp → timestamp with time zone|
Current date and time (start of current transaction); see Section 9.9.5|
```sql
current_timestamp ~→ 2019-12-23 14:39:53.662522-05
```||
||current_timestamp ( integer ) → timestamp with time zone|
Current date and time (start of current transaction), with limited precision; see Section 9.9.5|
```sql
current_timestamp(0) ~→ 2019-12-23 14:39:53-05
```||
||date_bin ( interval, timestamp, timestamp ) → timestamp|
Bin input into specified interval aligned with specified origin; see Section 9.9.3|
```sql
date_bin('15 minutes', timestamp '2001-02-16 20:38:40', timestamp '2001-02-16 20:05:00') → 2001-02-16 20:35:00
```||
||date_part ( text, timestamp ) → double precision|
Get timestamp subfield (equivalent to extract); see Section 9.9.1|
```sql
date_part('hour', timestamp '2001-02-16 20:38:40') → 20
```||
||date_part ( text, interval ) → double precision|
Get interval subfield (equivalent to extract); see Section 9.9.1|
```sql
date_part('month', interval '2 years 3 months') → 3
```||
||date_trunc ( text, timestamp ) → timestamp|
Truncate to specified precision; see Section 9.9.2|
```sql
date_trunc('hour', timestamp '2001-02-16 20:38:40') → 2001-02-16 20:00:00
```||
||date_trunc ( text, timestamp with time zone, text ) → timestamp with time zone|
Truncate to specified precision in the specified time zone; see Section 9.9.2 (NOT SUPPORTED)|
```sql
#date_trunc('day', timestamptz '2001-02-16 20:38:40+00', 'Australia/Sydney') → 2001-02-16 13:00:00+00
```||
||date_trunc ( text, interval ) → interval|
Truncate to specified precision; see Section 9.9.2|
```sql
date_trunc('hour', interval '2 days 3 hours 40 minutes') → 2 days 03:00:00
```||
||extract ( field from timestamp ) → numeric|
Get timestamp subfield; see Section 9.9.1|
```sql
extract(hour from timestamp '2001-02-16 20:38:40') → 20
```||
||extract ( field from interval ) → numeric|
Get interval subfield; see Section 9.9.1|
```sql
extract(month from interval '2 years 3 months') → 3
```||
||isfinite ( date ) → boolean|
Test for finite date (not +/-infinity)|
```sql
isfinite(date '2001-02-16') → true
```||
||isfinite ( timestamp ) → boolean|
Test for finite timestamp (not +/-infinity)|
```sql
isfinite(timestamp 'infinity') → false
```||
||isfinite ( interval ) → boolean|
Test for finite interval (currently always true)|
```sql
isfinite(interval '4 hours') → true
```||
||justify_days ( interval ) → interval|
Adjust interval so 30-day time periods are represented as months|
```sql
justify_days(interval '35 days') → 1 mon 5 days
```||
||justify_hours ( interval ) → interval|
Adjust interval so 24-hour time periods are represented as days|
```sql
justify_hours(interval '27 hours') → 1 day 03:00:00
```||
||justify_interval ( interval ) → interval|
Adjust interval using justify_days and justify_hours, with additional sign adjustments|
```sql
justify_interval(interval '1 mon -1 hour') → 29 days 23:00:00
```||
||localtime → time|
Current time of day; see Section 9.9.5 (NOT SUPPORTED)|
```sql
#localtime ~→ 14:39:53.662522
```||
||localtime ( integer ) → time|
Current time of day, with limited precision; see Section 9.9.5 (NOT SUPPORTED)|
```sql
#localtime(0) ~→ 14:39:53
```||
||localtimestamp → timestamp|
Current date and time (start of current transaction); see Section 9.9.5 (NOT SUPPORTED)|
```sql
#localtimestamp ~→ 2019-12-23 14:39:53.662522
```||
||localtimestamp ( integer ) → timestamp|
Current date and time (start of current transaction), with limited precision; see Section 9.9.5 (NOT SUPPORTED)|
```sql
#localtimestamp(2) ~→ 2019-12-23 14:39:53.66
```||
||make_date ( year int, month int, day int ) → date|
Create date from year, month and day fields (negative years signify BC)|
```sql
make_date(2013, 7, 15) → 2013-07-15
```||
||make_interval ( [ years int [, months int [, weeks int [, days int [, hours int [, mins int [, secs double precision ]]]]]]] ) → interval|
Create interval from years, months, weeks, days, hours, minutes and seconds fields, each of which can default to zero (NOT SUPPORTED)|
```sql
#make_interval(days => 10) → 10 days
```||
||make_time ( hour int, min int, sec double precision ) → time|
Create time from hour, minute and seconds fields|
```sql
make_time(8, 15, 23.5) → 08:15:23.5
```||
||make_timestamp ( year int, month int, day int, hour int, min int, sec double precision ) → timestamp|
Create timestamp from year, month, day, hour, minute and seconds fields (negative years signify BC)|
```sql
make_timestamp(2013, 7, 15, 8, 15, 23.5) → 2013-07-15 08:15:23.5
```||
||make_timestamptz ( year int, month int, day int, hour int, min int, sec double precision [, timezone text ] ) → timestamp with time zone|
Create timestamp with time zone from year, month, day, hour, minute and seconds fields (negative years signify BC). If timezone is not specified, the current time zone is used; the examples assume the session time zone is Europe/London|
```sql
make_timestamptz(2013, 7, 15, 8, 15, 23.5) ~→ 2013-07-15 08:15:23.5+01
#make_timestamptz(2013, 7, 15, 8, 15, 23.5, 'America/New_York') ~→ 2013-07-15 13:15:23.5+01
```||
||now ( ) → timestamp with time zone|
Current date and time (start of current transaction); see Section 9.9.5|
```sql
now() ~→ 2019-12-23 14:39:53.662522-05
```||
||statement_timestamp ( ) → timestamp with time zone|
Current date and time (start of current statement); see Section 9.9.5|
```sql
statement_timestamp() ~→ 2019-12-23 14:39:53.662522-05
```||
||timeofday ( ) → text|
Current date and time (like clock_timestamp, but as a text string); see Section 9.9.5|
```sql
timeofday() ~→ Mon Dec 23 14:39:53.662522 2019 EST
```||
||transaction_timestamp ( ) → timestamp with time zone|
Current date and time (start of current transaction); see Section 9.9.5|
```sql
transaction_timestamp() ~→ 2019-12-23 14:39:53.662522-05
```||
||to_timestamp ( double precision ) → timestamp with time zone|
Convert Unix epoch (seconds since 1970-01-01 00:00:00+00) to timestamp with time zone|
```sql
to_timestamp(1284352323) → 2010-09-13 04:32:03+00
```||
|#

In addition to these functions, the SQL OVERLAPS operator is supported: (NOT SUPPORTED)

```sql
(start1, end1) OVERLAPS (start2, end2)
(start1, length1) OVERLAPS (start2, length2)
```

This expression yields true when two time periods (defined by their endpoints) overlap, false when they do not overlap. The endpoints can be specified as pairs of dates, times, or time stamps; or as a date, time, or time stamp followed by an interval. When a pair of values is provided, either the start or the end can be written first; OVERLAPS automatically takes the earlier value of the pair as the start. Each time period is considered to represent the half-open interval start <= time < end, unless start and end are equal in which case it represents that single time instant. This means for instance that two time periods with only an endpoint in common do not overlap.

```sql
(DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') → true
#(DATE '2001-02-16', INTERVAL '100 days') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') → false
(DATE '2001-10-29', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31') → false
(DATE '2001-10-30', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31') → true
```

When adding an interval value to (or subtracting an interval value from) a timestamp with time zone value, the days component advances or decrements the date of the timestamp with time zone by the indicated number of days, keeping the time of day the same. Across daylight saving time changes (when the session time zone is set to a time zone that recognizes DST), this means interval '1 day' does not necessarily equal interval '24 hours'. For example, with the session time zone set to America/Denver:

```sql
timestamp with time zone '2005-04-02 12:00:00-07' + interval '1 day' ~→ 2005-04-03 12:00:00-06
timestamp with time zone '2005-04-02 12:00:00-07' + interval '24 hours' ~→ 2005-04-03 13:00:00-06
```

This happens because an hour was skipped due to a change in daylight saving time at 2005-04-03 02:00:00 in time zone America/Denver.

Note there can be ambiguity in the months field returned by age because different months have different numbers of days. PostgreSQL's approach uses the month from the earlier of the two dates when calculating partial months. For example, age('2004-06-01', '2004-04-30') uses April to yield 1 mon 1 day, while using May would yield 1 mon 2 days because May has 31 days, while April has only 30.

Subtraction of dates and timestamps can also be complex. One conceptually simple way to perform subtraction is to convert each value to a number of seconds using EXTRACT(EPOCH FROM ...), then subtract the results; this produces the number of seconds between the two values. This will adjust for the number of days in each month, timezone changes, and daylight saving time adjustments. Subtraction of date or timestamp values with the “-” operator returns the number of days (24-hours) and hours/minutes/seconds between the values, making the same adjustments. The age function returns years, months, days, and hours/minutes/seconds, performing field-by-field subtraction and then adjusting for negative field values. The following queries illustrate the differences in these approaches. The sample results were produced with timezone = 'US/Eastern'; there is a daylight saving time change between the two dates used:

```sql
EXTRACT(EPOCH FROM timestamptz '2013-07-01 12:00:00') - EXTRACT(EPOCH FROM timestamptz '2013-03-01 12:00:00') ~→ 10537200.000000
(EXTRACT(EPOCH FROM timestamptz '2013-07-01 12:00:00') - EXTRACT(EPOCH FROM timestamptz '2013-03-01 12:00:00')) / 60 / 60 / 24 ~→ 121.9583333333333333
timestamptz '2013-07-01 12:00:00' - timestamptz '2013-03-01 12:00:00' ~→ 121 days 23:00:00
age(timestamptz '2013-07-01 12:00:00', timestamptz '2013-03-01 12:00:00') → 4 mons
```

9.9.1. EXTRACT, date_part

```sql
EXTRACT(field FROM source)
```

The extract function retrieves subfields such as year or hour from date/time values. source must be a value expression of type timestamp, time, or interval. (Expressions of type date are cast to timestamp and can therefore be used as well.) field is an identifier or string that selects what field to extract from the source value. The extract function returns values of type numeric. The following are valid field names:

century
The century

```sql
EXTRACT(CENTURY FROM TIMESTAMP '2000-12-16 12:21:13') → 20
EXTRACT(CENTURY FROM TIMESTAMP '2001-02-16 20:38:40') → 21
```

The first century starts at 0001-01-01 00:00:00 AD, although they did not know it at the time. This definition applies to all Gregorian calendar countries. There is no century number 0, you go from -1 century to 1 century. If you disagree with this, please write your complaint to: Pope, Cathedral Saint-Peter of Roma, Vatican.

day
For timestamp values, the day (of the month) field (1–31) ; for interval values, the number of days

```sql
EXTRACT(DAY FROM TIMESTAMP '2001-02-16 20:38:40') → 16
 EXTRACT(DAY FROM INTERVAL '40 days 1 minute') → 40
```

decade
The year field divided by 10

```sql
EXTRACT(DECADE FROM TIMESTAMP '2001-02-16 20:38:40') → 200
```

dow
The day of the week as Sunday (0) to Saturday (6)

```sql
EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40') → 5
```

Note that extract's day of the week numbering differs from that of the to_char(..., 'D') function.

doy
The day of the year (1–365/366)

```sql
EXTRACT(DOY FROM TIMESTAMP '2001-02-16 20:38:40') → 47
```

epoch
For timestamp with time zone values, the number of seconds since 1970-01-01 00:00:00 UTC (negative for timestamps before that); for date and timestamp values, the nominal number of seconds since 1970-01-01 00:00:00, without regard to timezone or daylight-savings rules; for interval values, the total number of seconds in the interval

```sql
EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '2001-02-16 20:38:40.12-08') → 982384720.120000
EXTRACT(EPOCH FROM TIMESTAMP '2001-02-16 20:38:40.12') → 982355920.120000
EXTRACT(EPOCH FROM INTERVAL '5 days 3 hours') → 442800.000000
```

You can convert an epoch value back to a timestamp with time zone with to_timestamp:

```sql
to_timestamp(982384720.12) → 2001-02-17 04:38:40.12+00
```
Beware that applying to_timestamp to an epoch extracted from a date or timestamp value could produce a misleading result: the result will effectively assume that the original value had been given in UTC, which might not be the case.

hour
The hour field (0–23)

```sql
EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40') → 20
```

isodow
The day of the week as Monday (1) to Sunday (7)

```sql
EXTRACT(ISODOW FROM TIMESTAMP '2001-02-18 20:38:40') → 7
```

This is identical to dow except for Sunday. This matches the ISO 8601 day of the week numbering.

isoyear
The ISO 8601 week-numbering year that the date falls in (not applicable to intervals)

```sql
EXTRACT(ISOYEAR FROM DATE '2006-01-01') → 2005
EXTRACT(ISOYEAR FROM DATE '2006-01-02') → 2006
```

Each ISO 8601 week-numbering year begins with the Monday of the week containing the 4th of January, so in early January or late December the ISO year may be different from the Gregorian year. See the week field for more information.

This field is not available in PostgreSQL releases prior to 8.3.

julian
The Julian Date corresponding to the date or timestamp (not applicable to intervals). Timestamps that are not local midnight result in a fractional value. See Section B.7 for more information.

```sql
EXTRACT(JULIAN FROM DATE '2006-01-01') → 2453737
EXTRACT(JULIAN FROM TIMESTAMP '2006-01-01 12:00') → 2453737.50000000000000000000
```

microseconds
The seconds field, including fractional parts, multiplied by 1 000 000; note that this includes full seconds

```sql
EXTRACT(MICROSECONDS FROM TIME '17:12:28.5') → 28500000
```

millennium
The millennium

```sql
EXTRACT(MILLENNIUM FROM TIMESTAMP '2001-02-16 20:38:40') → 3
```
Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.

milliseconds
The seconds field, including fractional parts, multiplied by 1000. Note that this includes full seconds.

```sql
EXTRACT(MILLISECONDS FROM TIME '17:12:28.5') → 28500.000
```

minute
The minutes field (0–59)

```sql
EXTRACT(MINUTE FROM TIMESTAMP '2001-02-16 20:38:40') → 38
```

month
For timestamp values, the number of the month within the year (1–12) ; for interval values, the number of months, modulo 12 (0–11)

```sql
SELECT EXTRACT(MONTH FROM TIMESTAMP '2001-02-16 20:38:40') → 2
SELECT EXTRACT(MONTH FROM INTERVAL '2 years 3 months') → 3
SELECT EXTRACT(MONTH FROM INTERVAL '2 years 13 months') → 1
```

quarter
The quarter of the year (1–4) that the date is in

```sql
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2001-02-16 20:38:40') → 1
```

second
The seconds field, including any fractional seconds

```sql
SELECT EXTRACT(SECOND FROM TIMESTAMP '2001-02-16 20:38:40') → 40.000000
SELECT EXTRACT(SECOND FROM TIME '17:12:28.5') → 28.500000
```

timezone
The time zone offset from UTC, measured in seconds. Positive values correspond to time zones east of UTC, negative values to zones west of UTC. (Technically, PostgreSQL does not use UTC because leap seconds are not handled.)

timezone_hour
The hour component of the time zone offset

timezone_minute
The minute component of the time zone offset

week
The number of the ISO 8601 week-numbering week of the year. By definition, ISO weeks start on Mondays and the first week of a year contains January 4 of that year. In other words, the first Thursday of a year is in week 1 of that year.

In the ISO week-numbering system, it is possible for early-January dates to be part of the 52nd or 53rd week of the previous year, and for late-December dates to be part of the first week of the next year. For example, 2005-01-01 is part of the 53rd week of year 2004, and 2006-01-01 is part of the 52nd week of year 2005, while 2012-12-31 is part of the first week of 2013. It's recommended to use the isoyear field together with week to get consistent results.

```sql
EXTRACT(WEEK FROM TIMESTAMP '2001-02-16 20:38:40') → 7
```

year
The year field. Keep in mind there is no 0 AD, so subtracting BC years from AD years should be done with care.

```sql
EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40') → 2001
```

Note
When the input value is +/-Infinity, extract returns +/-Infinity for monotonically-increasing fields (epoch, julian, year, isoyear, decade, century, and millennium). For other fields, NULL is returned. PostgreSQL versions before 9.6 returned zero for all cases of infinite input.

The extract function is primarily intended for computational processing. For formatting date/time values for display, see Section 9.8.

The date_part function is modeled on the traditional Ingres equivalent to the SQL-standard function extract:

```sql
date_part('field', source)
```

Note that here the field parameter needs to be a string value, not a name. The valid field names for date_part are the same as for extract. For historical reasons, the date_part function returns values of type double precision. This can result in a loss of precision in certain uses. Using extract is recommended instead.

```sql
date_part('day', TIMESTAMP '2001-02-16 20:38:40') → 16
date_part('hour', INTERVAL '4 hours 3 minutes') → 4
```

9.9.2. date_trunc

The function date_trunc is conceptually similar to the trunc function for numbers.

```sql
date_trunc(field, source [, time_zone ])
```

source is a value expression of type timestamp, timestamp with time zone, or interval. (Values of type date and time are cast automatically to timestamp or interval, respectively.) field selects to which precision to truncate the input value. The return value is likewise of type timestamp, timestamp with time zone, or interval, and it has all fields that are less significant than the selected one set to zero (or one, for day and month).

Valid values for field are:

microseconds
milliseconds
second
minute
hour
day
week
month
quarter
year
decade
century
millennium

When the input value is of type timestamp with time zone, the truncation is performed with respect to a particular time zone; for example, truncation to day produces a value that is midnight in that zone. By default, truncation is done with respect to the current TimeZone setting, but the optional time_zone argument can be provided to specify a different time zone. The time zone name can be specified in any of the ways described in Section 8.5.3.

A time zone cannot be specified when processing timestamp without time zone or interval inputs. These are always taken at face value.

Examples (assuming the local time zone is America/New_York):

```sql
date_trunc('hour', TIMESTAMP '2001-02-16 20:38:40') ~→ 2001-02-16 20:00:00
date_trunc('year', TIMESTAMP '2001-02-16 20:38:40') ~→ 2001-01-01 00:00:00
date_trunc('day', TIMESTAMP WITH TIME ZONE '2001-02-16 20:38:40+00') ~→ 2001-02-16 00:00:00-05
#date_trunc('day', TIMESTAMP WITH TIME ZONE '2001-02-16 20:38:40+00', 'Australia/Sydney') ~→ 2001-02-16 08:00:00-05
date_trunc('hour', INTERVAL '3 days 02:47:33') → 3 days 02:00:00
```

9.9.3. date_bin

The function date_bin “bins” the input timestamp into the specified interval (the stride) aligned with a specified origin.

```sql
date_bin(stride, source, origin)
```

source is a value expression of type timestamp or timestamp with time zone. (Values of type date are cast automatically to timestamp.) stride is a value expression of type interval. The return value is likewise of type timestamp or timestamp with time zone, and it marks the beginning of the bin into which the source is placed.

Examples:

```sql
date_bin('15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01') → 2020-02-11 15:30:00
date_bin('15 minutes', TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01 00:02:30') → 2020-02-11 15:32:30
```

In the case of full units (1 minute, 1 hour, etc.), it gives the same result as the analogous date_trunc call, but the difference is that date_bin can truncate to an arbitrary interval.

The stride interval must be greater than zero and cannot contain units of month or larger.

9.9.4. AT TIME ZONE

The AT TIME ZONE operator converts time stamp without time zone to/from time stamp with time zone, and time with time zone values to different time zones. Table 9.33 shows its variants. (NOT SUPPORTED)

Table 9.33. AT TIME ZONE Variants

#|
||Operator|Description|Example(s)|
||timestamp without time zone AT TIME ZONE zone → timestamp with time zone|
Converts given time stamp without time zone to time stamp with time zone, assuming the given value is in the named time zone.|
```sql
#timestamp '2001-02-16 20:38:40' at time zone 'America/Denver' → 2001-02-17 03:38:40+00
```||
||timestamp with time zone AT TIME ZONE zone → timestamp without time zone|
Converts given time stamp with time zone to time stamp without time zone, as the time would appear in that zone.|
```sql
#timestamp with time zone '2001-02-16 20:38:40-05' at time zone 'America/Denver' → 2001-02-16 18:38:40
```||
||time with time zone AT TIME ZONE zone → time with time zone|
Converts given time with time zone to a new time zone. Since no date is supplied, this uses the currently active UTC offset for the named destination zone.|
```sql
#time with time zone '05:34:17-05' at time zone 'UTC' → 10:34:17+00
```||
|#

In these expressions, the desired time zone zone can be specified either as a text value (e.g., 'America/Los_Angeles') or as an interval (e.g., INTERVAL '-08:00'). In the text case, a time zone name can be specified in any of the ways described in Section 8.5.3. The interval case is only useful for zones that have fixed offsets from UTC, so it is not very common in practice.

Examples (assuming the current TimeZone setting is America/Los_Angeles):

```sql
#TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver' ~→ 2001-02-16 19:38:40-08
#TIMESTAMP WITH TIME ZONE '2001-02-16 20:38:40-05' AT TIME ZONE 'America/Denver' ~→ 2001-02-16 18:38:40
#TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'Asia/Tokyo' AT TIME ZONE 'America/Chicago' ~→ 2001-02-16 05:38:40
```

The first example adds a time zone to a value that lacks it, and displays the value using the current TimeZone setting. The second example shifts the time stamp with time zone value to the specified time zone, and returns the value without a time zone. This allows storage and display of values different from the current TimeZone setting. The third example converts Tokyo time to Chicago time.

The function timezone(zone, timestamp) is equivalent to the SQL-conforming construct timestamp AT TIME ZONE zone.

9.9.5. Current Date/Time

PostgreSQL provides a number of functions that return values related to the current date and time. These SQL-standard functions all return values based on the start time of the current transaction:

```sql
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
CURRENT_TIME(precision)
CURRENT_TIMESTAMP(precision)
LOCALTIME
LOCALTIMESTAMP
LOCALTIME(precision)
LOCALTIMESTAMP(precision)
```

CURRENT_TIME and CURRENT_TIMESTAMP deliver values with time zone; LOCALTIME and LOCALTIMESTAMP deliver values without time zone.

CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, and LOCALTIMESTAMP can optionally take a precision parameter, which causes the result to be rounded to that many fractional digits in the seconds field. Without a precision parameter, the result is given to the full available precision.

Some examples:

```sql
CURRENT_TIME ~→ 14:39:53.662522-05
CURRENT_DATE ~→ 2019-12-23
CURRENT_TIMESTAMP ~→ 2019-12-23 14:39:53.662522-05
CURRENT_TIMESTAMP(2) ~→ 2019-12-23 14:39:53.66-05
#LOCALTIMESTAMP ~→ 2019-12-23 14:39:53.662522
```

Since these functions return the start time of the current transaction, their values do not change during the transaction. This is considered a feature: the intent is to allow a single transaction to have a consistent notion of the “current” time, so that multiple modifications within the same transaction bear the same time stamp.

Note
Other database systems might advance these values more frequently.

PostgreSQL also provides functions that return the start time of the current statement, as well as the actual current time at the instant the function is called. The complete list of non-SQL-standard time functions is:

```sql
transaction_timestamp()
statement_timestamp()
clock_timestamp()
timeofday()
now()
```

transaction_timestamp() is equivalent to CURRENT_TIMESTAMP, but is named to clearly reflect what it returns. statement_timestamp() returns the start time of the current statement (more specifically, the time of receipt of the latest command message from the client). statement_timestamp() and transaction_timestamp() return the same value during the first command of a transaction, but might differ during subsequent commands. clock_timestamp() returns the actual current time, and therefore its value changes even within a single SQL command. timeofday() is a historical PostgreSQL function. Like clock_timestamp(), it returns the actual current time, but as a formatted text string rather than a timestamp with time zone value. now() is a traditional PostgreSQL equivalent to transaction_timestamp().

All the date/time data types also accept the special literal value now to specify the current date and time (again, interpreted as the transaction start time). Thus, the following three all return the same result:

```sql
SELECT CURRENT_TIMESTAMP;
SELECT now();
SELECT TIMESTAMP 'now';  /* but see tip below */
```

Tip
Do not use the third form when specifying a value to be evaluated later, for example in a DEFAULT clause for a table column. The system will convert now to a timestamp as soon as the constant is parsed, so that when the default value is needed, the time of the table creation would be used! The first two forms will not be evaluated until the default value is used, because they are function calls. Thus they will give the desired behavior of defaulting to the time of row insertion. (See also Section 8.5.1.4.)

9.9.6. Delaying Execution

The following functions are available to delay execution of the server process:

```sql
pg_sleep ( double precision )
pg_sleep_for ( interval )
pg_sleep_until ( timestamp with time zone )
```

pg_sleep makes the current session's process sleep until the given number of seconds have elapsed. Fractional-second delays can be specified. pg_sleep_for is a convenience function to allow the sleep time to be specified as an interval. pg_sleep_until is a convenience function for when a specific wake-up time is desired. For example:

```sql
SELECT pg_sleep(1.5);
SELECT pg_sleep_for('5 minutes');
SELECT pg_sleep_until('tomorrow 03:00');
```

Note
The effective resolution of the sleep interval is platform-specific; 0.01 seconds is a common value. The sleep delay will be at least as long as specified. It might be longer depending on factors such as server load. In particular, pg_sleep_until is not guaranteed to wake up exactly at the specified time, but it will not wake up any earlier.

Warning
Make sure that your session does not hold more locks than necessary when calling pg_sleep or its variants. Otherwise other sessions might have to wait for your sleeping process, slowing down the entire system.

## 9.10. Enum Support Functions (NOT SUPPORTED) {#enum-support-functions}

## 9.11. Geometric Functions and Operators {#geometric-functions}
The geometric types point, box, lseg, line, path, polygon, and circle have a large set of native support functions and operators, shown in Table 9.35, Table 9.36, and Table 9.37.

Table 9.35. Geometric Operators

#|
||Operator|Description|Example(s)||
||geometric_type + point → geometric_type|
Adds the coordinates of the second point to those of each point of the first argument, thus performing translation. Available for point, box, path, circle.|
```sql
box '(1,1),(0,0)' + point '(2,0)' → (3,1),(2,0)
```||
||path + path → path|
Concatenates two open paths (returns NULL if either path is closed).|
```sql
path '[(0,0),(1,1)]' + path '[(2,2),(3,3),(4,4)]' → [(0,0),(1,1),(2,2),(3,3),(4,4)]
```||
||geometric_type - point → geometric_type|
Subtracts the coordinates of the second point from those of each point of the first argument, thus performing translation. Available for point, box, path, circle.|
```sql
box '(1,1),(0,0)' - point '(2,0)' → (-1,1),(-2,0)
```||
||geometric_type * point → geometric_type|
Multiplies each point of the first argument by the second point (treating a point as being a complex number represented by real and imaginary parts, and performing standard complex multiplication). If one interprets the second point as a vector, this is equivalent to scaling the object's size and distance from the origin by the length of the vector, and rotating it counterclockwise around the origin by the vector's angle from the x axis. Available for point, box,[a] path, circle.|
```sql
path '((0,0),(1,0),(1,1))' * point '(3.0,0)' → ((0,0),(3,0),(3,3))
path '((0,0),(1,0),(1,1))' * point(cosd(45), sind(45)) → ((0,0),(0.7071067811865475,0.7071067811865475),(0,1.414213562373095))
```||
||geometric_type / point → geometric_type|
Divides each point of the first argument by the second point (treating a point as being a complex number represented by real and imaginary parts, and performing standard complex division). If one interprets the second point as a vector, this is equivalent to scaling the object's size and distance from the origin down by the length of the vector, and rotating it clockwise around the origin by the vector's angle from the x axis. Available for point, box,[a] path, circle.|
```sql
path '((0,0),(1,0),(1,1))' / point '(2.0,0)' → ((0,0),(0.5,0),(0.5,0.5))
path '((0,0),(1,0),(1,1))' / point(cosd(45), sind(45)) → ((0,0),(0.7071067811865476,-0.7071067811865476),(1.4142135623730951,0))
```||
||@-@ geometric_type → double precision|
Computes the total length. Available for lseg, path.|
```sql
@-@ path '[(0,0),(1,0),(1,1)]' → 2
```||
||@@ geometric_type → point|
Computes the center point. Available for box, lseg, polygon, circle.|
```sql
@@ box '(2,2),(0,0)' → (1,1)
```||
||\# geometric_type → integer|
Returns the number of points. Available for path, polygon.|
```sql
# path '((1,0),(0,1),(-1,0))' → 3
```||
||geometric_type # geometric_type → point|
Computes the point of intersection, or NULL if there is none. Available for lseg, line.|
```sql
lseg '[(0,0),(1,1)]' # lseg '[(1,0),(0,1)]' → (0.5,0.5)
```||
||box # box → box|
Computes the intersection of two boxes, or NULL if there is none.|
```sql
box '(2,2),(-1,-1)' # box '(1,1),(-2,-2)' → (1,1),(-1,-1)
```||
||geometric_type ## geometric_type → point|
Computes the closest point to the first object on the second object. Available for these pairs of types: (point, box), (point, lseg), (point, line), (lseg, box), (lseg, lseg), (line, lseg).|
```sql
point '(0,0)' ## lseg '[(2,0),(0,2)]' → (1,1)
```||
||geometric_type <-> geometric_type → double precision|
Computes the distance between the objects. Available for all geometric types except polygon, for all combinations of point with another geometric type, and for these additional pairs of types: (box, lseg), (lseg, line), (polygon, circle) (and the commutator cases).|
```sql
circle '<(0,0),1>' <-> circle '<(5,0),1>' → 3
```||
||geometric_type @> geometric_type → boolean|
Does first object contain second? Available for these pairs of types: (box, point), (box, box), (path, point), (polygon, point), (polygon, polygon), (circle, point), (circle, circle).|
```sql
circle '<(0,0),2>' @> point '(1,1)' → true
```||
||geometric_type <@ geometric_type → boolean|
Is first object contained in or on second? Available for these pairs of types: (point, box), (point, lseg), (point, line), (point, path), (point, polygon), (point, circle), (box, box), (lseg, box), (lseg, line), (polygon, polygon), (circle, circle).|
```sql
point '(1,1)' <@ circle '<(0,0),2>' → true
```||
||geometric_type && geometric_type → boolean|
Do these objects overlap? (One point in common makes this true.) Available for box, polygon, circle.|
```sql
box '(1,1),(0,0)' && box '(2,2),(0,0)' → true
```||
||geometric_type << geometric_type → boolean|
Is first object strictly left of second? Available for point, box, polygon, circle.|
```sql
circle '<(0,0),1>' << circle '<(5,0),1>' → true
```||
||geometric_type >> geometric_type → boolean|
Is first object strictly right of second? Available for point, box, polygon, circle.|
```sql
circle '<(5,0),1>' >> circle '<(0,0),1>' → true
```||
||geometric_type &< geometric_type → boolean|
Does first object not extend to the right of second? Available for box, polygon, circle.|
```sql
box '(1,1),(0,0)' &< box '(2,2),(0,0)' → true
```||
||geometric_type &> geometric_type → boolean|
Does first object not extend to the left of second? Available for box, polygon, circle.|
```sql
box '(3,3),(0,0)' &> box '(2,2),(0,0)' → true
```||
||geometric_type <<\| geometric_type → boolean|
Is first object strictly below second? Available for point, box, polygon, circle.|
```sql
box '(3,3),(0,0)' <<| box '(5,5),(3,4)' → true
```||
||geometric_type \|>> geometric_type → boolean|
Is first object strictly above second? Available for point, box, polygon, circle.|
```sql
box '(5,5),(3,4)' |>> box '(3,3),(0,0)' → true
```||
||geometric_type &<\| geometric_type → boolean|
Does first object not extend above second? Available for box, polygon, circle.|
```sql
box '(1,1),(0,0)' &<| box '(2,2),(0,0)' → true
```||
||geometric_type \|&> geometric_type → boolean|
Does first object not extend below second? Available for box, polygon, circle.|
```sql
box '(3,3),(0,0)' |&> box '(2,2),(0,0)' → true
```||
||box <^ box → boolean|
Is first object below second (allows edges to touch)?|
```sql
box '((1,1),(0,0))' <^ box '((2,2),(1,1))' → true
```||
||box >^ box → boolean|
Is first object above second (allows edges to touch)?|
```sql
box '((2,2),(1,1))' >^ box '((1,1),(0,0))' → true
```||
||geometric_type ?# geometric_type → boolean|
Do these objects intersect? Available for these pairs of types: (box, box), (lseg, box), (lseg, lseg), (lseg, line), (line, box), (line, line), (path, path).|
```sql
lseg '[(-1,0),(1,0)]' ?# box '(2,2),(-2,-2)' → true
```||
||?- line → boolean  
?- lseg → boolean|
Is line horizontal?|
```sql
?- lseg '[(-1,0),(1,0)]' → true
```||
||point ?- point → boolean|
Are points horizontally aligned (that is, have same y coordinate)?|
```sql
point '(1,0)' ?- point '(0,0)' → true
```||
||?\| line → boolean  
?\| lseg → boolean|
Is line vertical?|
```sql
?| lseg '[(-1,0),(1,0)]' → false
```||
||point ?\| point → boolean|
Are points vertically aligned (that is, have same x coordinate)?|
```sql
point '(0,1)' ?| point '(0,0)' → true
```||
||line ?-\| line → boolean  
lseg ?-\| lseg → boolean|
Are lines perpendicular?|
```sql
lseg '[(0,0),(0,1)]' ?-| lseg '[(0,0),(1,0)]' → true
```||
||line ?\|\| line → boolean  
lseg ?\|\| lseg → boolean|
Are lines parallel?|
```sql
lseg '[(-1,0),(1,0)]' ?|| lseg '[(-1,2),(1,2)]' → true
```||
||geometric_type ~= geometric_type → boolean|
Are these objects the same? Available for point, box, polygon, circle.|
```sql
polygon '((0,0),(1,1))' ~= polygon '((1,1),(0,0))' → true
```||
|#

[a] “Rotating” a box with these operators only moves its corner points: the box is still considered to have sides parallel to the axes. Hence the box's size is not preserved, as a true rotation would do.

Caution
Note that the “same as” operator, ~=, represents the usual notion of equality for the point, box, polygon, and circle types. Some of the geometric types also have an = operator, but = compares for equal areas only. The other scalar comparison operators (<= and so on), where available for these types, likewise compare areas.

Note
Before PostgreSQL 14, the point is strictly below/above comparison operators point <<\| point and point \|>> point were respectively called <^ and >^. These names are still available, but are deprecated and will eventually be removed.

Table 9.36. Geometric Functions

#|
||Function|Description|Example(s)||
||area ( geometric_type ) → double precision|
Computes area. Available for box, path, circle. A path input must be closed, else NULL is returned. Also, if the path is self-intersecting, the result may be meaningless.|
```sql
area(box '(2,2),(0,0)') → 4
```||
||center ( geometric_type ) → point|
Computes center point. Available for box, circle.|
```sql
center(box '(1,2),(0,0)') → (0.5,1)
```||
||diagonal ( box ) → lseg|
Extracts box's diagonal as a line segment (same as lseg(box)).|
```sql
diagonal(box '(1,2),(0,0)') → [(1,2),(0,0)]
```||
||diameter ( circle ) → double precision|
Computes diameter of circle.|
```sql
diameter(circle '<(0,0),2>') → 4
```||
||height ( box ) → double precision|
Computes vertical size of box.|
```sql
height(box '(1,2),(0,0)') → 2
```||
||isclosed ( path ) → boolean|
Is path closed?|
```sql
isclosed(path '((0,0),(1,1),(2,0))') → true
```||
||isopen ( path ) → boolean|
Is path open?|
```sql
isopen(path '[(0,0),(1,1),(2,0)]') → true
```||
||length ( geometric_type ) → double precision|
Computes the total length. Available for lseg, path.|
```sql
length(path '((-1,0),(1,0))') → 4
```||
||npoints ( geometric_type ) → integer|
Returns the number of points. Available for path, polygon.|
```sql
npoints(path '[(0,0),(1,1),(2,0)]') → 3
```||
||pclose ( path ) → path|
Converts path to closed form.|
```sql
pclose(path '[(0,0),(1,1),(2,0)]') → ((0,0),(1,1),(2,0))
```||
||popen ( path ) → path|
Converts path to open form.|
```sql
popen(path '((0,0),(1,1),(2,0))') → [(0,0),(1,1),(2,0)]
```||
||radius ( circle ) → double precision|
Computes radius of circle.|
```sql
radius(circle '<(0,0),2>') → 2
```||
||slope ( point, point ) → double precision|
Computes slope of a line drawn through the two points.|
```sql
slope(point '(0,0)', point '(2,1)') → 0.5
```||
||width ( box ) → double precision|
Computes horizontal size of box.|
```sql
width(box '(1,2),(0,0)') → 1
```||
|#

Table 9.37. Geometric Type Conversion Functions

#|
||Function|Description|Example(s)||
||box ( circle ) → box|
Computes box inscribed within the circle.|
```sql
box(circle '<(0,0),2>') → (1.414213562373095,1.414213562373095),(-1.414213562373095,-1.414213562373095)
```||
||box ( point ) → box|
Converts point to empty box.|
```sql
box(point '(1,0)') → (1,0),(1,0)
```||
||box ( point, point ) → box|
Converts any two corner points to box.|
```sql
box(point '(0,1)', point '(1,0)') → (1,1),(0,0)
```||
||box ( polygon ) → box|
Computes bounding box of polygon.|
```sql
box(polygon '((0,0),(1,1),(2,0))') → (2,1),(0,0)
```||
||bound_box ( box, box ) → box|
Computes bounding box of two boxes.|
```sql
bound_box(box '(1,1),(0,0)', box '(4,4),(3,3)') → (4,4),(0,0)
```||
||circle ( box ) → circle|
Computes smallest circle enclosing box.|
```sql
circle(box '(1,1),(0,0)') → <(0.5,0.5),0.7071067811865476>
```||
||circle ( point, double precision ) → circle|
Constructs circle from center and radius.|
```sql
circle(point '(0,0)', 2.0) → <(0,0),2>
```||
||circle ( polygon ) → circle|
Converts polygon to circle. The circle's center is the mean of the positions of the polygon's points, and the radius is the average distance of the polygon's points from that center.|
```sql
circle(polygon '((0,0),(1,3),(2,0))') → <(1,1),1.6094757082487299>
```||
||line ( point, point ) → line|
Converts two points to the line through them.|
```sql
line(point '(-1,0)', point '(1,0)') → {0,-1,0}
```||
||lseg ( box ) → lseg|
Extracts box's diagonal as a line segment.|
```sql
lseg(box '(1,0),(-1,0)') → [(1,0),(-1,0)]
```||
||lseg ( point, point ) → lseg|
Constructs line segment from two endpoints.|
```sql
lseg(point '(-1,0)', point '(1,0)') → [(-1,0),(1,0)]
```||
||path ( polygon ) → path|
Converts polygon to a closed path with the same list of points.|
```sql
path(polygon '((0,0),(1,1),(2,0))') → ((0,0),(1,1),(2,0))
```||
||point ( double precision, double precision ) → point|
Constructs point from its coordinates.|
```sql
point(23.4, -44.5) → (23.4,-44.5)
```||
||point ( box ) → point|
Computes center of box.|
```sql
point(box '(1,0),(-1,0)') → (0,0)
```||
||point ( circle ) → point|
Computes center of circle.|
```sql
point(circle '<(0,0),2>') → (0,0)
```||
||point ( lseg ) → point|
Computes center of line segment.|
```sql
point(lseg '[(-1,0),(1,0)]') → (0,0)
```||
||point ( polygon ) → point|
Computes center of polygon (the mean of the positions of the polygon's points).|
```sql
point(polygon '((0,0),(1,1),(2,0))') → (1,0.3333333333333333)
```||
||polygon ( box ) → polygon|
Converts box to a 4-point polygon.|
```sql
polygon(box '(1,1),(0,0)') → ((0,0),(0,1),(1,1),(1,0))
```||
||polygon ( circle ) → polygon|
Converts circle to a 12-point polygon. (NOT SUPPORTED)|
```sql
#polygon(circle '<(0,0),2>') → ((-2,0),​(-1.7320508075688774,0.9999999999999999),​(-1.0000000000000002,1.7320508075688772),​(-1.2246063538223773e-16,2),​(0.9999999999999996,1.7320508075688774),​(1.732050807568877,1.0000000000000007),​(2,2.4492127076447545e-16),​(1.7320508075688776,-0.9999999999999994),​(1.0000000000000009,-1.7320508075688767),​(3.673819061467132e-16,-2),​(-0.9999999999999987,-1.732050807568878),​(-1.7320508075688767,-1.0000000000000009))
```||
||polygon ( path ) → polygon|
Converts closed path to a polygon with the same list of points.|
```sql
polygon(path '((0,0),(1,1),(2,0))') → ((0,0),(1,1),(2,0))
```||
|#

## 9.12. Network Address Functions and Operators {#network-address-functions}
The IP network address types, cidr and inet, support the usual comparison operators shown in Table 9.1 as well as the specialized operators and functions shown in Table 9.38 and Table 9.39.

Any cidr value can be cast to inet implicitly; therefore, the operators and functions shown below as operating on inet also work on cidr values. (Where there are separate functions for inet and cidr, it is because the behavior should be different for the two cases.) Also, it is permitted to cast an inet value to cidr. When this is done, any bits to the right of the netmask are silently zeroed to create a valid cidr value.

Table 9.38. IP Address Operators

#|
||Operator|Description|Example(s)||
||inet << inet → boolean|
Is subnet strictly contained by subnet? This operator, and the next four, test for subnet inclusion. They consider only the network parts of the two addresses (ignoring any bits to the right of the netmasks) and determine whether one network is identical to or a subnet of the other.|
```sql
inet '192.168.1.5' << inet '192.168.1/24' → true
inet '192.168.0.5' << inet '192.168.1/24' → false
inet '192.168.1/24' << inet '192.168.1/24' → false
```||
||inet <<= inet → boolean|
Is subnet contained by or equal to subnet?|
```sql
inet '192.168.1/24' <<= inet '192.168.1/24' → true
```||
||inet >> inet → boolean|
Does subnet strictly contain subnet?|
```sql
inet '192.168.1/24' >> inet '192.168.1.5' → true
```||
||inet >>= inet → boolean|
Does subnet contain or equal subnet?|
```sql
inet '192.168.1/24' >>= inet '192.168.1/24' → true
```||
||inet && inet → boolean|
Does either subnet contain or equal the other?|
```sql
inet '192.168.1/24' && inet '192.168.1.80/28' → true
inet '192.168.1/24' && inet '192.168.2.0/28' → false
```||
||~ inet → inet|
Computes bitwise NOT.|
```sql
~ inet '192.168.1.6' → 63.87.254.249
```||
||inet & inet → inet|
Computes bitwise AND.|
```sql
inet '192.168.1.6' & inet '0.0.0.255' → 0.0.0.6
```||
||inet \| inet → inet|
Computes bitwise OR.|
```sql
inet '192.168.1.6' | inet '0.0.0.255' → 192.168.1.255
```||
||inet + bigint → inet|
Adds an offset to an address.|
```sql
inet '192.168.1.6' + 25 → 192.168.1.31
```||
||bigint + inet → inet|
Adds an offset to an address. (NOT SUPPORTED)|
```sql
#200 + inet '::ffff:fff0:1' → ::ffff:255.240.0.201
```||
||inet - bigint → inet|
Subtracts an offset from an address.|
```sql
inet '192.168.1.43' - 36 → 192.168.1.7
```||
||inet - inet → bigint|
Computes the difference of two addresses.|
```sql
inet '192.168.1.43' - inet '192.168.1.19' → 24
inet '::1' - inet '::ffff:1' → -4294901760
```||
|#

Table 9.39. IP Address Functions

#|
||Function|Description|Example(s)||
||abbrev ( inet ) → text|
Creates an abbreviated display format as text. (The result is the same as the inet output function produces; it is “abbreviated” only in comparison to the result of an explicit cast to text, which for historical reasons will never suppress the netmask part.)|
```sql
abbrev(inet '10.1.0.0/32') → 10.1.0.0
```||
||abbrev ( cidr ) → text|
Creates an abbreviated display format as text. (The abbreviation consists of dropping all-zero octets to the right of the netmask; more examples are in Table 8.22.)|
```sql
abbrev(cidr '10.1.0.0/16') → 10.1/16
```||
||broadcast ( inet ) → inet|
Computes the broadcast address for the address's network.|
```sql
broadcast(inet '192.168.1.5/24') → 192.168.1.255/24
```||
||family ( inet ) → integer|
Returns the address's family: 4 for IPv4, 6 for IPv6.|
```sql
family(inet '::1') → 6
```||
||host ( inet ) → text|
Returns the IP address as text, ignoring the netmask.|
```sql
host(inet '192.168.1.0/24') → 192.168.1.0
```||
||hostmask ( inet ) → inet|
Computes the host mask for the address's network.|
```sql
hostmask(inet '192.168.23.20/30') → 0.0.0.3
```||
||inet_merge ( inet, inet ) → cidr|
Computes the smallest network that includes both of the given networks.|
```sql
inet_merge(inet '192.168.1.5/24', inet '192.168.2.5/24') → 192.168.0.0/22
```||
||inet_same_family ( inet, inet ) → boolean|
Tests whether the addresses belong to the same IP family.|
```sql
inet_same_family(inet '192.168.1.5/24', inet '::1') → false
```||
||masklen ( inet ) → integer|
Returns the netmask length in bits.|
```sql
masklen(inet '192.168.1.5/24') → 24
```||
||netmask ( inet ) → inet|
Computes the network mask for the address's network.|
```sql
netmask(inet '192.168.1.5/24') → 255.255.255.0
```||
||network ( inet ) → cidr|
Returns the network part of the address, zeroing out whatever is to the right of the netmask. (This is equivalent to casting the value to cidr.)|
```sql
network(inet '192.168.1.5/24') → 192.168.1.0/24
```||
||set_masklen ( inet, integer ) → inet|
Sets the netmask length for an inet value. The address part does not change.|
```sql
set_masklen(inet '192.168.1.5/24', 16) → 192.168.1.5/16
```||
||set_masklen ( cidr, integer ) → cidr|
Sets the netmask length for a cidr value. Address bits to the right of the new netmask are set to zero.|
```sql
set_masklen(cidr '192.168.1.0/24', 16) → 192.168.0.0/16
```||
||text ( inet ) → text|
Returns the unabbreviated IP address and netmask length as text. (This has the same result as an explicit cast to text.)|
```sql
text(inet '192.168.1.5') → 192.168.1.5/32
```||
|#

Tip
The abbrev, host, and text functions are primarily intended to offer alternative display formats for IP addresses.

The MAC address types, macaddr and macaddr8, support the usual comparison operators shown in Table 9.1 as well as the specialized functions shown in Table 9.40. In addition, they support the bitwise logical operators ~, & and | (NOT, AND and OR), just as shown above for IP addresses.

Table 9.40. MAC Address Functions

#|
||Function|Description|Example(s)||
||trunc ( macaddr ) → macaddr|
Sets the last 3 bytes of the address to zero. The remaining prefix can be associated with a particular manufacturer (using data not included in PostgreSQL).|
```sql
trunc(macaddr '12:34:56:78:90:ab') → 12:34:56:00:00:00
```||
||trunc ( macaddr8 ) → macaddr8|
Sets the last 5 bytes of the address to zero. The remaining prefix can be associated with a particular manufacturer (using data not included in PostgreSQL).|
```sql
trunc(macaddr8 '12:34:56:78:90:ab:cd:ef') → 12:34:56:00:00:00:00:00
```||
||macaddr8_set7bit ( macaddr8 ) → macaddr8|
Sets the 7th bit of the address to one, creating what is known as modified EUI-64, for inclusion in an IPv6 address.|
```sql
macaddr8_set7bit(macaddr8 '00:34:56:ab:cd:ef') → 02:34:56:ff:fe:ab:cd:ef
```||
|#


## 9.13. Text Search Functions and Operators (NOT SUPPORTED) {#text-search-functions}

## 9.14. UUID Functions {#uuid-functions}

PostgreSQL includes one function to generate a UUID:

gen_random_uuid () → uuid

This function returns a version 4 (random) UUID. This is the most commonly used type of UUID and is appropriate for most applications.

## 9.15. XML Functions {#xml-functions}

The functions and function-like expressions described in this section operate on values of type xml. See Section 8.13 for information about the xml type. The function-like expressions xmlparse and xmlserialize for converting to and from type xml are documented there, not in this section.

Use of most of these functions requires PostgreSQL to have been built with configure --with-libxml.

9.15.1. Producing XML Content

A set of functions and function-like expressions is available for producing XML content from SQL data. As such, they are particularly suitable for formatting query results into XML documents for processing in client applications.

9.15.1.1. Xmlcomment

xmlcomment ( text ) → xml

The function xmlcomment creates an XML value containing an XML comment with the specified text as content. The text cannot contain “--” or end with a “-”, otherwise the resulting construct would not be a valid XML comment. If the argument is null, the result is null.

Example:
```sql
xmlcomment('hello') → <!--hello-->
```

9.15.1.2. Xmlconcat

xmlconcat ( xml [, ...] ) → xml (NOT SUPPORTED)

The function xmlconcat concatenates a list of individual XML values to create a single value containing an XML content fragment. Null values are omitted; the result is only null if there are no nonnull arguments.

Example:
```sql
#xmlconcat('<abc/>', '<bar>foo</bar>') → <abc/><bar>foo</bar>
```

XML declarations, if present, are combined as follows. If all argument values have the same XML version declaration, that version is used in the result, else no version is used. If all argument values have the standalone declaration value “yes”, then that value is used in the result. If all argument values have a standalone declaration value and at least one is “no”, then that is used in the result. Else the result will have no standalone declaration. If the result is determined to require a standalone declaration but no version declaration, a version declaration with version 1.0 will be used because XML requires an XML declaration to contain a version declaration. Encoding declarations are ignored and removed in all cases.

Example:
```sql
#xmlconcat('<?xml version="1.1"?><foo/>', '<?xml version="1.1" standalone="no"?><bar/>') → <?xml version="1.1"?><foo/><bar/>
```

9.15.1.3. Xmlelement

xmlelement ( NAME name [, XMLATTRIBUTES ( attvalue [ AS attname ] [, ...] ) ] [, content [, ...]] ) → xml (NOT SUPPORTED)

```sql
#xmlelement(name foo) → <foo/>
#xmlelement(name foo, xmlattributes('xyz' as bar)) → <foo bar="xyz"/>
#xmlelement(name foo, xmlattributes(current_date as bar), 'cont', 'ent') ~→ <foo bar="2007-01-26">content</foo>
```

Element and attribute names that are not valid XML names are escaped by replacing the offending characters by the sequence _xHHHH_, where HHHH is the character's Unicode codepoint in hexadecimal notation. For example:

```sql
#xmlelement(name "foo$bar", xmlattributes('xyz' as "a&b")) → <foo_x0024_bar a_x0026_b="xyz"/>
```

An explicit attribute name need not be specified if the attribute value is a column reference, in which case the column's name will be used as the attribute name by default. In other cases, the attribute must be given an explicit name. So this example is valid:

Element content, if specified, will be formatted according to its data type. If the content is itself of type xml, complex XML documents can be constructed. For example:

```sql
#xmlelement(name foo, xmlattributes('xyz' as bar), xmlelement(name abc), xmlcomment('test'), xmlelement(name xyz)) → <foo bar="xyz"><abc/><!--test--><xyz/></foo>
```

Content of other types will be formatted into valid XML character data. This means in particular that the characters <, >, and & will be converted to entities. Binary data (data type bytea) will be represented in base64 or hex encoding, depending on the setting of the configuration parameter xmlbinary. The particular behavior for individual data types is expected to evolve in order to align the PostgreSQL mappings with those specified in SQL:2006 and later, as discussed in Section D.3.1.3.

9.15.1.4. Xmlforest

xmlforest ( content [ AS name ] [, ...] ) → xml (NOT SUPPORTED)

9.15.1.5. Xmlpi

xmlpi ( NAME name [, content ] ) → xml (NOT SUPPORTED)

9.15.1.6. Xmlroot

xmlroot ( xml, VERSION {text|NO VALUE} [, STANDALONE {YES|NO|NO VALUE} ] ) → xml (NOT SUPPORTED)

9.15.1.7. Xmlagg

xmlagg ( xml ) → xml

The function xmlagg is, unlike the other functions described here, an aggregate function. It concatenates the input values to the aggregate function call, much like xmlconcat does, except that concatenation occurs across rows rather than across expressions in a single row. See Section 9.21 for additional information about aggregate functions.

Example:
```sql
SELECT xmlagg(x::xml) from (values ('<a/>'),('<b/>')) a(x) → <a/><b/>
```

9.15.2. XML Predicates
The expressions described in this section check properties of xml values.

9.15.2.1. IS DOCUMENT

xml IS DOCUMENT → boolean (NOT SUPPORTED)

The expression IS DOCUMENT returns true if the argument XML value is a proper XML document, false if it is not (that is, it is a content fragment), or null if the argument is null. See Section 8.13 about the difference between documents and content fragments.

9.15.2.2. IS NOT DOCUMENT

xml IS NOT DOCUMENT → boolean (NOT SUPPORTED)

The expression IS NOT DOCUMENT returns false if the argument XML value is a proper XML document, true if it is not (that is, it is a content fragment), or null if the argument is null.

9.15.2.3. XMLEXISTS

XMLEXISTS ( text PASSING [BY {REF|VALUE}] xml [BY {REF|VALUE}] ) → boolean

The function xmlexists evaluates an XPath 1.0 expression (the first argument), with the passed XML value as its context item. The function returns false if the result of that evaluation yields an empty node-set, true if it yields any other value. The function returns null if any argument is null. A nonnull value passed as the context item must be an XML document, not a content fragment or any non-XML value.

Example:
```sql
xmlexists('//town[text() = ''Toronto'']' PASSING BY VALUE '<towns><town>Toronto</town><town>Ottawa</town></towns>') → true
```

9.15.2.4. Xml_is_well_formed (NOT SUPPORTED)

xml_is_well_formed ( text ) → boolean
xml_is_well_formed_document ( text ) → boolean
xml_is_well_formed_content ( text ) → boolean

9.15.3. Processing XML
To process values of data type xml, PostgreSQL offers the functions xpath and xpath_exists, which evaluate XPath 1.0 expressions, and the XMLTABLE table function.

9.15.3.1. Xpath

xpath ( xpath text, xml xml [, nsarray text[] ] ) → xml[] (NOT SUPPORTED)


9.15.3.2. Xpath_exists

xpath_exists ( xpath text, xml xml [, nsarray text[] ] ) → boolean

The function xpath_exists is a specialized form of the xpath function. Instead of returning the individual XML values that satisfy the XPath 1.0 expression, this function returns a Boolean indicating whether the query was satisfied or not (specifically, whether it produced any value other than an empty node-set). This function is equivalent to the XMLEXISTS predicate, except that it also offers support for a namespace mapping argument.

Example:
```sql
xpath_exists('/my:a/text()', '<my:a xmlns:my="http://example.com">test</my:a>',ARRAY[ARRAY['my', 'http://example.com']]) → true
```

9.15.3.3. Xmltable

XMLTABLE (
    [ XMLNAMESPACES ( namespace_uri AS namespace_name [, ...] ), ]
    row_expression PASSING [BY {REF|VALUE}] document_expression [BY {REF|VALUE}]
    COLUMNS name { type [PATH column_expression] [DEFAULT default_expression] [NOT NULL | NULL]
                  | FOR ORDINALITY }
            [, ...]
) → setof record (NOT SUPPORTED)

9.15.4. Mapping Tables to XML (NOT SUPPORTED)

The following functions map the contents of relational tables to XML values. They can be thought of as XML export functionality:

table_to_xml ( table regclass, nulls boolean,
               tableforest boolean, targetns text ) → xml
query_to_xml ( query text, nulls boolean,
               tableforest boolean, targetns text ) → xml
cursor_to_xml ( cursor refcursor, count integer, nulls boolean,
                tableforest boolean, targetns text ) → xml

## 9.16. JSON Functions and Operators {#json-functions}

This section describes:

functions and operators for processing and creating JSON data

the SQL/JSON path language

To learn more about the SQL/JSON standard, see [sqltr-19075-6]. For details on JSON types supported in PostgreSQL, see Section 8.14.

9.16.1. Processing and Creating JSON Data

Table 9.44 shows the operators that are available for use with JSON data types (see Section 8.14). In addition, the usual comparison operators shown in Table 9.1 are available for jsonb, though not for json. The comparison operators follow the ordering rules for B-tree operations outlined in Section 8.14.4. See also Section 9.21 for the aggregate function json_agg which aggregates record values as JSON, the aggregate function json_object_agg which aggregates pairs of values into a JSON object, and their jsonb equivalents, jsonb_agg and jsonb_object_agg.

Table 9.44. json and jsonb Operators

#|
||Operator|Description|Example(s)||
||json -> integer → json  
jsonb -> integer → jsonb|
Extracts n'th element of JSON array (array elements are indexed from zero, but negative integers count from the end).|
```sql
'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json -> 2 → {"c":"baz"}
'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json -> -3 → {"a":"foo"}
```||
||json -> text → json  
jsonb -> text → jsonb|
Extracts JSON object field with the given key.|
```sql
'{"a": {"b":"foo"}}'::json -> 'a' → {"b":"foo"}
```||
||json ->> integer → text  
jsonb ->> integer → text|
Extracts n'th element of JSON array, as text.|
```sql
'[1,2,3]'::json ->> 2 → 3
```||
||json ->> text → text  
jsonb ->> text → text|
Extracts JSON object field with the given key, as text.|
```sql
'{"a":1,"b":2}'::json ->> 'b' → 2
```||
||json #> text[] → json  
jsonb #> text[] → jsonb|
Extracts JSON sub-object at the specified path, where path elements can be either field keys or array indexes. (NOT SUPPORTED)|
```sql
#'{"a": {"b": ["foo","bar"]}}'::json #> '{a,b,1}' → "bar"
```||
||json #>> text[] → text  
jsonb #>> text[] → text|
Extracts JSON sub-object at the specified path as text. (NOT SUPPORTED)|
```sql
#'{"a": {"b": ["foo","bar"]}}'::json #>> '{a,b,1}' → bar
```||
|#

Note
The field/element/path extraction operators return NULL, rather than failing, if the JSON input does not have the right structure to match the request; for example if no such key or array element exists.

Some further operators exist only for jsonb, as shown in Table 9.45. Section 8.14.4 describes how these operators can be used to effectively search indexed jsonb data.

Table 9.45. Additional jsonb Operators

#|
||Operator|Description|Example(s)||
||jsonb @> jsonb → boolean|
Does the first JSON value contain the second? (See Section 8.14.3 for details about containment.)|
```sql
'{"a":1, "b":2}'::jsonb @> '{"b":2}'::jsonb → true
```||
||jsonb <@ jsonb → boolean|
Is the first JSON value contained in the second?|
```sql
'{"b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb → true
```||
||jsonb ? text → boolean|
Does the text string exist as a top-level key or array element within the JSON value?|
```sql
'{"a":1, "b":2}'::jsonb ? 'b' → true
'["a", "b", "c"]'::jsonb ? 'b' → true
```||
||jsonb ?\| text[] → boolean|
Do any of the strings in the text array exist as top-level keys or array elements?|
```sql
'{"a":1, "b":2, "c":3}'::jsonb ?| array['b', 'd'] → true
```||
||jsonb ?& text[] → boolean|
Do all of the strings in the text array exist as top-level keys or array elements?|
```sql
'["a", "b", "c"]'::jsonb ?& array['a', 'b'] → true
```||
||jsonb \|\| jsonb → jsonb|
Concatenates two jsonb values. Concatenating two arrays generates an array containing all the elements of each input. Concatenating two objects generates an object containing the union of their keys, taking the second object's value when there are duplicate keys. All other cases are treated by converting a non-array input into a single-element array, and then proceeding as for two arrays. Does not operate recursively: only the top-level array or object structure is merged.|
```sql
'["a", "b"]'::jsonb || '["a", "d"]'::jsonb → ["a", "b", "a", "d"]
'{"a": "b"}'::jsonb || '{"c": "d"}'::jsonb → {"a": "b", "c": "d"}
'[1, 2]'::jsonb || '3'::jsonb → [1, 2, 3]
'{"a": "b"}'::jsonb || '42'::jsonb → [{"a": "b"}, 42]
```

To append an array to another array as a single entry, wrap it in an additional layer of array, for example:

```sql
'[1, 2]'::jsonb || jsonb_build_array('[3, 4]'::jsonb) → [1, 2, [3, 4]]
```||
||jsonb - text → jsonb|
Deletes a key (and its value) from a JSON object, or matching string value(s) from a JSON array.|
```sql
'{"a": "b", "c": "d"}'::jsonb - 'a' → {"c": "d"}
'["a", "b", "c", "b"]'::jsonb - 'b' → ["a", "c"]
```||
||jsonb - text[] → jsonb|
Deletes all matching keys or array elements from the left operand. (NOT SUPPORTED)|
```sql
#'{"a": "b", "c": "d"}'::jsonb - '{a,c}'::text[] → {}
```||
||jsonb - integer → jsonb|
Deletes the array element with specified index (negative integers count from the end). Throws an error if JSON value is not an array.|
```sql
'["a", "b"]'::jsonb - 1 → ["a"]
```||
||jsonb #- text[] → jsonb|
Deletes the field or array element at the specified path, where path elements can be either field keys or array indexes.|
```sql
'["a", {"b":1}]'::jsonb #- '{1,b}' → ["a", {}]
```||
||jsonb @? jsonpath → boolean|
Does JSON path return any item for the specified JSON value?|
```sql
'{"a":[1,2,3,4,5]}'::jsonb @? '$.a[*] ? (@ > 2)' → true
```||
||jsonb @@ jsonpath → boolean|
Returns the result of a JSON path predicate check for the specified JSON value. Only the first item of the result is taken into account. If the result is not Boolean, then NULL is returned.|
```sql
'{"a":[1,2,3,4,5]}'::jsonb @@ '$.a[*] > 2' → true
```||
|#

Note
The jsonpath operators @? and @@ suppress the following errors: missing object field or array element, unexpected JSON item type, datetime and numeric errors. The jsonpath-related functions described below can also be told to suppress these types of errors. This behavior might be helpful when searching JSON document collections of varying structure.

Table 9.46 shows the functions that are available for constructing json and jsonb values.

Table 9.46. JSON Creation Functions

#|
||Function|Description|Example(s)||
||to_json ( anyelement ) → json  
to_jsonb ( anyelement ) → jsonb|
Converts any SQL value to json or jsonb. Arrays and composites are converted recursively to arrays and objects (multidimensional arrays become arrays of arrays in JSON). Otherwise, if there is a cast from the SQL data type to json, the cast function will be used to perform the conversion;[a] otherwise, a scalar JSON value is produced. For any scalar other than a number, a Boolean, or a null value, the text representation will be used, with escaping as necessary to make it a valid JSON string value. (NOT SUPPORTED)|
```sql
#to_json('Fred said "Hi."'::text) → "Fred said \"Hi.\""
#to_jsonb(row(42, 'Fred said "Hi."'::text)) → {"f1": 42, "f2": "Fred said \"Hi.\""}
```||
||array_to_json ( anyarray [, boolean ] ) → json|
Converts an SQL array to a JSON array. The behavior is the same as to_json except that line feeds will be added between top-level array elements if the optional boolean parameter is true.|
```sql
array_to_json('{{1,5},{99,100}}'::int[]) → [[1,5],[99,100]]
```||
||row_to_json ( record [, boolean ] ) → json|
Converts an SQL composite value to a JSON object. The behavior is the same as to_json except that line feeds will be added between top-level elements if the optional boolean parameter is true. (NOT SUPPORTED)|
```sql
#row_to_json(row(1,'foo')) → {"f1":1,"f2":"foo"}
```||
||json_build_array ( VARIADIC "any" ) → json  
jsonb_build_array ( VARIADIC "any" ) → jsonb|
Builds a possibly-heterogeneously-typed JSON array out of a variadic argument list. Each argument is converted as per to_json or to_jsonb. (NOT SUPPORTED)|
```sql
json_build_array(1, 2, 'foo', 4, 5) → [1, 2, "foo", 4, 5]
```||
||json_build_object ( VARIADIC "any" ) → json  
jsonb_build_object ( VARIADIC "any" ) → jsonb|
Builds a JSON object out of a variadic argument list. By convention, the argument list consists of alternating keys and values. Key arguments are coerced to text; value arguments are converted as per to_json or to_jsonb. (NOT SUPPORTED)|
```sql
#json_build_object('foo', 1, 2, row(3,'bar')) → {"foo" : 1, "2" : {"f1":3,"f2":"bar"}}
```||
||json_object ( text[] ) → json  
jsonb_object ( text[] ) → jsonb|
Builds a JSON object out of a text array. The array must have either exactly one dimension with an even number of members, in which case they are taken as alternating key/value pairs, or two dimensions such that each inner array has exactly two elements, which are taken as a key/value pair. All values are converted to JSON strings.|
```sql
json_object('{a, 1, b, "def", c, 3.5}') → {"a" : "1", "b" : "def", "c" : "3.5"}
json_object('{{a, 1}, {b, "def"}, {c, 3.5}}') → {"a" : "1", "b" : "def", "c" : "3.5"}
```||
||json_object ( keys text[], values text[] ) → json  
jsonb_object ( keys text[], values text[] ) → jsonb|
This form of json_object takes keys and values pairwise from separate text arrays. Otherwise it is identical to the one-argument form.|
```sql
json_object('{a,b}', '{1,2}') → {"a" : "1", "b" : "2"}
```||
|#

Table 9.47 shows the functions that are available for processing json and jsonb values.

Table 9.47. JSON Processing Functions

#|
||Function|Description|Example(s)||
||json_array_elements ( json ) → setof json  
jsonb_array_elements ( jsonb ) → setof jsonb|
Expands the top-level JSON array into a set of JSON values.|
```sql
SELECT * FROM json_array_elements('[1,true, [2,false]]') as a → [
1
true
[2,false]
]
```||
||json_array_elements_text ( json ) → setof text  
jsonb_array_elements_text ( jsonb ) → setof text|
Expands the top-level JSON array into a set of text values.|
```sql
SELECT * FROM json_array_elements_text('["foo", "bar"]') as a → [
foo
bar
]
```||
||json_array_length ( json ) → integer  
jsonb_array_length ( jsonb ) → integer|
Returns the number of elements in the top-level JSON array.|
```sql
json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') → 5
jsonb_array_length('[]') → 0
```||
||json_each ( json ) → setof record ( key text, value json )  
jsonb_each ( jsonb ) → setof record ( key text, value jsonb )|
Expands the top-level JSON object into a set of key/value pairs.|

```sql
SELECT * FROM json_each('{"a":"foo", "b":"bar"}') as a → [
a,"foo"
b,"bar"
]
```||
||json_each_text ( json ) → setof record ( key text, value text )  
jsonb_each_text ( jsonb ) → setof record ( key text, value text )|
Expands the top-level JSON object into a set of key/value pairs. The returned values will be of type text.|
```sql
SELECT * FROM json_each_text('{"a":"foo", "b":"bar"}') as a → [
a,foo
b,bar
]
```||
||json_extract_path ( from_json json, VARIADIC path_elems text[] ) → json  
jsonb_extract_path ( from_json jsonb, VARIADIC path_elems text[] ) → jsonb|
Extracts JSON sub-object at the specified path. (This is functionally equivalent to the #> operator, but writing the path out as a variadic list can be more convenient in some cases.) (NOT SUPPORTED)|
```sql
json_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', 'f4', 'f6') → "foo"
```||
||json_extract_path_text ( from_json json, VARIADIC path_elems text[] ) → text  
jsonb_extract_path_text ( from_json jsonb, VARIADIC path_elems text[] ) → text|
Extracts JSON sub-object at the specified path as text. (This is functionally equivalent to the #>> operator.) (NOT SUPPORTED)|
```sql
json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}', 'f4', 'f6') → foo
```||
||json_object_keys ( json ) → setof text  
jsonb_object_keys ( jsonb ) → setof text|
Returns the set of keys in the top-level JSON object.|
```sql
SELECT * FROM json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}') as a → [
f1
f2
]
```||
||json_populate_record ( base anyelement, from_json json ) → anyelement  
jsonb_populate_record ( base anyelement, from_json jsonb ) → anyelement|
Expands the top-level JSON object to a row having the composite type of the base argument. The JSON object is scanned for fields whose names match column names of the output row type, and their values are inserted into those columns of the output. (Fields that do not correspond to any output column name are ignored.) In typical use, the value of base is just NULL, which means that any output columns that do not match any object field will be filled with nulls. However, if base isn't NULL then the values it contains will be used for unmatched columns. (NOT SUPPORTED)

To convert a JSON value to the SQL type of an output column, the following rules are applied in sequence:

A JSON null value is converted to an SQL null in all cases.

If the output column is of type json or jsonb, the JSON value is just reproduced exactly.

If the output column is a composite (row) type, and the JSON value is a JSON object, the fields of the object are converted to columns of the output row type by recursive application of these rules.

Likewise, if the output column is an array type and the JSON value is a JSON array, the elements of the JSON array are converted to elements of the output array by recursive application of these rules.

Otherwise, if the JSON value is a string, the contents of the string are fed to the input conversion function for the column's data type.

Otherwise, the ordinary text representation of the JSON value is fed to the input conversion function for the column's data type.

While the example below uses a constant JSON value, typical use would be to reference a json or jsonb column laterally from another table in the query's FROM clause. Writing json_populate_record in the FROM clause is good practice, since all of the extracted columns are available for use without duplicate function calls.

```sql
#CREATE TYPE subrowtype as (d int, e text); 
#CREATE type myrowtype as (a int, b text[], c subrowtype);
#SELECT * FROM json_populate_record(null::myrowtype, '{"a": 1, "b": ["2", "a b"], "c": {"d": 4, "e": "a b c"}, "x": "foo"}') → 1,{2,"a b"},(4,"a b c")
```||
||json_populate_recordset ( base anyelement, from_json json ) → setof anyelement  
jsonb_populate_recordset ( base anyelement, from_json jsonb ) → setof anyelement|
Expands the top-level JSON array of objects to a set of rows having the composite type of the base argument. Each element of the JSON array is processed as described above for json[b]_populate_record. (NOT SUPPORTED)|
```sql
#CREATE TYPE twoints as (a int, b int);
#SELECT * FROM json_populate_recordset(null::twoints, '[{"a":1,"b":2}, {"a":3,"b":4}]') → [
1,2
3,4
]
```||
||json_to_record ( json ) → record  
jsonb_to_record ( jsonb ) → record|
Expands the top-level JSON object to a row having the composite type defined by an AS clause. (As with all functions returning record, the calling query must explicitly define the structure of the record with an AS clause.) The output record is filled from fields of the JSON object, in the same way as described above for json[b]_populate_record. Since there is no input record value, unmatched columns are always filled with nulls. (NOT SUPPORTED)|
```sql
#CREATE TYPE myrowtype as (a int, b text);
#SELECT * FROM json_to_record('{"a":1,"b":[1,2,3],"c":[1,2,3],"e":"bar","r": {"a": 123, "b": "a b c"}}') as x(a int, b text, c int[], d text, r myrowtype) → 1,[1,2,3],{1,2,3},(123,"a b c")
```||
||json_to_recordset ( json ) → setof record  
jsonb_to_recordset ( jsonb ) → setof record|
Expands the top-level JSON array of objects to a set of rows having the composite type defined by an AS clause. (As with all functions returning record, the calling query must explicitly define the structure of the record with an AS clause.) Each element of the JSON array is processed as described above for json[b]_populate_record. (NOT SUPPORTED)|
```SQL
#SELECT * from json_to_recordset('[{"a":1,"b":"foo"}, {"a":"2","c":"bar"}]') as x(a int, b text) → [
1,foo
2,
]
```||
||jsonb_set ( target jsonb, path text[], new_value jsonb [, create_if_missing boolean ] ) → jsonb|
Returns target with the item designated by path replaced by new_value, or with new_value added if create_if_missing is true (which is the default) and the item designated by path does not exist. All earlier steps in the path must exist, or the target is returned unchanged. As with the path oriented operators, negative integers that appear in the path count from the end of JSON arrays. If the last path step is an array index that is out of range, and create_if_missing is true, the new value is added at the beginning of the array if the index is negative, or at the end of the array if it is positive.|
```sql
jsonb_set('[{"f1":1,"f2":null},2,null,3]', '{0,f1}', '[2,3,4]', false) → [{"f1": [2, 3, 4], "f2": null}, 2, null, 3]
#jsonb_set('[{"f1":1,"f2":null},2]', '{0,f3}', '[2,3,4]') → [{"f1": 1, "f2": null, "f3": [2, 3, 4]}, 2]
```||
||jsonb_set_lax ( target jsonb, path text[], new_value jsonb [, create_if_missing boolean [, null_value_treatment text ]] ) → jsonb|
If new_value is not NULL, behaves identically to jsonb_set. Otherwise behaves according to the value of null_value_treatment which must be one of 'raise_exception', 'use_json_null', 'delete_key', or 'return_target'. The default is 'use_json_null'.|
```sql
#jsonb_set_lax('[{"f1":1,"f2":null},2,null,3]', '{0,f1}', null) → [{"f1": null, "f2": null}, 2, null, 3]
jsonb_set_lax('[{"f1":99,"f2":null},2]', '{0,f3}', null, true, 'return_target') → [{"f1": 99, "f2": null}, 2]
```||
||jsonb_insert ( target jsonb, path text[], new_value jsonb [, insert_after boolean ] ) → jsonb|
Returns target with new_value inserted. If the item designated by the path is an array element, new_value will be inserted before that item if insert_after is false (which is the default), or after it if insert_after is true. If the item designated by the path is an object field, new_value will be inserted only if the object does not already contain that key. All earlier steps in the path must exist, or the target is returned unchanged. As with the path oriented operators, negative integers that appear in the path count from the end of JSON arrays. If the last path step is an array index that is out of range, the new value is added at the beginning of the array if the index is negative, or at the end of the array if it is positive.|
```sql
#jsonb_insert('{"a": [0,1,2]}', '{a, 1}', '"new_value"') → {"a": [0, "new_value", 1, 2]}
jsonb_insert('{"a": [0,1,2]}', '{a, 1}', '"new_value"', true) → {"a": [0, 1, "new_value", 2]}
```||
||json_strip_nulls ( json ) → json  
jsonb_strip_nulls ( jsonb ) → jsonb|
Deletes all object fields that have null values from the given JSON value, recursively. Null values that are not object fields are untouched.|
```sql
json_strip_nulls('[{"f1":1, "f2":null}, 2, null, 3]') → [{"f1":1},2,null,3]
```||
||jsonb_path_exists ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → boolean|
Checks whether the JSON path returns any item for the specified JSON value. If the vars argument is specified, it must be a JSON object, and its fields provide named values to be substituted into the jsonpath expression. If the silent argument is specified and is true, the function suppresses the same errors as the @? and @@ operators do.|
```sql
jsonb_path_exists('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}', false) → true
```||
||jsonb_path_match ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → boolean|
Returns the result of a JSON path predicate check for the specified JSON value. Only the first item of the result is taken into account. If the result is not Boolean, then NULL is returned. The optional vars and silent arguments act the same as for jsonb_path_exists.|
```sql
jsonb_path_match('{"a":[1,2,3,4,5]}', 'exists($.a[*] ? (@ >= $min && @ <= $max))', '{"min":2, "max":4}', false) → true
```||
||jsonb_path_query ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → setof jsonb|
Returns all JSON items returned by the JSON path for the specified JSON value. The optional vars and silent arguments act the same as for jsonb_path_exists.|
```sql
SELECT * FROM jsonb_path_query('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}', false) as a → [
2
3
4
]
```||
||jsonb_path_query_array ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → jsonb|
Returns all JSON items returned by the JSON path for the specified JSON value, as a JSON array. The optional vars and silent arguments act the same as for jsonb_path_exists.|
```sql
jsonb_path_query_array('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}', false) → [2, 3, 4]
```||
||jsonb_path_query_first ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → jsonb|
Returns the first JSON item returned by the JSON path for the specified JSON value. Returns NULL if there are no results. The optional vars and silent arguments act the same as for jsonb_path_exists.|
```sql
jsonb_path_query_first('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}', false) → 2
```||
||jsonb_path_exists_tz ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → boolean  
jsonb_path_match_tz ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → boolean  
jsonb_path_query_tz ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → setof jsonb  
jsonb_path_query_array_tz ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → jsonb  
jsonb_path_query_first_tz ( target jsonb, path jsonpath [, vars jsonb [, silent boolean ]] ) → jsonb|
These functions act like their counterparts described above without the _tz suffix, except that these functions support comparisons of date/time values that require timezone-aware conversions. The example below requires interpretation of the date-only value 2015-08-02 as a timestamp with time zone, so the result depends on the current TimeZone setting. Due to this dependency, these functions are marked as stable, which means these functions cannot be used in indexes. Their counterparts are immutable, and so can be used in indexes; but they will throw errors if asked to make such comparisons. (NOT SUPPORTED)|
```sql
jsonb_path_exists_tz('["2015-08-01 12:00:00-05"]', '$[*] ? (@.datetime() < "2015-08-02".datetime())', '{}', false) → true
```||
||jsonb_pretty ( jsonb ) → text|
Converts the given JSON value to pretty-printed, indented text.|
```sql
jsonb_pretty('[{"f1":1,"f2":null}, 2]') → """[
    {
        "f1": 1,
        "f2": null
    },
    2
]"""
```||
||json_typeof ( json ) → text  
jsonb_typeof ( jsonb ) → text|
Returns the type of the top-level JSON value as a text string. Possible types are object, array, string, number, boolean, and null. (The null result should not be confused with an SQL NULL; see the examples.)|
```sql
json_typeof('-123.4') → number
json_typeof('null'::json) → null
json_typeof(NULL::json) IS NULL → true
```||
|#

9.16.2. The SQL/JSON Path Language

SQL/JSON path expressions specify the items to be retrieved from the JSON data, similar to XPath expressions used for SQL access to XML. In PostgreSQL, path expressions are implemented as the jsonpath data type and can use any elements described in Section 8.14.7.

JSON query functions and operators pass the provided path expression to the path engine for evaluation. If the expression matches the queried JSON data, the corresponding JSON item, or set of items, is returned. Path expressions are written in the SQL/JSON path language and can include arithmetic expressions and functions.

A path expression consists of a sequence of elements allowed by the jsonpath data type. The path expression is normally evaluated from left to right, but you can use parentheses to change the order of operations. If the evaluation is successful, a sequence of JSON items is produced, and the evaluation result is returned to the JSON query function that completes the specified computation.

To refer to the JSON value being queried (the context item), use the $ variable in the path expression. It can be followed by one or more accessor operators, which go down the JSON structure level by level to retrieve sub-items of the context item. Each operator that follows deals with the result of the previous evaluation step.

For example, suppose you have some JSON data from a GPS tracker that you would like to parse, such as:

{
  "track": {
    "segments": [
      {
        "location":   [ 47.763, 13.4034 ],
        "start time": "2018-10-14 10:05:14",
        "HR": 73
      },
      {
        "location":   [ 47.706, 13.2635 ],
        "start time": "2018-10-14 10:39:21",
        "HR": 135
      }
    ]
  }
}
To retrieve the available track segments, you need to use the .key accessor operator to descend through surrounding JSON objects:

$.track.segments
To retrieve the contents of an array, you typically use the [*] operator. For example, the following path will return the location coordinates for all the available track segments:

$.track.segments[*].location
To return the coordinates of the first segment only, you can specify the corresponding subscript in the [] accessor operator. Recall that JSON array indexes are 0-relative:

$.track.segments[0].location
The result of each path evaluation step can be processed by one or more jsonpath operators and methods listed in Section 9.16.2.2. Each method name must be preceded by a dot. For example, you can get the size of an array:

$.track.segments.size()
More examples of using jsonpath operators and methods within path expressions appear below in Section 9.16.2.2.

When defining a path, you can also use one or more filter expressions that work similarly to the WHERE clause in SQL. A filter expression begins with a question mark and provides a condition in parentheses:

? (condition)
Filter expressions must be written just after the path evaluation step to which they should apply. The result of that step is filtered to include only those items that satisfy the provided condition. SQL/JSON defines three-valued logic, so the condition can be true, false, or unknown. The unknown value plays the same role as SQL NULL and can be tested for with the is unknown predicate. Further path evaluation steps use only those items for which the filter expression returned true.

The functions and operators that can be used in filter expressions are listed in Table 9.49. Within a filter expression, the @ variable denotes the value being filtered (i.e., one result of the preceding path step). You can write accessor operators after @ to retrieve component items.

For example, suppose you would like to retrieve all heart rate values higher than 130. You can achieve this using the following expression:

$.track.segments[*].HR ? (@ > 130)
To get the start times of segments with such values, you have to filter out irrelevant segments before returning the start times, so the filter expression is applied to the previous step, and the path used in the condition is different:

$.track.segments[*] ? (@.HR > 130)."start time"
You can use several filter expressions in sequence, if required. For example, the following expression selects start times of all segments that contain locations with relevant coordinates and high heart rate values:

$.track.segments[*] ? (@.location[1] < 13.4) ? (@.HR > 130)."start time"
Using filter expressions at different nesting levels is also allowed. The following example first filters all segments by location, and then returns high heart rate values for these segments, if available:

$.track.segments[*] ? (@.location[1] < 13.4).HR ? (@ > 130)
You can also nest filter expressions within each other:

$.track ? (exists(@.segments[*] ? (@.HR > 130))).segments.size()
This expression returns the size of the track if it contains any segments with high heart rate values, or an empty sequence otherwise.

PostgreSQL's implementation of the SQL/JSON path language has the following deviations from the SQL/JSON standard:

A path expression can be a Boolean predicate, although the SQL/JSON standard allows predicates only in filters. This is necessary for implementation of the @@ operator. For example, the following jsonpath expression is valid in PostgreSQL:

$.track.segments[*].HR < 70
There are minor differences in the interpretation of regular expression patterns used in like_regex filters, as described in Section 9.16.2.3.

9.16.2.1. Strict And Lax Modes

When you query JSON data, the path expression may not match the actual JSON data structure. An attempt to access a non-existent member of an object or element of an array results in a structural error. SQL/JSON path expressions have two modes of handling structural errors:

lax (default) — the path engine implicitly adapts the queried data to the specified path. Any remaining structural errors are suppressed and converted to empty SQL/JSON sequences.

strict — if a structural error occurs, an error is raised.

The lax mode facilitates matching of a JSON document structure and path expression if the JSON data does not conform to the expected schema. If an operand does not match the requirements of a particular operation, it can be automatically wrapped as an SQL/JSON array or unwrapped by converting its elements into an SQL/JSON sequence before performing this operation. Besides, comparison operators automatically unwrap their operands in the lax mode, so you can compare SQL/JSON arrays out-of-the-box. An array of size 1 is considered equal to its sole element. Automatic unwrapping is not performed only when:

The path expression contains type() or size() methods that return the type and the number of elements in the array, respectively.

The queried JSON data contain nested arrays. In this case, only the outermost array is unwrapped, while all the inner arrays remain unchanged. Thus, implicit unwrapping can only go one level down within each path evaluation step.

For example, when querying the GPS data listed above, you can abstract from the fact that it stores an array of segments when using the lax mode:

lax $.track.segments.location
In the strict mode, the specified path must exactly match the structure of the queried JSON document to return an SQL/JSON item, so using this path expression will cause an error. To get the same result as in the lax mode, you have to explicitly unwrap the segments array:

strict $.track.segments[*].location
The .\*\* accessor can lead to surprising results when using the lax mode. For instance, the following query selects every HR value twice:

lax $.\*\*.HR
This happens because the .\*\* accessor selects both the segments array and each of its elements, while the .HR accessor automatically unwraps arrays when using the lax mode. To avoid surprising results, we recommend using the .\*\* accessor only in the strict mode. The following query selects each HR value just once:

strict $.\*\*.HR

9.16.2.2. SQL/JSON Path Operators And Methods

Table 9.48 shows the operators and methods available in jsonpath. Note that while the unary operators and methods can be applied to multiple values resulting from a preceding path step, the binary operators (addition etc.) can only be applied to single values.

Table 9.48. jsonpath Operators and Methods

#|
||Operator/Method|Description|Example(s)||
||number + number → number|
Addition|
```sql
jsonb_path_query_array('[2]', '$[0] + 3', '{}', false) → [5]
```||
||\+ number → number|
Unary plus (no operation); unlike addition, this can iterate over multiple values|
```sql
jsonb_path_query_array('{"x": [2,3,4]}', '+ $.x', '{}', false) → [2, 3, 4]
```||
||number - number → number|
Subtraction|
```sql
jsonb_path_query_array('[2]', '7 - $[0]', '{}', false) → [5]
```||
||\- number → number|
Negation; unlike subtraction, this can iterate over multiple values|
```sql
jsonb_path_query_array('{"x": [2,3,4]}', '- $.x', '{}', false) → [-2, -3, -4]
```||
||number * number → number|
Multiplication|
```sql
jsonb_path_query_array('[4]', '2 * $[0]', '{}', false) → [8]
```||
||number / number → number|
Division|
```sql
jsonb_path_query_array('[8.5]', '$[0] / 2', '{}', false) → [4.2500000000000000]
```||
||number % number → number|
Modulo (remainder)|
```sql
jsonb_path_query_array('[32]', '$[0] % 10', '{}', false) → [2]
```||
||value . type() → string|
Type of the JSON item (see json_typeof)|
```sql
jsonb_path_query_array('[1, "2", {}]', '$[*].type()', '{}', false) → ["number", "string", "object"]
```||
||value . size() → number|
Size of the JSON item (number of array elements, or 1 if not an array)|
```sql
jsonb_path_query_array('{"m": [11, 15]}', '$.m.size()', '{}', false) → [2]
```||
||value . double() → number|
Approximate floating-point number converted from a JSON number or string|
```sql
jsonb_path_query_array('{"len": "1.9"}', '$.len.double() * 2', '{}', false) → [3.8]
```||
||number . ceiling() → number|
Nearest integer greater than or equal to the given number|
```sql
jsonb_path_query_array('{"h": 1.3}', '$.h.ceiling()', '{}', false) → [2]
```||
||number . floor() → number|
Nearest integer less than or equal to the given number|
```sql
jsonb_path_query_array('{"h": 1.7}', '$.h.floor()', '{}', false) → [1]
```||
||number . abs() → number|
Absolute value of the given number|
```sql
jsonb_path_query_array('{"z": -0.3}', '$.z.abs()', '{}', false) → [0.3]
```||
||string . datetime() → datetime_type (see note)|
Date/time value converted from a string (NOT SUPPORTED)|
```sql
jsonb_path_query_array('["2015-8-1", "2015-08-12"]', '$[*] ? (@.datetime() < "2015-08-2".datetime())', '{}', false) → ["2015-8-1"]
```||
||string . datetime(template) → datetime_type (see note)|
Date/time value converted from a string using the specified to_timestamp template (NOT SUPPORTED)|
```sql
jsonb_path_query_array('["12:30", "18:40"]', '$[*].datetime("HH24:MI")', '{}', false) → ["12:30:00", "18:40:00"]
```||
||object . keyvalue() → array|
The object's key-value pairs, represented as an array of objects containing three fields: "key", "value", and "id"; "id" is a unique identifier of the object the key-value pair belongs to|
```sql
jsonb_path_query_array('{"x": "20", "y": 32}', '$.keyvalue()', '{}', false) → [{"id": 0, "key": "x", "value": "20"}, {"id": 0, "key": "y", "value": 32}]
```||
|#

Note
The result type of the datetime() and datetime(template) methods can be date, timetz, time, timestamptz, or timestamp. Both methods determine their result type dynamically.

The datetime() method sequentially tries to match its input string to the ISO formats for date, timetz, time, timestamptz, and timestamp. It stops on the first matching format and emits the corresponding data type.

The datetime(template) method determines the result type according to the fields used in the provided template string.

The datetime() and datetime(template) methods use the same parsing rules as the to_timestamp SQL function does (see Section 9.8), with three exceptions. First, these methods don't allow unmatched template patterns. Second, only the following separators are allowed in the template string: minus sign, period, solidus (slash), comma, apostrophe, semicolon, colon and space. Third, separators in the template string must exactly match the input string.

If different date/time types need to be compared, an implicit cast is applied. A date value can be cast to timestamp or timestamptz, timestamp can be cast to timestamptz, and time to timetz. However, all but the first of these conversions depend on the current TimeZone setting, and thus can only be performed within timezone-aware jsonpath functions.

Table 9.49 shows the available filter expression elements.

Table 9.49. jsonpath Filter Expression Elements

#|
||Predicate/Value|Description|Example(s)||
||value == value → boolean|
Equality comparison (this, and the other comparison operators, work on all JSON scalar values)|
```sql
jsonb_path_query_array('[1, "a", 1, 3]', '$[*] ? (@ == 1)', '{}', false) → [1, 1]
jsonb_path_query_array('[1, "a", 1, 3]', '$[*] ? (@ == "a")', '{}', false) → ["a"]
```||
||value != value → boolean  
value <> value → boolean|
Non-equality comparison|
```sql
jsonb_path_query_array('[1, 2, 1, 3]', '$[*] ? (@ != 1)', '{}', false) → [2, 3]
jsonb_path_query_array('["a", "b", "c"]', '$[*] ? (@ <> "b")', '{}', false) → ["a", "c"]
```||
||value < value → boolean|
Less-than comparison|
```sql
jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ < 2)', '{}', false) → [1]
```||
||value <= value → boolean|
Less-than-or-equal-to comparison|
```sql
jsonb_path_query_array('["a", "b", "c"]', '$[*] ? (@ <= "b")', '{}', false) → ["a", "b"]
```||
||value > value → boolean|
Greater-than comparison|
```sql
jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ > 2)', '{}', false) → [3]
```||
||value >= value → boolean|
Greater-than-or-equal-to comparison|
```sql
jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ >= 2)', '{}', false) → [2, 3]
```||
||true → boolean|
JSON constant true|
```sql
jsonb_path_query_array('[{"name": "John", "parent": false}, {"name": "Chris", "parent": true}]', '$[*] ? (@.parent == true)', '{}', false) → [{"name": "Chris", "parent": true}]
```||
||false → boolean|
JSON constant false|
```sql
jsonb_path_query_array('[{"name": "John", "parent": false}, {"name": "Chris", "parent": true}]', '$[*] ? (@.parent == false)', '{}', false) → [{"name": "John", "parent": false}]
```||
||null → value|
JSON constant null (note that, unlike in SQL, comparison to null works normally)|
```sql
jsonb_path_query_array('[{"name": "Mary", "job": null}, {"name": "Michael", "job": "driver"}]', '$[*] ? (@.job == null) .name', '{}', false) → ["Mary"]
```||
||boolean && boolean → boolean|
Boolean AND|
```sql
jsonb_path_query_array('[1, 3, 7]', '$[*] ? (@ > 1 && @ < 5)', '{}', false) → [3]
```||
||boolean \|\| boolean → boolean|
Boolean OR|
```sql
jsonb_path_query_array('[1, 3, 7]', '$[*] ? (@ < 1 || @ > 5)', '{}', false) → [7]
```||
||! boolean → boolean|
Boolean NOT|
```sql
jsonb_path_query_array('[1, 3, 7]', '$[*] ? (!(@ < 5))', '{}', false) → [7]
```||
||boolean is unknown → boolean|
Tests whether a Boolean condition is unknown.|
```sql
jsonb_path_query_array('[-1, 2, 7, "foo"]', '$[*] ? ((@ > 0) is unknown)', '{}', false) → ["foo"]
```||
||string like_regex string [ flag string ] → boolean|
Tests whether the first operand matches the regular expression given by the second operand, optionally with modifications described by a string of flag characters (see Section 9.16.2.3).|
```sql
jsonb_path_query_array('["abc", "abd", "aBdC", "abdacb", "babc"]', '$[*] ? (@ like_regex "^ab.*c")', '{}', false) → ["abc", "abdacb"]
jsonb_path_query_array('["abc", "abd", "aBdC", "abdacb", "babc"]', '$[*] ? (@ like_regex "^ab.*c" flag "i")', '{}', false) → ["abc", "aBdC", "abdacb"]
```||
||string starts with string → boolean|
Tests whether the second operand is an initial substring of the first operand.|
```sql
jsonb_path_query_array('["John Smith", "Mary Stone", "Bob Johnson"]', '$[*] ? (@ starts with "John")', '{}', false) → ["John Smith"]
```||
||exists ( path_expression ) → boolean|
Tests whether a path expression matches at least one SQL/JSON item. Returns unknown if the path expression would result in an error; the second example uses this to avoid a no-such-key error in strict mode.|
```sql
jsonb_path_query_array('{"x": [1, 2], "y": [2, 4]}', 'strict $.* ? (exists (@ ? (@[*] > 2)))', '{}', false) → [[2, 4]]
jsonb_path_query_array('{"value": 41}', 'strict $ ? (exists (@.name)) .name', '{}', false) → []
```||
|#

9.16.2.3. SQL/JSON Regular Expressions
SQL/JSON path expressions allow matching text to a regular expression with the like_regex filter. For example, the following SQL/JSON path query would case-insensitively match all strings in an array that start with an English vowel:

$[*] ? (@ like_regex "^[aeiou]" flag "i")

The optional flag string may include one or more of the characters i for case-insensitive match, m to allow ^ and $ to match at newlines, s to allow . to match a newline, and q to quote the whole pattern (reducing the behavior to a simple substring match).

The SQL/JSON standard borrows its definition for regular expressions from the LIKE_REGEX operator, which in turn uses the XQuery standard. PostgreSQL does not currently support the LIKE_REGEX operator. Therefore, the like_regex filter is implemented using the POSIX regular expression engine described in Section 9.7.3. This leads to various minor discrepancies from standard SQL/JSON behavior, which are cataloged in Section 9.7.3.8. Note, however, that the flag-letter incompatibilities described there do not apply to SQL/JSON, as it translates the XQuery flag letters to match what the POSIX engine expects.

Keep in mind that the pattern argument of like_regex is a JSON path string literal, written according to the rules given in Section 8.14.7. This means in particular that any backslashes you want to use in the regular expression must be doubled. For example, to match string values of the root document that contain only digits:

`$.* ? (@ like_regex "^\\d+$")`

## 9.17. Sequence Manipulation Functions (NOT SUPPORTED) {#sequence-manipulation-functions}

## 9.18. Conditional Expressions {#conditional-expressions}

9.18.1. CASE
The SQL CASE expression is a generic conditional expression, similar to if/else statements in other programming languages:

CASE WHEN condition THEN result
     [WHEN ...]
     [ELSE result]
END
CASE clauses can be used wherever an expression is valid. Each condition is an expression that returns a boolean result. If the condition's result is true, the value of the CASE expression is the result that follows the condition, and the remainder of the CASE expression is not processed. If the condition's result is not true, any subsequent WHEN clauses are examined in the same manner. If no WHEN condition yields true, the value of the CASE expression is the result of the ELSE clause. If the ELSE clause is omitted and no condition is true, the result is null.

An example:

```sql
SELECT a,
       CASE WHEN a=1 THEN 'one'
            WHEN a=2 THEN 'two'
            ELSE 'other'
       END as case
    FROM (VALUES (1),(2),(3)) as x(a)

 a | case
---+-------
 1 | one
 2 | two
 3 | other
```

The data types of all the result expressions must be convertible to a single output type. See Section 10.5 for more details.

There is a “simple” form of CASE expression that is a variant of the general form above:

CASE expression
    WHEN value THEN result
    [WHEN ...]
    [ELSE result]
END

The first expression is computed, then compared to each of the value expressions in the WHEN clauses until one is found that is equal to it. If no match is found, the result of the ELSE clause (or a null value) is returned. This is similar to the switch statement in C.

The example above can be written using the simple CASE syntax:

```sql
SELECT a,
       CASE a WHEN 1 THEN 'one'
              WHEN 2 THEN 'two'
              ELSE 'other'
       END as case
    FROM (VALUES (1),(2),(3)) as x(a)

 a | case
---+-------
 1 | one
 2 | two
 3 | other
```

A CASE expression does not evaluate any subexpressions that are not needed to determine the result. For example, this is a possible way of avoiding a division-by-zero failure:

```sql
SELECT ... WHERE CASE WHEN x <> 0 THEN y/x > 1.5 ELSE false END;
```

Note
As described in Section 4.2.14, there are various situations in which subexpressions of an expression are evaluated at different times, so that the principle that “CASE evaluates only necessary subexpressions” is not ironclad. For example a constant 1/0 subexpression will usually result in a division-by-zero failure at planning time, even if it's within a CASE arm that would never be entered at run time.

9.18.2. COALESCE

COALESCE(value [, ...])
The COALESCE function returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display, for example:

```sql
SELECT COALESCE(description, short_description, '(none)') ...
```

This returns description if it is not null, otherwise short_description if it is not null, otherwise (none).

The arguments must all be convertible to a common data type, which will be the type of the result (see Section 10.5 for details).

Like a CASE expression, COALESCE only evaluates the arguments that are needed to determine the result; that is, arguments to the right of the first non-null argument are not evaluated. This SQL-standard function provides capabilities similar to NVL and IFNULL, which are used in some other database systems.

9.18.3. NULLIF

NULLIF(value1, value2) (NOT SUPPORTED)
The NULLIF function returns a null value if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE example given above:

```sql
SELECT NULLIF(value, '(none)') ...
```

In this example, if value is (none), null is returned, otherwise the value of value is returned.

The two arguments must be of comparable types. To be specific, they are compared exactly as if you had written value1 = value2, so there must be a suitable = operator available.

The result has the same type as the first argument — but there is a subtlety. What is actually returned is the first argument of the implied = operator, and in some cases that will have been promoted to match the second argument's type. For example, NULLIF(1, 2.2) yields numeric, because there is no integer = numeric operator, only numeric = numeric.

9.18.4. GREATEST and LEAST

GREATEST(value [, ...])
LEAST(value [, ...])

The GREATEST and LEAST functions select the largest or smallest value from a list of any number of expressions. The expressions must all be convertible to a common data type, which will be the type of the result (see Section 10.5 for details). NULL values in the list are ignored. The result will be NULL only if all the expressions evaluate to NULL. (NOT SUPPORTED)

Note that GREATEST and LEAST are not in the SQL standard, but are a common extension. Some other databases make them return NULL if any argument is NULL, rather than only when all are NULL.

## 9.19. Array Functions and Operators {#array-functions}

Table 9.51 shows the specialized operators available for array types. In addition to those, the usual comparison operators shown in Table 9.1 are available for arrays. The comparison operators compare the array contents element-by-element, using the default B-tree comparison function for the element data type, and sort based on the first difference. In multidimensional arrays the elements are visited in row-major order (last subscript varies most rapidly). If the contents of two arrays are equal but the dimensionality is different, the first difference in the dimensionality information determines the sort order.

Table 9.51. Array Operators

#|
||Operator|Description|Example(s)||
||anyarray @> anyarray → boolean|
Does the first array contain the second, that is, does each element appearing in the second array equal some element of the first array? (Duplicates are not treated specially, thus ARRAY[1] and ARRAY[1,1] are each considered to contain the other.)|
```sql
ARRAY[1,4,3] @> ARRAY[3,1,3] → true
```||
||anyarray <@ anyarray → boolean|
Is the first array contained by the second?|
```sql
ARRAY[2,2,7] <@ ARRAY[1,7,4,2,6] → true
```||
||anyarray && anyarray → boolean|
Do the arrays overlap, that is, have any elements in common?|
```sql
ARRAY[1,4,3] && ARRAY[2,1] → true
```||
||anycompatiblearray \|\| anycompatiblearray → anycompatiblearray|
Concatenates the two arrays. Concatenating a null or empty array is a no-op; otherwise the arrays must have the same number of dimensions (as illustrated by the first example) or differ in number of dimensions by one (as illustrated by the second). If the arrays are not of identical element types, they will be coerced to a common type (see Section 10.5). (NOT SUPPORTED)|
```sql
#ARRAY[1,2,3] || ARRAY[4,5,6,7] → {1,2,3,4,5,6,7}
#ARRAY[1,2,3] || ARRAY[[4,5,6],[7,8,9.9]] → {{1,2,3},{4,5,6},{7,8,9.9}}
```||
||anycompatible \|\| anycompatiblearray → anycompatiblearray|
Concatenates an element onto the front of an array (which must be empty or one-dimensional). (NOT SUPPORTED)|
```sql
#3 || ARRAY[4,5,6] → {3,4,5,6}
```||
||anycompatiblearray \|\| anycompatible → anycompatiblearray|
Concatenates an element onto the end of an array (which must be empty or one-dimensional). (NOT SUPPORTED)|
```sql
#ARRAY[4,5,6] || 7 → {4,5,6,7}
```||
|#

See Section 8.15 for more details about array operator behavior. See Section 11.2 for more details about which operators support indexed operations.

Table 9.52 shows the functions available for use with array types. See Section 8.15 for more information and examples of the use of these functions.

Table 9.52. Array Functions

#|
||Function|Description|Example(s)||
||array_append ( anycompatiblearray, anycompatible ) → anycompatiblearray|
Appends an element to the end of an array (same as the anycompatiblearray \|\| anycompatible operator). (NOT SUPPORTED)|
```sql
#array_append(ARRAY[1,2], 3) → {1,2,3}
```||
||array_cat ( anycompatiblearray, anycompatiblearray ) → anycompatiblearray|
Concatenates two arrays (same as the anycompatiblearray \|\| anycompatiblearray operator). (NOT SUPPORTED)|
```sql
#array_cat(ARRAY[1,2,3], ARRAY[4,5]) → {1,2,3,4,5}
```||
||array_dims ( anyarray ) → text|
Returns a text representation of the array's dimensions.|
```sql
array_dims(ARRAY[[1,2,3], [4,5,6]]) → [1:2][1:3]
```||
||array_fill ( anyelement, integer[] [, integer[] ] ) → anyarray|
Returns an array filled with copies of the given value, having dimensions of the lengths specified by the second argument. The optional third argument supplies lower-bound values for each dimension (which default to all 1). (NOT SUPPORTED)|
```sql
#array_fill(11, ARRAY[2,3]) → {{11,11,11},{11,11,11}}
#array_fill(7, ARRAY[3], ARRAY[2]) → [2:4]={7,7,7}
```||
||array_length ( anyarray, integer ) → integer|
Returns the length of the requested array dimension. (Produces NULL instead of 0 for empty or missing array dimensions.)|
```sql
array_length(array[1,2,3], 1) → 3
#array_length(array[]::int[], 1) → NULL
array_length(array['text'], 2) → NULL
```||
||array_lower ( anyarray, integer ) → integer|
Returns the lower bound of the requested array dimension.|
```sql
array_lower('[0:2]={1,2,3}'::integer[], 1) → 0
```||
||array_ndims ( anyarray ) → integer|
Returns the number of dimensions of the array.|
```sql
array_ndims(ARRAY[[1,2,3], [4,5,6]]) → 2
```||
||array_position ( anycompatiblearray, anycompatible [, integer ] ) → integer|
Returns the subscript of the first occurrence of the second argument in the array, or NULL if it's not present. If the third argument is given, the search begins at that subscript. The array must be one-dimensional. Comparisons are done using IS NOT DISTINCT FROM semantics, so it is possible to search for NULL. (NOT SUPPORTED)|
```sql
#array_position(ARRAY['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'], 'mon') → 2
```||
||array_positions ( anycompatiblearray, anycompatible ) → integer[]|
Returns an array of the subscripts of all occurrences of the second argument in the array given as first argument. The array must be one-dimensional. Comparisons are done using IS NOT DISTINCT FROM semantics, so it is possible to search for NULL. NULL is returned only if the array is NULL; if the value is not found in the array, an empty array is returned. (NOT SUPPORTED)|
```sql
#array_positions(ARRAY['A','A','B','A'], 'A') → {1,2,4}
```||
||array_prepend ( anycompatible, anycompatiblearray ) → anycompatiblearray|
Prepends an element to the beginning of an array (same as the anycompatible \|\| anycompatiblearray operator). (NOT SUPPORTED)|
```sql
#array_prepend(1, ARRAY[2,3]) → {1,2,3}
```||
||array_remove ( anycompatiblearray, anycompatible ) → anycompatiblearray|
Removes all elements equal to the given value from the array. The array must be one-dimensional. Comparisons are done using IS NOT DISTINCT FROM semantics, so it is possible to remove NULLs. (NOT SUPPORTED)|
```sql
#array_remove(ARRAY[1,2,3,2], 2) → {1,3}
```||
||array_replace ( anycompatiblearray, anycompatible, anycompatible ) → anycompatiblearray|
Replaces each array element equal to the second argument with the third argument. (NOT SUPPORTED)|
```sql
#array_replace(ARRAY[1,2,5,4], 5, 3) → {1,2,3,4}
```||
||array_to_string ( array anyarray, delimiter text [, null_string text ] ) → text|
Converts each array element to its text representation, and concatenates those separated by the delimiter string. If null_string is given and is not NULL, then NULL array entries are represented by that string; otherwise, they are omitted.|
```sql
array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*') → 1,2,3,*,5
```||
||array_upper ( anyarray, integer ) → integer|
Returns the upper bound of the requested array dimension.|
```sql
array_upper(ARRAY[1,8,3,7], 1) → 4
```||
||cardinality ( anyarray ) → integer|
Returns the total number of elements in the array, or 0 if the array is empty.|
```sql
cardinality(ARRAY[[1,2],[3,4]]) → 4
```||
||trim_array ( array anyarray, n integer ) → anyarray|
Trims an array by removing the last n elements. If the array is multidimensional, only the first dimension is trimmed.|
```sql
trim_array(ARRAY[1,2,3,4,5,6], 2) → {1,2,3,4}
```||
||unnest ( anyarray ) → setof anyelement|
Expands an array into a set of rows. The array's elements are read out in storage order. (NOT SUPPORTED)|

```sql
SELECT * FROM unnest(ARRAY[1,2]) as a → [
1
2
]

SELECT * FROM unnest(ARRAY[['foo','bar'],['baz','quux']]) as a → [
foo
bar
baz
quux
]
```||
||unnest ( anyarray, anyarray [, ... ] ) → setof anyelement, anyelement [, ... ]|
Expands multiple arrays (possibly of different data types) into a set of rows. If the arrays are not all the same length then the shorter ones are padded with NULLs. This form is only allowed in a query's FROM clause; see Section 7.2.1.4. (NOT SUPPORTED)|
```sql
#SELECT * FROM unnest(ARRAY[1,2], ARRAY['foo','bar','baz']) as x(a,b) → [
1,foo
2,bar
,baz
]
```||
|#

Note
There are two differences in the behavior of string_to_array from pre-9.1 versions of PostgreSQL. First, it will return an empty (zero-element) array rather than NULL when the input string is of zero length. Second, if the delimiter string is NULL, the function splits the input into individual characters, rather than returning NULL as before.

See also Section 9.21 about the aggregate function array_agg for use with arrays.

## 9.20. Range/Multirange Functions and Operators (NOT SUPPORTED) {#range-multirange-functions}

## 9.21. Aggregate Functions {#aggregate-functions}

Aggregate functions compute a single result from a set of input values. The built-in general-purpose aggregate functions are listed in Table 9.57 while statistical aggregates are in Table 9.58. The built-in within-group ordered-set aggregate functions are listed in Table 9.59 while the built-in within-group hypothetical-set ones are in Table 9.60. Grouping operations, which are closely related to aggregate functions, are listed in Table 9.61. The special syntax considerations for aggregate functions are explained in Section 4.2.7. Consult Section 2.7 for additional introductory information.

Aggregate functions that support Partial Mode are eligible to participate in various optimizations, such as parallel aggregation.

Table 9.57. General-Purpose Aggregate Functions

#|
||Function|Description|Partial Mode|Example||
||array_agg ( anynonarray ) → anyarray|
Collects all the input values, including nulls, into an array.|
No|
```sql
SELECT array_agg(x) FROM (VALUES (1),(2)) a(x) → {1,2}
```||
||array_agg ( anyarray ) → anyarray|
Concatenates all the input arrays into an array of one higher dimension. (The inputs must all have the same dimensionality, and cannot be empty or null.)|
No|
```sql
SELECT array_agg(x) FROM (VALUES (Array[1,2]),(Array[3,4])) a(x) → {{1,2},{3,4}}
```||
||avg ( smallint ) → numeric  
avg ( integer ) → numeric  
avg ( bigint ) → numeric  
avg ( numeric ) → numeric  
avg ( real ) → double precision  
avg ( double precision ) → double precision  
avg ( interval ) → interval|
Computes the average (arithmetic mean) of all the non-null input values.|
Yes|
```sql
SELECT avg(x::smallint) FROM (VALUES (1),(2),(3)) a(x) → 2.0000000000000000
SELECT avg(x::integer) FROM (VALUES (1),(2),(3)) a(x) → 2.0000000000000000
SELECT avg(x::bigint) FROM (VALUES (1),(2),(3)) a(x) → 2.0000000000000000
SELECT avg(x::numeric) FROM (VALUES (1),(2),(3)) a(x) → 2.0000000000000000
SELECT avg(x::real) FROM (VALUES (1),(2),(3)) a(x) → 2
SELECT avg(x::double precision) FROM (VALUES (1),(2),(3)) a(x) → 2
SELECT avg(cast(x as interval day)) FROM (VALUES ('1'),('2'),('3')) a(x) → 2 days
```||
||bit_and ( smallint ) → smallint  
bit_and ( integer ) → integer  
bit_and ( bigint ) → bigint  
bit_and ( bit ) → bit|
Computes the bitwise AND of all non-null input values.|
Yes|
```sql
SELECT bit_and(x::smallint) FROM (VALUES (5),(6),(7)) a(x) → 4
SELECT bit_and(x::integer) FROM (VALUES (5),(6),(7)) a(x) → 4
SELECT bit_and(x::bigint) FROM (VALUES (5),(6),(7)) a(x) → 4
SELECT bit_and(x::bit(3)) FROM (VALUES ('101'),('110'),('111')) a(x) → 100
```||
||bit_or ( smallint ) → smallint  
bit_or ( integer ) → integer  
bit_or ( bigint ) → bigint  
bit_or ( bit ) → bit|
Computes the bitwise OR of all non-null input values.|
Yes|
```sql
SELECT bit_or(x::smallint) FROM (VALUES (4),(5),(6)) a(x) → 7
SELECT bit_or(x::integer) FROM (VALUES (4),(5),(6)) a(x) → 7
SELECT bit_or(x::bigint) FROM (VALUES (4),(5),(6)) a(x) → 7
SELECT bit_or(x::bit(3)) FROM (VALUES ('100'),('101'),('110')) a(x) → 111
```||
||bit_xor ( smallint ) → smallint  
bit_xor ( integer ) → integer  
bit_xor ( bigint ) → bigint  
bit_xor ( bit ) → bit|
Computes the bitwise exclusive OR of all non-null input values. Can be useful as a checksum for an unordered set of values.|
Yes|
```sql
SELECT bit_xor(x::smallint) FROM (VALUES (5),(6),(6)) a(x) → 5
SELECT bit_xor(x::integer) FROM (VALUES (5),(6),(6)) a(x) → 5
SELECT bit_xor(x::bigint) FROM (VALUES (5),(6),(6)) a(x) → 5
SELECT bit_xor(x::bit(3)) FROM (VALUES ('101'),('110'),('110')) a(x) → 101
```||
||bool_and ( boolean ) → boolean|
Returns true if all non-null input values are true, otherwise false.|
Yes|
```sql
SELECT bool_and(x) FROM (VALUES (null),(false),(true)) a(x) → false
SELECT bool_and(x) FROM (VALUES (null),(true),(true)) a(x) → true
SELECT bool_and(x) FROM (VALUES (null::bool),(null::bool),(null::bool)) a(x) → NULL
```||
||bool_or ( boolean ) → boolean|
Returns true if any non-null input value is true, otherwise false.|
Yes|
```sql
SELECT bool_or(x) FROM (VALUES (null),(false),(false)) a(x) → false
SELECT bool_or(x) FROM (VALUES (null),(false),(true)) a(x) → true
SELECT bool_or(x) FROM (VALUES (null::bool),(null::bool),(null::bool)) a(x) → NULL
```||
||count ( * ) → bigint|
Computes the number of input rows.|
Yes|
```sql
SELECT count(*) FROM (VALUES (4),(5),(6)) a(x) → 3
```||
||count ( any ) → bigint|
Computes the number of input rows in which the input value is not null.|
Yes|
```sql
SELECT count(x) FROM (VALUES (4),(null),(6)) a(x) → 2
```||
||every ( boolean ) → boolean|
This is the SQL standard's equivalent to bool_and|
Yes|
```sql
SELECT every(x) FROM (VALUES (null),(false),(true)) a(x) → false
SELECT every(x) FROM (VALUES (null),(true),(true)) a(x) → true
SELECT every(x) FROM (VALUES (null::bool),(null::bool),(null::bool)) a(x) → NULL
```||
||json_agg ( anyelement ) → json  
jsonb_agg ( anyelement ) → jsonb|
Collects all the input values, including nulls, into a JSON array. Values are converted to JSON as per to_json or to_jsonb. (NOT SUPPORTED)|
No|
```sql
#SELECT json_agg(x) FROM (VALUES (1),(2),(3)) a(x) → [1,2,3]
#SELECT jsonb_agg(x) FROM (VALUES ('a'),('b'),('c')) a(x) → ["a","b","c"]
```||
||json_object_agg ( key any, value any ) → json  
jsonb_object_agg ( key any, value any ) → jsonb|
Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per to_json or to_jsonb. Values can be null, but not keys.|
No|
```sql
SELECT json_object_agg(x,y) FROM (VALUES ('a',1),('b',2),('c',3)) a(x,y) → [
{ "a" : 1, "b" : 2, "c" : 3 }
]

SELECT jsonb_object_agg(x,y) FROM (VALUES ('x','a'),('y','b'),('z','c')) a(x,y) → [
{"x": "a", "y": "b", "z": "c"}
]

```||
||max ( see text ) → same as input type|
Computes the maximum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as inet, interval, money, oid, pg_lsn, tid, and arrays of any of these types. (Arrays aren't supported)|
Yes|
```sql
SELECT max(x::smallint) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x::integer) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x::bigint) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x::real) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x::double precision) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x::numeric) FROM (VALUES (1),(2),(3)) a(x) → 3
SELECT max(x) FROM (VALUES ('a'),('b'),('c')) a(x) → 'c'
SELECT max(x::date) FROM (VALUES ('2001-01-01'),('2001-02-03'),('2002-01-01')) a(x) → 2002-01-01
SELECT max(x::timestamp) FROM (VALUES ('2001-01-01 23:05:04'),('2001-01-01 23:06:03'),('2001-01-01 23:59:00')) a(x) → 2001-01-01 23:59:00
SELECT max(x::time) FROM (VALUES ('10:00:05'),('11:00:01'),('12:50:00')) a(x) → 12:50:00
SELECT max(x) FROM (VALUES (interval '1' day),(interval '2' day),(interval '3' day)) a(x) → 3 days

SELECT max(array[x,x]::smallint[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]::integer[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]::bigint[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]::real[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]::double precision[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]::numeric[]) FROM (VALUES (1),(2),(3)) a(x) → {3,3}
SELECT max(array[x,x]) FROM (VALUES ('a'),('b'),('c')) a(x) → {c,c}
SELECT max(array[x,x]::date[]) FROM (VALUES ('2001-01-01'),('2001-02-03'),('2002-01-01')) a(x) → {2002-01-01,2002-01-01}
SELECT max(array[x,x]::timestamp[]) FROM (VALUES ('2001-01-01 23:05:04'),('2001-01-01 23:06:03'),('2001-01-01 23:59:00')) a(x) → {"2001-01-01 23:59:00","2001-01-01 23:59:00"}
SELECT max(array[x,x]::time[]) FROM (VALUES ('10:00:05'),('11:00:01'),('12:50:00')) a(x) → {12:50:00,12:50:00}
SELECT max(array[x,x]) FROM (VALUES (interval '1' day),(interval '2' day),(interval '3' day)) a(x) → {"3 days","3 days"}
```||
||min ( see text ) → same as input type|
Computes the minimum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as inet, interval, money, oid, pg_lsn, tid, and arrays of any of these types. (Arrays aren't supported)|
Yes|
```sql
SELECT min(x::smallint) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x::integer) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x::bigint) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x::real) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x::double precision) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x::numeric) FROM (VALUES (1),(2),(3)) a(x) → 1
SELECT min(x) FROM (VALUES ('a'),('b'),('c')) a(x) → 'a'
SELECT min(x::date) FROM (VALUES ('2001-01-01'),('2001-02-03'),('2002-01-01')) a(x) → 2001-01-01
SELECT min(x::timestamp) FROM (VALUES ('2001-01-01 23:05:04'),('2001-01-01 23:06:03'),('2001-01-01 23:59:00')) a(x) → 2001-01-01 23:05:04
SELECT min(x::time) FROM (VALUES ('10:00:05'),('11:00:01'),('12:50:00')) a(x) → 10:00:05
SELECT min(x) FROM (VALUES (interval '1' day),(interval '2' day),(interval '3' day)) a(x) → 1 day

SELECT min(array[x,x]::smallint[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]::integer[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]::bigint[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]::real[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]::double precision[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]::numeric[]) FROM (VALUES (1),(2),(3)) a(x) → {1,1}
SELECT min(array[x,x]) FROM (VALUES ('a'),('b'),('c')) a(x) → {a,a}
SELECT min(array[x,x]::date[]) FROM (VALUES ('2001-01-01'),('2001-02-03'),('2002-01-01')) a(x) → {2001-01-01,2001-01-01}
SELECT min(array[x,x]::timestamp[]) FROM (VALUES ('2001-01-01 23:05:04'),('2001-01-01 23:06:03'),('2001-01-01 23:59:00')) a(x) → {"2001-01-01 23:05:04","2001-01-01 23:05:04"}
SELECT min(array[x,x]::time[]) FROM (VALUES ('10:00:05'),('11:00:01'),('12:50:00')) a(x) → {10:00:05,10:00:05}
SELECT min(array[x,x]) FROM (VALUES (interval '1' day),(interval '2' day),(interval '3' day)) a(x) → {"1 day","1 day"}
```||
||range_agg ( value anyrange ) → anymultirange|
Computes the union of the non-null input values. (NOT SUPPORTED)|
No|
||
||range_intersect_agg ( value anyrange ) → anyrange  
range_intersect_agg ( value anymultirange ) → anymultirange|
Computes the intersection of the non-null input values. (NOT SUPPORTED)|
No|
||
||string_agg ( value text, delimiter text ) → text  
string_agg ( value bytea, delimiter bytea ) → bytea|
Concatenates the non-null input values into a string. Each value after the first is preceded by the corresponding delimiter (if it's not null).|
No|
```sql
SELECT string_agg(x,'') FROM (VALUES ('a'),('b'),('c')) a(x) → abc
SELECT string_agg(x::bytea,','::bytea) FROM (VALUES ('a'),('b'),('c')) a(x) → a,b,c
```||
||sum ( smallint ) → bigint  
sum ( integer ) → bigint  
sum ( bigint ) → numeric  
sum ( numeric ) → numeric  
sum ( real ) → real  
sum ( double precision ) → double precision  
sum ( interval ) → interval  
sum ( money ) → money|
Computes the sum of the non-null input values.|
Yes|
```sql
SELECT sum(x::smallint) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x::integer) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x::bigint) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x::real) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x::double precision) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x::numeric) FROM (VALUES (1),(2),(3)) a(x) → 6
SELECT sum(x) FROM (VALUES (interval '1' day),(interval '2' day),(interval '3' day)) a(x) → 6 days
```||
||xmlagg ( xml ) → xml|
Concatenates the non-null XML input values (see Section 9.15.1.7).|
No|
||
|#

It should be noted that except for count, these functions return a null value when no rows are selected. In particular, sum of no rows returns null, not zero as one might expect, and array_agg returns null rather than an empty array when there are no input rows. The coalesce function can be used to substitute zero or an empty array for null when necessary.

The aggregate functions array_agg, json_agg, jsonb_agg, json_object_agg, jsonb_object_agg, string_agg, and xmlagg, as well as similar user-defined aggregate functions, produce meaningfully different result values depending on the order of the input values. This ordering is unspecified by default, but can be controlled by writing an ORDER BY clause within the aggregate call, as shown in Section 4.2.7. Alternatively, supplying the input values from a sorted subquery will usually work. For example:

```sql
SELECT xmlagg(x) FROM (SELECT x FROM test ORDER BY y DESC) AS tab;
```

Beware that this approach can fail if the outer query level contains additional processing, such as a join, because that might cause the subquery's output to be reordered before the aggregate is computed.

Note
The boolean aggregates bool_and and bool_or correspond to the standard SQL aggregates every and any or some. PostgreSQL supports every, but not any or some, because there is an ambiguity built into the standard syntax:

```sql
SELECT b1 = ANY((SELECT b2 FROM t2 ...)) FROM t1 ...;
```

Here ANY can be considered either as introducing a subquery, or as being an aggregate function, if the subquery returns one row with a Boolean value. Thus the standard name cannot be given to these aggregates.

Note
Users accustomed to working with other SQL database management systems might be disappointed by the performance of the count aggregate when it is applied to the entire table. A query like:

SELECT count(*) FROM sometable;
will require effort proportional to the size of the table: PostgreSQL will need to scan either the entire table or the entirety of an index that includes all rows in the table.

Table 9.58 shows aggregate functions typically used in statistical analysis. (These are separated out merely to avoid cluttering the listing of more-commonly-used aggregates.) Functions shown as accepting numeric_type are available for all the types smallint, integer, bigint, numeric, real, and double precision. Where the description mentions N, it means the number of input rows for which all the input expressions are non-null. In all cases, null is returned if the computation is meaningless, for example when N is zero.

Table 9.58. Aggregate Functions for Statistics

#|
||Function|Description|Partial Mode|Examples||
||corr ( Y double precision, X double precision ) → double precision|
Computes the correlation coefficient.|
Yes|
```sql
SELECT corr(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → -0.5
```||
||covar_pop ( Y double precision, X double precision ) → double precision|
Computes the population covariance.|
Yes|
```sql
SELECT covar_pop(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → -0.3333333333333333
```||
||covar_samp ( Y double precision, X double precision ) → double precision|
Computes the sample covariance.|
Yes|
```sql
SELECT covar_samp(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → -0.5
```||
||regr_avgx ( Y double precision, X double precision ) → double precision|
Computes the average of the independent variable, sum(X)/N.|
Yes|
```sql
SELECT regr_avgx(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 2
```||
||regr_avgy ( Y double precision, X double precision ) → double precision|
Computes the average of the dependent variable, sum(Y)/N.|
Yes|
```sql
SELECT regr_avgy(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 2
```||
||regr_count ( Y double precision, X double precision ) → bigint|
Computes the number of rows in which both inputs are non-null.|
Yes|
```sql
SELECT regr_count(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 3
```||
||regr_intercept ( Y double precision, X double precision ) → double precision|
Computes the y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs.|
Yes|
```sql
SELECT regr_intercept(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 3
```||
||regr_r2 ( Y double precision, X double precision ) → double precision|
Computes the square of the correlation coefficient.|
Yes|
```sql
SELECT regr_r2(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 0.25
```||
||regr_slope ( Y double precision, X double precision ) → double precision|
Computes the slope of the least-squares-fit linear equation determined by the (X, Y) pairs.|
Yes|
```sql
SELECT regr_slope(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → -0.5
```||
||regr_sxx ( Y double precision, X double precision ) → double precision|
Computes the “sum of squares” of the independent variable, sum(X^2) - sum(X)^2/N.|
Yes|
```sql
SELECT regr_sxx(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 2
```||
||regr_sxy ( Y double precision, X double precision ) → double precision|
Computes the “sum of products” of independent times dependent variables, sum(X*Y) - sum(X) * sum(Y)/N.|
Yes|
```sql
SELECT regr_sxy(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → -1
```||
||regr_syy ( Y double precision, X double precision ) → double precision|
Computes the “sum of squares” of the dependent variable, sum(Y^2) - sum(Y)^2/N.|
Yes|
```sql
SELECT regr_syy(x,y) FROM (VALUES (1,2),(2,3),(3,1)) a(x,y) → 2
```||
||stddev ( numeric_type ) → double precision for real or double precision, otherwise numeric|
This is a historical alias for stddev_samp.|
Yes|
```sql
SELECT stddev(x) FROM (VALUES (1),(2),(3)) a(x) → 1.00000000000000000000
```||
||stddev_pop ( numeric_type ) → double precision for real or double precision, otherwise numeric|
Computes the population standard deviation of the input values.|
Yes|
```sql
SELECT stddev_pop(x) FROM (VALUES (1),(2),(3)) a(x) → 0.81649658092772603273
```||
||stddev_samp ( numeric_type ) → double precision for real or double precision, otherwise numeric|
Computes the sample standard deviation of the input values.|
Yes|
```sql
SELECT stddev_samp(x) FROM (VALUES (1),(2),(3)) a(x) → 1.00000000000000000000
```||
||variance ( numeric_type ) → double precision for real or double precision, otherwise numeric|
This is a historical alias for var_samp.|
Yes|
```sql
SELECT variance(x) FROM (VALUES (1),(2),(3)) a(x) → 1.00000000000000000000
```||
||var_pop ( numeric_type ) → double precision for real or double precision, otherwise numeric|
Computes the population variance of the input values (square of the population standard deviation).|
Yes|
```sql
SELECT var_pop(x) FROM (VALUES (1),(2),(3)) a(x) → 0.66666666666666666667
```||
||var_samp ( numeric_type ) → double precision for real or double precision, otherwise numeric|
Computes the sample variance of the input values (square of the sample standard deviation).|
Yes|
```sql
SELECT var_samp(x) FROM (VALUES (1),(2),(3)) a(x) → 1.00000000000000000000
```||
|#

Table 9.59 shows some aggregate functions that use the ordered-set aggregate syntax. These functions are sometimes referred to as “inverse distribution” functions. Their aggregated input is introduced by ORDER BY, and they may also take a direct argument that is not aggregated, but is computed only once. All these functions ignore null values in their aggregated input. For those that take a fraction parameter, the fraction value must be between 0 and 1; an error is thrown if not. However, a null fraction value simply produces a null result.

Table 9.59. Ordered-Set Aggregate Functions (NOT SUPPORTED)

#|
||Function|Description|Partial Mode||
||mode () WITHIN GROUP ( ORDER BY anyelement ) → anyelement|
Computes the mode, the most frequent value of the aggregated argument (arbitrarily choosing the first one if there are multiple equally-frequent values). The aggregated argument must be of a sortable type.|
No||
||percentile_cont ( fraction double precision ) WITHIN GROUP ( ORDER BY double precision ) → double precision  
percentile_cont ( fraction double precision ) WITHIN GROUP ( ORDER BY interval ) → interval|
Computes the continuous percentile, a value corresponding to the specified fraction within the ordered set of aggregated argument values. This will interpolate between adjacent input items if needed.|
No||
||percentile_cont ( fractions double precision[] ) WITHIN GROUP ( ORDER BY double precision ) → double precision[]  
percentile_cont ( fractions double precision[] ) WITHIN GROUP ( ORDER BY interval ) → interval[]|
Computes multiple continuous percentiles. The result is an array of the same dimensions as the fractions parameter, with each non-null element replaced by the (possibly interpolated) value corresponding to that percentile.|
No||
||percentile_disc ( fraction double precision ) WITHIN GROUP ( ORDER BY anyelement ) → anyelement|
Computes the discrete percentile, the first value within the ordered set of aggregated argument values whose position in the ordering equals or exceeds the specified fraction. The aggregated argument must be of a sortable type.|
No||
||percentile_disc ( fractions double precision[] ) WITHIN GROUP ( ORDER BY anyelement ) → anyarray|
Computes multiple discrete percentiles. The result is an array of the same dimensions as the fractions parameter, with each non-null element replaced by the input value corresponding to that percentile. The aggregated argument must be of a sortable type.|
No||
|#

Each of the “hypothetical-set” aggregates listed in Table 9.60 is associated with a window function of the same name defined in Section 9.22. In each case, the aggregate's result is the value that the associated window function would have returned for the “hypothetical” row constructed from args, if such a row had been added to the sorted group of rows represented by the sorted_args. For each of these functions, the list of direct arguments given in args must match the number and types of the aggregated arguments given in sorted_args. Unlike most built-in aggregates, these aggregates are not strict, that is they do not drop input rows containing nulls. Null values sort according to the rule specified in the ORDER BY clause.

Table 9.60. Hypothetical-Set Aggregate Functions (NOT SUPPORTED)

#|
||Function|Description|Partial Mode||
||rank ( args ) WITHIN GROUP ( ORDER BY sorted_args ) → bigint|
Computes the rank of the hypothetical row, with gaps; that is, the row number of the first row in its peer group.|
No||
||dense_rank ( args ) WITHIN GROUP ( ORDER BY sorted_args ) → bigint|
Computes the rank of the hypothetical row, without gaps; this function effectively counts peer groups.|
No||
||percent_rank ( args ) WITHIN GROUP ( ORDER BY sorted_args ) → double precision|
Computes the relative rank of the hypothetical row, that is (rank - 1) / (total rows - 1). The value thus ranges from 0 to 1 inclusive.|
No||
||cume_dist ( args ) WITHIN GROUP ( ORDER BY sorted_args ) → double precision|
Computes the cumulative distribution, that is (number of rows preceding or peers with hypothetical row) / (total rows). The value thus ranges from 1/N to 1.|
No||
|#

Table 9.61. Grouping Operations

#|
||Function|Description||
||GROUPING ( group_by_expression(s) ) → integer|
Returns a bit mask indicating which GROUP BY expressions are not included in the current grouping set. Bits are assigned with the rightmost argument corresponding to the least-significant bit; each bit is 0 if the corresponding expression is included in the grouping criteria of the grouping set generating the current result row, and 1 if it is not included. (NOT SUPPORTED)||
|#

The grouping operations shown in Table 9.61 are used in conjunction with grouping sets (see Section 7.2.4) to distinguish result rows. The arguments to the GROUPING function are not actually evaluated, but they must exactly match expressions given in the GROUP BY clause of the associated query level. For example:

```sql
=> SELECT * FROM items_sold;
 make  | model | sales
-------+-------+-------
 Foo   | GT    |  10
 Foo   | Tour  |  20
 Bar   | City  |  15
 Bar   | Sport |  5
(4 rows)

=> SELECT make, model, GROUPING(make,model), sum(sales) FROM items_sold GROUP BY ROLLUP(make,model);
 make  | model | grouping | sum
-------+-------+----------+-----
 Foo   | GT    |        0 | 10
 Foo   | Tour  |        0 | 20
 Bar   | City  |        0 | 15
 Bar   | Sport |        0 | 5
 Foo   |       |        1 | 30
 Bar   |       |        1 | 20
       |       |        3 | 50
(7 rows)
```

Here, the grouping value 0 in the first four rows shows that those have been grouped normally, over both the grouping columns. The value 1 indicates that model was not grouped by in the next-to-last two rows, and the value 3 indicates that neither make nor model was grouped by in the last row (which therefore is an aggregate over all the input rows).

## 9.22. Window Functions {#window-functions}

Window functions provide the ability to perform calculations across sets of rows that are related to the current query row. See Section 3.5 for an introduction to this feature, and Section 4.2.8 for syntax details.

The built-in window functions are listed in Table 9.62. Note that these functions must be invoked using window function syntax, i.e., an OVER clause is required.

In addition to these functions, any built-in or user-defined ordinary aggregate (i.e., not ordered-set or hypothetical-set aggregates) can be used as a window function; see Section 9.21 for a list of the built-in aggregates. Aggregate functions act as window functions only when an OVER clause follows the call; otherwise they act as plain aggregates and return a single row for the entire set.

Table 9.62. General-Purpose Window Functions

#|
||Function|Description|Examples||
||row_number () → bigint|
Returns the number of the current row within its partition, counting from 1.|
```sql
SELECT row_number() OVER (ORDER BY x) FROM (VALUES (4),(5),(6)) a(x) → [
1
2
3
]
```||
||rank () → bigint|
Returns the rank of the current row, with gaps; that is, the row_number of the first row in its peer group.|
```sql
SELECT rank() OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
1
2
2
4
]
```||
||dense_rank () → bigint|
Returns the rank of the current row, without gaps; this function effectively counts peer groups.|
```sql
SELECT dense_rank() OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
1
2
2
3
]
```||
||percent_rank () → double precision|
Returns the relative rank of the current row, that is (rank - 1) / (total partition rows - 1). The value thus ranges from 0 to 1 inclusive.|
```sql
SELECT percent_rank() OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
0
0.3333333333333333
0.3333333333333333
1
]
```||
||cume_dist () → double precision|
Returns the cumulative distribution, that is (number of partition rows preceding or peers with current row) / (total partition rows). The value thus ranges from 1/N to 1.|
```sql
SELECT cume_dist() OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
0.25
0.75
0.75
1
]
```||
||ntile ( num_buckets integer ) → integer|
Returns an integer ranging from 1 to the argument value, dividing the partition as equally as possible.|
```sql
SELECT ntile(2) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
1
1
2
2
]
```||
||lag ( value anycompatible [, offset integer [, default anycompatible ]] ) → anycompatible|
Returns value evaluated at the row that is offset rows before the current row within the partition; if there is no such row, instead returns default (which must be of a type compatible with value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to NULL.|
```sql
SELECT lag(x) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
NULL
4
5
5
]
```||
||lead ( value anycompatible [, offset integer [, default anycompatible ]] ) → anycompatible|
Returns value evaluated at the row that is offset rows after the current row within the partition; if there is no such row, instead returns default (which must be of a type compatible with value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to NULL.|
```sql
SELECT lead(x) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
5
5
6
NULL
]
```||
||first_value ( value anyelement ) → anyelement|
Returns value evaluated at the row that is the first row of the window frame.|
```sql
SELECT first_value(x) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
4
4
4
4
]
```||
||last_value ( value anyelement ) → anyelement|
Returns value evaluated at the row that is the last row of the window frame.|
```sql
SELECT last_value(x) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
4
5
5
6
]
```||
||nth_value ( value anyelement, n integer ) → anyelement|
Returns value evaluated at the row that is the n'th row of the window frame (counting from 1); returns NULL if there is no such row.|
```sql
SELECT nth_value(x,2) OVER (ORDER BY x) FROM (VALUES (4),(5),(5),(6)) a(x) → [
NULL
5
5
5
]
```||
|#

All of the functions listed in Table 9.62 depend on the sort ordering specified by the ORDER BY clause of the associated window definition. Rows that are not distinct when considering only the ORDER BY columns are said to be peers. The four ranking functions (including cume_dist) are defined so that they give the same answer for all rows of a peer group.

Note that first_value, last_value, and nth_value consider only the rows within the “window frame”, which by default contains the rows from the start of the partition through the last peer of the current row. This is likely to give unhelpful results for last_value and sometimes also nth_value. You can redefine the frame by adding a suitable frame specification (RANGE, ROWS or GROUPS) to the OVER clause. See Section 4.2.8 for more information about frame specifications.

When an aggregate function is used as a window function, it aggregates over the rows within the current row's window frame. An aggregate used with ORDER BY and the default window frame definition produces a “running sum” type of behavior, which may or may not be what's wanted. To obtain aggregation over the whole partition, omit ORDER BY or use ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING. Other frame specifications can be used to obtain other effects.

Note
The SQL standard defines a RESPECT NULLS or IGNORE NULLS option for lead, lag, first_value, last_value, and nth_value. This is not implemented in PostgreSQL: the behavior is always the same as the standard's default, namely RESPECT NULLS. Likewise, the standard's FROM FIRST or FROM LAST option for nth_value is not implemented: only the default FROM FIRST behavior is supported. (You can achieve the result of FROM LAST by reversing the ORDER BY ordering.)

## 9.23. Subquery Expressions {#subquery-expressions}

This section describes the SQL-compliant subquery expressions available in PostgreSQL. All of the expression forms documented in this section return Boolean (true/false) results.

9.23.1. EXISTS

EXISTS (subquery)
The argument of EXISTS is an arbitrary SELECT statement, or subquery. The subquery is evaluated to determine whether it returns any rows. If it returns at least one row, the result of EXISTS is “true”; if the subquery returns no rows, the result of EXISTS is “false”.

The subquery can refer to variables from the surrounding query, which will act as constants during any one evaluation of the subquery.

The subquery will generally only be executed long enough to determine whether at least one row is returned, not all the way to completion. It is unwise to write a subquery that has side effects (such as calling sequence functions); whether the side effects occur might be unpredictable.

Since the result depends only on whether any rows are returned, and not on the contents of those rows, the output list of the subquery is normally unimportant. A common coding convention is to write all EXISTS tests in the form EXISTS(SELECT 1 WHERE ...). There are exceptions to this rule however, such as subqueries that use INTERSECT.

This simple example is like an inner join on col2, but it produces at most one output row for each tab1 row, even if there are several matching tab2 rows:

```sql
SELECT col1
FROM tab1
WHERE EXISTS (SELECT 1 FROM tab2 WHERE col2 = tab1.col2);
```

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE EXISTS (SELECT 1 FROM (VALUES (3),(4),(5)) b(y) WHERE x=y) → [
3
]
```

9.23.2. IN

expression IN (subquery)

The right-hand side is a parenthesized subquery, which must return exactly one column. The left-hand expression is evaluated and compared to each row of the subquery result. The result of IN is “true” if any equal subquery row is found. The result is “false” if no equal row is found (including the case where the subquery returns no rows).

Note that if the left-hand expression yields null, or if there are no equal right-hand values and at least one right-hand row yields null, the result of the IN construct will be null, not false. This is in accordance with SQL's normal rules for Boolean combinations of null values.

As with EXISTS, it's unwise to assume that the subquery will be evaluated completely.

row_constructor IN (subquery) (NOT SUPPORTED)

The left-hand side of this form of IN is a row constructor, as described in Section 4.2.13. The right-hand side is a parenthesized subquery, which must return exactly as many columns as there are expressions in the left-hand row. The left-hand expressions are evaluated and compared row-wise to each row of the subquery result. The result of IN is “true” if any equal subquery row is found. The result is “false” if no equal row is found (including the case where the subquery returns no rows).

As usual, null values in the rows are combined per the normal rules of SQL Boolean expressions. Two rows are considered equal if all their corresponding members are non-null and equal; the rows are unequal if any corresponding members are non-null and unequal; otherwise the result of that row comparison is unknown (null). If all the per-row results are either unequal or null, with at least one null, then the result of IN is null.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x IN (SELECT y FROM (VALUES (3),(4),(5)) b(y)) → [
3
]
```

9.23.3. NOT IN

expression NOT IN (subquery)

The right-hand side is a parenthesized subquery, which must return exactly one column. The left-hand expression is evaluated and compared to each row of the subquery result. The result of NOT IN is “true” if only unequal subquery rows are found (including the case where the subquery returns no rows). The result is “false” if any equal row is found.

Note that if the left-hand expression yields null, or if there are no equal right-hand values and at least one right-hand row yields null, the result of the NOT IN construct will be null, not true. This is in accordance with SQL's normal rules for Boolean combinations of null values.

As with EXISTS, it's unwise to assume that the subquery will be evaluated completely.

row_constructor NOT IN (subquery) (NOT SUPPORTED)

The left-hand side of this form of NOT IN is a row constructor, as described in Section 4.2.13. The right-hand side is a parenthesized subquery, which must return exactly as many columns as there are expressions in the left-hand row. The left-hand expressions are evaluated and compared row-wise to each row of the subquery result. The result of NOT IN is “true” if only unequal subquery rows are found (including the case where the subquery returns no rows). The result is “false” if any equal row is found.

As usual, null values in the rows are combined per the normal rules of SQL Boolean expressions. Two rows are considered equal if all their corresponding members are non-null and equal; the rows are unequal if any corresponding members are non-null and unequal; otherwise the result of that row comparison is unknown (null). If all the per-row results are either unequal or null, with at least one null, then the result of NOT IN is null.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x NOT IN (SELECT y FROM (VALUES (3),(4),(5)) b(y)) ORDER BY x → [
1
2
]
```

9.23.4. ANY/SOME

expression operator ANY (subquery)
expression operator SOME (subquery)

The right-hand side is a parenthesized subquery, which must return exactly one column. The left-hand expression is evaluated and compared to each row of the subquery result using the given operator, which must yield a Boolean result. The result of ANY is “true” if any true result is obtained. The result is “false” if no true result is found (including the case where the subquery returns no rows).

SOME is a synonym for ANY. IN is equivalent to = ANY.

Note that if there are no successes and at least one right-hand row yields null for the operator's result, the result of the ANY construct will be null, not false. This is in accordance with SQL's normal rules for Boolean combinations of null values.

As with EXISTS, it's unwise to assume that the subquery will be evaluated completely.

row_constructor operator ANY (subquery) (NOT SUPPORTED)
row_constructor operator SOME (subquery) (NOT SUPPORTED)

The left-hand side of this form of ANY is a row constructor, as described in Section 4.2.13. The right-hand side is a parenthesized subquery, which must return exactly as many columns as there are expressions in the left-hand row. The left-hand expressions are evaluated and compared row-wise to each row of the subquery result, using the given operator. The result of ANY is “true” if the comparison returns true for any subquery row. The result is “false” if the comparison returns false for every subquery row (including the case where the subquery returns no rows). The result is NULL if no comparison with a subquery row returns true, and at least one comparison returns NULL.

See Section 9.24.5 for details about the meaning of a row constructor comparison.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x = ANY (SELECT y FROM (VALUES (3),(4),(5)) b(y)) → [
3
]
```

9.23.5. ALL

expression operator ALL (subquery)

The right-hand side is a parenthesized subquery, which must return exactly one column. The left-hand expression is evaluated and compared to each row of the subquery result using the given operator, which must yield a Boolean result. The result of ALL is “true” if all rows yield true (including the case where the subquery returns no rows). The result is “false” if any false result is found. The result is NULL if no comparison with a subquery row returns false, and at least one comparison returns NULL.

NOT IN is equivalent to <> ALL.

As with EXISTS, it's unwise to assume that the subquery will be evaluated completely.

row_constructor operator ALL (subquery) (NOT SUPPORTED)

The left-hand side of this form of ALL is a row constructor, as described in Section 4.2.13. The right-hand side is a parenthesized subquery, which must return exactly as many columns as there are expressions in the left-hand row. The left-hand expressions are evaluated and compared row-wise to each row of the subquery result, using the given operator. The result of ALL is “true” if the comparison returns true for all subquery rows (including the case where the subquery returns no rows). The result is “false” if the comparison returns false for any subquery row. The result is NULL if no comparison with a subquery row returns false, and at least one comparison returns NULL.

See Section 9.24.5 for details about the meaning of a row constructor comparison.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x <> ALL (SELECT y FROM (VALUES (3),(4),(5)) b(y)) ORDER BY x → [
1
2
]
```

9.23.6. Single-Row Comparison

row_constructor operator (subquery) (NOT SUPPORTED)

The left-hand side is a row constructor, as described in Section 4.2.13. The right-hand side is a parenthesized subquery, which must return exactly as many columns as there are expressions in the left-hand row. Furthermore, the subquery cannot return more than one row. (If it returns zero rows, the result is taken to be null.) The left-hand side is evaluated and compared row-wise to the single subquery result row.

See Section 9.24.5 for details about the meaning of a row constructor comparison.

## 9.24. Row and Array Comparisons {#row-and-array-comparisons}

This section describes several specialized constructs for making multiple comparisons between groups of values. These forms are syntactically related to the subquery forms of the previous section, but do not involve subqueries. The forms involving array subexpressions are PostgreSQL extensions; the rest are SQL-compliant. All of the expression forms documented in this section return Boolean (true/false) results.

9.24.1. IN

expression IN (value [, ...])
The right-hand side is a parenthesized list of expressions. The result is “true” if the left-hand expression's result is equal to any of the right-hand expressions. This is a shorthand notation for

expression = value1
OR
expression = value2
OR
...
Note that if the left-hand expression yields null, or if there are no equal right-hand values and at least one right-hand expression yields null, the result of the IN construct will be null, not false. This is in accordance with SQL's normal rules for Boolean combinations of null values.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x IN (1,2) ORDER BY x → [
1
2
]

SELECT x IN (y, z) FROM (VALUES (1,1,2),(2,3,null),(3,4,5),(4,null,null)) a(x,y,z) ORDER BY x → [
true
NULL
false
NULL
]
```

9.24.2. NOT IN

expression NOT IN (value [, ...])
The right-hand side is a parenthesized list of expressions. The result is “true” if the left-hand expression's result is unequal to all of the right-hand expressions. This is a shorthand notation for

expression <> value1
AND
expression <> value2
AND
...
Note that if the left-hand expression yields null, or if there are no equal right-hand values and at least one right-hand expression yields null, the result of the NOT IN construct will be null, not true as one might naively expect. This is in accordance with SQL's normal rules for Boolean combinations of null values.

Tip
x NOT IN y is equivalent to NOT (x IN y) in all cases. However, null values are much more likely to trip up the novice when working with NOT IN than when working with IN. It is best to express your condition positively if possible.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x NOT IN (1,2) ORDER BY x → [
3
]

SELECT x NOT IN (y, z) FROM (VALUES (1,1,2),(2,3,null),(3,4,5),(4,null,null)) a(x,y,z) ORDER BY x → [
false
NULL
true
NULL
]
```

9.24.3. ANY/SOME (array)

expression operator ANY (array expression)
expression operator SOME (array expression)

The right-hand side is a parenthesized expression, which must yield an array value. The left-hand expression is evaluated and compared to each element of the array using the given operator, which must yield a Boolean result. The result of ANY is “true” if any true result is obtained. The result is “false” if no true result is found (including the case where the array has zero elements).

If the array expression yields a null array, the result of ANY will be null. If the left-hand expression yields null, the result of ANY is ordinarily null (though a non-strict comparison operator could possibly yield a different result). Also, if the right-hand array contains any null elements and no true comparison result is obtained, the result of ANY will be null, not false (again, assuming a strict comparison operator). This is in accordance with SQL's normal rules for Boolean combinations of null values.

SOME is a synonym for ANY.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x = ANY (array[1,2]) ORDER BY x → [
1
2
]
]
```

9.24.4. ALL (array)

expression operator ALL (array expression)

The right-hand side is a parenthesized expression, which must yield an array value. The left-hand expression is evaluated and compared to each element of the array using the given operator, which must yield a Boolean result. The result of ALL is “true” if all comparisons yield true (including the case where the array has zero elements). The result is “false” if any false result is found.

If the array expression yields a null array, the result of ALL will be null. If the left-hand expression yields null, the result of ALL is ordinarily null (though a non-strict comparison operator could possibly yield a different result). Also, if the right-hand array contains any null elements and no false comparison result is obtained, the result of ALL will be null, not true (again, assuming a strict comparison operator). This is in accordance with SQL's normal rules for Boolean combinations of null values.

Example
```sql
SELECT x FROM (VALUES (1),(2),(3)) a(x) WHERE x <> ALL (array[1,2]) ORDER BY x → [
3
]
]
```

9.24.5. Row Constructor Comparison (NOT SUPPORTED)

row_constructor operator row_constructor

Each side is a row constructor, as described in Section 4.2.13. The two row constructors must have the same number of fields. The given operator is applied to each pair of corresponding fields. (Since the fields could be of different types, this means that a different specific operator could be selected for each pair.) All the selected operators must be members of some B-tree operator class, or be the negator of an = member of a B-tree operator class, meaning that row constructor comparison is only possible when the operator is =, <>, <, <=, >, or >=, or has semantics similar to one of these.

The = and <> cases work slightly differently from the others. Two rows are considered equal if all their corresponding members are non-null and equal; the rows are unequal if any corresponding members are non-null and unequal; otherwise the result of the row comparison is unknown (null).

For the <, <=, > and >= cases, the row elements are compared left-to-right, stopping as soon as an unequal or null pair of elements is found. If either of this pair of elements is null, the result of the row comparison is unknown (null); otherwise comparison of this pair of elements determines the result. For example, ROW(1,2,NULL) < ROW(1,3,0) yields true, not null, because the third pair of elements are not considered.

Note
Prior to PostgreSQL 8.2, the <, <=, > and >= cases were not handled per SQL specification. A comparison like ROW(a,b) < ROW(c,d) was implemented as a < c AND b < d whereas the correct behavior is equivalent to a < c OR (a = c AND b < d).

row_constructor IS DISTINCT FROM row_constructor

This construct is similar to a <> row comparison, but it does not yield null for null inputs. Instead, any null value is considered unequal to (distinct from) any non-null value, and any two nulls are considered equal (not distinct). Thus the result will either be true or false, never null.

row_constructor IS NOT DISTINCT FROM row_constructor

This construct is similar to a = row comparison, but it does not yield null for null inputs. Instead, any null value is considered unequal to (distinct from) any non-null value, and any two nulls are considered equal (not distinct). Thus the result will always be either true or false, never null.

9.24.6. Composite Type Comparison (NOT SUPPORTED)

record operator record

The SQL specification requires row-wise comparison to return NULL if the result depends on comparing two NULL values or a NULL and a non-NULL. PostgreSQL does this only when comparing the results of two row constructors (as in Section 9.24.5) or comparing a row constructor to the output of a subquery (as in Section 9.23). In other contexts where two composite-type values are compared, two NULL field values are considered equal, and a NULL is considered larger than a non-NULL. This is necessary in order to have consistent sorting and indexing behavior for composite types.

Each side is evaluated and they are compared row-wise. Composite type comparisons are allowed when the operator is =, <>, <, <=, > or >=, or has semantics similar to one of these. (To be specific, an operator can be a row comparison operator if it is a member of a B-tree operator class, or is the negator of the = member of a B-tree operator class.) The default behavior of the above operators is the same as for IS [ NOT ] DISTINCT FROM for row constructors (see Section 9.24.5).

To support matching of rows which include elements without a default B-tree operator class, the following operators are defined for composite type comparison: *=, *<>, *<, *<=, *>, and *>=. These operators compare the internal binary representation of the two rows. Two rows might have a different binary representation even though comparisons of the two rows with the equality operator is true. The ordering of rows under these comparison operators is deterministic but not otherwise meaningful. These operators are used internally for materialized views and might be useful for other specialized purposes such as replication and B-Tree deduplication (see Section 64.4.3). They are not intended to be generally useful for writing queries, though.

## 9.25. Set Returning Functions {#set-returning-functions}

This section describes functions that possibly return more than one row. The most widely used functions in this class are series generating functions, as detailed in Table 9.63 and Table 9.64. Other, more specialized set-returning functions are described elsewhere in this manual. See Section 7.2.1.4 for ways to combine multiple set-returning functions.

Table 9.63. Series Generating Functions

#|
||Function|Description||
||generate_series ( start integer, stop integer [, step integer ] ) → setof integer  
generate_series ( start bigint, stop bigint [, step bigint ] ) → setof bigint  
generate_series ( start numeric, stop numeric [, step numeric ] ) → setof numeric|
Generates a series of values from start to stop, with a step size of step. step defaults to 1.||
||generate_series ( start timestamp, stop timestamp, step interval ) → setof timestamp  
generate_series ( start timestamp with time zone, stop timestamp with time zone, step interval ) → setof timestamp with time zone|
Generates a series of values from start to stop, with a step size of step.||
|#

When step is positive, zero rows are returned if start is greater than stop. Conversely, when step is negative, zero rows are returned if start is less than stop. Zero rows are also returned if any input is NULL. It is an error for step to be zero. Some examples follow:

```sql
SELECT * FROM generate_series(2,4) a → [
2
3
4
]

SELECT * FROM generate_series(5,1,-2) a → [
5
3
1
]

SELECT * FROM generate_series(4,3) a → [
]

SELECT * FROM generate_series(1.1, 4, 1.3) a → [
1.1
2.4
3.7
]

-- this example relies on the date-plus-integer operator:
SELECT date '2004-02-05' + s.a AS dates FROM generate_series(0,14,7) AS s(a) → [
2004-02-05
2004-02-12
2004-02-19
]

SELECT * FROM generate_series('2008-03-01 00:00'::timestamp, '2008-03-04 12:00', '10 hours') a → [
2008-03-01 00:00:00
2008-03-01 10:00:00
2008-03-01 20:00:00
2008-03-02 06:00:00
2008-03-02 16:00:00
2008-03-03 02:00:00
2008-03-03 12:00:00
2008-03-03 22:00:00
2008-03-04 08:00:00
]
```

Table 9.64. Subscript Generating Functions (NOT SUPPORTED)

#|
||Function|Description||
||generate_subscripts ( array anyarray, dim integer ) → setof integer|
Generates a series comprising the valid subscripts of the dim'th dimension of the given array.||
||generate_subscripts ( array anyarray, dim integer, reverse boolean ) → setof integer|
Generates a series comprising the valid subscripts of the dim'th dimension of the given array. When reverse is true, returns the series in reverse order.||
|#

```sql
-- basic usage:
#SELECT generate_subscripts('{NULL,1,NULL,2}'::int[], 1) AS s → [
1
2
3
4
]

SELECT a AS array, s AS subscript, a[s] AS value FROM (SELECT generate_subscripts(a, 1) AS s, a FROM (VALUES (array[-1,-2]),(array[100,200,300])) s(a)) foo;
```

## 9.26. System Information Functions and Operators (NOT SUPPORTED) {#system-information-functions}

## 9.27. System Administration Functions (NOT SUPPORTED) {#system-administration-functions}

## 9.28. Trigger Functions (NOT SUPPORTED) {#trigger-functions}

## 9.29. Event Trigger Functions (NOT SUPPORTED) {#event-trigger-functions}

## 9.30. Statistics Information Functions (NOT SUPPORTED) {#statistics-information-functions}
