
# 9.1. Logical Operators

The usual logical operators are available:

* boolean AND boolean → boolean
* boolean OR boolean → boolean
* NOT boolean → boolean

# 9.2. Comparison Functions and Operators


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
#true IS TRUE → true
#NULL::boolean IS TRUE → false
```||
|#

# 9.3. Mathematical Functions and Operators

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

# 9.4. String Functions and Operators
## 9.4.1. format
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
#concat('abcde', 2, NULL, 22) → abcde222
```||
||concat_ws ( sep text, val1 "any" [, val2 "any" [, ...] ] ) → text|
Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored. (NOT SUPPORTED)|
```sql
#concat_ws(',', 'abcde', 2, NULL, 22) → abcde,2,22
```||
||format ( formatstr text [, formatarg "any" [, ...] ] ) → text|
Formats arguments according to a format string; see Section 9.4.1. This function is similar to the C function sprintf. (NOT SUPPORTED)|
```sql
#format('Hello %s, %1$s', 'World') → Hello World, World
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
quote_literal(E'O\'Reilly') → 'O''Reilly'
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
Splits string using a POSIX regular expression as the delimiter, producing an array of results; see Section 9.7.3. (NOT SUPPORTED)|
```sql
#regexp_split_to_array('hello world', '\s+') → {hello,world}
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
Splits the string at occurrences of delimiter and forms the resulting fields into a text array. If delimiter is NULL, each character in the string will become a separate element in the array. If delimiter is an empty string, then the string is treated as a single field. If null_string is supplied and is not NULL, fields matching that string are replaced by NULL. (NOT SUPPORTED)|
```sql
#string_to_array('xx~~yy~~zz', '~~', 'yy') → {xx,NULL,zz}
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

9.5. Binary String Functions and Operators
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
