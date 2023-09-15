
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

# 9.5. Binary String Functions and Operators
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

# 9.6. Bit String Functions and Operators
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

# 9.7. Pattern Matching

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
'abcd' ~ 'a.c'    → true -- dot matches any character
'abcd' ~ 'a.*d'   → true --* repeats the preceding pattern item
'abcd' ~ '(b|x)'  → true --| means OR, parentheses group
'abcd' ~ '^a'     → true -- ^ anchors to start of string
'abcd' ~ '^(b|c)' → false --  would match except for anchoring
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
#SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', '\s+') → {the,quick,brown,fox,jumps,over,the,lazy,dog}
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

# 9.8. Data Type Formatting Functions

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

# 9.9. Date/Time Functions and Operators

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
#(DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') → true
#(DATE '2001-02-16', INTERVAL '100 days') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') → false
#(DATE '2001-10-29', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31') → false
#(DATE '2001-10-30', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31') → true
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
SELECT TIMESTAMP 'now';  -- but see tip below
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

# 9.10. Enum Support Functions (NOT SUPPORTED)

# 9.11. Geometric Functions and Operators
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

# 9.12. Network Address Functions and Operators
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
