## SUBSTRING {#substring}

Returns a substring.

Required arguments:

* Source string;
* Position: The offset from the beginning of the string in bytes (integer) or `NULL` meaning "from the beginning".

Optional arguments:

* Substring length: The number of bytes starting from the specified position (an integer, or the default `NULL` meaning "up to the end of the source string").

Indexing starts from zero. If the specified position and length are beyond the string, returns an empty string.
If the input string is optional, the result is also optional.

**Examples**

```yql
SELECT SUBSTRING("abcdefg", 3, 1); -- d
```

```yql
SELECT SUBSTRING("abcdefg", 3); -- defg
```

```yql
SELECT SUBSTRING("abcdefg", NULL, 3); -- abc
```

