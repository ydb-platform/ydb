## FIND {#find}

Finding the position of a substring in a string.

Required arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default that means "from the beginning of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

**Examples**

```yql
SELECT FIND("abcdefg_abcdefg", "abc"); -- 0
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 1); -- 8
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 9); -- null
```

## RFIND {#rfind}

Reverse finding the position of a substring in a string, from the end to the beginning.

Required arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default, meaning "from the end of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

**Examples**

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd"); -- 9
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 8); -- 1
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 0); -- null
```

