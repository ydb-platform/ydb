# Unicode

Functions for Unicode strings.

**List of functions**

* ```Unicode::IsUtf(String) -> Bool```

  Checks whether a string is a valid UTF-8 sequence. For example, the string ```"\xF0"``` isn't a valid UTF-8 sequence, but the string ```"\xF0\x9F\x90\xB1"``` correctly describes a UTF-8 cat emoji.

* ```Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64```

  Returns the length of a utf-8 string in unicode code points. Surrogate pairs are counted as one character.

```sql
SELECT Unicode::GetLength("жніўня"); -- 6
```

* ```Unicode::Find(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?```

* ```Unicode::RFind(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?```

  Finding the first (```RFind``` - the last) occurrence of a substring in a string starting from the ```pos``` position. Returns the position of the first character from the found substring. In case of failure, returns Null.

```sql
SELECT Unicode::Find("aaa", "bb"); -- Null
```

* ```Unicode::Substring(string:Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8```

  Returns a ```string``` substring starting with ```from``` that is ```len``` characters long. If the ```len``` argument is omitted, the substring is taken to the end of the source string.
If ```from``` exceeds the length of the original string, an empty string ```""``` is returned.

```sql
select Unicode::Substring("0123456789abcdefghij", 10); -- "abcdefghij"
```

* The ```Unicode::Normalize...``` functions convert the passed UTF-8 string to a [normalization form](https://unicode.org/reports/tr15/#Norm_Forms):
  * ```Unicode::Normalize(Utf8{Flags:AutoMap}) -> Utf8``` -- NFC
  * ```Unicode::NormalizeNFD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFC(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKC(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::Translit(string:Utf8{Flags:AutoMap}, [lang:String?]) -> Utf8```

  Transliterates with Latin letters the words from the passed string, consisting entirely of characters of the alphabet of the language passed by the second argument. If no language is specified, the words are transliterated from Russian. Available languages: "kaz", "rus", "tur", and "ukr".

```sql
select Unicode::Translit("Тот уголок земли, где я провел"); -- "Tot ugolok zemli, gde ya provel"
```

* ```Unicode::LevensteinDistance(stringA:Utf8{Flags:AutoMap}, stringB:Utf8{Flags:AutoMap}) -> Uint64```

  Calculates the Levenshtein distance for the passed strings.

* ```Unicode::Fold(Utf8{Flags:AutoMap}, [ Language:String?, DoLowerCase:Bool?, DoRenyxa:Bool?, DoSimpleCyr:Bool?, FillOffset:Bool? ]) -> Utf8```

  Performs [case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) on the passed string.
Parameters:
  - ```Language``` is set according to the same rules as in ```Unicode::Translit()```.
  - ```DoLowerCase``` converts a string to lowercase letters, defaults to ```true```.
  - ```DoRenyxa``` converts diacritical characters to similar Latin characters, defaults to ```true```.
  - ```DoSimpleCyr``` converts diacritical Cyrillic characters to similar Latin characters, defaults to ```true```.
  - ```FillOffset``` parameter is not used.

```sql
select Unicode::Fold("Kongreßstraße", false AS DoSimpleCyr, false AS DoRenyxa); -- "kongressstrasse"
select Unicode::Fold("ҫурт"); -- "сурт"
SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
```

* ```Unicode::ReplaceAll(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```

* ```Unicode::ReplaceFirst(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```

* ```Unicode::ReplaceLast(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```

  Replaces all/first/last occurrences of the ```find``` string in the ```input``` with ```replacement```.

* ```Unicode::RemoveAll(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```

* ```Unicode::RemoveFirst(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```

* ```Unicode::RemoveLast(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```

  Deletes all/first/last occurrences of characters in the ```symbols``` set from the ```input```. The second argument is interpreted as an unordered set of characters to be removed.

```sql
select Unicode::ReplaceLast("absence", "enc", ""); -- "abse"
select Unicode::RemoveAll("abandon", "an"); -- "bdo"
```

* ```Unicode::ToCodePointList(Utf8{Flags:AutoMap}) -> List<Uint32>```

  Splits a string into a Unicode sequence of codepoints.

* ```Unicode::FromCodePointList(List<Uint32>{Flags:AutoMap}) -> Utf8```

  Generates a Unicode string from codepoints.

```sql
select Unicode::ToCodePointList("Щавель"); -- [1065, 1072, 1074, 1077, 1083, 1100]
select Unicode::FromCodePointList(AsList(99,111,100,101,32,112,111,105,110,116,115,32,99,111,110,118,101,114,116,101,114)); -- "code points converter"
```

* ```Unicode::Reverse(Utf8{Flags:AutoMap}) -> Utf8```

  Reverses a string.

* ```Unicode::ToLower(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToUpper(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToTitle(Utf8{Flags:AutoMap}) -> Utf8```

  Converts a string to UPPER, lower, or Title case.

* ```Unicode::SplitToList( string:Utf8?, separator:Utf8, [ DelimeterString:Bool?, SkipEmpty:Bool?, Limit:Uint64? ]) -> List<Utf8>```

  Splits a string into substrings by separator.
```string``` -- Source string. ```separator``` -- Separator. Parameters:
  - DelimeterString:Bool? — treating a delimiter as a string (true, by default) or a set of characters "any of" (false)
  - SkipEmpty:Bool? - whether to skip empty strings in the result, is false by default
  - Limit:Uint64? - Limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item

* ```Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, separator:Utf8) -> Utf8```

  Concatenates a list of strings via a ```separator``` into a single string.

```sql
select Unicode::SplitToList("One, two, three, four, five", ", ", 2 AS Limit); -- ["One", "two", "three, four, five"]
select Unicode::JoinFromList(["One", "two", "three", "four", "five"], ";"); -- "One;two;three;four;five"
```

* ```Unicode::ToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64```

  Converts a string to a number.
The second optional argument sets the number system. By default, 0 (automatic detection by prefix).
Supported prefixes: 0x(0X) - base-16, 0 - base-8. Defaults to base-10.
The '-' sign before a number is interpreted as in C unsigned arithmetic. For example, -0x1 -> UI64_MAX.
If there are incorrect characters in a string or a number goes beyond ui64, the function terminates with an error.

* ```Unicode::TryToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64?```

  Similar to the Unicode::ToUint64() function, except that it returns Null instead of an error.

```sql
select Unicode::ToUint64("77741"); -- 77741
select Unicode::ToUint64("-77741"); -- 18446744073709473875
select Unicode::TryToUint64("asdh831"); -- Null
```

