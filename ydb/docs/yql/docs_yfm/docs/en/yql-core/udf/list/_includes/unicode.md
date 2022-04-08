# Unicode

Functions for Unicode strings.

**List of functions**

* ```Unicode::IsUtf(String) -> Bool```
Checks whether a string is a valid UTF-8 sequence. For example, the string ```"\xF0"``` isn't a valid UTF-8 sequence, but the string ```"\xF0\x9F\x90\xB1"```  correctly describes a UTF-8 cat emoji.

* ```Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64```
Returns the length of a utf-8 string in unicode code points. Surrogate pairs are counted as one character.

* ```Unicode::Find(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```

* ```Unicode::RFind(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```

* ```Unicode::Substring(Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8```
Returns a substring starting with ```from``` with the length of ```len``` characters. If the ```len``` argument is omitted, the substring is moved to the end of the source string.

* The ```Unicode::Normalize...``` functions convert the passed UTF-8 string to a [normalization form](https://unicode.org/reports/tr15/#Norm_Forms):
  * ```Unicode::Normalize(Utf8{Flags:AutoMap}) -> Utf8``` -- NFC
  * ```Unicode::NormalizeNFD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFC(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKC(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::Translit(Utf8{Flags:AutoMap}, [String?]) -> Utf8```
  Transliterates with Latin letters the words from the passed string, consisting entirely of characters of the alphabet of the language passed by the second argument. If no language is specified, the words are transliterated from Russian. Available languages: "kaz", "rus", "tur", and "ukr".

* ```Unicode::LevensteinDistance(Utf8{Flags:AutoMap}, Utf8{Flags:AutoMap}) -> Uint64```
  Calculates the Levenshtein distance for the passed strings.

* ```Unicode::Fold(Utf8{Flags:AutoMap}, [ Language:String?, DoLowerCase:Bool?, DoRenyxa:Bool?, DoSimpleCyr:Bool?, FillOffset:Bool? ]) -> Utf8```
  [A case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) is performed on the passed string. ```Language``` is set according to the same rules as in ```Unicode::Translit()```. ```DoLowerCase``` converts a string to lowercase letters, defaults to ```true```.

* ```Unicode::ReplaceAll(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Arguments: ```input```, ```find```, ```replacement```. Replaces all occurrences of the ```find``` string in the ```input``` with ```replacement```.

* ```Unicode::ReplaceFirst(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Arguments: ```input```, ```findSymbol```, ```replacementSymbol```. Replaces the first occurrence of the ```findSymbol``` character in the  ```input``` with ```replacementSymbol```. The character can't be a surrogate pair.

* ```Unicode::ReplaceLast(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Arguments: ```input```, ```findSymbol```, ```replacementSymbol```. Replaces the last occurrence of the ```findSymbol``` character in the ```input``` with ```replacementSymbol```. The character can't be a surrogate pair.

* ```Unicode::RemoveAll(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  The second argument is interpreted as an unordered set of characters to be removed. Removes all occurrences.

* ```Unicode::RemoveFirst(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  The second argument is interpreted as an unordered set of characters to be removed. Removes the first occurrence.

* ```Unicode::RemoveLast(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  The second argument is interpreted as an unordered set of characters to be removed. Removes the last occurrence.

* ```Unicode::ToCodePointList(Utf8{Flags:AutoMap}) -> List<Uint32>```

* ```Unicode::FromCodePointList(List<Uint32>{Flags:AutoMap}) -> Utf8```

* ```Unicode::Reverse(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToLower(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToUpper(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToTitle(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::SplitToList( Utf8?, Utf8, [ DelimeterString:Bool?,  SkipEmpty:Bool?, Limit:Uint64? ]) -> List<Utf8>```
  The first argument is the source string
  The second argument is a delimiter
  The third argument includes the following parameters:
    - DelimeterString:Bool? — treating a delimiter as a string (true, by default) or a set of characters "any of" (false)
    - SkipEmpty:Bool? - whether to skip empty strings in the result, is false by default
    - Limit:Uint64? - Limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item

* ```Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, Utf8) -> Utf8```

**Examples**

```sql
SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
SELECT Unicode::GetLength("жніўня");                  -- 6
```

