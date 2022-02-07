# Unicode

Functions for Unicode strings.

**List of functions **

* ```Unicode::IsUtf(String) -> Bool```
Проверяет является ли строка валидной utf-8 последовательностью. Например, строка ```"\xF0"``` isn't a valid UTF-8 sequence, but the ```"\xF0\x9F\x90\xB1"```  string correctly describes a UTF-8 cat emoji.

* ```Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64```
Возвращает длину utf-8 строки в символах (unicode code points). Суррогатные пары учитываются как один символ.

* ```Unicode::Find(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```

* ```Unicode::RFind(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```

* ```Unicode::Substring(Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8```
Возвращает подстроку начиная с символа ```from``` with the length of ```len``` characters. If the ```len``` argument is omitted, the substring is taken to the end of the source string.

* The ```Unicode::Normalize...``` functions convert the passed UTF-8 string to a [normalization form](https://unicode.org/reports/tr15/#Norm_Forms):
  * ```Unicode::Normalize(Utf8{Flags:AutoMap}) -> Utf8``` -- NFC
  * ```Unicode::NormalizeNFD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFC(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKC(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::Translit(Utf8{Flags:AutoMap}, [String?]) -> Utf8```
Транслитерирует в латинский алфавит слова переданной строки, целиком состоящие из символов алфавита языка, переданного вторым аргументом. Если язык не указан, то транслитерация ведется с русского. Доступные языки: "kaz", "rus", "tur", "ukr".

* ```Unicode::LevensteinDistance(Utf8{Flags:AutoMap}, Utf8{Flags:AutoMap}) -> Uint64```
Вычисляет расстояние Левенштейна для переданных строк.

* ```Unicode::Fold(Utf8{Flags:AutoMap}, [ Language:String?, DoLowerCase:Bool?, DoRenyxa:Bool?, DoSimpleCyr:Bool?, FillOffset:Bool? ]) -> Utf8```
Выполняет [A case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) on the passed string. ```Language``` is set according to the same rules as in ```Unicode::Translit()```. ```DoLowerCase``` converts a string to lowercase letters, defaults to ```true```.

* ```Unicode::ReplaceAll(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
Aргументы: ```input```, ```find```, ```replacement```. Replaces all occurrences of the ```find``` string in the ```input``` with ```replacement```.

* ```Unicode::ReplaceFirst(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
Aргументы: ```input```, ```findSymbol```, ```replacementSymbol```. Replaces the first occurrence of the ```findSymbol``` character in the  ```input``` with ```replacementSymbol```. The character can't be a surrogate pair.

* ```Unicode::ReplaceLast(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
Aргументы: ```input```, ```findSymbol```, ```replacementSymbol```. Replaces the last occurrence of the ```findSymbol``` character in the ```input``` with ```replacementSymbol```. The character can't be a surrogate pair.

* ```Unicode::RemoveAll(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляются все вхождения.

* ```Unicode::RemoveFirst(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляется первое вхождение.

* ```Unicode::RemoveLast(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляется последнее вхождение.

* ```Unicode::ToCodePointList(Utf8{Flags:AutoMap}) -> List<Uint32>```

* ```Unicode::FromCodePointList(List<Uint32>{Flags:AutoMap}) -> Utf8```

* ```Unicode::Reverse(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToLower(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToUpper(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::ToTitle(Utf8{Flags:AutoMap}) -> Utf8```

* ```Unicode::SplitToList( Utf8?, Utf8, [ DelimeterString:Bool?,  SkipEmpty:Bool?, Limit:Uint64? ]) -> List<Utf8>```
Первый аргумент -- исходная строка
Второй аргумент -- разделитель
Третий аргумент -- параметры:
    - DelimeterString:Bool? — treating a delimiter as a string (true, by default) or a set of characters "any of" (false)
    - SkipEmpty:Bool? - whether to skip empty strings in the result, is false by default
    - Limit:Uint64? - Limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item

* ```Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, Utf8) -> Utf8```

**Examples**

```sql
SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
SELECT Unicode::GetLength("жніўня");                  -- 6
```

