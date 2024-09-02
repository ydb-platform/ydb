# Unicode
Функции для работы с Unicode строками.

**Список функций**

* ```Unicode::IsUtf(String) -> Bool```

  Проверяет является ли строка валидной utf-8 последовательностью. Например, строка ```"\xF0"``` не является валидной utf-8 последовательностью, а строка ```"\xF0\x9F\x90\xB1"``` корректно описывает utf-8 emoji с котиком.

* ```Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64```

  Возвращает длину utf-8 строки в символах (unicode code points). Суррогатные пары учитываются как один символ.
```sql
SELECT Unicode::GetLength("жніўня"); -- 6
```

* ```Unicode::Find(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?```
* ```Unicode::RFind(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?```

  Поиск первого(```RFind``` - последнего) вхождения подстроки в строку начиная с позиции ```pos```. Возвращает позицию первого символа от найденной подстроки, в случае неуспеха возвращается Null.

```sql
SELECT Unicode::Find("aaa", "bb"); -- Null
```

* ```Unicode::Substring(string:Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8```

  Возвращает подстроку от ```string``` начиная с символа ```from``` длиной в ```len``` символов. Если аргумент ```len``` опущен, то подстрока берется до конца исходной строки.
  В случае ```from``` больше длины исходной строки, возвращается пустая строка ```""```

```sql
select Unicode::Substring("0123456789abcdefghij", 10); -- "abcdefghij"
```

* Функции ```Unicode::Normalize...``` приводят переданную utf-8 строку в одну из [нормальных форм](https://unicode.org/reports/tr15/#Norm_Forms):

  * ```Unicode::Normalize(Utf8{Flags:AutoMap}) -> Utf8``` -- NFC
  * ```Unicode::NormalizeNFD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFC(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKD(Utf8{Flags:AutoMap}) -> Utf8```
  * ```Unicode::NormalizeNFKC(Utf8{Flags:AutoMap}) -> Utf8```


* ```Unicode::Translit(string:Utf8{Flags:AutoMap}, [lang:String?]) -> Utf8```

  Транслитерирует в латинский алфавит слова переданной строки, целиком состоящие из символов алфавита языка, переданного вторым аргументом. Если язык не указан, то транслитерация ведется с русского. Доступные языки: "kaz", "rus", "tur", "ukr".

```sql
select Unicode::Translit("Тот уголок земли, где я провел"); -- "Tot ugolok zemli, gde ya provel"
```

* ```Unicode::LevensteinDistance(stringA:Utf8{Flags:AutoMap}, stringB:Utf8{Flags:AutoMap}) -> Uint64```

  Вычисляет расстояние Левенштейна для переданных строк.

* ```Unicode::Fold(Utf8{Flags:AutoMap}, [ Language:String?, DoLowerCase:Bool?, DoRenyxa:Bool?, DoSimpleCyr:Bool?, FillOffset:Bool? ]) -> Utf8```

  Выполняет [case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) для переданной строки.
  Параметры:
  - ```Language``` задается по тем же правилам, что и в ```Unicode::Translit()```
  - ```DoLowerCase``` приводит строку к нижнему регистру, по умолчанию ```true```
  - ```DoRenyxa``` приводить символы с диокрифами к аналогичным латинским символам, по умолчанию ```true```
  - ```DoSimpleCyr``` приводить кирилические символы с диокрифами к аналогичным латинским символам, по умолчанию ```true```
  - ```FillOffset``` параметр не используется

```sql
select Unicode::Fold("Kongreßstraße",  false AS DoSimpleCyr, false AS DoRenyxa); -- "kongressstrasse"
select Unicode::Fold("ҫурт"); -- "сурт"
SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
```

* ```Unicode::ReplaceAll(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```
* ```Unicode::ReplaceFirst(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```
* ```Unicode::ReplaceLast(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8```

  Заменяет все/первое/последнее вхождения строки ```find``` в ```input``` на ```replacement```.

* ```Unicode::RemoveAll(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```
* ```Unicode::RemoveFirst(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```
* ```Unicode::RemoveLast(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8```

  Удаляются все/первое/последнее вхождения символов в наборе ```symbols``` из ```input```. Второй аргумент интерпретируется как неупорядоченный набор символов для удаления.
```sql
select Unicode::ReplaceLast("absence", "enc", ""); -- "abse"
select Unicode::RemoveAll("abandon", "an"); -- "bdo"
```

* ```Unicode::ToCodePointList(Utf8{Flags:AutoMap}) -> List<Uint32>```

  Разбить строку на unicode'ую последовательность codepoint'ов.
* ```Unicode::FromCodePointList(List<Uint32>{Flags:AutoMap}) -> Utf8```

  Сформировать unicode строку из codepoint'ов.

```sql
select Unicode::ToCodePointList("Щавель"); -- [1065, 1072, 1074, 1077, 1083, 1100]
select Unicode::FromCodePointList(AsList(99,111,100,101,32,112,111,105,110,116,115,32,99,111,110,118,101,114,116,101,114)); -- "code points converter"
```

* ```Unicode::Reverse(Utf8{Flags:AutoMap}) -> Utf8```

  Перевернуть строку.

* ```Unicode::ToLower(Utf8{Flags:AutoMap}) -> Utf8```
* ```Unicode::ToUpper(Utf8{Flags:AutoMap}) -> Utf8```
* ```Unicode::ToTitle(Utf8{Flags:AutoMap}) -> Utf8```

  Привести регистр строки к ВЕРХНЕМУ, нижнему или Заглавному виду.

* ```Unicode::SplitToList( string:Utf8?, separator:Utf8, [ DelimeterString:Bool?,  SkipEmpty:Bool?, Limit:Uint64? ]) -> List<Utf8>```

  Разбиение строки на подстроки по разделителю.
  ```string``` -- исходная строка
  ```separator``` -- разделитель
  Параметры:
  - DelimeterString:Bool? — считать разделитель строкой (true, по умолчанию) или набором символов "любой из" (false)
  - SkipEmpty:Bool? - пропускать ли пустые строки в результате, по умолчанию false
  - Limit:Uint64? - ограничение на число извлекаемых компонент, по умолчанию не ограничено; необработанный суффикс оригинальной строки возвращается последним элементом при превышении лимита

* ```Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, separator:Utf8) -> Utf8```

  Конкатенация списка строк через ```separator``` в единую строку.

```sql
select Unicode::SplitToList("One, two, three, four, five", ", ", 2 AS Limit); -- ["One", "two", "three, four, five"]
select Unicode::JoinFromList(["One", "two", "three", "four", "five"], ";"); -- "One;two;three;four;five"
```

* ```Unicode::ToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64```

  Конвертация из строки в число.
  Второй опциональный аргумент задает систему счисления, по умолчанию 0 - автоматическое определение по префиксу.
  Поддерживаемые префиксы : 0x(0X) - base-16, 0 - base-8. Система по-умолчанию - base-10.
  Знак '-' перед числом интерпретируется как в беззнаковой арифметике языка C, например -0x1 -> UI64_MAX.
  В случае наличия в строке некорректных символов или выхода числа за границы ui64 функция завершается с ошибкой.
* ```Unicode::TryToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64?```

  Аналогично функции Unicode::ToUint64(), но вместо ошибки возвращает Null.
```sql
select Unicode::ToUint64("77741"); -- 77741
select Unicode::ToUint64("-77741"); -- 18446744073709473875
select Unicode::TryToUint64("asdh831"); -- Null
```

* ```Unicode::Strip(string:Utf8{Flags:AutoMap}) -> Utf8```

  Вырезает из строки крайние символы Unicode-категории Space.
```sql
select Unicode::Strip("\u200ыкль\u2002"u); -- "ыкль"
```

* ```Unicode::IsAscii(string:Utf8{Flags:AutoMap}) -> Bool```

  Проверяет, состоит ли utf-8 строка исключительно из символов ascii.
* ```Unicode::IsSpace(string:Utf8{Flags:AutoMap}) -> Bool```
* ```Unicode::IsUpper(string:Utf8{Flags:AutoMap}) -> Bool```
* ```Unicode::IsLower(string:Utf8{Flags:AutoMap}) -> Bool```
* ```Unicode::IsAlpha(string:Utf8{Flags:AutoMap}) -> Bool```
* ```Unicode::IsAlnum(string:Utf8{Flags:AutoMap}) -> Bool```
* ```Unicode::IsHex(string:Utf8{Flags:AutoMap}) -> Bool```

  Проверяют, отвечает ли utf-8 строка указанному условию.

* ```Unicode::IsUnicodeSet(string:Utf8{Flags:AutoMap}, unicode_set:Utf8) -> Bool```

  Проверяет, состоит ли utf-8 строка ```string``` исключительно из символов, указанных в ```unicode_set```. Символы в ```unicode_set``` нужно указывать в квадратных скобках.
```sql
select Unicode::IsUnicodeSet("ваоао"u, "[вао]"u); -- true
select Unicode::IsUnicodeSet("ваоао"u, "[ваб]"u); -- false
```
