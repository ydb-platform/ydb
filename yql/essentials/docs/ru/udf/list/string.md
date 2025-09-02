# String

Функции для работы с ASCII-строками.

## Список функций

* `String::Base32Encode(string:String{Flags:AutoMap}) -> String`
* `String::Base32Decode(string:String) -> String?`
* `String::Base32StrictDecode(string:String) -> String?`
* `String::Base64Encode(string:String{Flags:AutoMap}) -> String`
* `String::Base64Decode(string:String) -> String?`
* `String::Base64StrictDecode(string:String) -> String?`
* `String::EscapeC(string:String{Flags:AutoMap}) -> String`
* `String::UnescapeC(string:String{Flags:AutoMap}) -> String`
* `String::HexEncode(string:String{Flags:AutoMap}) -> String`
* `String::HexDecode(string:String) -> String?`
* `String::EncodeHtml(string:String{Flags:AutoMap}) -> String`
* `String::DecodeHtml(string:String{Flags:AutoMap}) -> String`
* `String::CgiEscape(string:String{Flags:AutoMap}) -> String`
* `String::CgiUnescape(string:String{Flags:AutoMap}) -> String`

Кодирует или декодирует строку указанным образом.

## Примеры

```yql
SELECT String::Base64Encode("YQL"); -- "WVFM"
```

`String::Strip(string:String{Flags:AutoMap}) -> String`

Вырезает из строки крайние пробелы.

```yql
SELECT String::Strip("YQL ");       -- "YQL"
```

* `String::Collapse(string:String{Flags:AutoMap}) -> String`

  Заменяет множественные пробелы внутри строки одиночными.

* `String::CollapseText(string:String{Flags:AutoMap}, limit:Uint64) -> String`

  Укорачивает текст до указанного размера с добавлением троеточия.

* `String::Contains(string:String?, substring:String) -> Bool`

  Проверяет наличие подстроки в строке.

* `String::AsciiContainsIgnoreCase(string:String?, substring:String) -> Bool`
* `String::AsciiEqualsIgnoreCase(left:String?, right:String) -> Bool`
  Проверяют наличие подстроки или полное равенство строк без учета регистра символов - добавлены в версии [2025.02](../../changelog/2025.02.md#string-module)

* `String::Find(string:String{Flags:AutoMap}, String, [Uint64?]) -> Int64` - Устаревшая: используйте встроенную функцию [Find](../../builtins/basic.md#find)
* `String::ReverseFind(string:String{Flags:AutoMap}, String, [Uint64?]) -> Int64` - Устаревшая: используйте встроенную функцию [RFind](../../builtins/basic.md#rfind)
* `String::Substring(string:String{Flags:AutoMap}, [Uint64?, Uint64?]) -> String` - Устаревшая: используйте встроенную функцию [Substring](../../builtins/basic.md#substring)
* `String::HasPrefix(string:String?, prefix:String) -> Bool` - Устаревшая: используйте встроенную функцию [StartsWith](../../builtins/basic.md#starts_ends_with)
* `String::StartsWith(string:String?, prefix:String) -> Bool` - Устаревшая: используйте встроенную функцию [StartsWith](../../builtins/basic.md#starts_ends_with)
* `String::HasSuffix(string:String?, suffix:String) -> Bool` - Устаревшая: используйте встроенную функцию [EndsWith](../../builtins/basic.md#starts_ends_with)
* `String::EndsWith(string:String?, suffix:String) -> Bool` - Устаревшая: используйте встроенную функцию [EndsWith](../../builtins/basic.md#starts_ends_with)
* `String::Reverse(string:String?) -> String?` - удалена в версии [2025.02](../../changelog/2025.02.md#string-module)

  Устаревшие функции, к использованию не рекомендуются.

* `String::AsciiStartsWithIgnoreCase(string:String?, prefix:String) -> Bool`
* `String::AsciiEndsWithIgnoreCase(string:String?, suffix:String) -> Bool`
* `String::HasPrefixIgnoreCase(string:String?, prefix:String) -> Bool` - удалена в версии [2025.02](../../changelog/2025.02.md#string-module)
* `String::StartsWithIgnoreCase(string:String?, prefix:String) -> Bool` - удалена в версии [2025.02](../../changelog/2025.02.md#string-module)
* `String::HasSuffixIgnoreCase(string:String?, suffix:String) -> Bool` - удалена в версии [2025.02](../../changelog/2025.02.md#string-module)
* `String::EndsWithIgnoreCase(string:String?, suffix:String) -> Bool` - удалена в версии [2025.02](../../changelog/2025.02.md#string-module)

  Проверяют наличие префикса или суффикса в строке без учёта региста символов.

* `String::AsciiToLower(string:String{Flags:AutoMap}) -> String` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToLower
* `String::AsciiToUpper(string:String{Flags:AutoMap}) -> String` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToUpper
* `String::AsciiToTitle(string:String{Flags:AutoMap}) -> String` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToTitle

  Переводят регистр ascii символов строки к ВЕРХНЕМУ, нижнему или Заглавному виду.

* `String::SplitToList(string:String?, delimeter:String, [ DelimeterString:Bool?, SkipEmpty:Bool?, Limit:Uint64? ]) -> List<String>`

  Разбивает строку на подстроки по разделителю.
  `string` -- исходная строка
  `delimeter` -- разделитель
  Именованные параметры:

  - `DelimeterString:Bool?` — считать разделитель строкой (true, по умолчанию) или набором символов "любой из" (false)
  - `SkipEmpty:Bool?` - пропускать ли пустые строки в результате, по умолчанию false
  - `Limit:Uint64?` - ограничение на число извлекаемых компонент, по умолчанию не ограничено; необработанный суффикс оригинальной строки возвращается последним элементом при превышении лимита

```yql
SELECT String::SplitToList("1,2,3,4,5,6,7", ",", 3 as Limit); -- ["1", "2", "3", "4,5,6,7"]
```

* `String::JoinFromList(strings:List<String>{Flags:AutoMap}, separator:String) -> String`

  Конкатенирует список строк через разделитель в единую строку.

* `String::ToByteList(string:String) -> List<Byte>`

  Разбивает строку на список байт.

* `String::FromByteList(bytes:List<Uint8>) -> String`

  Собирает список байт в строку.

* `String::ReplaceAll(input:String{Flags:AutoMap}, find:String, replacement:String) -> String`
* `String::ReplaceFirst(input:String{Flags:AutoMap}, find:String, replacement:String) -> String`
* `String::ReplaceLast(input:String{Flags:AutoMap}, find:String, replacement:String) -> String`

  Заменяют все/первое/последнее вхождения(е) строки `find` в `input` на `replacement`.

* `String::RemoveAll(input:String{Flags:AutoMap}, symbols:String) -> String`
* `String::RemoveFirst(input:String{Flags:AutoMap}, symbols:String) -> String`
* `String::RemoveLast(input:String{Flags:AutoMap}, symbols:String) -> String`

  Удаляют все/первое/последнее вхождения(е) символа в наборе `symbols` из `input`. Второй аргумент интерпретируется как неупорядоченный набор символов для удаления.

* `String::ReverseBytes(input:String{Flags:AutoMap}) -> String` - добавлена в версии [2025.02](../../changelog/2025.02.md#string-module)
  Разворачивает строку, рассматривая ее как байтовую последовательность.

* `String::ReverseBits(input:String{Flags:AutoMap}) -> String` - добавлена в версии [2025.02](../../changelog/2025.02.md#string-module)
  Разворачивает строку, рассматривая ее как битовую последовательность.

* `String::IsAscii(string:String{Flags:AutoMap}) -> Bool`

  Проверяет, является ли строка валидной ascii последовательностью.

* `String::IsAsciiSpace(string:String{Flags:AutoMap}) -> Bool`
* `String::IsAsciiUpper(string:String{Flags:AutoMap}) -> Bool`
* `String::IsAsciiLower(string:String{Flags:AutoMap}) -> Bool`
* `String::IsAsciiAlpha(string:String{Flags:AutoMap}) -> Bool`
* `String::IsAsciiAlnum(string:String{Flags:AutoMap}) -> Bool`
* `String::IsAsciiHex(string:String{Flags:AutoMap}) -> Bool`

  Проверяют, отвечает ли ascii строка указанному условию.

* `String::LevensteinDistance(stringOne:String{Flags:AutoMap}, stringTwo:String{Flags:AutoMap}) -> Uint64`

  Вычисляет расстояние Левенштейна для переданных строк.

* `String::LeftPad(string:String{Flags:AutoMap}, size:Uint64, filler:[String?]) -> String`
* `String::RightPad(string:String{Flags:AutoMap}, size:Uint64, filler:[String?]) -> String`

  Выравнивает текст до указанного размера с дополнением указанным символом или пробелами.

* `String::Hex(value:Uint64{Flags:AutoMap}) -> String`
* `String::SHex(value:Int64{Flags:AutoMap}) -> String`
* `String::Bin(value:Uint64{Flags:AutoMap}) -> String`
* `String::SBin(value:Int64{Flags:AutoMap}) -> String`
* `String::HexText(string:String{Flags:AutoMap}) -> String`
* `String::BinText(string:String{Flags:AutoMap}) -> String`
* `String::HumanReadableDuration(value:Uint64{Flags:AutoMap}) -> String`
* `String::HumanReadableQuantity(value:Uint64{Flags:AutoMap}) -> String`
* `String::HumanReadableBytes(value:Uint64{Flags:AutoMap}) -> String`
* `String::Prec(Double{Flags:AutoMap}, digits:Uint64) -> String`

  Распечатывает значение указанным образом.

{% note alert %}

Функции из библиотеки String не поддерживают кириллицу и умеют работать только с ASCII символами. Для работы со строками в кодировке UTF-8 используйте функции из [Unicode](unicode.md).

{% endnote %}
