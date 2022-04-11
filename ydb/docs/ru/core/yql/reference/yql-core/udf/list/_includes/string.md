# String
Функции для работы с ASCII-строками.

**Список функций**

* ```String::Base64Encode(String{Flags:AutoMap}) -> String```
* ```String::Base64Decode(String) -> String?```
* ```String::Base64StrictDecode(String) -> String?```
* ```String::EscapeC(String{Flags:AutoMap}) -> String```
* ```String::UnescapeC(String{Flags:AutoMap}) -> String```
* ```String::HexEncode(String{Flags:AutoMap}) -> String```
* ```String::HexDecode(String) -> String?```
* ```String::EncodeHtml(String{Flags:AutoMap}) -> String```
* ```String::DecodeHtml(String{Flags:AutoMap}) -> String```
* ```String::CgiEscape(String{Flags:AutoMap}) -> String```
* ```String::CgiUnescape(String{Flags:AutoMap}) -> String```
* ```String::Strip(String{Flags:AutoMap}) -> String```
* ```String::Collapse(String{Flags:AutoMap}) -> String```
* ```String::CollapseText(String{Flags:AutoMap}, Uint64) -> String```
* ```String::Contains(String?, String) -> Bool```
* ```String::Find(String{Flags:AutoMap}, String, [Uint64?]) -> Int64``` - возвращает первую найденную позицию или -1; опциональный аргумент - отступ от начала строки
* ```String::ReverseFind(String{Flags:AutoMap}, String, [Uint64?]) -> Int64``` - возвращает последнюю найденную позицию или -1; опциональный аргумент - отступ от начала строки
* ```String::HasPrefix(String?, String) -> Bool```
* ```String::HasPrefixIgnoreCase(String?, String) -> Bool```
* ```String::StartsWith(String?, String) -> Bool```
* ```String::StartsWithIgnoreCase(String?, String) -> Bool```
* ```String::HasSuffix(String?, String) -> Bool```
* ```String::HasSuffixIgnoreCase(String?, String) -> Bool```
* ```String::EndsWith(String?, String) -> Bool```
* ```String::EndsWithIgnoreCase(String?, String) -> Bool```
* ```String::Substring(String{Flags:AutoMap}, [Uint64?, Uint64?]) -> String```
* ```String::AsciiToLower(String{Flags:AutoMap}) -> String``` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToLower
* ```String::AsciiToUpper(String{Flags:AutoMap}) -> String``` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToUpper
* ```String::AsciiToTitle(String{Flags:AutoMap}) -> String``` - меняет только латинские символы. Для работы с другими алфавитами см. Unicode::ToTitle
* ```String::SplitToList( String?, String, [ DelimeterString:Bool?, SkipEmpty:Bool?, Limit:Uint64? ]) -> List<String>```
  Первый аргумент -- исходная строка
  Второй аргумент -- разделитель
  Третий аргумент -- параметры:
    - DelimeterString:Bool? — считать разделитель строкой (true, по умолчанию) или набором символов "любой из" (false)
    - SkipEmpty:Bool? - пропускать ли пустые строки в результате, по умолчанию false
    - Limit:Uint64? - ограничение на число извлекаемых компонент, по умолчанию не ограничено; необработанный суффикс оригинальной строки возвращается последним элементом при превышении лимита

* ```String::JoinFromList(List<String>{Flags:AutoMap}, String) -> String```
* ```String::ToByteList(List<String>{Flags:AutoMap}) -> List<Byte>```
* ```String::FromByteList(List<Uint8>) -> String```
* ```String::ReplaceAll(String{Flags:AutoMap}, String, String) -> String``` - аргументы: input, find, replacement
* ```String::ReplaceFirst(String{Flags:AutoMap}, String, String) -> String``` - аргументы: input, find, replacement
* ```String::ReplaceLast(String{Flags:AutoMap}, String, String) -> String``` - аргументы: input, find, replacement
* ```String::RemoveAll(String{Flags:AutoMap}, String) -> String ``` - неупорядоченный набор символов во втором аргументе, удаляются все вхождения символов из набора
* ```String::RemoveFirst(String{Flags:AutoMap}, String) -> String ``` - неупорядоченный набор символов во втором аргументе, удаляется только первый встреченный символ из набора
* ```String::RemoveLast(String{Flags:AutoMap}, String) -> String ``` - неупорядоченный набор символов во втором аргументе, удаляется только последний встреченный символ из набора
* ```String::IsAscii(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiSpace(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiUpper(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiLower(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiAlpha(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiAlnum(String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiHex(String{Flags:AutoMap}) -> Bool```
* ```String::LevensteinDistance(String{Flags:AutoMap}, String{Flags:AutoMap}) -> Uint64```
* ```String::LeftPad(String{Flags:AutoMap}, Uint64, [String?]) -> String```
* ```String::RightPad(String{Flags:AutoMap}, Uint64) -> String```
* ```String::Hex(Uint64{Flags:AutoMap}) -> String```
* ```String::SHex(Int64{Flags:AutoMap}) -> String```
* ```String::Bin(Uint64{Flags:AutoMap}) -> String```
* ```String::SBin(Int64{Flags:AutoMap}) -> String```
* ```String::HexText(String{Flags:AutoMap}) -> String```
* ```String::BinText(String{Flags:AutoMap}) -> String```
* ```String::HumanReadableDuration(Uint64{Flags:AutoMap}) -> String```
* ```String::HumanReadableQuantity(Uint64{Flags:AutoMap}) -> String```
* ```String::HumanReadableBytes(Uint64{Flags:AutoMap}) -> String```
* ```String::Prec(Double{Flags:AutoMap}, Uint64) -> String* ```
* ```String::Reverse(String?) -> String?```

{% note alert %}

Функции из библиотеки String не поддерживают кириллицу и умеют работать только с ASCII символами. Для работы со строками в кодировке UTF-8 используйте функции из [Unicode](../unicode.md).

{% endnote %}

**Примеры**

```sql
SELECT String::Base64Encode("YQL"); -- "WVFM"
SELECT String::Strip("YQL ");       -- "YQL"
SELECT String::SplitToList("1,2,3,4,5,6,7", ",", 3 as Limit); -- ["1", "2", "3", "4,5,6,7"]
```
