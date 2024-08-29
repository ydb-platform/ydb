# String

Functions for ASCII strings:

**List of functions**

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

* ```String::Find(String{Flags:AutoMap}, String, [Uint64?]) -> Int64```: Returns the first position found or -1. The optional argument is the offset from the beginning of the string.

* ```String::ReverseFind(String{Flags:AutoMap}, String, [Uint64?]) -> Int64```: Returns the last position found or -1. The optional argument is the offset from the beginning of the string.

* ```String::HasPrefix(String?, String) -> Bool```

* ```String::HasPrefixIgnoreCase(String?, String) -> Bool```

* ```String::StartsWith(String?, String) -> Bool```

* ```String::StartsWithIgnoreCase(String?, String) -> Bool```

* ```String::HasSuffix(String?, String) -> Bool```

* ```String::HasSuffixIgnoreCase(String?, String) -> Bool```

* ```String::EndsWith(String?, String) -> Bool```

* ```String::EndsWithIgnoreCase(String?, String) -> Bool```

* ```String::Substring(String{Flags:AutoMap}, [Uint64?, Uint64?]) -> String```

* ```String::AsciiToLower(String{Flags:AutoMap}) -> String```: Changes only Latin characters. For working with other alphabets, see Unicode::ToLower

* ```String::AsciiToUpper(String{Flags:AutoMap}) -> String```: Changes only Latin characters. For working with other alphabets, see Unicode::ToUpper

* ```String::AsciiToTitle(String{Flags:AutoMap}) -> String```: Changes only Latin characters. For working with other alphabets, see Unicode::ToTitle

* ```String::SplitToList( String?, String, [ DelimeterString:Bool?, SkipEmpty:Bool?, Limit:Uint64? ]) -> List<String>```
  The first argument is the source string
  The second argument is a delimiter
  The third argument includes the following parameters:

  - DelimeterString:Bool? — treating a delimiter as a string (true, by default) or a set of characters "any of" (false)
  - SkipEmpty:Bool? — whether to skip empty strings in the result, is false by default
  - Limit:Uint64? — Limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item

* ```String::JoinFromList(List<String>{Flags:AutoMap}, String) -> String```

* ```String::ToByteList(List<String>{Flags:AutoMap}) -> List<Byte>```

* ```String::FromByteList(List<Uint8>) -> String```

* ```String::ReplaceAll(String{Flags:AutoMap}, String, String) -> String```: Arguments: input, find, replacement

* ```String::ReplaceFirst(String{Flags:AutoMap}, String, String) -> String```: Arguments: input, find, replacement

* ```String::ReplaceLast(String{Flags:AutoMap}, String, String) -> String```: Arguments: input, find, replacement

* ```String::RemoveAll(String{Flags:AutoMap}, String) -> String```: The second argument is interpreted as an unordered set of characters to delete

* ```String::RemoveFirst(String{Flags:AutoMap}, String) -> String```: An unordered set of characters in the second argument, only the first encountered character from set is deleted

* ```String::RemoveLast(String{Flags:AutoMap}, String) -> String```: An unordered set of characters in the second argument, only the last encountered character from the set is deleted

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

The functions from the String library don't support Cyrillic and can only work with ASCII characters. To work with UTF-8 encoded strings, use functions from [Unicode](../unicode.md).

{% endnote %}

**Examples**

```sql
SELECT String::Base64Encode("YQL"); -- "WVFM"
SELECT String::Strip("YQL ");       -- "YQL"
SELECT String::SplitToList("1,2,3,4,5,6,7", ",", 3 as Limit); -- ["1", "2", "3", "4,5,6,7"]
```

