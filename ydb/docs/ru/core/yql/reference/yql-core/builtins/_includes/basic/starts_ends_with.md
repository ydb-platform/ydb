## StartsWith, EndsWith {#starts_ends_with}

Проверка наличия префикса или суффикса в строке.

**Сигнатуры**
```
StartsWith(Utf8, Utf8)->Bool
StartsWith(Utf8[?], Utf8[?])->Bool?
StartsWith(String, String)->Bool
StartsWith(String[?], String[?])->Bool?

EndsWith(Utf8, Utf8)->Bool
EndsWith(Utf8[?], Utf8[?])->Bool?
EndsWith(String, String)->Bool
EndsWith(String[?], String[?])->Bool?
```

Обязательные аргументы:

* Исходная строка;
* Искомая подстрока.

Аргументы могут быть типов `String` или `Utf8` и могут быть опциональными.

**Примеры**
``` yql
SELECT StartsWith("abc_efg", "abc") AND EndsWith("abc_efg", "efg"); -- true
```
``` yql
SELECT StartsWith("abc_efg", "efg") OR EndsWith("abc_efg", "abc"); -- false
```
``` yql
SELECT StartsWith("abcd", NULL); -- null
```
``` yql
SELECT EndsWith(NULL, Utf8("")); -- null
```
