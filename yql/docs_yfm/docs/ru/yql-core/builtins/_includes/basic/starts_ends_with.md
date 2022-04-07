## StartsWith, EndsWith {#starts_ends_with}

Проверка наличия префикса или суффикса в строке.

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
