## StartsWith, EndsWith {#starts_ends_with}

Проверка наличия префикса или суффикса в строке.

**Сигнатуры**
```
StartsWith(T str, U prefix)->Bool[?]

EndsWith(T str, U suffix)->Bool[?]
```

Обязательные аргументы:

* Исходная строка;
* Искомая подстрока.

Аргументы должны иметь тип `String`/`Utf8` (или опциональный `String`/`Utf8`) либо строковый PostgreSQL тип (`PgText`/`PgBytea`/`PgVarchar`).
Результатом функции является опциональный Bool, за исключением случая, когда оба аргумента неопциональные – в этом случае возвращается Bool.

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
``` yql
SELECT StartsWith("abc_efg"u, "abc"p) AND EndsWith("abc_efg", "efg"pv); -- true
```
