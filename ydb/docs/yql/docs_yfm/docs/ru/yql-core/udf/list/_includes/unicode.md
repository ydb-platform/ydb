# Unicode
Функции для работы с Unicode строками.

**Список функций**

* ```Unicode::IsUtf(String) -> Bool```
  Проверяет является ли строка валидной utf-8 последовательностью. Например, строка ```"\xF0"``` не является валидной utf-8 последовательностью, а строка ```"\xF0\x9F\x90\xB1"``` корректно описывает utf-8 emoji с котиком.

* ```Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64```
  Возвращает длину utf-8 строки в символах (unicode code points). Суррогатные пары учитываются как один символ.
* ```Unicode::Find(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```
* ```Unicode::RFind(Utf8{Flags:AutoMap}, Utf8, [Uint64?]) -> Uint64?```
* ```Unicode::Substring(Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8```
  Возвращает подстроку начиная с символа ```from``` длиной в ```len``` символов. Если аргумент ```len``` опущен, то подстрока берется до конца исходной строки.

* Функции ```Unicode::Normalize...``` приводят переданную utf-8 строку в одну из [нормальных форм](https://unicode.org/reports/tr15/#Norm_Forms):

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
  Выполняет [case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) для переданной строки. ```Language``` задается по тем же правилам, что и в ```Unicode::Translit()```; ```DoLowerCase``` приводит строку к нижнему регистру, по умолчанию ```true```.

* ```Unicode::ReplaceAll(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Aргументы: ```input```, ```find```, ```replacement```. Заменяет все вхождения строки ```find``` в ```input``` на ```replacement```.

* ```Unicode::ReplaceFirst(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Aргументы: ```input```, ```find```, ```replacement```. Заменяет первое вхождение строки ```find``` в ```input``` на ```replacement```.

* ```Unicode::ReplaceLast(Utf8{Flags:AutoMap}, Utf8, Utf8) -> Utf8```
  Aргументы: ```input```, ```find```, ```replacement```. Заменяет последнее вхождение строки ```find``` в ```input``` на ```replacement```.

* ```Unicode::RemoveAll(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляются все вхождения символов из набора.

* ```Unicode::RemoveFirst(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляется только первое вхождение символа из набора.

* ```Unicode::RemoveLast(Utf8{Flags:AutoMap}, Utf8) -> Utf8```
  Второй аргумент интерпретируется как неупорядоченный набор символов для удаления. Удаляется только последнее вхождение символа из набора.

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
    - DelimeterString:Bool? — считать разделитель строкой (true, по умолчанию) или набором символов "любой из" (false)
    - SkipEmpty:Bool? - пропускать ли пустые строки в результате, по умолчанию false
    - Limit:Uint64? - ограничение на число извлекаемых компонент, по умолчанию не ограничено; необработанный суффикс оригинальной строки возвращается последним элементом при превышении лимита

* ```Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, Utf8) -> Utf8```

* ```Unicode::ToUint64(Utf8{Flags:AutoMap}, [Uint16?]) -> Uint64```
 Второй опциональный аргумент задает систему счисления, по умолчанию 0 - автоматическое определение по префиксу.
 Поддерживаемые префиксы : 0x(0X) - base-16, 0 - base-8. Система по-умолчанию - base-10.
 Знак '-' перед числом интерпретируется как в беззнаковой арифметике языка C, например -0x1 -> UI64_MAX.
 В случае наличия в строке некорректных символов или выхода числа за границы ui64 функция завершается с ошибкой.
* ```Unicode::TryToUint64(Utf8{Flags:AutoMap}, [Uint16?]) -> Uint64?```
 Аналогично функции Unicode::ToUint64(), но вместо ошибки возвращает Nothing.


**Примеры**

```sql
SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
SELECT Unicode::GetLength("жніўня");                  -- 6
```
