# Re2

**Список функций**

* ```Re2::Grep(pattern:String, options:Struct<...>?) -> (string:String?) -> Bool```
* ```Re2::Match(pattern:String, options:Struct<...>?) -> (string:String?) -> Bool```
* ```Re2::Capture(pattern:String, options:Struct<...>?) -> (string:String?) -> Struct<_1:String?,foo:String?,...>```
* ```Re2::FindAndConsume(pattern:String, options:Struct<...>?) -> (string:String?) -> List<String>```
* ```Re2::Replace(pattern:String, options:Struct<...>?) -> (string:String?, replacement:String) -> String?```
* ```Re2::Count(pattern:String, options:Struct<...>?) -> (string:String?) -> Uint32```
* ```Re2::Options([CaseSensitive:Bool?,DotNl:Bool?,Literal:Bool?,LogErrors:Bool?,LongestMatch:Bool?,MaxMem:Uint64?,NeverCapture:Bool?,NeverNl:Bool?,OneLine:Bool?,PerlClasses:Bool?,PosixSyntax:Bool?,Utf8:Bool?,WordBoundary:Bool?]) -> Struct<CaseSensitive:Bool,DotNl:Bool,Literal:Bool,LogErrors:Bool,LongestMatch:Bool,MaxMem:Uint64,NeverCapture:Bool,NeverNl:Bool,OneLine:Bool,PerlClasses:Bool,PosixSyntax:Bool,Utf8:Bool,WordBoundary:Bool>```

Модуль Re2 реализует поддержку регулярных выражений на основе [google::RE2](https://github.com/google/re2), где предоставляется широкий ассортимент возможностей ([см. официальную документацию](https://github.com/google/re2/wiki/Syntax)).

По умолчанию UTF-8 режим включается автоматически, если регулярное выражение является валидной строкой в кодировке UTF-8, но не является валидной ASCII-строкой. Вручную настройками библиотеки re2 можно управлять с помощью передачи результата функции `Re2::Options` вторым аргументом другим функциям модуля, рядом с регулярным выражением.

{% note warning %}

Все обратные слеши в регулярных выражениях (если они записаны в строке с кавычками) нужно удваивать, так как стандартные строковые литералы в SQL рассматриваются как С-escaped строки. Также можно записывать регулярное выражение в форме raw строки `@@regexp@@` — в этом случае удвоение слешей не требуется.

{% endnote %}

**Примеры**

```sql
$value = "xaaxaaxaa";
$options = Re2::Options(false AS CaseSensitive);
$match = Re2::Match("[ax]+\\d");
$grep = Re2::Grep("a.*");
$capture = Re2::Capture(".*(?P<foo>xa?)(a{2,}).*");
$replace = Re2::Replace("x(a+)x");
$count = Re2::Count("a", $options);

SELECT
  $match($value) AS match,                -- false
  $grep($value) AS grep,                  -- true
  $capture($value) AS capture,            -- (_0: 'xaaxaaxaa', _1: 'aa', foo: 'x')
  $capture($value)._1 AS capture_member,  -- "aa"
  $replace($value, "b\\1z") AS replace,   -- "baazaaxaa"
  $count($value) AS count;                -- 6
```

## Re2::Grep / Re2::Match {#match}

Если вынести за скобки детали реализации и синтаксиса регулярных выражений, эти функции полностью аналогичны [одноименным функциям](pire.md#match) из модуля Pire. При прочих равных и отсутствии каких-либо специфических предпочтений мы рекомендуем пользоваться `Pire::Grep / Pire::Match`.

Функцию `Re2::Grep` можно вызвать с помощью выражения `REGEXP` (см. [описание базового синтаксиса выражений](../../syntax/expressions.md#regexp)).

Например, следующие два запроса эквивалентны (в том числе по эффективности вычислений):

* ```$grep = Re2::Grep("b+"); SELECT $grep("aaabccc");```
* ```SELECT "aaabccc" REGEXP "b+";```

## Re2::Capture {#capture}

В отличие от [Pire::Capture](pire.md#capture) в `Re2::Capture` поддерживаются множественные и именованные группы захвата (capturing groups).
Тип результата: структура с полями типа `String?`.

* Каждое поле соответствует группе захвата с соответствующим именем.
* Для неименованных групп генерируются имена вида: `_1`, `_2` и т.д.
* В результат всегда включается поле `_0`, в котором доступна вся совпавшая с регулярным выражением подстрока.

Подробнее про работу со структурами в YQL см. в [разделе про контейнеры](../../types/containers.md).

## Re2::FindAndConsume {#findandconsume}

Ищет все вхождения регулярного выражения в переданный текст и возвращает список значений, соответствующих обрамленной в круглые скобки части регулярного выражения для каждого вхождения.

## Re2::Replace {#replace}

Работает следующим образом:

* Во входной строке (первый аргумент) все непересекающиеся подстроки, совпавшие с регулярным выражением, заменяются на указанную строку (второй аргумент).
* В строке с заменой можно использовать содержимое групп захвата (capturing groups) из регулярного выражения с помощью ссылок вида: `\\1`, `\\2` и т.д. Ссылка `\\0` обозначает всю совпавшую с регулярным выражением подстроку.

## Re2::Count {#count}

Возвращает количество совпавших с регулярным выражением непересекающихся подстрок во входной строке.

## Re2::Options {#options}

Пояснения к параметрам Re2::Options из официального [репозитория](https://github.com/google/re2/blob/main/re2/re2.h#L595-L617)

| Параметр                                                                                                                                                                                                                                | По умолчанию | Комментарий                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------------------|
| CaseSensitive:Bool?                                                                                                                                                                                                                     | true     | match is case-sensitive (regexp can override with (?i) unless in posix_syntax mode) |
| DotNl:Bool?                                                                                                                                                                                                                             | false    | let `.` match `\n` (default )                                                       |
| Literal:Bool?                                                                                                                                                                                                                           | false    | interpret string as literal, not regexp                                             |
| LogErrors:Bool?                                                                                                                                                                                                                         | true     | log syntax and execution errors to ERROR                                            |
| LongestMatch:Bool?                                                                                                                                                                                                                      | false    | search for longest match, not first match                                           |
| MaxMem:Uint64?                                                                                                                                                                                                                          | -        | (see below)  approx. max memory footprint of RE2                                    |
| NeverCapture:Bool?                                                                                                                                                                                                                      | false    | parse all parens as non-capturing                                                   |
| NeverNl:Bool?                                                                                                                                                                                                                           | false    | never match \n, even if it is in regexp                                             |
| PosixSyntax:Bool?                                                                                                                                                                                                                       | false    | restrict regexps to POSIX egrep syntax                                              |
| Utf8:Bool?                                                                                                                                                                                                                              | true     | text and pattern are UTF-8; otherwise Latin-1                                       |
| The following options are only consulted when PosixSyntax == true. <bt>When PosixSyntax == false, these features are always enabled and cannot be turned off; to perform multi-line matching in that case, begin the regexp with (?m).               ||
| PerlClasses:Bool?                                                                                                                                                                                                                       | false    | allow Perl's \d \s \w \D \S \W                                                      |
| WordBoundary:Bool?                                                                                                                                                                                                                      | false    | allow Perl's \b \B (word boundary and not)                                          |
| OneLine:Bool?                                                                                                                                                                                                                           | false    | ^ and $ only match beginning and end of text                                        |

Не рекомендуется Re2::Options использовать в коде. Большинство параметров можно заменить на флаги регулярного выражения.

**Пример использования флагов**

```sql
$value = "Foo bar FOO"u;
-- включить режим без учета регистра
$capture = Re2::Capture(@@(?i)(foo)@@);

SELECT
    $capture($value) AS capture; -- ("_0": "Foo", "_1": "Foo")

$capture = Re2::Capture(@@(?i)(?P<foo>FOO).*(?P<bar>bar)@@);

SELECT
    $capture($value) AS capture; -- ("_0": "Foo bar", "bar": "bar", "foo": "Foo")
```

В обоих случаях слово FOO будет найдено. Применение raw строки @@regexp@@ позволяет не удваивать слеши.
