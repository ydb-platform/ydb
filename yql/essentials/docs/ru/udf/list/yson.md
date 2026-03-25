# Yson

<!-- markdownlint-disable no-trailing-punctuation -->

YSON — разработанный в Яндексе формат данных, похожий на JSON.

* Сходства с JSON:

  * не имеет строгой схемы;
  * помимо простых типов данных поддерживает словари и списки в произвольных комбинациях.

* Некоторые отличия от JSON:

  * Помимо текстового представления имеет и бинарное;
  * В текстовом представлении вместо запятых — точки с запятой, а вместо двоеточий — равно;

* Поддерживается концепция «атрибутов», то есть именованных свойств, которые могут быть присвоены узлу в дереве.

Особенности реализации и функциональность модуля:

* Наравне с YSON данный модуль поддерживает и стандартный JSON, что несколько расширяет область его применения.
* Работает с DOM представлением YSON в памяти, которое в терминах YQL передается между функциями как «ресурс» (см. [описание специальных типов данных](../../types/special.md)). Большинство функций модуля имеют семантику запроса на выполнение указанной операции с ресурсом и возвращают пустой [optional](../../types/optional.md), если операция не удалась из-за несоответствия фактического типа данных ожидаемому.
* Предоставляет несколько основных классов функций (полный список и подробное описание функций см. ниже):

  * `Yson::Parse***` — получение ресурса с DOM-объектом из сериализованных данных, все дальнейшие операции выполняются уже над полученным ресурсом;
  * `Yson::From` — получение ресурса с DOM-объектом из простых типов данных YQL или контейнеров (списков или словарей);
  * `Yson::As***`,`Yson::TryAs***` — приведение ресурса в нужный тип данных;
  * `Yson::ConvertTo***` — преобразовать ресурс к [простым типам данных](../../types/primitive.md) или [контейнерам](../../types/containers.md);
  * `Yson::Lookup***` — получение одного элемента списка или словаря с опциональным преобразованием в нужный тип данных;
  * `Yson::YPath***` — получение одного элемента дерева документа по указанному относительному пути с опциональным преобразованием в нужный тип данных;
  * `Yson::Serialize***` — получить из ресурса копию его данных, сериализованную в одном из форматов;

* Для удобства при передаче сериализованного Yson и Json в функции, ожидающие на входе ресурс с DOM-объектом, неявное преобразование через `Yson::Parse` или `Yson::ParseJson` происходит автоматически. Также в SQL синтаксисе оператор точки или квадратных скобок автоматически добавляет вызов `Yson::Lookup`. Для сериализации ресурса по-прежнему нужно вызывать `Yson::ConvertTo***` или `Yson::Serialize***`. Таким образом, например, получение элемента "foo" словаря из колонки mycolumn типа Yson в виде строки может выглядеть так: `SELECT Yson::ConvertToString(mycolumn["foo"]) FROM mytable;` или `SELECT Yson::ConvertToString(mycolumn.foo) FROM mytable;`. В варианте с точкой можно экранировать спецсимволы по [общим правилам для идентификаторов](../../syntax/expressions.md#escape).

Функции модуля стоит рассматривать как «кубики», из которых можно собирать разные конструкции, например:

* `Yson::Parse*** -> Yson::Serialize***` — конвертация из одного формата в другой;
* `Yson::Parse*** -> Yson::Lookup -> Yson::Serialize***` — извлечение значения указанного поддерева в исходном дереве YSON;
* `Yson::Parse*** -> Yson::ConvertToList -> ListMap -> Yson::Lookup***` — извлечение элементов по ключу из YSON списка.



## Примеры

```yql
$node = Json(@@
  {"abc": {"def": 123, "ghi": "привет"}}
@@);
SELECT Yson::SerializeText($node.abc) AS `yson`;
-- {"def"=123;"ghi"="\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82"}
```

```yql
$node = Yson(@@
  <a=z;x=y>[
    {abc=123; def=456};
    {abc=234; xyz=789};
  ]
@@);
$attrs = Yson::YPath($node, "/@");

SELECT
  ListMap(Yson::ConvertToList($node), ($x) -> { return Yson::LookupInt64($x, "abc") }) AS abcs,
  Yson::ConvertToStringDict($attrs) AS attrs,
  Yson::SerializePretty(Yson::Lookup($node, "7", Yson::Options(false AS Strict))) AS miss;

/*
- abcs: `[123; 234]`
- attrs: `{"a"="z";"x"="y"}`
- miss: `NULL`
*/
```

## Yson::Parse... {#ysonparse}

```yql
Yson::Parse(Yson{Flags:AutoMap}) -> Resource<'Yson2.Node'>
Yson::ParseJson(Json{Flags:AutoMap}) -> Resource<'Yson2.Node'>
Yson::ParseJsonDecodeUtf8(Json{Flags:AutoMap}) -> Resource<'Yson2.Node'>

Yson::Parse(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>? -- принимает YSON в любом формате
Yson::ParseJson(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
Yson::ParseJsonDecodeUtf8(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
```

Результат всех трёх функций является несериализуемым: его можно только передать на вход другой функции из библиотеки Yson, но нельзя сохранить в таблицу или вернуть на клиент в результате операции — попытка так сделать приведет к ошибке типизации. Также запрещено возвращать его за пределы [подзапросов](../../syntax/select/index.md): если это требуется, то надо вызвать [Yson::Serialize](#ysonserialize), а оптимизатор уберёт лишнюю сериализацию и десериализацию, если материализация в конечном счёте не потребуется.

{% note info %}

Функция `Yson::ParseJsonDecodeUtf8` ожидает, что символы, выходящие за пределы ASCII, должны быть дополнительно заэкранированы.

{% endnote %}

## Yson::From {#ysonfrom}

```yql
Yson::From(T) -> Resource<'Yson2.Node'>
```

`Yson::From` является полиморфной функцией, преобразующей в Yson ресурс большинство примитивных типов данных и контейнеров (списки, словари, кортежи, структуры и т.п.). Тип исходного объекта должен быть совместим с Yson. Например, в ключах словарей допустимы только типы `String` или `Utf8`, а вот `String?` или `Utf8?` уже нет.

#### Пример

```yql
SELECT Yson::Serialize(Yson::From(TableRow())) FROM table1;
```

## Yson::WithAttributes

```yql
Yson::WithAttributes(Resource<'Yson2.Node'>{Flags:AutoMap}, Resource<'Yson2.Node'>{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
```

Добавляет к узлу Yson (первый аргумент) атрибуты (второй аргумент). Атрибуты должны представлять из себя узел map.

## Yson::Equals

```yql
Yson::Equals(Resource<'Yson2.Node'>{Flags:AutoMap}, Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool
```

Проверка деревьев в памяти на равенство, толерантная к исходному формату сериализации и порядку перечисления ключей в словарях.

## Yson::GetHash

```yql
Yson::GetHash(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64
```

Вычисление 64-битного хэша от дерева объектов.

## Yson::Is... {#ysonis}

```yql
Yson::IsEntity(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
```

Проверка, что текущий узел имеет соответствующий тип. Entity это `#`.

## Yson::GetLength

```yql
Yson::GetLength(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
```

Получение числа элементов в списке или словаре.

## Yson::ConvertTo... {#ysonconvertto}

```yql
Yson::ConvertTo(Resource<'Yson2.Node'>{Flags:AutoMap}, Type<T>) -> T
Yson::ConvertToBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool?
Yson::ConvertToInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64?
Yson::ConvertToUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
Yson::ConvertToDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double?
Yson::ConvertToString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String?
Yson::ConvertToList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>
Yson::ConvertToBoolList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Bool>
Yson::ConvertToInt64List(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Int64>
Yson::ConvertToUint64List(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Uint64>
Yson::ConvertToDoubleList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Double>
Yson::ConvertToStringList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<String>
Yson::ConvertToDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Resource<'Yson2.Node'>>
Yson::ConvertToBoolDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Bool>
Yson::ConvertToInt64Dict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Int64>
Yson::ConvertToUint64Dict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Uint64>
Yson::ConvertToDoubleDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Double>
Yson::ConvertToStringDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,String>
```

{% note warning %}

Данные функции по умолчанию не делают неявного приведения типов, то есть значение в аргументе должно в точности соответствовать вызываемой функции.

Начиная с версии 2025.05, для приведения Yson узла к конкретному примитивному типу, списку или словарю рекомендуется использовать функции [As*/TryAs*](#ysonas) и `Yson::ConvertTo` в остальных случаях.

{% endnote %}

`Yson::ConvertTo` является полиморфной функцией, преобразующей Yson узел в указанный во втором аргументе тип данных с поддержкой вложенных контейнеров (списки, словари, кортежи, структуры и т.п.).

#### Пример

```yql
$data = Yson(@@{
    "name" = "Anya";
    "age" = 15u;
    "params" = {
        "ip" = "95.106.17.32";
        "last_time_on_site" = 0.5;
        "region" = 213;
        "user_agent" = "Mozilla/5.0"
    }
}@@);
SELECT Yson::ConvertTo($data,
    Struct<
        name: String,
        age: Uint32,
        params: Dict<String,Yson>
    >
);
```

## Yson::Contains {#ysoncontains}

```yql
Yson::Contains(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
```

Проверяет наличие ключа в словаре. Если тип объекта map, то ищем среди ключей.
Если тип объекта список, то ключ должен быть десятичным числом - индексом в списке.


## Yson::Lookup... {#ysonlookup}

```yql
Yson::Lookup(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Resource<'Yson2.Node'>?
Yson::LookupBool(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
Yson::LookupInt64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Int64?
Yson::LookupUint64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Uint64?
Yson::LookupDouble(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Double?
Yson::LookupString(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> String?
Yson::LookupDict(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Dict<String,Resource<'Yson2.Node'>>?
Yson::LookupList(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> List<Resource<'Yson2.Node'>>?
```

Перечисленные выше функции представляют собой краткую форму записи для типичного сценария использования: `Yson::YPath` — переход в словарь на один уровень с последующим извлечением значения — `Yson::ConvertTo***`. Второй аргумент для всех перечисленных функций — имя ключа в словаре (в отличие от YPath, без префикса `/`) или индекс в списке (например, `7`).

{% note warning %}

Начиная с версии 2025.05 рекомендуется использовать `Yson::Lookup` для получения узла по ключу и функции [As*/TryAs*](#ysonas) при необходимости дальнейшего уточнения типов.

В большинстве случаев вместо множественных функций `Yson::Lookup` следует рассмотреть преобразование данных с помощью функции `Yson::ConvertTo` в набор словарей, списков и примитивных типов.

{% endnote %}


## Yson::YPath {#ysonypath}

```yql
Yson::YPath(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Resource<'Yson2.Node'>?
Yson::YPathBool(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
Yson::YPathInt64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Int64?
Yson::YPathUint64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Uint64?
Yson::YPathDouble(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Double?
Yson::YPathString(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> String?
Yson::YPathDict(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Dict<String,Resource<'Yson2.Node'>>?
Yson::YPathList(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> List<Resource<'Yson2.Node'>>?
```

Позволяет по входному ресурсу и пути на языке YPath получить ресурс, указывающий на соответствующую пути часть исходного ресурса.

{% note warning %}

Начиная с версии 2025.05 рекомендуется использовать `Yson::YPath` для получения узла по ключу и функции [As*/TryAs*](#ysonas) при необходимости дальнейшего уточнения типов.

В большинстве случаев вместо множественных функций `Yson::YPath` следует рассмотреть преобразование данных с помощью функции `Yson::ConvertTo` в набор словарей, списков и примитивных типов.

{% endnote %}


## Yson::Attributes {#ysonattributes}

```yql
Yson::Attributes(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Resource<'Yson2.Node'>>
```

Получение всех атрибутов узла в виде словаря.

## Yson::Serialize... {#ysonserialize}

```yql
Yson::Serialize(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson -- бинарное представление
Yson::SerializeText(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson
Yson::SerializePretty(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson -- чтобы увидеть именно текстовый результат, можно обернуть его в ToBytes(...)
```

## Yson::SerializeJson {#ysonserializejson}

```yql
Yson::SerializeJson(Resource<'Yson2.Node'>{Flags:AutoMap}, [Resource<'Yson2.Options'>?, SkipMapEntity:Bool?, EncodeUtf8:Bool?, WriteNanAsString:Bool?]) -> Json?
```

* `SkipMapEntity` отвечает за сериализацию значений в словарях, имеющих значение `#`. На значение атрибутов флаг не влияет. По умолчанию `false`.
* `EncodeUtf8` отвечает за экранирование символов, выходящих за пределы ASCII. По умолчанию `false`.
* `WriteNanAsString` разрешает сериализацию значений `NaN` и `Inf` в json в виде строк. По умолчанию `false`.

Типы данных `Yson` и `Json`, возвращаемые функциями сериализации, представляет собой частный случай строки, про которую известно, что в ней находятся данные в соответствующем формате (Yson/Json).

## Yson::Options {#ysonoptions}

```yql
Yson::Options([AutoConvert:Bool?, Strict:Bool?]) -> Resource<'Yson2.Options'>
```

Передаётся последним опциональным аргументом (который для краткости не указан) в методы `Parse...`, `ConvertTo...`, `Contains`, `Lookup...` и `YPath...`, которые принимают результат вызова `Yson::Options`. По умолчанию все поля `Yson::Options` выключены (false), а при включении (true) модифицируют поведение следующим образом:

* **AutoConvert** — если переданное в Yson значение не в точности соответствует типу данных результата, то значение будет по возможности сконвертировано. Например, `Yson::ConvertToInt64` в этом режиме будет делать Int64 даже из чисел типа Double.
* **Strict** — по умолчанию все функции из библиотеки Yson возвращают ошибку в случае проблем в ходе выполнения запроса (например, попытка парсинга строки не являющейся Yson/Json, или попытка поиска по ключу в скалярном типе, или запрошено преобразование в несовместимый тип данных, и т.п.), а если отключить строгий режим, то вместо ошибки в большинстве случаев будет возвращаться `NULL`. При преобразовании в словарь или список (`ConvertTo<Type>Dict` или `ConvertTo<Type>List`) плохие элементы будут выброшены из полученной коллекции.

Обратите внимание: если вы явно передаёте объект `Yson::Options`, значения его полей по умолчанию могут отличаться от настроек, которые используются, когда опции не передаются вообще. Например, метод `Yson::Parse` по умолчанию работает в строгом режиме, но если вы создаёте и передаёте объект опций без указания значения поля `Strict`, оно будет установлено в `false` и метод будет работать в нестрогом режиме.

#### Пример

```yql
$yson = @@{y = true; x = 5.5}@@y;
SELECT Yson::LookupBool($yson, "z"); --- null
SELECT Yson::LookupBool($yson, "y"); --- true

-- SELECT Yson::LookupInt64($yson, "x"); --- Ошибка
SELECT Yson::LookupInt64($yson, "x", Yson::Options(false as Strict)); --- null
SELECT Yson::LookupInt64($yson, "x", Yson::Options(true as AutoConvert)); --- 5

-- SELECT Yson::ConvertToBoolDict($yson); --- Ошибка
SELECT Yson::ConvertToBoolDict($yson, Yson::Options(false as Strict)); --- { "y": true }
SELECT Yson::ConvertToDoubleDict($yson, Yson::Options(false as Strict)); --- { "x": 5.5 }
```

Если во всём запросе требуется применять одинаковые значения настроек библиотеки Yson, то удобнее воспользоваться [PRAGMA yson.AutoConvert;](../../syntax/pragma/yson.md#autoconvert) и/или [PRAGMA yson.Strict;](../../syntax/pragma/yson.md#strict). Также эти `PRAGMA` являются единственным способом повлиять на неявные вызовы библиотеки Yson, которые возникают при работе с типами данных Yson/Json.

## Yson::Iterate {#ysoniterate}

```yql
Yson::Iterate(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Variant<
    'BeginAttributes':Void,
    'BeginList':Void,
    'BeginMap':Void,
    'EndAttributes':Void,
    'EndList':Void,
    'EndMap':Void,
    'Item':Void,
    'Key':String,
    'PostValue':Resource<'Yson2.Node'>,
    'PreValue':Resource<'Yson2.Node'>,
    'Value':Resource<'Yson2.Node'>>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Получение списка всех событий при обходе Yson-дерева.
Листовые узлы (`Entity`, `Bool`, `Int64`, `Uint64`, `Double`, `String`) передаются в виде события `Value`.

Для узла с типом `List` выдается такая последовательность:
* `PreValue` с самим узлом
* `BeginList`
* `Item` - перед каждым элементом `List`
* события для элемента `List`
* `EndList`
* `PostValue` с самим узлом

Для узла с типом `Map` выдается такая последовательность:
* `PreValue` с самим узлом
* `BeginMap`
* `Key` - перед каждым элементом `Map`
* события для элемента `Map`
* `EndMap`
* `PostValue` с самим узлом
Порядок выдачи ключей может быть произвольным.

Для узла с непустыми атрибутами выдается такая последовательность:
* `PreValue` с самим узлом
* `BeginAttributes`
* `Key` - перед каждым именем атрибута
* события для атрибута
* `EndAttributes`
* события для узла без атрибутов
* `PostValue` с самим узлом
Порядок выдачи атрибутов может быть произвольным.

#### Примеры

```yql
-- Просмотр всей выдачи функции Yson::Iterate
$dump = ($x) -> (
    (
        Way($x),
        $x.Key,
        Yson::Serialize($x.PreValue),
        Yson::Serialize($x.Value),
        Yson::Serialize($x.PostValue)
    )
);

SELECT ListMap(Yson::Iterate('{a=1;b=<c="foo">[2u;%true;#;-3.2]}'y), $dump);

/*
События:
    PreValue [1]
    BeginMap
    Key a
    Value 1
    Key b
    PreValue [2]
    BeginAttributes
    Key c
    Value foo
    EndAttributes
    PreValue [3]
    BeginList
    Item
    Value 2
    Item
    Value %true
    Item
    Value #
    Item
    Value -3.2
    EndList
    PostValue [3]
    PostValue [2]
    EndMap
    PostValue [1]
*/
```

```yql
-- Получение всех листовых значений - раскрытие всех списков
$yson = '[[1;2];[3;4]]'y;
SELECT ListFlatMap(Yson::Iterate($yson), ($x)->(IF($x.Value IS NOT NULL, $x.Value))); -- [1;2;3;4]
```

```yql
-- Поиск ключа с заданным именем на любом уровне
$yson = '{a={b={c=1}};e={f=2}}'y;
SELECT ListHasItems(ListFilter(Yson::Iterate($yson), ($x)->($x.Key == 'b'))); -- true
```

```yql
-- Поиск строки в значениях любом уровне
$yson = '{a={b={c="x"}};e={f="y"}}'y;
SELECT ListHasItems(ListFilter(Yson::Iterate($yson), ($x)->(Yson::ConvertToString($x.Value) == 'y'))); -- true
```

```yql
-- Получение атрибутов name для всех Map узлов без атрибута children
$yson = @@{
    name=foo;
    children=[
        {
            name=bar
        }
    ]
}@@y;

SELECT ListFlatMap(Yson::Iterate($yson), ($x)->(
    IF(Yson::IsDict($x.PreValue) and not Yson::Contains($x.PreValue,'children'), Yson::LookupString($x.PreValue, 'name')))); -- [bar]
```

## Yson::As... и Yson::TryAs... {#ysonas}

```yql
Yson::AsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String
Yson::AsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double
Yson::AsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64
Yson::AsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64
Yson::AsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool
Yson::AsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>
Yson::AsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String, Resource<'Yson2.Node'>>

Yson::TryAsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String?
Yson::TryAsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double?
Yson::TryAsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
Yson::TryAsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64?
Yson::TryAsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool?
Yson::TryAsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>?
Yson::TryAsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String, Resource<'Yson2.Node'>>?
```

Доступны начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Приводят Yson узел к заданному типу.
`TryAs*` функции при неверном типе Yson узла возвращают `NULL`, а `As*` функции в этом случае приведут к ошибке запроса.
Для обработки узла с типом `Entity` ('#') следует использовать функцию [`IsEntity`](#ysonis).

## In-place изменения Yson узлов {#yson-modify}

Начиная с версии [2025.05](../../changelog/2025.05.md#yson-module) появилась возможность работать с изменяемым Yson деревом на базе [linear](../../types/linear.md) типов. Для создания нового дерева используется функция [`MutCreate`](#mutcreate), а для импорта существующего неизменяемого дерева в изменяемую форму - [`Mutate`](#mutate).
Само построение или трансформацию дерева следует выполнять внутри функции [`Block`](../../builtins/basic.md#block), при этом изменяемое Yson дерево в конечном счете должно быть преобразовано в неизменяемое с помощью функции [`MutFreeze`](#mutfreeze).
В каждом экземпляре изменяемого Yson дерева есть единственный текущий узел, который можно перемещать, попутно создавая новые узлы при необходимости.

## Yson::MutCreate {#mutcreate}

```yql
Yson::MutCreate() -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Создает новое пустое дерево с единственным узлом в `Invalid` состоянии - с неопределенным типом (значение этого узла можно поменять, например, через функцию [`MutUpsert`](#mutupsert)). При этом текущий узел выставляется на корень дерева.

Т.к. эта функция создает экземпляр linear типа, то она должна использоваться с указанием зависимых узлов через функцию [`Udf`](../../builtins/basic.md#udf).

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    return Yson::MutFreeze($m); -- Ошибка: Invalid or deleted node is not allowed
})
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutUpsert($m, '1'y);
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::Mutate {#mutate}

```yql
Yson::Mutate(Resource<Yson2.Node>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Создает изменяемое Yson дерево из неизменяемого. При этом текущий узел выставляется на корень дерева.

Т.к. эта функция создает экземпляр linear типа, то она должна использоваться с указанием зависимых узлов через функцию [`Udf`](../../builtins/basic.md#udf).

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutFreeze {#mutfreeze}

```yql
Yson::MutFreeze(Linear<Resource<Yson2.MutNode>>) -> Resource<Yson2.Node>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Преобразует изменяемое Yson дерево в неизменяемое, при этом удаляются все узлы из словарей и списков в состоянии `Deleted`.
Корневой узел в изменяемом дереве не должен быть в состоянии `Invalid` или `Deleted`, а дочерние узлы не должны быть в состоянии `Invalid`, иначе преобразование завершится ошибкой.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    return Yson::MutFreeze($m); -- 1
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)(); -- Корневой узел в состоянии Invalid
    return Yson::MutFreeze($m); -- Ошибка: Invalid or deleted node is not allowed
})
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutRemove($m); -- Переводим узел в состояние Deleted
    return Yson::MutFreeze($m); -- Ошибка: Invalid or deleted node is not allowed
})
```

## Yson::MutUpsert {#mutupsert}

```yql
Yson::MutUpsert(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Заменяет текущий узел в изменяемом Yson дереве на заданное Yson поддерево.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutUpsert($m, '2'y);
    return Yson::MutFreeze($m); -- 2
});
```

## Yson::MutInsert {#mutinsert}

```yql
Yson::MutInsert(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Заменяет текущий узел в изменяемом Yson дереве на заданное Yson поддерево если текущий узел находится в `Invalid` или `Deleted` состоянии.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutInsert($m, '1'y);
    $m = Yson::MutInsert($m, '2'y); -- не имеет эффекта
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutUpdate {#mutupdate}

```yql
Yson::MutUpdate(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Заменяет текущий узел в изменяемом Yson дереве на заданное Yson поддерево если текущий узел не находится в `Invalid` или `Deleted` состоянии.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutUpdate($m, '1'y); -- не имеет эффекта
    $m = Yson::MutInsert($m, '2'y);
    $m = Yson::MutUpdate($m, '3'y);
    return Yson::MutFreeze($m); -- 3
});
```

## Yson::MutRemove {#mutremove}

```yql
Yson::MutRemove(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Переводит текущий узел в `Deleted` состояние.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutRemove($m);
    $m = Yson::MutInsert($m, '2'y);
    return Yson::MutFreeze($m); -- 2
});
```

## Yson::MutRewind {#mutrewind}

```yql
Yson::MutRewind(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Передвигает текущий узел в корень дерева.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- переместились к ключу 'a' в словаре
    $m = Yson::MutRewind($m); -- переместились в корень
    $m = Yson::MutUpsert($m, '1'y); -- заменили дерево в корне
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutUp {#mutup}

```yql
Yson::MutUp(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Передвигает текущий узел на уровень ближе к корню дерева. Если текущий узел уже находится в корне дерева, то генерируется ошибка.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- переместились к ключу 'a' в словаре
    $m = Yson::MutUp($m); -- переместились в корень
    $m = Yson::MutUpsert($m, '1'y); -- заменили дерево в корне
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutDown, Yson::MutDownOrCreate, Yson::MutTryDown {#mutdown}

```yql
Yson::MutDown(Linear<Resource<Yson2.MutNode>>,location:String) -> Linear<Resource<Yson2.MutNode>>
Yson::MutDownOrCreate(Linear<Resource<Yson2.MutNode>>,location:String) -> Linear<Resource<Yson2.MutNode>>
Yson::MutTryDown(Linear<Resource<Yson2.MutNode>>,location:String) -> Tuple<Linear<Resource<Yson2.MutNode>>, Bool>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Передвигает текущий узел к элементу словаря или списка, при этом может быть создан новый узел в состоянии `Invalid`.

В строке location используется экранирование символов через обратный слеш `\`.
Если строка location начинается с символов `<`, `=` или `>`, то за ним должен быть указан индекс списка в формате десятичного числа (начиная с 0), `first` (0-й элемент списка) или `last` (последний элемент списка). Иначе location интерпретируется как ключ в словаре.
Для пустого списка `first` и `last` описывают положение за концом списка.

Для функции `MutDown` запрещается создавать новые ключи в словарях, в ней можно использовать только модификатор `=` для списков и она генерирует ошибку если перемещение текущего узла невозможно.
Функция `MutTryDown` в отличии от `MutDown` возвращает `false` во втором элементе кортежа в случае невозможности перемещения.
Если текущий узел имеет состояние `Invalid` или `Deleted`, то функция `MutDownOrCreate` сначала создает его в виде пустого списка или словаря, в зависимости от вида location.
Если тип текущего узла (список или словарь) не совпадает с видом location, то `MutDown` и `MutDownOrCreate` выдадут ошибку.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- переместились к ключу 'a' в словаре
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- {a=3;b=2}
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDownOrCreate($m, 'c'); -- переместились к ключу 'c' в словаре - он создан в состоянии Invalid
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- {a=1;b=2;c=3}
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'c'); -- переместились к ключу 'c' в словаре, ошибка, т.к. он не существует
    return Yson::MutFreeze($m);
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m, $ok = Yson::MutTryDown($m, 'c'); -- пытались переместились к ключу 'c' в словаре, вернули false
    return (Yson::MutFreeze($m), $ok); -- ({a=1;b=2}, false)
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '>last'); -- добавили узел в конец списка в состоянии Invalid
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- [1;2;3]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '<1'); -- добавили узел перед узлом с индексом 1 (значение '2')
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- [1;3;2]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '<first'); -- добавили узел в начало списка
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- [3;1;2]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDown($m, '=last'); -- переместились на последний узел списка
    $m = Yson::MutUpsert($m, '3'y); -- заменили текущий узел
    return Yson::MutFreeze($m); -- [1;3]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, 'a'); -- ошибка, текущий узел не словарь
    return Yson::MutFreeze($m);
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[a=1;b=2]'y);
    $m = Yson::MutDownOrCreate($m, '=0'); -- ошибка, текущий узел не список
    return Yson::MutFreeze($m);
});
```

## Yson::MutExists {#mutexists}

```yql
Yson::MutExists(Linear<Resource<Yson2.MutNode>>) -> Tuple<Linear<Resource<Yson2.MutNode>>,Bool>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Проверяет, что текущий узел не находится в состоянии `Invalid` или `Deleted`.
Само изменяемое дерево при этом не меняется, как и положение текущего узла в нем.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m, $exists = Yson::MutExists($m); -- текущий узел в состоянии Invalid
    return LinearDestroy($exists, $m); -- возвращаем значение $exists=false, поглощая linear тип $m
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m, $exists = Yson::MutExists($m); -- текущий узел не является Invalid
    return LinearDestroy($exists, $m); -- возвращаем значение $exists=true, поглощая linear тип $m
});
```

## Yson::MutView {#mutview}

```yql
Yson::MutView(Linear<Resource<Yson2.MutNode>>) -> Tuple<Linear<Resource<Yson2.MutNode>>, Optional<Resource<Yson2.Node>>>
```

Доступна начиная с версии [2025.05](../../changelog/2025.05.md#yson-module).
Возвращает неизменяемое Yson дерево начиная с текущего узла, если он не находится в состоянии `Invalid` или `Deleted`, в противном случае возвращает `NULL`.
Как и в функции `MutFreeze` среди дочерних узлов по отношению к текущему не должно быть узлов в состоянии `Invalid`, а `Deleted` узлы будут пропущены при построении результирующего Yson дерева.
Само изменяемое дерево при этом не меняется, как и положение текущего узла в нем.

#### Примеры

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m, $view = Yson::MutView($m); -- текущий узел в состоянии Invalid
    return LinearDestroy($view, $m); -- возвращаем значение $view=NULL, поглощая linear тип $m
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m, $view = Yson::MutView($m); -- текущий узел не является Invalid
    return LinearDestroy($view, $m); -- возвращаем значение $view='1'y, поглощая linear тип $m
});
```

## Смотрите также

* [{#T}](../../recipes/accessing-json.md)
* [{#T}](../../recipes/modifying-json.md)
