## Variant {#variant}

`Variant()` создает значение варианта над кортежем или структурой.

### Сигнатура

```yql
Variant(T, String, Type<Variant<...>>)->Variant<...>
```

Аргументы:

* Значение
* Строка с именем поля или индексом кортежа
* Тип варианта

### Пример

```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

## AsVariant {#asvariant}

`AsVariant()` создает значение [варианта над структурой](../../../types/containers.md) с одним полем. Это значение может быть неявно преобразовано к любому варианту над структурой, в которой совпадает для этого имени поля тип данных и могут быть дополнительные поля с другими именами.

### Сигнатура

```yql
AsVariant(T, String)->Variant
```

Аргументы:

* Значение
* Строка с именем поля

### Пример

```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```

## Visit, VisitOrDefault {#visit}

Обрабатывает возможные значения варианта над структурой или кортежом с помощью предоставленных функций-обработчиков для каждого из полей/элементов варианта.

### Сигнатура

```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>, [K1->R, [K2->R, ...]], R)->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>, [K1->R AS key1, [K2->R AS key2, ...]], R)->R
```

### Аргументы:

* Для варианта над структурой принимает сам вариант как позиционный аргумент, и по одному именованному аргументу-обработчику на каждое из полей варианта.
* Для варианта над кортежом принимает как позиционные аргументы сам вариант и по одному аргументу-обработчику на каждый элемент варианта.
* Модификация VisitOrDefault принимает дополнительный последний позиционный аргумент -- значение по умолчанию, и позволяет опустить некоторые из обработчиков.

Также реализует поведение auto-map для первого аргумента.

### Пример

```yql
$vartype = Variant<num: Int32, flag: Bool, str: String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num AS num, $handle_flag AS flag, $handle_str AS str); };
SELECT
    $visitor(Variant(5, "num", $vartype)),                -- 10
    $visitor(Just(Variant(True, "flag", $vartype))),      -- Just(200)
    $visitor(Just(Variant("somestr", "str", $vartype))),  -- Just(7)
    $visitor(Nothing(OptionalType($vartype))),            -- Nothing(Optional<Int32>)
    $visitor(NULL)                                        -- NULL
;
```

## VariantItem {#variantitem}

Возвращает значение гомогенного варианта (содержащего поля/элементы только одного типа).

### Сигнатура

```yql
VariantItem(Variant<key1: K, key2: K, ...>)->K
VariantItem(Variant<K, K, ...>)->K
```
Также реализует поведение auto-map для аргумента.

### Пример

```yql
$vartype1 = Variant<num1: Int32, num2: Int32, num3: Int32>;
SELECT
    VariantItem(Variant(7, "num2", $vartype1)),          -- 7
    VariantItem(Just(Variant(5, "num1", $vartype1))),    -- Just(5)
    VariantItem(Nothing(OptionalType($vartype1))),       -- Nothing(Optional<Int32>)
    VariantItem(NULL)                                    -- NULL
;
```

