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

Обрабатывает возможные значения варианта, представленного структурой или кортежем, с использованием предоставленных функций-обработчиков для каждого из его полей/элементов.

### Сигнатура

```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>{Flags:AutoMap}, R, [K1->R, [K2->R, ...]])->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap}, R, [K1->R AS key1, [K2->R AS key2, ...]])->R
```

### Аргументы

* Для варианта над структурой функция принимает сам вариант в качестве позиционного аргумента и по одному именованному аргументу-обработчику для каждого поля этой структуры.
* Для варианта над кортежем функция принимает сам вариант и по одному обработчику на каждый элемент кортежа в качестве позиционных аргументов.
* Модификация `VisitOrDefault` принимает дополнительный позиционный аргумент (на втором месте), представляющий значение по умолчанию, и позволяет не указывать некоторые обработчики.

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

Возвращает значение гомогенного варианта (т.е. содержащего поля/элементы одного типа).

### Сигнатура

```yql
VariantItem(Variant<key1: K, key2: K, ...>{Flags:AutoMap})->K
VariantItem(Variant<K, K, ...>{Flags:AutoMap})->K
```

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

## Way {#way}

Возвращает активное поле (активный индекс) варианта поверх структуры (кортежа)

### Сигнатура

```yql
VariantItem(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap})->Utf8
VariantItem(Variant<K1, K2, ...>{Flags:AutoMap})->Uint32
```

### Пример

```yql
$vr = Variant(1, "0", Variant<Int32, String>);
$vrs = Variant(1, "a", Variant<a:Int32, b:String>);


SELECT Way($vr);  -- 0
SELECT Way($vrs); -- "a"

```

