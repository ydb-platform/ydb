# Math
Набор обёрток вокруг функций из библиотеки libm, а также утилит Яндекс.

## Константы {#constants}
**Список функций**

* ```Math::Pi() -> Double```
* ```Math::E() -> Double```
* ```Math::Eps() -> Double```

**Примеры**

```sql
SELECT Math::Pi();  -- 3.141592654
SELECT Math::E();   -- 2.718281828
SELECT Math::Eps(); -- 2.220446049250313e-16
```

## (Double) -> Bool {#double-bool}

**Список функций**

* ```Math::IsInf(Double{Flags:AutoMap}) -> Bool```
* ```Math::IsNaN(Double{Flags:AutoMap}) -> Bool```
* ```Math::IsFinite(Double{Flags:AutoMap}) -> Bool```

**Примеры**

```sql
SELECT Math::IsNaN(0.0/0.0);    -- true
SELECT Math::IsFinite(1.0/0.0); -- false
```

## (Double) -> Double {#double-double}

**Список функций**

* ```Math::Abs(Double{Flags:AutoMap}) -> Double```
* ```Math::Acos(Double{Flags:AutoMap}) -> Double```
* ```Math::Asin(Double{Flags:AutoMap}) -> Double```
* ```Math::Asinh(Double{Flags:AutoMap}) -> Double```
* ```Math::Atan(Double{Flags:AutoMap}) -> Double```
* ```Math::Cbrt(Double{Flags:AutoMap}) -> Double```
* ```Math::Ceil(Double{Flags:AutoMap}) -> Double```
* ```Math::Cos(Double{Flags:AutoMap}) -> Double```
* ```Math::Cosh(Double{Flags:AutoMap}) -> Double```
* ```Math::Erf(Double{Flags:AutoMap}) -> Double```
* ```Math::ErfInv(Double{Flags:AutoMap}) -> Double```
* ```Math::ErfcInv(Double{Flags:AutoMap}) -> Double```
* ```Math::Exp(Double{Flags:AutoMap}) -> Double```
* ```Math::Exp2(Double{Flags:AutoMap}) -> Double```
* ```Math::Fabs(Double{Flags:AutoMap}) -> Double```
* ```Math::Floor(Double{Flags:AutoMap}) -> Double```
* ```Math::Lgamma(Double{Flags:AutoMap}) -> Double```
* ```Math::Rint(Double{Flags:AutoMap}) -> Double```
* ```Math::Sigmoid(Double{Flags:AutoMap}) -> Double```
* ```Math::Sin(Double{Flags:AutoMap}) -> Double```
* ```Math::Sinh(Double{Flags:AutoMap}) -> Double```
* ```Math::Sqrt(Double{Flags:AutoMap}) -> Double```
* ```Math::Tan(Double{Flags:AutoMap}) -> Double```
* ```Math::Tanh(Double{Flags:AutoMap}) -> Double```
* ```Math::Tgamma(Double{Flags:AutoMap}) -> Double```
* ```Math::Trunc(Double{Flags:AutoMap}) -> Double```
* ```Math::Log(Double{Flags:AutoMap}) -> Double```
* ```Math::Log2(Double{Flags:AutoMap}) -> Double```
* ```Math::Log10(Double{Flags:AutoMap}) -> Double```

**Примеры**

```sql
SELECT Math::Sqrt(256);     -- 16
SELECT Math::Trunc(1.2345); -- 1
```

## (Double, Double) -> Double {#doubledouble-double}

**Список функций**

* ```Math::Atan2(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Fmod(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Hypot(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Pow(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Remainder(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```

**Примеры**

```sql
SELECT Math::Atan2(1, 0);       -- 1.570796327
SELECT Math::Remainder(2.1, 2); -- 0.1
```

## (Double, Int32) -> Double {#doubleint32-double}

**Список функций**

* ```Math::Ldexp(Double{Flags:AutoMap}, Int32{Flags:AutoMap}) -> Double```
* ```Math::Round(Double{Flags:AutoMap}, [Int32?]) -> Double``` - во втором аргументе указывается степень 10, до которой округляем (отрицательная для знаков после запятой и положительная для округления до десятков—тысяч—миллионов); по умолчанию 0

**Примеры**

```sql
SELECT Math::Pow(2, 10);        -- 1024
SELECT Math::Round(1.2345, -2); -- 1.23
```

## (Double, Double, \[Double?\]) -> Bool {#doubledouble-bool}

**Список функций**

* ```Math::FuzzyEquals(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Double?]) -> Bool``` - сравнивает два Double на нахождение внутри окрестности, задаваемой третьим аргументом; по умолчанию 1.0e-13. Окрестность указывается в относительных единицах от минимального по модулю аргумента.

{% note alert %}

Если один из сравниваемых аргументов равен `0.0`, то функция всегда возвращает `false`.

{% endnote %}

**Примеры**

```sql
SELECT Math::FuzzyEquals(1.01, 1.0, 0.05); -- true
```

## Функции взятия остатка

**Список функций**

* ```Math::Mod(Int64{Flags:AutoMap}, Int64) -> Int64?```
* ```Math::Rem(Int64{Flags:AutoMap}, Int64) -> Int64?```

Ведут себя аналогично встроенному оператору % в случае неотрицательных аргументов. Различия заметны при отрицательных аргументах:
* Math::Mod сохраняет знак второго аргумента (делителя).
* Math::Rem сохраняет знак первого аргумента (делимого).
Функции возвращают null, если делитель равен нулю.

**Примеры**

```sql
SELECT Math::Mod(-1, 7);        -- 6
SELECT Math::Rem(-1, 7);        -- -1
```

## Функции округления до целого числа в заданном режиме

**Список функций**

* ```Math::RoundDownward() -> Tagged<Uint32, MathRoundingMode>``` -- rounding towards negative infinity
* ```Math::RoundToNearest() -> Tagged<Uint32, MathRoundingMode>``` -- rounding towards nearest representable value
* ```Math::RoundTowardZero() -> Tagged<Uint32, MathRoundingMode>``` -- rounding towards zero
* ```Math::RoundUpward() -> Tagged<Uint32, MathRoundingMode>``` -- rounding towards positive infinity
* ```Math::NearbyInt(AutoMap<Double>, Tagged<Uint32, MathRoundingMode>) -> Optional<Int64>```

Функция Math::NearbyInt округляет первый аргумент до целого числа в соответсвии с режимом, заданным вторым аргументом.
Если результат выходит за пределы 64-битного целого числа, возращается NULL.

**Примеры**

```sql
SELECT Math::NearbyInt(1.5, Math::RoundDownward()); -- 1
SELECT Math::NearbyInt(1.5, Math::RoundToNearest()); -- 2
SELECT Math::NearbyInt(2.5, Math::RoundToNearest()); -- 2
SELECT Math::NearbyInt(1.5, Math::RoundTowardZero()); -- 1
SELECT Math::NearbyInt(1.5, Math::RoundUpward()); -- 2
SELECT Math::NearbyInt(-1.5, Math::RoundDownward()); -- -2
SELECT Math::NearbyInt(-1.5, Math::RoundToNearest()); -- -2
SELECT Math::NearbyInt(-2.5, Math::RoundToNearest()); -- -2
SELECT Math::NearbyInt(-1.5, Math::RoundTowardZero()); -- -1
SELECT Math::NearbyInt(-1.5, Math::RoundUpward()); -- -1
```
