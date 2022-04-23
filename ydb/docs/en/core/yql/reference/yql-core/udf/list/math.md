# Math

A set of wrappers around the functions from the libm library and the Yandex utilities.

## Constants {#constants}

**List of functions**

* ```Math::Pi() -> Double```
* ```Math::E() -> Double```
* ```Math::Eps() -> Double```

**Examples**

```sql
SELECT Math::Pi();  -- 3.141592654
SELECT Math::E();   -- 2.718281828
SELECT Math::Eps(); -- 2.220446049250313e-16
```

## (Double) -> Bool {#double-bool}

**List of functions**

* ```Math::IsInf(Double{Flags:AutoMap}) -> Bool```
* ```Math::IsNaN(Double{Flags:AutoMap}) -> Bool```
* ```Math::IsFinite(Double{Flags:AutoMap}) -> Bool```

**Examples**

```sql
SELECT Math::IsNaN(0.0/0.0);    -- true
SELECT Math::IsFinite(1.0/0.0); -- false
```

## (Double) -> Double {#double-double}

**List of functions**

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

**Examples**

```sql
SELECT Math::Sqrt(256);     -- 16
SELECT Math::Trunc(1.2345); -- 1
```

## (Double, Double) -> Double {#doubledouble-double}

**List of functions**

* ```Math::Atan2(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Fmod(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Hypot(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Pow(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```
* ```Math::Remainder(Double{Flags:AutoMap}, Double{Flags:AutoMap}) -> Double```

**Examples**

```sql
SELECT Math::Atan2(1, 0);       -- 1.570796327
SELECT Math::Remainder(2.1, 2); -- 0.1
```

## (Double, Int32) -> Double {#doubleint32-double}

**List of functions**

* ```Math::Ldexp(Double{Flags:AutoMap}, Int32{Flags:AutoMap}) -> Double```
* ```Math::Round(Double{Flags:AutoMap}, [Int32?]) -> Double```: The second argument indicates the power of 10 to which we round (it's negative for decimal digits and positive for rounding to tens, thousands, or millions); the default value is 0

**Examples**

```sql
SELECT Math::Pow(2, 10);        -- 1024
SELECT Math::Round(1.2345, -2); -- 1.23
```

## (Double, Double, \[Double?\]) -> Bool {#doubledouble-bool}

**List of functions**

* ```Math::FuzzyEquals(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Double?]) -> Bool```: Compares two Doubles for being inside the neighborhood specified by the third argument; the default value is 1.0e-13

**Examples**

```sql
SELECT Math::FuzzyEquals(1.01, 1.0, 0.05); -- true
```

## Functions for computing remainders

**List of functions**

* ```Math::Mod(Int64{Flags:AutoMap}, Int64) -> Int64?```
* ```Math::Rem(Int64{Flags:AutoMap}, Int64) -> Int64?```

These functions behave similarly to the built-in % operator in the case of non-negative arguments. The differences are noticeable in the case of negative arguments:

* Math::Mod preserves the sign of the second argument (the denominator).
* Math::Rem preserves the sign of the first argument (the numerator).
Functions return null if the divisor is zero.

**Examples**

```sql
SELECT Math::Mod(-1, 7);        -- 6
SELECT Math::Rem(-1, 7);        -- -1
```

