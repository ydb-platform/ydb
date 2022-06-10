## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()` и `FlipBit()` - проверить, сбросить, установить или инвертировать бит в беззнаковом числе по указанному порядковому номеру бита.

**Сигнатуры**
```
TestBit(T, Uint8)->Bool
TestBit(T?, Uint8)->Bool?

ClearBit(T, Uint8)->T
ClearBit(T?, Uint8)->T?

SetBit(T, Uint8)->T
SetBit(T?, Uint8)->T?

FlipBit(T, Uint8)->T
FlipBit(T?, Uint8)->T?
```

Аргументы:

1. Беззнаковое число, над которым выполнять требуемую операцию. TestBit также реализован и для строк.
2. Номер бита.

TestBit возвращает `true/false`. Остальные функции возвращают копию своего первого аргумента с проведенным соответствующим преобразованием.

**Примеры:**
``` yql
SELECT
    TestBit(1u, 0), -- true
    SetBit(8u, 0); -- 9
```
