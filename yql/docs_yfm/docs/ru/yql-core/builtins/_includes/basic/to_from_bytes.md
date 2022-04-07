## ToBytes и FromBytes {#to-from-bytes}

Конвертация [простых типов данных](../../../types/primitive.md) в строку со своим бинарным представлением и обратно. Числа представляются в [little endian](https://en.wikipedia.org/wiki/Endianness#Little-endian).

**Примеры**
``` yql
SELECT
    ToBytes(123), -- "\u0001\u0000\u0000\u0000"
    FromBytes(
        "\xd2\x02\x96\x49\x00\x00\x00\x00",
        Uint64
    ); -- 1234567890ul
```
