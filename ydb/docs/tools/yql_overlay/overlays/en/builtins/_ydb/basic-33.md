1. An unsigned number that's subject to the operation. TestBit is also implemented for strings.
2. Number of the bit.

TestBit returns `true/false`. The other functions return a copy of their first argument with the corresponding conversion.

### Examples

```yql
SELECT
    TestBit(1u, 0), -- true
    SetBit(8u, 0); -- 9
```
