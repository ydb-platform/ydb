Generate a sequence of numbers with the specified step. It's similar to `xrange` in Python 2, but additionally supports floats.

Arguments:

1. Start
2. End
3. Step (optional, 1 by default)

Specifics:

* The end is not included, i.e. `ListFromRange(1,3) == AsList(1,2)`.
* The type for the resulting elements is selected as the broadest from the argument types. For example, `ListFromRange(1, 2, 0.5)` results in a `Double` list.
* If the start and the end is one of the date representing type, the step has to be `Interval`.
* The list is "lazy", but if it's used incorrectly, it can still consume a lot of RAM.
* If the step is positive and the end is less than or equal to the start, the result list is empty.
* If the step is negative and the end is greater than or equal to the start, the result list is empty.
* If the step is neither positive nor negative (0 or NaN), the result list is empty.
* If any of the parameters is optional, the result list is optional.
* If any of the parameters is `NULL`, the result is `NULL`.

### Examples

```yql
SELECT
    ListFromRange(-2, 2), -- [-2, -1, 0, 1]
    ListFromRange(2, 1, -0.5); -- [2.0, 1.5]
```

### Signature
