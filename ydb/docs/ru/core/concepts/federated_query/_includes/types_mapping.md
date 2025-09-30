|YQL тип|arrow тип|Комментарий|
|----|----|---|
|`Bool`|`uint8`||
|`IntX`|`intX`||
|`UintX`|`uintX`||
|`Float`|`float32`||
|`Double`|`float64`||
|`Date`|`uint16`|количество дней, прошедших с начала [Unix-эпохи](https://ru.wikipedia.org/wiki/Unix-время)|
|`Date32`|`date(seconds)`||
|`Datetime`|`uint32`|количество секунд, прошедших с начала [Unix-эпохи](https://ru.wikipedia.org/wiki/Unix-время)|
|`Datetime64`|`timestamp(micro)`||
|`Timestamp`|`timestamp(micro)`||
|`Timestamp64`|`timestamp(micro)`||
|`Decimal(n, m)`|`decimal(n, m)`||
|`String`|`binary`||
|`Utf8`|`binary`||
|`Json`|`binary`||
