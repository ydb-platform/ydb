Необязательное *определение рамки* (`frame_definition`) может быть задано одним из двух способов:

* ```ROWS frame_begin```
* ```ROWS BETWEEN frame_begin AND frame_end```

*Начало рамки* (`frame_begin`) и *конец рамки* (`frame_end`) задаются одним из следующих способов:

* ```UNBOUNDED PRECEDING```
* ```offset PRECEDING```
* ```CURRENT ROW```
* ```offset FOLLOWING```
* ```UNBOUNDED FOLLOWING```

Здесь *смещение рамки* (`offset`) – неотрицательный числовой литерал. Если конец рамки не задан, то подразумевается `CURRENT ROW`.
