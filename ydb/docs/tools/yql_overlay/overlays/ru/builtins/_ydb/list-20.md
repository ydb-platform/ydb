### Примеры

```yql
SELECT ListUniq([1, 2, 3, 2, 4, 5, 1]) -- [5, 4, 2, 1, 3]
SELECT ListUniqStable([1, 2, 3, 2, 4, 5, 1]) -- [1, 2, 3, 4, 5]
SELECT ListUniqStable([1, 2, null, 7, 2, 8, null]) -- [1, 2, null, 7, 8]
```

### Сигнатура
