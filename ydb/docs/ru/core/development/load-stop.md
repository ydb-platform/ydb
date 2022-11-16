# Остановка нагрузки

## Описание
```proto
message TLoadStop {
    optional uint64 Tag = 1; // Тег нагрузки, может быть задан для остановки конкретной нагрузки. Тег можно посмотреть в UI load-actor
    optional bool RemoveAllTags = 2; // Остановить все нагрузочные акторы на ноде
}
```

## Примеры

```proto
NodeId: 1
Event: {
    LoadStop: {
        RemoveAllTags: true
    }
}
```
или
```proto
NodeId: 1
Event: {
    LoadStop: {
        Tag: 123
    }
}
```