# Остановка нагрузки

С помощью актора LoadStop вы можете остановить нагрузку на указанной ноде.

## Структура {#proto}

```proto
message TLoadStop {
    optional uint64 Tag = 1;
    optional bool RemoveAllTags = 2;
}
```

## Параметры {#options}

### TLoadStop

Параметр | Описание
--- | ---
`Tag` | Тег нагрузки, может быть задан для остановки конкретной нагрузки. Тег можно посмотреть в UI load-actor `uint64`.
`RemoveAllTags` | Остановить все нагрузочные акторы на ноде `bool`.

## Примеры {#examples}

Остановить акторы с тегом `123` на ноде с идентификатором `1`:

```proto
NodeId: 1
Event: {
    LoadStop: {
        Tag: 123
    }
}
```

Остановить все нагрузочные акторы на ноде с идентификатором `1`:

```proto
NodeId: 1
Event: {
    LoadStop: {
        RemoveAllTags: true
    }
}
```
