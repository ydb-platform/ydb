# Вложенные вызовы ретраера внутри ретраера

## Проблема

В {{ ydb-short-name }} SDK есть ретраеры, которые берут на себя необходимость создать и подготовить сессию, для выполнения на ней запросов, складированием ее в пул и т.п. Все ретраеры во всех SDK имеют интерфейс с лямбдами, в которые SDK передает подготовленную сессию и требует возврата ошибки:

```go
db.Query().Do(ctx, func(ctx context.Context, s query.Session) error { ... })
```

Если внутри такой лямбды вызвать вновь `db.Query().Do(...)`, то можно получить `DEADLOCK` на попытке взять сессию из пула. Устройство ретраеров — в [{#T}](../../recipes/ydb-sdk/retry.md).

## Решение

Используйте переданную сессию напрямую внутри лямбды ретраера, не вызывая другие ретраеры


{% cut "Плохой пример" %}
```go
err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
  return db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
    // do with session
    return nil
  })
})
err := db.Query().Do(ctx, 
  func(ctx context.Context, s query.Session) error {  
     └── quot()
           └── licet()
                └── iovi()
                     └── non()
                          └── licet()
                               └── bovi()
                                    └── return db.Query().Do(ctx, func(ctx, s) error {...})
  }
)
```
{% endcut %}

{% cut "Хороший пример" %}
```go
db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
  // do with session
  return nil
})
err := db.Query().Do(ctx, 
  func(ctx context.Context, session query.Session) error {  
     └── quot( session)
           └── licet( session)
                └── iovi( session)
                     └── non( session)
                          └── licet( session)
                               └── bovi( session)
                                    └── // do with session
  }
)
```
{% endcut %}
