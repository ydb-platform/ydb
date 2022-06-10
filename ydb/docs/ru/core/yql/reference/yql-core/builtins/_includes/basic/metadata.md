## Доступ к метаданным текущей операции {#metadata}

При запуске YQL операций через веб-интерфейс или HTTP API, предоставляется доступ к следующей информации:

* `CurrentOperationId()` — приватный идентификатор операции;
* `CurrentOperationSharedId()` — публичный идентификатор операции;
* `CurrentAuthenticatedUser()` — логин текущего пользователя.

**Сигнатуры**
```
CurrentOperationId()->String
CurrentOperationSharedId()->String
CurrentAuthenticatedUser()->String
```

Аргументов нет.

При отсутствии данной информации, например, при запуске в embedded режиме, возвращают пустую строку.

**Примеры**
``` yql
SELECT
    CurrentOperationId(),
    CurrentOperationSharedId(),
    CurrentAuthenticatedUser();
```
