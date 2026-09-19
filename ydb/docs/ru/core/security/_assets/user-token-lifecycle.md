```mermaid
sequenceDiagram
    actor user as ```mermaid
sequenceDiagram
    actor user as Пользователь
    participant node as 
    participant node as Узел YDB
    participant cache as 
    participant cache as Кэш узла
    participant auth as 
    participant auth as Подсистема аутентификации

    user->>node: 

    user->>node: Запрос с теми же данными
    node->>cache: 
    node->>cache: Найти запись
    cache-->>node: 
    cache-->>node: Запись найдена
    Note right of cache: 
    Note right of cache: Обратный отсчёт life_time перезапускается

    opt 

    opt Время обновления достигнуто
        node->>auth: 
        node->>auth: Повторная проверка токена аутентификации
        alt 
        alt Успешная проверка
            auth-->>node: 
            auth-->>node: Результат проверки
            node->>node: 
            node->>node: Создать новый токен пользователя
            node->>cache: 
            node->>cache: Обновить запись
        else 
        else Повторяемая ошибка
            auth-->>node: 
            auth-->>node: Ошибка
            Note right of node: 
            Note right of node: Запланировать повтор
        else 
        else Постоянная ошибка
            auth-->>node: 
            auth-->>node: Ошибка
            node->>cache: 
            node->>cache: Прекратить использование токена пользователя
        end
    end

    alt 
        end
    end

    alt Запись не использовалась в течение life_time или истекла
        node->>cache: 
        node->>cache: Удалить запись
    end

    end
```

```