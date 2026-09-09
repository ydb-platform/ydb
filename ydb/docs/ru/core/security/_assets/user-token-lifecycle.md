```mermaid
sequenceDiagram
    actor user as Пользователь
    participant node as Узел YDB
    participant cache as Кеш узла
    participant auth as Подсистема аутентификации

    user->>node: Запрос с теми же данными
    node->>cache: Найти запись
    cache-->>node: Запись найдена
    Note right of cache: Отсчёт life_time начинается заново

    opt Наступило время обновления
        node->>auth: Повторно проверить аутентификационный токен
        alt Успешная проверка
            auth-->>node: Результат проверки
            node->>node: Создать новый токен пользователя
            node->>cache: Обновить запись
        else Ретрабельная ошибка
            auth-->>node: Ошибка
            Note right of node: Запланировать повторную попытку
        else Постоянная ошибка
            auth-->>node: Ошибка
            node->>cache: Перестать использовать токен пользователя
        end
    end

    alt Запись не использовалась в течение life_time или истёк срок действия
        node->>cache: Удалить запись
    end
```
