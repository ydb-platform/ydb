```mermaid
sequenceDiagram
    actor user as Пользователь
    participant node as Узел YDB
    participant auth as Подсистема аутентификации

    user->>node: Запрос с аутентификационным токеном
    node->>auth: Проверить аутентификационный токен
    auth->>node: Результат проверки

    activate node
    node->>node: Создать и сохранить токен пользователя
    user->>node: Запрос с тем же аутентификационным токеном
    node->>node: Получить токен пользователя из кеша
    deactivate node
```
