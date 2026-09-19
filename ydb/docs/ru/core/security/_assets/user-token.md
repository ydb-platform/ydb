```mermaid
sequenceDiagram
    %% ```mermaid
sequenceDiagram
    actor user as 
    actor user as Пользователь
    %% 
    participant node as 
    participant node as Узел YDB
    %% 
    participant cache as 
    participant cache as Кэш узла
    %% 
    participant auth as 
    participant auth as Подсистема аутентификации

    %% 

    user->>node: 
    user->>node: Первый запрос с токеном аутентификации
    %% 
    node->>cache: 
    node->>cache: Найти запись по ключу
    %% 
    cache-->>node: 
    cache-->>node: Запись не найдена
    %% 
    node->>auth: 
    node->>auth: Проверить токен аутентификации
    %% 
    auth-->>node: 
    auth-->>node: Проверить результат
    %% 
    node->>node: 
    node->>node: Создать пользовательский токен
    %% 
    node->>cache: 
    node->>cache: Сохранить пользовательский токен
    %% 
    node-->>user: 
    node-->>user: Обработать запрос

    %% 

    user->>node: 
    user->>node: Следующий запрос с тем же ключом
    %% 
    node->>cache: 
    node->>cache: Найти запись по ключу
    %% 
    cache-->>node: 
    cache-->>node: Пользовательский токен
    %% 
    node-->>user: 
    node-->>user: Обработать запрос
    %% 
```

```