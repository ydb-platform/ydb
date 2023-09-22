# Синтаксис PostgreSQL

{% note warning %}

На данный момент PostgreSQL совместимость находится в разработке и доступна для тестирования в виде Docker-контейнера, который можно развернуть, следуя данной [инструкции](docker-connect.md). Поддерживаемый синтаксис PostgreSQL описан в разделе [{#T}](statements.md), а доступные для использования функции изложены в разделе [{#T}](functions.md).

{% endnote %}

Для удобства навигации синтаксис PostgreSQL можно разделить на следующие категории:

1. **Data Query Language (DQL)** – инструкции запроса данных:
    * [SELECT](#select) – запрашивает данные из одной или нескольких таблиц.
2. **Data Modification Language (DML)** – инструкции, которые влияют на данные:
    * [INSERT](#insert) – добавляет новые строки в таблицу или обновляет существующие с опцией `ON CONFLICT`;
    * [UPDATE](#update) - изменяет существующие строки в таблице;
    * [DELETE](#delete) - удаляет строки из таблицы.
3. **Data Definition Language (DDL)** – инструкции, которые определяют или изменяют структуру базы данных:
    * [CREATE]({#create}) - создает новые объекты (такие, как таблицы);
    * [DROP]({#drop}) –  удаляет объекты.
4. **Transaction Control Statements (TCS)** – команды управления транзакциями:
    * [BEGIN](#transactions) – начинает новую транзакцию;
    * [COMMIT](#transactions) – подтверждает изменения, сделанные в рамках транзакции;
    * [ROLLBACK](#transactions) – отменяет изменения, сделанные в рамках транзакции.    

В списке приведены только те инструкции использования систаксиса PostgreSQL, который сейчас реализованы в совместимости YDB с PostgreSQL. 

{% include [./_includes/statements/create_table.md](./_includes/statements/create_table.md) %}

{% include [./_includes/statements/insert_into.md](./_includes/statements/insert_into.md) %}

{% include [./_includes/statements/select.md](./_includes/statements/select.md) %}

{% include [./_includes/statements/update.md](./_includes/statements/update.md) %}

{% include [./_includes/statements/drop_table.md](./_includes/statements/drop_table.md) %}

{% include [./_includes/statements/begin_commit_rollback.md](./_includes/statements/begin_commit_rollback.md) %}