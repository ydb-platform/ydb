## Иерархия сущностей на кластере {#hierarchy} {#domain} {#account} {#directory} {#dir}

Для размещения БД в {{ ydb-short-name }} принято использовать следующую структуру директорий: `/domain/account/directory/database`, где:

* _Домен_ (domain) — элемент первого уровня, в котором располагаются все ресурсы кластера. На одном кластере может быть только один домен.
* _Аккаунт_ (account) — элемент второго уровня, определяющий владельца группы БД.
* _Директория_ (directory) — элемент третьего уровня, определяемый владельцем аккаунта для группировки его БД.
* _База данных_ (database) — элемент четвертого уровня, соответствующий БД.

На верхнем уровне уровне иерархии в кластере расположен один домен, в нем располагаются один или несколько аккаунтов, в аккаунте — одна или несколько директорий, в которых в свою очередь располагаются базы данных.

Иерархия сущностей позволяет назначать права доступа, а также получать агрегированные или отфильтрованные показатели мониторинга, по элементам любого уровня иерархии.
