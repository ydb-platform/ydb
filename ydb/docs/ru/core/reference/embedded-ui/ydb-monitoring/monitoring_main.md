# Главная страница {#main_page}

Главная страница доступна по адресу:

```text
http://<ендпоинт>:8765/monitoring/cluster/tenants
```

Пример главной страницы:

![Monitoring_main_page](../_assets/web-ui-home.png)

На странице отображается сводная информация о кластере.

На вкладке **Overview** показаны [индикаторы состояния](index.md#colored_indicator) по ключевым ресурсам:

* **CPU** — загрузка процессора;
* **Storage** — использование дисковой подсистемы;
* **Memory** — использование оперативной памяти;
* **Network** — использование сетевых ресурсов.

Далее представлен набор вкладок:

* **[Databases](tab-databases.md)** — список [баз данных](../../../concepts/glossary.md#database), развернутых в кластере;
* **[Nodes](tab-nodes.md)** — список [узлов кластера](../../../concepts/glossary.md#node);
* **[Storage](tab-storage.md)** — список [групп хранения](../../../concepts/glossary.md#storage-group) и использование ими дискового пространства;
* **[Tablets](tab-tablets.md)** — список запущенных [таблеток](../../../concepts/glossary.md#tablet);
* **[Versions](tab-versions.md)** — версии {{ ydb-short-name }}, запущенные на узлах кластера.

Подробное описание каждой вкладки можно найти на соответствующих страницах:

* [Вкладка Databases](tab-databases.md);
* [Вкладка Nodes](tab-nodes.md);
* [Вкладка Storage](tab-storage.md);
* [Вкладка Tablets](tab-tablets.md);
* [Вкладка Versions](tab-versions.md).

Подробное описание страниц, открываемых по ссылкам из вкладок:

* [Страница Databases](database.md);
* [Страница Nodes](nodes.md);
* [Страница Storage](storage.md);
* [Страница Tablets](tablets.md).
