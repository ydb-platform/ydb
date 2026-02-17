# Вкладка Tablets

На вкладке отображается список таблеток, работающих в кластере. Вкладка входит в набор вкладок [главной страницы](monitoring_main.md) вместе с [Databases](tab-databases.md), [Nodes](tab-nodes.md), [Storage](tab-storage.md) и [Versions](tab-versions.md).

![Tablets_list](../_assets/tablets_list.png)

На странице доступны поиск по **TabletID** и счетчик таблеток.

В таблице отображаются:

* **Type** — тип таблетки;
* **TabletID** — идентификатор таблетки. По ссылке можно перейти на [страницу таблетки](tablets.md);
* **State** — состояние таблетки;
* **NodeID** — идентификатор узла, на котором работает таблетка; см. [вкладку Nodes](tab-nodes.md) и [страницу узла](nodes.md);
* **NodeFQDN** — полное доменное имя узла;
* **Generation** — [поколение](../../../concepts/glossary.md#tablet-generation) таблетки;
* **Uptime** — время работы таблетки.
