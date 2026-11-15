### Синтаксис

1. `DO` — выполнение действия.
1. Именованное выражение, по которому объявлено действие.
1. В круглых скобках — список значений для использования в роли параметров.

`EMPTY_ACTION` — действие, которое ничего не выполняет.

{% if feature_mapreduce %}

{% note info %}

В больших запросах объявление действий можно выносить в отдельные файлы и подключать их в основной запрос с помощью [EXPORT](../export_import.md#export) + [IMPORT](../export_import.md#import), чтобы вместо одного длинного текста получилось несколько логических частей, в которых проще ориентироваться. Важный нюанс: директива `USE my_cluster;` в импортирующем запросе не влияет на поведение объявленных в других файлах действий.

{% endnote %}

{% endif %}

### Пример

```yql
DEFINE ACTION $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

DO EMPTY_ACTION();
DO $hello_world(NULL);
DO $hello_world("John");
DO $hello_world(NULL, "Earth");
```
