### Syntax

1. `DO`: Executing an action.
1. The named expression for which the action is defined.
1. The values to be used as parameters are listed in parentheses.

`EMPTY_ACTION`: An action that does nothing.

{% if feature_mapreduce %} <!-- In fact, if user file system integration is supported in the product. YQL service over YDB may also be here. -->

{% note info %}

In large queries, you can use separate files for action definition and include them to the main query using [EXPORT](../export_import.md#export) + [IMPORT](../export_import.md#import) so that instead of one long text you can have several logical parts that are easier to navigate. An important nuance: the `USE my_cluster;` directive in the import query does not affect behavior of actions declared in other files.

{% endnote %}

{% endif %}

### Example

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
