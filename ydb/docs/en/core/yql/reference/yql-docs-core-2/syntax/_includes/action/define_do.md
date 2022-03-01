## DEFINE ACTION {#define-action}

Specifies a named action that is a parameterizable block of multiple top-level expressions.

**Syntax**

1. `DEFINE ACTION`: action definition.
1. [Action name](../../expressions.md#named-nodes) that will be used to access the defined action further in the query.
1. The values of parameter names are listed in parentheses.
1. `AS` keyword.
1. List of top-level expressions.
1. `END DEFINE`: The marker of the last expression inside the action.

One or more of the last parameters can be marked with a question mark `?` as optional. If they are omitted during the call, they will be assigned the `NULL` value.

## DO {#do}

Executes an `ACTION` with the specified parameters.

**Syntax**

1. `DO`: Executing an action.
1. The named expression for which the action is defined.
1. The values to be used as parameters are listed in parentheses.

`EMPTY_ACTION`: An action that does nothing.

{% if feature_mapreduce %} <!-- In fact, if user file system integration is supported in the product. YQL service over YDB may also be here. -->

{% note info %}

In large queries, you can use separate files for action definition and include them to the main query using [EXPORT](../../export_import.md#export) + [IMPORT](../../export_import.md#import) so that instead of one long text you can have several logical parts that are easier to navigate. An important nuance: the `USE my_cluster;` directive in the import query does not affect behavior of actions declared in other files.

{% endnote %}

{% endif %}

**Example**

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

