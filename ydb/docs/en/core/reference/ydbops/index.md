## ydbops utility overview

{% note info %}

The article is being updated. Expect new content to appear and minor fixes to existing content.

{% endnote %}

`ydbops` utility automates some advanced operational tasks. To install the utility, please follow the [instructions](install.md).

For available configuration options, see [configuration reference](configuration.md).

The source code of `ydbops` can be found [on Github](https://github.com/ydb-platform/ydbops).

## Currently supported scenarious
- performing a [rolling restart of the cluster](rolling-restart-scenario.md).

## Scenarious in development
- requesting a permission to take out a set of YDB nodes for maintenance without breaking YDB fault model invariants.
