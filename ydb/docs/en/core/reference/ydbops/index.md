## ydbops utility overview

{% note info %}

The article is being updated. Expect new content to appear and minor fixes to existing content.

{% endnote %}

`ydbops` utility automates some operational tasks on {{ ydb-short-name }} clusters. It supports clusters deployed using [Ansible](../../devops/ansible/index.md), [Kubernetes](../../devops/kubernetes/index.md), or [manually](../../devops/manual/index.md).

## See also

* To install the utility, follow the [instructions](install.md).
* see [configuration reference](configuration.md) for available configuration options.
* The source code of `ydbops` can be found [on Github](https://github.com/ydb-platform/ydbops).

## Currently supported scenarious
- performing a [rolling restart of the cluster](rolling-restart-scenario.md).

## Scenarious in development
- requesting a permission to take out a set of YDB nodes for maintenance without breaking YDB fault model invariants.
