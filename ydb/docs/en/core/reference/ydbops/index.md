# ydbops utility overview

{% include [warning.md](_includes/warning.md) %}

`ydbops` utility automates some operational tasks on {{ ydb-short-name }} clusters. It supports clusters deployed using [Ansible](../../devops/deployment-options/ansible/index.md), [Kubernetes](../../devops/deployment-options/kubernetes/index.md), or [manually](../../devops/deployment-options/manual/index.md).

## See also

* To install the utility, follow the [instructions](install.md).
* See [configuration reference](configuration.md) for available configuration options.
* The source code of `ydbops` can be found [on GitHub](https://github.com/ydb-platform/ydbops).

## Currently supported scenarios

See the list of currently supported scenarios [here](rolling-restart-scenario.md).

## Scenarios in development

- Requesting permission to take out a set of {{ ydb-short-name }} nodes for maintenance without breaking {{ ydb-short-name }} fault model invariants.
