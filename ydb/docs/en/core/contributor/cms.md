# Cluster Management System

The cluster management system (CMS) is a [system tablet](../concepts/glossary.md#tablet-types) designed to ensure that planned cluster maintenance (such as updates or hardware replacements) does not compromise data availability by violating system fault tolerance.

Instead of manually analyzing the state of components, administrators create a maintenance task in the CMS to decommission specific nodes or disks. The CMS verifies whether executing this task would violate the failure models of storage groups or state storage, or exceed resource limits. If the check passes, the CMS acquires exclusive locks on the target components, temporarily excluding them from fault tolerance calculations. This ensures safe maintenance operations.

Thus, the CMS formalizes and automates the decision-making logic in distributed systems, enabling continuous data availability alongside necessary planned maintenance.

## Dynamic Configuration Management {#dynamic-configuration-management}

{{ ydb-short-name }} allows you to change certain settings directly via the administrator interface. However, these changes do not persist after a cluster restart. To apply settings permanently, modify the [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md).

## See Also

* [{#T}](../devops/concepts/maintenance-without-downtime.md)
