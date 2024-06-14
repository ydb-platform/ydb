# {{ ydb-short-name }} Monitoring

{{ ydb-short-name }} Monitoring is a set of web pages that display the system health according to a range of different aspects. The pages contain lists of components and their current parameters. Many components have a health [color indicator](#colored_indicator) before the name.

## Home page {#main_page}

The page is available at:

```
http://<endpoint>:8765/monitoring/cluster
```

An example of the page layout is shown in the screenshot below.

![Monitoring_main_page](_assets/monitoring_main_page.png)

In the upper-right corner of the page, you can see details about the node that created the current page:

* host.
* node uptime.
* ydb version run by the node.

Below is the cluster summary:

* cluster name, list of running tablets.
* **Nodes**: The number of nodes.
* **Load**: The total CPU utilization level on all nodes.
* **Storage**: The used/total storage space.
* **Versions**: The list of {{ ydb-short-name }} versions run on the cluster nodes.

Next, you will find lists of [tenants](#tenant_list_page) and [nodes](#node_list_page) on the Tenants and Nodes tabs, respectively.

### Tenant list {#tenant_list_page}

{{ ydb-short-name }} is a multi-tenant DBMS that lets you execute isolated queries against databases of different users called tenants. Each database belongs to one tenant.

The tenants list contains the following information on each tenant:

* **Tenant**: Tenant path.
* **Name**: Tenant name.
* **Type**: Tenant type.
* **State**: Tenant health.
* **CPU**: CPU utilization by the tenant's nodes.
* **Memory**: RAM consumption by the tenant's nodes.
* **Storage**: Estimated amount of data stored by the tenant.
* **Pools usage**: CPU usage by the nodes broken down by the internal stream pools.
* **Tablets States**: Tablets running in the given tenant.

`Domain` type tenants serve the system components needed to ensure all tenants can operate. This includes all storage nodes and system tablets. `Dedicated` type tenants perform database maintenance.

If you click on the tenant's path, you can go to the [tenant page](#tenant_page).

### Node list {#node_list_page}

The list includes all nodes in the cluster. For each node, you can see:

* **#**: Node ID.
* **Host**: The host running the node.
* **Endpoints**: The ports being listened to.
* **Version**: The {{ ydb-short-name }} version being run.
* **Uptime**: The node uptime.
* **Memory used**: The amount of RAM used.
* **Memory limit**: The RAM utilization limit set via cgroup.
* **Pools usage**: CPU utilization broken down by the internal stream pools.
* **Load average**: The average CPU utilization on the host.

To open the [node page](#node_page), click the host name.

## Node page {#node_page}

The page is available at:

```
http://<endpoint>:8765/monitoring/node/<node-id>/
```

Information about the node is presented in the following sections:

* **Pools**: CPU utilization broken down by the internal stream pools, with roughly the following pool functions:

   * **System**: The tasks of critical system components.
   * **User**: User tasks, queries executed by tablets.
   * **Batch**: Long-running background tasks.
   * **IO**: Blocking I/O operations.
   * **IC**: Networking operations.

   High pool utilization might degrade performance and increase the system response time.

* **Common info**: Basic information about the node:

   * **Version**: The {{ ydb-short-name }} version.
   * **Uptime**: The node uptime.
   * **DC**: The availability zone where the node resides.
   * **Rack**: The ID of the rack where the node resides.

* **Load average**: Average host CPU utilization for different time intervals:

   * 1 minute.
   * 5 minutes.
   * 15 minutes.

The node page has the Storage and Tablets tabs with a [list of storage groups](#node_storage_page) and a [list of tablets](#node_tablets_page), respectively.

### List of storage groups on the node {#node_storage_page}

For storage nodes, the list includes storage groups that store data on this node's disk. For dynamic nodes, the list shows the storage groups of the tenant that the node belongs to.

Storage groups are grouped by storage pools. Each pool can be expanded into a list of groups.

For each group, the following information is provided:

* The ID of the storage group.
* The performance indicator of the group.
* The number of VDisks in the group.
* The data storage topology.

Each storage group can also be expanded into a list of VDisks, with the following information provided for each VDisk:

* VDiskID.
* The unique (within the node) PDiskID where the VDisk resides.
* The ID of the node where the VDisk resides.
* Free/available space on the block store volume.
* Path used to access block storage.

This list can be used to identify storage groups that were affected by disk or node failure.

### List of tablets residing on the node {#node_tablets_page}

Many {{ ydb-short-name }} components are implemented as tablets. The system can move tablets between nodes. A certain number of tablets can be run on any node.

In the upper part of the list of tablets, there's a big indicator for tablets running on the given node. It shows the ratio of fully launched and running tablets.

Under the indicator, you can see a list of tablets, where each tablet is shown as a small [color indicator](#colored_indicator) icon. When you hover over the indicator, the tablet summary is shown:

* **Tablet**: The ID of the tablet.
* **NodeID**: The ID of the node where the tablet resides.
* **State**: The state of the tablet.
* **Type**: The type of tablet.
* **Uptime**: The uptime since the tablet was last launched.
* **Generation**: The tablet's generation (the number of the current launch attempt).

## Tenant page {#tenant_page}

```
http://<endpoint>:8765/monitoring/tenant/healthcheck?name=<tenant-path>
```

Like the previous pages, this page includes the tenant summary, but unlike the other pages, this section is initially collapsed.

In the `Tenant Info` section, you can see the following information:

* **Pools**: Total CPU utilization by the tenant nodes broken down by internal stream pools (for more information about pools, see the [tenant page](#tenant_page)).

* **Metrics**: Data about tablet utilization for this tenant:
  * **Memory**: The RAM utilized by tablets.
  * **CPU**: CPU utilized by tablets.
  * **Storage**: The amount of data stored by tablets.
  * **Network**: The estimated amount of data transferred between nodes.
  * **Read throughput**: The read stream created by the tenant's tablets.
  * **Write throughput**: The write stream created by the tenant's tablets.

* **Tablets/running**: The number of running tablets.

The tenant page also includes the following tabs:

* **HealthCheck**: The report regarding cluster issues, if any.
* **Storage**: [List of storage groups](#tenant_storage_page) showing the nodes and block store volumes hosting each VDisk.
* **Compute**: [List of nodes](#tenant_compute_page) showing the nodes and tablets running on them.
* **Schema**: [Tenant schema](#tenant_scheme) that enables you to view tables, execute YQL queries, view a list of the slowest queries and the most loaded shards.
* **Network**: [Cluster network health](#tenant_network).

### List of storage groups in the tenant {#tenant_storage_page}

Similarly to the [list of groups in the node](#node_storage_page), this page shows a list of storage groups in the given tenant. Storage groups are grouped by storage pools in the list. Each pool can be expanded into a list of groups.

### List of nodes belonging to the tenant {#tenant_compute_page}

Here you can see a list of nodes belonging to the current tenant. If the tenant has the domain type, the list includes storage nodes.

Each node is represented by the following parameters:

* **#**: Node ID.
* **Host**: The host running the node.
* **Uptime**: The node uptime.
* **Endpoints**: The ports being listened to.
* **Version**: The {{ ydb-short-name }} version being run.
* **Pools usage**: CPU utilization broken down by the internal stream pools.
* **CPU**: CPU utilization by tablets.
* **Memory**: RAM consumption by tablets.
* **Network**: Network usage by tablets.
* **Tablets**: A list of tablets running on the node, grouped by type.

### Tenant schema {#tenant_scheme}

The left part of the page shows a diagram of tenant objects where you can select specific tables and view their details.

The right part includes the tabs:

* **Info**: Information about the table and its schema.
* **Preview**: A preview of the first items in the table.
* **Graph**: A range of built-in graphs with information about the table status.
* **Describe**: The result of running the describe command.
* **Query**: The form for executing YQL queries.
* **Tablets**: The list of tablets serving the table.
* **ACL**: The Access Control List.
* **Top queries**: The top most time-consuming queries to the table.
* **Top shards**: The top most loaded table shards.

### Network health {#tenant_network}

On the left side of the page, you can see the tenant's nodes as squares of [health indicators](#colored_indicator).

Whenever you select a node, the right side of the screen shows details about the health of the network connection between this node and other nodes.

Whenever you select the ID and Racks checkboxes, you can also see the IDs of nodes and their location in racks.

## Monitoring static groups {#static-group}

To perform a health check on a static group, go to the ![embedded-storage](../../_assets/embedded-storage.svg) **Storage** panel. By default, it shows a list of groups with issues.

Enter `static` in the search bar. If the result is empty, no issues have been found for the static group. But if the panel shows a **static** group, check the VDisk health status in it. Acceptable health indicators are green (no issues) and blue (VDisk replication is in progress). Red indicator signals of an issue. Hover over the indicator to get a text description of the issue.

## Health indicators {#colored_indicator}

To the left of a component name, you might see a color indicating its health status.

The indicator colors have the following meaning:

* **Green**: There are no problems, the component is working normally.
* **Blue**: Data replication is in progress, no other issues observed.
* **Yellow**: There might be problems, the component is still running.
* **Red**: There are critical problems, the component is down (or runs with limitations).

If a component includes other components, then in the absence of its own issues, the state is determined by aggregating the states of its parts.
