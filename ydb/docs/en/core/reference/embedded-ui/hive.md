# Hive web-viewer

The Hive web-viewer provides an interface for working with Hive.
Hive can be shared by a cluster or be tenant.
You can go to the Hive web-viewer from the {{ ydb-short-name }} Monitoring.

## Home page

The home page provides information about the distribution and usage of resources by tablets on each node in the form of a table.

The table is preceded by the following brief information:

* **Tenant**: The tenant that Hive is responsible for.
* **Tablets**: The percentage of tablets started and the number of running tablets to their total number.
* **Boot Queue**: The number of tablets in the boot queue.
* **Wait Queue**: The number of tablets that can't be started.
* **Resource Total**: Resource utilization by tablets (cpu, net).
* **Resource StDev**: Standard deviation of resource utilization (cnt, cpu, mem, net).

Then there is a table where each row represents a node managed by Hive. It has the following columns:

* **Node**: The node number.
* **Name**: The node FQDN and ic-port.
* **DC**: The datacenter where the node resides.
* **Domain**: The node tenant.
* **Uptime**: The node uptime.
* **Unknown**: The number of tablets whose state is unknown.
* **Starting**: The number of tablets being started.
* **Running**: The number of tablets running.
* **Types**: Tablet distribution by type.
* **Usage**: A normalized dominant resource.
* **Resources** :
  * **cnt**: The number of tablets that are not using any resources.
  * **cpu**: CPU usage by tablets.
  * **mem**: RAM usage by tablets.
  * **net**: Bandwidth usage by tablets.
* **Active**: Enables/disables the node to move tablets to this node.
* **Freeze**: Disables tablets to be deployed on other nodes.
* **Kick**: Moves all tablets from the node at once.
* **Drain**: Smoothly moves all tablets from the node.

Additional pages are presented below the table:

* **Bad tablets**: A list of tablets having issues or errors.
* **Heavy tablets**: A list of tablets utilizing a lot of resources.
* **Waiting tablets**: A list of tablets that can't be started.
* **Resources**: Resource utilization by each tablet.
* **Tenants**: A list of tenants indicating their local Hive tablets.
* **Nodes**: A list of nodes.
* **Storage**: A list of storage group pools.
* **Groups**: A list of storage groups for each tablet.
* **Settings**: The Hive configuration page.
* **Reassign Groups**: The page for reassigning storage groups across tablets.

You can also view what tablets use a particular group and, vice versa, what groups are used on a particular tablet.

## Reassign Groups {#reassign_groups}

Click **Reassign Groups** to open the window with parameters for balancing:

* **Storage pool**: Pool of storage groups for balancing.
* **Storage group**: If the previous item is not specified, you can specify only one group separately.
* **Type**: Type of tablets that balancing will be performed for.
* **Channels**: Range of channels that balancing will be performed for.
* **Percent**: Percentage of the total number of tablet channels that will be moved as a result of balancing.
* **Inflight**: The number of tablets being moved to other groups at the same time.

After specifying all the parameters, click "Query" to get the number of channels moved and unlock the "Reassign" button.
Clicking this button starts reassigning.

