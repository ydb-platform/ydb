# Hardware issues

Malfunctioning memory modules and network cards, until replaced, significantly impact database performance up to total unavailability of the affected server.

## Diagnostics

Use the hardware monitoring tools that your data center provides to diagnose hardware issues.

You can also use the **Healthcheck** in [Embedded UI](../../../../reference/embedded-ui/index.md) to diagnose some hardware issues:

- **Storage issues**

    On the **Storage** tab, select the **Degraded** filter to list storage groups or nodes that contain degraded or failed storage.

- **Network issues**

    <!-- The include is added to allow partial overrides in overlays  -->
    {% include notitle [network issues](./_includes/network.md) %}

- **Availability of nodes on racks**

    On the **Nodes** tab, see if nodes on specific racks are not available. Analyze the health indicators in the **Host** and **Rack** columns.

## Recommendations

Contact the support team of your data center.
