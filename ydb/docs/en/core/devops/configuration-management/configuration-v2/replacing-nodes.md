# Replacing a Static Node FQDN

{% include [_](../_includes/experimental_v2.md) %}

The fully qualified domain name (FQDN) of a [static node](../../../concepts/glossary.md#static-node) is specified by the `host` parameter in the [`hosts`](../../../reference/configuration/hosts.md) section. To change the FQDN without replacing the server or its disks, follow these steps.

Create a DNS record for the new FQDN that points to the same server. The new FQDN must be resolvable from every static cluster node. Keep the old DNS record until the procedure is complete. If the cluster uses TLS, prepare a [node certificate](../../deployment-options/manual/initial-deployment/deployment-preparation.md#tls-certificates) that contains the new FQDN.

1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. In `config.yaml`, change the `host` value of the target node without changing the order of entries in the `hosts` list.

    For example, for node `1`, replace `node-1.example.com` with `node-1-new.example.com`:

    ```yaml
    config:
      hosts:
      - host: node-1-new.example.com
        node_id: 1
    ```

1. Apply the updated configuration using the [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md) command:

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

1. Wait until the new FQDN appears in the local copy of `config.yaml` on the target node.

1. Make sure that the process can be [stopped safely](../../../maintenance/manual/node_restarting.md#restart_process). If the cluster uses TLS, install the prepared certificate.

    If the node starts with `--node static`, make sure that the new `host` value matches the output of either `hostname` or `hostname -f`. If neither command returns the new value, change the hostname:

    ```bash
    sudo hostnamectl set-hostname node-1-new.example.com
    ```

    Restart the static node process. For example, when using `systemd`:

    ```bash
    sudo systemctl restart ydbd-storage
    ```

    The service name may differ depending on the deployment method.

1. On the **Nodes** tab of the [cluster monitoring page](../../../reference/embedded-ui/ydb-monitoring.md#node_list_page), make sure the node has reconnected with its original ID and the new FQDN. You can then remove the old DNS record.
