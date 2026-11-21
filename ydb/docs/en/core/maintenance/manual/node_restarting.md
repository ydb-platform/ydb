# Safe restart and shutdown of nodes

## Stopping/restarting a {{ ydb-short-name }} process on a node {#restart_process}

To make sure that the process is stoppable, follow these steps.

1. Access the node via SSH.

1. Execute the command

   ```bash
   ydbd cms request restart host {node_id} --user {user} --duration 60 --dry --reason 'some-reason'
   ```

   If the process is stoppable, you'll see `ALLOW`.

1. Stop the process

   ```bash
   sudo service ydbd stop
   ```

1. Restart the process if needed

   ```bash
    sudo service ydbd start
   ```

## Replacing equipment {#replace-hardware}

Before replacing equipment, make sure that the {{ ydb-short-name }} process is [stoppable](#restart_process).
If the replacement is going to take a long time, first move all the VDisks from this node and wait until replication is complete.
After replication is complete, you can safely shut down the node.

To relocate VDisks through CMS, use a request with `EvictVDisks` enabled, which is set by the `--evict-vdisks` flag in the `ydbd cms request` command. The [`MAINTENANCE`](../../devops/concepts/selfheal-storage.md#planned-maintenance) mode performs this relocation while keeping healthy PDisks `ACTIVE`. For configuration details, see [`cms_config`](../../reference/configuration/cms_config.md#evict-vdisks-status). Do not shut down the host until CMS grants permission.

To make sure that disabling the dynamic node doesn't affect query handling, drain the tablets from this node first.

Go to the [Hive web-viewer](../../reference/ydb-ui/hive.md) page.
Click "View Nodes" to see a list of all nodes.

Before disabling the node, first disable the transfer of tablets through the Active button, then click Drain, and wait for all the tablets to be moved away.
