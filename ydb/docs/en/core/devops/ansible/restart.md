# Restarting {{ ydb-short-name }} clusters deployed with Ansible

{{ ydb-short-name }} clusters provide strong availability guarantees; thus, the cluster's fault tolerance model needs to be considered during any maintenance, including cluster restarts. There are two kinds of nodes that might need to be restarted:

* Database nodes (also known as dynamic) are stateless; thus, the primary consideration is having enough of them running to handle each database's load. A basic rolling restart with a little delay is usually sufficient for dynamic nodes.
* Storage nodes (also known as static) are stateful and responsible for safely persisting data. Thus, they require special handling to ensure data availability. Each {{ ydb-short-name }} cluster has a dedicated component that keeps track of all outages and maintenance and can tell if it is currently safe to stop or restart a particular node. Thus, asking for its permission for each operation is essential, and a complete restart of storage nodes often takes a while.

## Restart via Ansible playbook

[ydb-ansible](https://github.com/ydb-platform/ydb-ansible) repository contains a playbook called `ydb_platform.ydb.restart` that can be used to restart a {{ ydb-short-name }} cluster. Run it from the same directory used for the [initial deployment](initial-deployment.md).

### Restart all nodes

By default, the `ydb_platform.ydb.restart` restarts all cluster nodes. Static nodes go first, then dynamic nodes. The command to run it:

```bash
ansible-playbook ydb_platform.ydb.restart
```

### Filter by node type

Tasks in the `ydb_platform.ydb.restart` playbook are tagged with node types, so you can use Ansible's tags functionality to filter nodes by their kind. 

These two commands are equivalent and will restart all storage nodes:

```bash
ansible-playbook ydb_platform.ydb.restart --tags storage
ansible-playbook ydb_platform.ydb.restart --tags static
```

These two commands are equivalent and will restart all database nodes:
```bash
ansible-playbook ydb_platform.ydb.restart --tags database
ansible-playbook ydb_platform.ydb.restart --tags dynamic
```

### Filter by hostname 

To restart a specific host or subset of hosts, use the `--limit` argument:

```bash
ansible-playbook ydb_platform.ydb.restart --limit='<hostname>'
ansible-playbook ydb_platform.ydb.restart --limit='<hostname-1,hosntname-2>'
```

It can be used together with tags, too:
```bash
ansible-playbook ydb_platform.ydb.restart --tags database --limit='<hostname>'
```

## Restart nodes manually

The [ydbops](https://github.com/ydb-platform/ydbops) tool properly implements various {{ ydb-short-name }}cluster manipulations, including restarts. The `ydb_platform.ydb.restart` playbook explained above uses it behind the scenes, but it can be used manually, too.

There are more guidelines and information on how this works in the [{#T}](../manual/maintenance-without-downtime.md) article.