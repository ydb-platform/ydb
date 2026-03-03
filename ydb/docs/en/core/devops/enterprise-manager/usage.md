# Getting Started {#usage}

After a successful deployment of {{ ydb-short-name }} Enterprise Manager, you can start using the system through its web interface.

## Accessing the Web Interface {#web-ui}

Open the following address in your browser:

```
https://<FQDN_of_host_from_ydb_em_group>:8789/ui/clusters
```

Where `<FQDN_of_host_from_ydb_em_group>` is the fully qualified domain name of any host from the `ydb_em` group in the Ansible inventory.

{% note tip %}

For example, if the `ydb_em` group contains the host `ydb-node01.ru-central1.internal`, the web interface address will be:

```
https://ydb-node01.ru-central1.internal:8789/ui/clusters
```

{% endnote %}

<!-- TODO: Clarify the default port for Gateway (8789) and whether it can be changed -->
<!-- TODO: Describe the initial authentication process in the web interface -->

## Web Interface Features {#web-ui-features}

<!-- TODO: Describe the main sections and features of the web interface -->
<!-- TODO: Add screenshots of the interface -->
<!-- TODO: Describe cluster management -->
<!-- TODO: Describe database management -->
<!-- TODO: Describe resource and dynamic slot management -->
<!-- TODO: Describe monitoring and observability -->

## API {#api}

<!-- TODO: Describe available API endpoints -->
<!-- TODO: Describe request and response formats -->
<!-- TODO: Provide examples of API usage -->
