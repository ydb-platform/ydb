Prepare a configuration file for {{ ydb-short-name }}:

1. Download a sample config for the appropriate failure model of your cluster:

   * [block-4-2](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/block-4-2.yaml): For a single-data center cluster.
   * [mirror-3dc](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/mirror-3dc-9-nodes.yaml): For a cross-data center cluster consisting of 9 nodes.
   * [mirror-3dc-3nodes](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/mirror-3dc-3-nodes.yaml): For a cross-data center cluster consisting of 3 nodes.

1. In the `host_configs` section, specify all disks and their types on each cluster node. Possible disk types:
   * ROT: Rotational, HDD.
   * SSD: SSD or NVMe.

   ```json
   host_configs:
   - drive:
     - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
       type: SSD
     host_config_id: 1
   ```

1. In the `hosts` section, specify the FQDN of each node, their configuration and location in a `data_center` or `rack`:

   ```json
   hosts:
   - host: node1.ydb.tech
     host_config_id: 1
     walle_location:
       body: 1
       data_center: 'zone-a'
       rack: '1'
   - host: node2.ydb.tech
     host_config_id: 1
     walle_location:
       body: 2
       data_center: 'zone-b'
       rack: '1'
   - host: node3.ydb.tech
     host_config_id: 1
     walle_location:
       body: 3
       data_center: 'zone-c'
       rack: '1'
   ```

1. Under `blob_storage_config`, edit the FQDNs of all the nodes accommodating your static storage group:

   * For the `mirror-3-dc` scheme, specify FQDNs for nine nodes.
   * For the `block-4-2` scheme, specify FQDNs for eight nodes.

1. Enable user authentication (optional).

   If you plan to use authentication and user access differentiation features in the {{ ydb-short-name }} cluster, add the following parameters to the `domains_config` section:

   ```json
   domains_config:
     security_config:
       enforce_user_token_requirement: true
       monitoring_allowed_sids:
       - "root"
       - "ADMINS"
       - "DATABASE-ADMINS"
       administration_allowed_sids:
       - "root"
       - "ADMINS"
       - "DATABASE-ADMINS"
       viewer_allowed_sids:
       - "root"
       - "ADMINS"
       - "DATABASE-ADMINS"
   ```
