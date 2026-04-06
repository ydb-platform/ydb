There are several options to specify which {{ ydb-short-name }} executables you want to use for the cluster:

- `ydb_version`: automatically download one of the [official {{ ydb-short-name }} releases](../../../../../downloads/index.md#ydb-server) by version number. For example, `23.4.11`.
- `ydb_archive`: a local filesystem path to a {{ ydb-short-name }} distribution archive [downloaded](../../../../../downloads/index.md#ydb-server) or otherwise prepared in advance.

Installing a [connector](../../../../../concepts/federated_query/architecture.md#connectors) may be required for using [federated queries](../../../../../concepts/federated_query/index.md). The playbook can deploy [fq-connector-go](../../../manual/federated-queries/connector-deployment.md#fq-connector-go) on hosts with dynamic nodes. Use the following settings:

- `ydb_install_fq_connector` — set to `true` to install the connector.
- Choose one of the available options for deploying fq-connector-go executables:
  - `ydb_fq_connector_version`: automatically download one of the [fq-connector-go official releases](https://github.com/ydb-platform/fq-connector-go/releases) by version number. For example, `v0.7.1`.
  - `ydb_fq_connector_git_version`: automatically compile the fq-connector-go executable from source code downloaded from the [official GitHub repository](https://github.com/ydb-platform/fq-connector-go). The setting value is a branch, tag, or commit name. For example, `main`.
  - `ydb_fq_connector_archive`: a local filesystem path to a fq-connector-go distribution archive [downloaded](https://github.com/ydb-platform/fq-connector-go/releases) or otherwise prepared in advance.
  - `ydb_fq_connector_binary`: local filesystem paths to the fq-connector-go executable, [downloaded](https://github.com/ydb-platform/fq-connector-go/releases) or otherwise prepared in advance.

- `ydb_tls_dir` — specify a local path to a folder with TLS certificates prepared in advance. It must contain the `ca.crt` file and subdirectories whose names match the node hostnames, each containing certificates for that node. If not specified, self-signed TLS certificates will be generated automatically for the entire {{ ydb-short-name }} cluster.
- `ydb_brokers` — list the FQDNs of the broker nodes. For example:

  ```yaml
  ydb_brokers:
      - static-node-1.ydb-cluster.com
      - static-node-2.ydb-cluster.com
      - static-node-3.ydb-cluster.com
  ```

The optimal value for the `ydb_database_groups` setting in the `vars` section depends on the available disks. Assuming only one database in the cluster, use the following logic:

- For production deployments, use disks with capacity over 800 GB and high IOPS, then choose the value for this setting based on cluster topology:
  - For `block-4-2`, set `ydb_database_groups` to 95% of the total number of disks, rounded down.
  - For `mirror-3-dc`, set `ydb_database_groups` to 84% of the total number of disks, rounded down.
- For testing {{ ydb-short-name }} on small disks, set `ydb_database_groups` to 1 regardless of cluster topology.

The values of the `system_timezone` and `system_ntp_servers` variables depend on the infrastructure where the {{ ydb-short-name }} cluster is deployed. By default, `system_ntp_servers` includes a set of NTP servers without regard to the geographic location of the infrastructure. We strongly recommend using a local NTP server for on-premise infrastructure and the following NTP servers for cloud providers:

{% list tabs %}

- AWS

  - `system_timezone`: USA/<region_name>
  - `system_ntp_servers`: [169.254.169.123, time.aws.com] [Learn more](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#configure-time-sync) about AWS NTP server settings.

- Azure

  - You can read about how time synchronization is configured on Azure virtual machines in [this](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/time-sync) article.

- Alibaba

  - The specifics of connecting to NTP servers in Alibaba are described in [this article](https://www.alibabacloud.com/help/en/ecs/user-guide/alibaba-cloud-ntp-server).

- Yandex Cloud

  - `system_timezone`: Europe/Moscow
  - `system_ntp_servers`: [0.ru.pool.ntp.org, 1.ru.pool.ntp.org, ntp0.NL.net, ntp2.vniiftri.ru, ntp.ix.ru, ntps1-1.cs.tu-berlin.de] [Learn more](https://yandex.cloud/en/docs/tutorials/infrastructure-management/ntp) about Yandex Cloud NTP server settings.

{% endlist %}
