# Deployment of a single-node {{ ydb-short-name }} cluster 

This section contains articles describing simple scenarios to deploy a local single-node YDB cluster running a single database. Such database can be used for development and functional testing purposes.

{% include [simple_options](simple_options.md) %}

Single-node deployments of YDB are not recommended for performance testing, as YDB architecture is designed for horizontal scalability, with relevant impact to applied algorithms and computing overhead. Deployment of a mutli-node cluster is described in the [Cluster management](../../../deploy/index.md) section.

