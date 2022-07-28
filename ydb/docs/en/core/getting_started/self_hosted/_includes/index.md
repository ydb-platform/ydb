# Deploying a single-node {{ ydb-short-name }} cluster

This section contains articles describing simple scenarios for deploying a single-node YDB cluster that serves a single database on a local machine. This database can be used for development or functional testing.

{% include [simple_options](simple_options.md) %}

We do not recommend using single-node configurations for performance benchmarking because the YDB architecture is designed to run in clusters with a relevant impact on applied algorithms and overhead costs. Deployment of multi-node clusters is described in the [Cluster management](../../../deploy/index.md) section.

Before starting, see the [system requirements and recommendations](../../../cluster/system-requirements.md).
