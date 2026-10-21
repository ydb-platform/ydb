{% note warning %}

External connectors are an experimental feature of {{ ydb-full-name }}. To work with external DBMS through connectors, you need to deploy [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) and explicitly enable the corresponding sources in the cluster configuration {{ ydb-short-name }}. The functionality may change, so use in production is not recommended without thorough testing.

{% endnote %}
