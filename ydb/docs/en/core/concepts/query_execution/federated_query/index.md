# Federated queries

Federated queries allow you to retrieve information from various data sources without the need to transfer data from those sources into {{ ydb-full-name }}. Using YQL queries, you can access external databases without duplicating data between systems.

To work with data stored in external DBMSs, simply create an [external data source](../../datamodel/external_data_source.md). To work with unschematized data stored in S3 buckets, you additionally need to create an [external table](../../datamodel/external_table.md). In both cases, you must first create [secrets](../../datamodel/secrets.md) that store confidential data required for authentication in external systems.

You can learn about the internal structure of the federated query processing system in the section on [architecture](./architecture.md). Detailed information on working with various data sources is provided in the respective sections:

{% include [!](_includes/supported_eds.md) %}

{% cut "Experimental sources data" %}

{% include [!](_includes/experimental_eds.md) %}

{% endcut %}
