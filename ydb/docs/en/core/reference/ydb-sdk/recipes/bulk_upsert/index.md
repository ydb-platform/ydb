# Bulk-upserting data

{% include [work in progress message](../_includes/addition.md) %}

{{ ydb-short-name }} supports bulk-upserting data without atomicity guarantees. Writing is being performed in multiple independent parallel transactions, within a single partition each. For that reason, `BulkUpsert` is more efficient than `YQL`. When completed successfully, `BulkUpsert` guarantees that all data is written and committed. 

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools for bulk-upserting data:

{% list tabs %}

- Go


  {% include [go.md](_includes/go.md) %}

{% endlist %}
