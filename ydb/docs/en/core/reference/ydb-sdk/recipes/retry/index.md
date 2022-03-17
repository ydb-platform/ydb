# Retrying

{% include [work in progress message](../_includes/addition.md) %}

{{ ydb-short-name }} is a distributed database management system with automatic load scaling.
Routine maintenance can be carried out on the server side, with server racks or entire data centers temporarily shut down.
This may result in errors arising from {{ ydb-short-name }} operation.
There are different response scenarios depending on the error type.
{{ ydb-short-name }} To ensure high database availability, SDKs provide built-in tools for retries, accounting for error types and responses to them.

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools for retries:

{% list tabs %}

- Go

  {% include [go.md](_includes/go.md) %}

{% endlist %}
