# Authentication

{% include [work in progress message](../../_includes/addition.md) %}

{{ ydb-short-name }} supports multiple authentication methods when connecting to the server side. Each of them is usually specific to a particular environment pair: where the client application is located (in the trusted {{ ydb-short-name }} zone or outside it) and the {{ ydb-short-name }} server side (a Docker container, {{ yandex-cloud }}, data cloud, or deployment on a separate cluster).

This section contains code recipes with authentication settings in different {{ ydb-short-name }} SDKs. For a general description of the SDK authentication principles, see the [Authentication in an SDK](../../../auth.md) article.

