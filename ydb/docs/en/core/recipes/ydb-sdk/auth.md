---
title: "Instructions for authenticating when connecting to the server side in {{ ydb-short-name }}"
description: "The section contains code recipes with authentication settings in different {{ ydb-short-name }} SDKs."
---

# Authentication

{% include [work in progress message](_includes/addition.md) %}

{{ ydb-short-name }} supports multiple authentication methods when connecting to the server side. Each of them is usually specific to a particular pair of environments, that is, depends on where you run your client application (in the trusted {{ ydb-short-name }} zone or outside it) and the {{ ydb-short-name }} server part (in a Docker container, {{ yandex-cloud }}, data cloud, or an independent cluster).

This section contains code recipes with authentication settings in different {{ ydb-short-name }} SDKs. For a general description of the SDK authentication principles, see the [Authentication in an SDK](auth.md).

Table of contents:
- [Using a token](auth-access-token.md)
- [Anonymous](auth-anonymous.md)
- [Service account file](auth-service-account.md)
- [Metadata service](auth-metadata.md)
- [Using environment variables](auth-env.md)
- [Username and password based](auth-static.md)
