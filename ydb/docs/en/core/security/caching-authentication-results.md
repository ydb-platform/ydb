# Caching authentication results

During authentication, a user session receives an [authentication token](../concepts/glossary.md#auth-token), which is sent with every request to the {{ ydb-short-name }} cluster. Because {{ ydb-short-name }} is a distributed system, user requests are eventually processed by one or more [{{ ydb-short-name }} nodes](../concepts/glossary.md#node). When a node receives a request, it [validates the authentication token](./authentication.md#token-validation) and, if validation succeeds, creates a **user token**.

A user token contains the user identifier and group list, is valid **only within the current {{ ydb-short-name }} node**, and is used to authorize operations. The node stores the user token in its cache. The cache key includes the authentication token and validation context, such as the database and requested permissions. For subsequent requests with the same data, the node retrieves the ready user token from its cache. The client never receives the user token and continues to send the authentication token with every request.

{% include [Creating and using a user token](_assets/user-token.md) %}

User-token lifetime and other key aspects are configured in the [`auth_config` section of the {{ ydb-short-name }} configuration](../reference/configuration/auth_config.md#caching-auth-results).

## User-token lifetime

A user token is removed from the cache when either of the following conditions is met:

- The user token has not been used for `auth_config.life_time`. Each request with the corresponding authentication token updates the last-access time. The default value is `1h`.
- The cache entry has expired. For most authentication-token types, the lifetime is configured by `auth_config.expire_time`; the default value is `24h`. After a successful refresh, the countdown starts again. Tokens issued through login-and-password authentication and external identity provider tokens use the authentication token's own expiration time. Requests signed with an access key use the separate `auth_config.as_signature_expire_time` parameter.

Removal occurs while the token queue is processed periodically. The interval between processing runs is configured by `auth_config.refresh_period`, so a token is not necessarily removed at the exact moment a condition is met.

{% note warning %}

If an application regularly sends requests to the same node, the user token is not removed based on `life_time`. When a refresh fails with a temporary error, the node continues to use the previously created user token, including the group list stored in it. For AccessService, each temporary error postpones cache-entry removal: by `expire_time` for regular tokens and by `as_signature_expire_time` for requests signed with an access key. Retries can therefore continue until a refresh succeeds, a permanent error occurs, or requests stop for `life_time`. For tokens issued through login-and-password authentication and external identity provider tokens, retries are additionally limited by authentication-token expiration.

Until the next successful refresh, the cached group list may remain stale, so account changes can take effect on a particular node with a delay. Account changes in this context include deleting a local or external user and changing the user's membership in [user groups](./authorization.md#group).

[Access rights](../concepts/glossary.md#access-right) in {{ ydb-short-name }} are associated with an [access object](../concepts/glossary.md#access-object) and stored in [ACLs](../concepts/glossary.md#access-control-list) (Access Control Lists) for both users and groups. ACLs themselves are not cached: ACL changes take effect during the next access check. The user token caches the group list.

Suppose the [ACL](../concepts/glossary.md#access-control-list) of table `Tbl1` grants write access to group `Grp1`. After a user is removed from this group, the user token created for that user on a particular node may continue to contain `Grp1` until a successful refresh. Conversely, a user added to the group receives the corresponding permission on that node after a successful user-token refresh.

{% endnote %}

{% include [User-token lifecycle](_assets/user-token-lifecycle.md) %}

## Refreshing user tokens {#refreshing-user-tokens}

For authentication-token types that support refresh, the node periodically repeats validation and replaces the user token in the cache. Depending on the authentication method, refresh uses the local {{ ydb-short-name }} state or requires a call to an external system. A successful refresh updates the user identifier and group list.

The `auth_config.refresh_time` parameter sets the base refresh interval; the default value is `1h`. After successful validation, the next refresh is scheduled at a random point between `refresh_time/2` and `refresh_time`.

After a temporary error, the node retries the operation. The initial base delay is configured by `min_error_refresh_time`; after each error, it increases subject to `max_error_refresh_time`. The actual delay before each attempt is selected randomly between half and all of the current base delay. These parameters limit the delay between attempts but not the total retry duration.

For AccessService, retries may continue until a refresh succeeds, a permanent error occurs, or the cache entry is removed after receiving no requests for `life_time`. For tokens issued through login-and-password authentication and external identity provider tokens, retries also stop when the authentication token expires.

{% note info %}

Ticket Parser token-processing errors, including errors during initial validation and subsequent refresh, are available in the [metrics](../reference/observability/metrics/index.md) `auth.TicketParser.TicketsErrors`, `auth.TicketParser.TicketsErrorsPermanent`, and `auth.TicketParser.TicketsErrorsRetryable`, or in the `Error` field on **Developer UI** > **Actors** > **Ticket Parser** (`https://<ydb-server-address>:<embedded-ui-port>/actors/ticket_parser`).

Errors may be caused by the following issues:

- The {{ ydb-short-name }} node cannot reach the external authentication system because of network problems.
- The external authentication system is unavailable.
- The {{ ydb-short-name }} node is heavily overloaded, although this is the least likely cause.

{% endnote %}
