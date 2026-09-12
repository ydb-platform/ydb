# Caching authentication results

During authentication, a user session receives an [authentication token](../concepts/glossary.md#auth-token), which is passed with each request to the {{ ydb-short-name }} cluster. Since {{ ydb-short-name }} is a distributed system, requests are processed on different [nodes](../concepts/glossary.md#node). Upon receiving a request, each node independently [checks the authentication token](./authentication.md#token-validation) and, if the check succeeds, creates a [user token](../concepts/glossary.md#user-token).

The authentication token and the user token are different entities. The authentication token is passed by the client. The user token is created inside the node based on the check results, contains the [SID](../concepts/glossary.md#access-sid) of the user and groups, and is used for [authorization](./authorization.md). It is stored only on a specific node and is not passed to the client.

The node stores the user token in the cache. The record key includes the authentication token and the verification context, such as the database, attributes, and requested [access rights](../concepts/glossary.md#access-right). A subsequent request uses the cached record only if the key matches.

{% include [Creating and using token user](_assets/user-token.md) %}

The user token lifetime and other caching aspects are configured in the [section `auth_config` of the {{ ydb-short-name }} configuration](../reference/configuration/auth_config.md#caching-auth-results).

## Cache record lifetime

A record with a user token is removed from the cache if one of the following conditions is met:

- The record has not been used for `auth_config.life_time`. Only a request that matches the cache key (the authentication token and verification context) updates the record's last use time.
- The record has expired. For login and password entry tokens and external identity provider tokens, the record lifetime is determined by the authentication token's expiration. For requests signed with an access key, `auth_config.as_signature_expire_time` is used; for other types, `auth_config.expire_time` is used.

An error is considered retryable if the check can be repeated without changing the request. The type of error is determined by the corresponding authentication subsystem.

{% note warning %}

On a retryable update error, the node continues to use the stored user token. Therefore, account deletion or a change in the user's membership in [groups](./authorization.md#group) may take effect on a specific node only after a successful update or removal of the record from the cache.

[ACL](../concepts/glossary.md#access-control-list) are not cached in the user token, so ACL changes are applied at the next permission check.

{% endnote %}

{% include [Life cycle record in cache](_assets/user-token-lifecycle.md) %}

## Updating user tokens {#refreshing-user-tokens}

For refreshable authentication tokens, the node periodically re-validates and replaces the user's token in the cache. Depending on the authentication method, validation is performed locally or requires contacting an external system. After successful validation, the user's SID and group SIDs are updated.

The `auth_config.refresh_time` parameter sets the maximum interval before the next validation. The exact moment is chosen in the range from `refresh_time/2` to `refresh_time`.

After a retryable error, retries continue until a successful update, a permanent error, or removal of the entry from the cache. The intervals between attempts are set by the `min_error_refresh_time` and `max_error_refresh_time` parameters. For login/password tokens and external identity provider tokens, attempts also stop after the authentication token expires.

Possible causes of errors when validating and updating cached results include, but are not limited to:

- Network errors and timeouts when contacting the external authentication system.
- Unavailability or overload of the external authentication system.
- Unavailability of the local security state required for validation.
- Overload of the {{ ydb-short-name }} node or request processing timeout.
