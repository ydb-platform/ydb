If any other value is used, the default value will be taken - `ldap`.

Default value: `ldap`
    ||
|| `requested_group_attribute`
| Group reverse membership attribute. By default `memberOf`
    ||
|| `extended_settings.enable_nested_groups_search`
| Flag determines whether a request will be executed to obtain the entire tree of groups that include the user's immediate groups.

Possible values:

- `true` — {{ ydb-short-name }} requests information about all groups that include the user's immediate groups. Queries about all parent groups can take a long time.
- `false` — {{ ydb-short-name }} requests a flat list of the user's groups. Such a request does not obtain information about possible nested parent groups.

Default value: `false`
    ||
|| `extended_settings.enable_sasl_external_bind`
| Flag determines whether [service account authentication](../../security/authentication.md#ldap-service-account-auth) will be performed using the SASL protocol with the EXTERNAL mechanism.

Possible values:

- `true` - For service account authentication, the SASL protocol with the EXTERNAL mechanism will be used (authentication via client TLS certificate within mTLS). The client certificate specified in the parameters `use_tls.cert_file` and `use_tls.key_file` is used as authentication information. The parameters `bind_dn` and `bind_password` are not set in this case.
- `false` - For service account authentication, the simple bind method will be used. The parameters `bind_dn` and `bind_password` must be specified.

Default value: `false`
    ||
|| `host`
| Hostname where the LDAP server runs. This is a deprecated parameter; instead, the parameter `hosts`
     should be used||
|| `ldap_authentication_domain`
| Username suffix that allows distinguishing users from the LDAP directory from users authenticated by other providers.

Default value: `ldap`
    ||
|#

## Client certificate authentication configuration {#certificate-auth-config}

{{ ydb-short-name }} supports [client certificate authentication](../../security/authentication.md#client-certificate). Certificate verification rules are set in the [client_certificate_authorization](client_certificate_authorization.md) section. Additionally, in the `auth_config` section, a suffix for usernames of users authenticated by certificate can be specified:

#|
|| Parameter | Description ||
|| `certificate_authentication_domain`
| Username suffix that allows distinguishing users authenticated by client certificate from users authenticated by other methods.

Default value: `cert` (i.e., the default SID suffix is `@cert`)
    ||
|#

## Authentication configuration using an external IdP {#external-idp-auth-config}

{{ ydb-short-name }} supports [authentication via JWT tokens of an external identity provider using OpenID Connect](../../security/authentication.md#external-idp). To enable authentication, add the `external_idp_config` section to `auth_config`.

Configuration example:

```yaml
auth_config:
  #...
  external_idp_config:
    issuer: "https://idp.example.com"
    audience: "ydb-cluster"
    allowed_clock_skew: "30s"
    subject_claim_name: "username"
    groups_claim_name: "groups"
    discovery_periodic_settings:
      success_refresh_period: "1h"
      min_error_refresh_period: "1s"
      max_error_refresh_period: "5m"
      request_timeout: "15s"
    jwks_periodic_settings:
      success_refresh_period: "30m"
      min_error_refresh_period: "1s"
      max_error_refresh_period: "10s"
      request_timeout: "15s"
    jwks_cache_settings:
      timeout: "2h"
  external_idp_authentication_domain: "sso"
  use_access_service: false
  #...
```

#|
|| Parameter | Description ||
|| `external_idp_config.issuer`
| Expected value of the `iss` (issuer) field, which identifies the issuer of the JWT token, and the base URL for OIDC Discovery. Required parameter; must start with `https://` and must not end with the character `/`. The values `issuer` in the Discovery document and `iss` in the JWT must exactly match the specified value.

Default value: empty string
    ||
|| `external_idp_config.audience`
| Expected value of the `aud` (audience) field, which identifies the recipient of the JWT token. If the parameter is not set, the token recipient is not checked.

Default value: empty string
    ||
|| `external_idp_config.allowed_clock_skew`
| Allowed clock skew when checking time-related JWT fields: `exp` sets the token expiration time, `nbf` — the time when it becomes valid, `iat` — the issue time.

Default value: `30s`
    ||
|| `external_idp_config.subject_claim_name`
| Name of the string JWT field from which the user SID is formed. If the field is missing or has a different type, the field `sub` is used.

Default value: `sub`
    ||
|| `external_idp_config.groups_claim_name`
| Name of the JWT field with an array of user groups. Only string elements are extracted from the array.

Default value: `groups`
    ||
|| `external_idp_config.discovery_periodic_settings.success_refresh_period`
| Period for refreshing the Discovery document after a successful request.

Default value: `1h`
    ||
|| `external_idp_config.discovery_periodic_settings.min_error_refresh_period`
| Minimum interval before re-requesting the Discovery document after an error.

Default value: `1s`
    ||
|| `external_idp_config.discovery_periodic_settings.max_error_refresh_period`
| Maximum interval before re-requesting the Discovery document after an error.

Default value: `5m`
    ||
|| `external_idp_config.discovery_periodic_settings.request_timeout`
| Timeout for the Discovery document request.

Default value: `15s`
    ||
|| `external_idp_config.jwks_periodic_settings.success_refresh_period`
| Period for refreshing JWKS after a successful request.



The parameter applies to refreshable authentication tokens, such as login-password tokens and tokens from an external identity provider.

Default value: `1h`
    ||
|| `life_time`
| The period for storing a user token in the node cache {{ ydb-short-name }} since its last use. If no requests from the user for whom the token was created have reached the node {{ ydb-short-name }} within the specified period, the node removes that user token from its cache.

Default value: `1h`
    ||
|| `expire_time`
| The validity period of a successful verification result for most types of authentication tokens. After a successful refresh, the countdown restarts. After the period expires, the entry is removed from the cache regardless of `life_time`.

For login-password tokens and tokens from an external identity provider, the expiration time of the authentication token itself is used. For requests signed with an access key, a separate parameter `as_signature_expire_time` is used.

Default value: `24h`
    ||
|| `as_signature_expire_time`
| The validity period of the verification result for a request authenticated using an access key signature.

Default value: `1m`
    ||
|| `min_error_refresh_time`
| The initial interval between retries after a retryable error in refreshing a user token.

After a retryable error, the first retry is performed immediately. If it also ends with a retryable error, the delay before the next retry is chosen randomly in the range from `min_error_refresh_time/2` to `min_error_refresh_time`. After each subsequent retryable error, the current interval doubles, but does not exceed the difference `max_error_refresh_time - min_error_refresh_time`. For each current interval `D`, the actual delay is chosen randomly in the range from `D/2` to `D`.

{% note warning %}

It is not recommended to set the parameter value to `0`, as immediate retries create excessive load.

{% endnote %}

Default value: `1s`
    ||
|| `max_error_refresh_time`
| Limits the increase of the interval between retries after retryable errors in refreshing a user token. Does not limit the total duration of retries.

Default value: `1m`
    ||
|#

Example for login-password authentication:

```yaml
auth_config:
  refresh_period: "1s"
  refresh_time: "1h"
  life_time: "2h"
  expire_time: "6h"
  login_token_expire_time: "12h"
  min_error_refresh_time: "1s"
  max_error_refresh_time: "1m"
```

After a user token is created, the next scheduled check is performed after an interval randomly chosen in the range from `30m` to `1h` (`refresh_time`). This condition is checked every `1s` (`refresh_period`). After a successful check, the interval is chosen anew. If a retryable error occurs, the first retry is performed immediately. If it also ends with a retryable error, the next check is performed after an interval randomly chosen in the range from `500ms` to `1s` (`min_error_refresh_time`). Then the current interval doubles, but does not exceed `59s` (`max_error_refresh_time - min_error_refresh_time`). The actual delay is each time chosen randomly in the range from half to the full current interval.

For an authentication token obtained via login and password, the parameter `expire_time` does not apply: the expiration time of such a token is set by `login_token_expire_time`, so it becomes invalid after `12h`. The cached entry stops being used at the nearest check after this condition, but may be removed earlier due to lack of requests for `2h` (`life_time`) or a permanent error.

Example for requests signed with an access key:

```yaml
auth_config:
  refresh_period: "1s"
  life_time: "30m"
  as_signature_expire_time: "1h"
```

The user token for a request signed with an access key is not refreshed on a schedule. The entry is considered valid for `1h` (`as_signature_expire_time`) from the moment of successful verification and is removed at the nearest cache check. With the specified values, it will be removed no later than after `1h + 1s` (`as_signature_expire_time + refresh_period`). The entry may be removed earlier due to lack of requests for `30m` (`life_time`).

## The node registration token configuration {#node-registration-token}

{{ ydb-short-name }} allows configuring the authentication type of database nodes when they register in the cluster. This type is configured via the parameter `node_registration_token` of the section `auth_config`.

#|
|| Parameter | Description ||
|| `node_registration_token`
| Defines the authentication type of database nodes when they register in the cluster {{ ydb-short-name }}.

Possible values:

- Empty string (`""`) — the node authentication mode via TLS certificates is used. In this case, nodes must use certificates for authentication when registering in the cluster. For more information about configuring node authentication via certificates, see   in the section [Authentication and authorization of database nodes](../../devops/configuration-management/configuration-v1/node-authorization.md).
- "root@builtin" is an authentication mode using a special debug token. This mode is planned to be removed in future releases and is not recommended for use: to ensure cluster security, it is recommended to use node authentication via TLS certificates by setting the parameter to an empty value.

    ||
|#

Example of a section `auth_config` with configuration for node registration by certificate:

```yaml
auth_config:
  #...
  node_registration_token: ""
  #...
```
