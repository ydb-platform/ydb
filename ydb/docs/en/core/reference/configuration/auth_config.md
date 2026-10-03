# auth_config

{{ ydb-short-name }} allows you to use various methods of user authentication in the system. Authentication settings and authentication providers are set in the `auth_config` section of the {{ ydb-short-name }} configuration file.

## Local user authentication configuration {{ ydb-short-name }} {#local-auth-config}

For more information about [local user authentication](../../concepts/glossary.md#access-user), see the section on [login and password authentication](../../security/authentication.md#static-credentials). To configure local user authentication using a login and password, you need to specify the following parameters in the `auth_config` section:

#|
|| Parameter | Description ||
|| `use_login_provider`
| The flag allows authentication of local users using auth tokens received as a result of logging in with a username and password. The login procedure in {{ ydb-short-name }} is the exchange of a username and password for an authentication token.

Possible values:

- `true` — allows local users to authenticate using authentication tokens;
- `false` — prohibits local user authentication using authentication tokens.

Default value: `true`

{% note info %}

For the possibility of creating and authenticating local users, the parameters `use_login_provider` and `enable_login_authentication` must have the value `true`. Otherwise, local users will not be able to authenticate in {{ ydb-short-name }}.

{% endnote %}
    ||
|| `enable_login_authentication`
| The flag allows the creation of local users and obtaining an authentication token for them in exchange for a username and password.

Possible values:

- `true` — allows the creation of local users and obtaining an authentication token for them;
- `false` — prohibits the creation of local users and the issuance of an authentication token for them.

Default value: `true`
    ||
|| `domain_login_only`
| The flag defines the boundaries of local users' access rights in the {{ ydb-short-name }} cluster.

Possible values:

- `true` — local users {{ ydb-short-name }} exist at the cluster level and can be granted access rights to multiple [databases](../../concepts/glossary.md#database).

- `false` — local users can exist both at the cluster level and at the level of each individual database. The scope of access rights for local users created at the database level is limited to the database in which they are created.

Default value: `true`
    ||
|| `login_token_expire_time`
| The lifetime of the authentication token created in exchange for the local user's login and password.

Default value: `12h`
    ||
|#

### User lock configuration for incorrectly entered password {#account-lockout}

{{ ydb-short-name }} allows you to prohibit a user from authenticating if they have made several unsuccessful password entry attempts. To configure the conditions for blocking a user, you need to fill out the `account_lockout` section.

Example of a section `account_lockout`:

```yaml
auth_config:
  #...
  account_lockout:
    attempt_threshold: 4
    attempt_reset_duration: "1h"
  #...
```

#|
|| Parameter | Description ||
|| `attempt_threshold`
| The number of incorrect password entry attempts after which the user account is temporarily blocked. If a user enters the wrong password the specified number of times in a row, they are prohibited from authenticating for the time specified in the `attempt_reset_duration` parameter.

If the parameter has the value `0`, the number of attempts to enter an incorrect password is unlimited. After successful authentication (entering the correct username and password), the counter of unsuccessful attempts is reset to 0.

Default value: `4`
    ||
|| `attempt_reset_duration`
| The period of time during which the user is considered blocked. During this period, the user will not be able to authenticate in the system even if they enter the correct username and password. The blocking period starts from the moment of the last incorrect password entry attempt.

If a zero value is specified (`"0s"` — an entry equivalent to 0 seconds), the user will be blocked indefinitely. In this case, the block can be removed using the [ALTER USER ... LOGIN](../../yql/reference/syntax/alter-user.md) command.

The minimum lock time interval is 1 second.

Supported units of measurement:

- Seconds. `30s`
- Minutes. `20m`
- Hours. `5h`
- Days. `3d`

It is not allowed to combine units of measurement in one line. For example, the following entry is incorrect: `1d12h`. This entry should be replaced with an equivalent one, for example `36h`.

Default value: `1h`
    ||
|#

### Password complexity requirements configuration {#password-complexity}

{{ ydb-short-name }} allows you to authenticate users by username and password. For more details, see the section [username and password authentication](../../security/authentication.md#static-credentials). To enhance security, {{ ydb-short-name }} provides the option to configure the complexity of passwords used by [local users](../../concepts/glossary.md#access-user). To configure password requirements, you need to describe the `password_complexity` section.

Example of a section `password_complexity`:

```yaml
auth_config:
  #...
  password_complexity:
    min_length: 8
    min_lower_case_count: 1
    min_upper_case_count: 1
    min_numbers_count: 1
    min_special_chars_count: 1
    special_chars: "!@#$%^&*()_+{}|<>?="
    can_contain_username: false
  #...
```

#|
|| Parameter | Description ||
|| `min_length`
| Minimum password length.

Default value: `0` (no limit)
    ||
|| `min_lower_case_count`
| The minimum number of lowercase letters in the password.

Default value: `0` (unlimited)
    ||
|| `min_upper_case_count`
| The minimum number of uppercase letters in the password.

Default value: `0` (unlimited)
    ||
|| `min_numbers_count`
| The minimum number of digits in the password.

Default value: `0` (unlimited)
    ||
|| `min_special_chars_count`
| The minimum number of special characters in the password from those specified in the parameter `special_chars`.

Default value: `0` (unlimited)
    ||
|| `special_chars`
| List of special characters allowed when setting a password.

Valid values: `!@#$%^&*()_+{}\|<>?=`

Default value: empty string (allows the use of all valid special characters)
    ||
|| `can_contain_username`
| The flag determines whether it is permissible to include the username in the password.

Default value: `false`
    ||
|#

{% note info %}

Any changes to the password policy do not affect already active user passwords, so there is no need to change existing passwords; they will be accepted in their current form.

{% endnote %}

## LDAP authentication configuration {#ldap-auth-config}

One of the ways to authenticate users in {{ ydb-short-name }} is to use [LDAP](https://ru.wikipedia.org/wiki/LDAP) directory. More information about this type of authentication is available in the section about [using LDAP directory](../../security/authentication.md#ldap). To configure LDAP authentication, you need to describe the `ldap_authentication` section.

Example of a section `ldap_authentication`:

```yaml
auth_config:
  #...
  ldap_authentication:
    hosts:
      - "ldap-hostname-01.example.net"
      - "ldap-hostname-02.example.net"
      - "ldap-hostname-03.example.net"
    port: 389
    base_dn: "dc=mycompany,dc=net"
    bind_dn: "cn=serviceAccaunt,dc=mycompany,dc=net"
    bind_password: "serviceAccauntPassword"
    search_filter: "uid=$username"
    scheme: "ldap"
    requested_group_attribute: "memberOf"
    extended_settings:
      enable_nested_groups_search: true
      enable_sasl_external_bind: true
    use_tls:
      enable: true
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
      cert_file: "/path/to/client-cert.pem"
      key_file: "/path/to/client-key.pem"
  ldap_authentication_domain: "ldap"
  refresh_time: "1h"
  #...
```

#|
|| Parameter | Description ||
|| `hosts`
| List of hostnames where the LDAP server is running
    ||
|| `port`
| Port for connecting to the LDAP server
    ||
|| `base_dn`
| The root of the subtree in the LDAP directory from which the user record search will be performed
    ||
|| `bind_dn`
| Distinguished Name (DN) of the service account under which the user record search is performed
    ||
|| `bind_password`
| The password of the service account on behalf of which the user record search is performed. Not set when `extended_settings.enable_sasl_external_bind: true`
    ||
|| `search_filter`
| A filter for searching a user record in the LDAP directory. The filter string may contain the sequence of characters *$username*, which will be replaced with the username requested for authentication in the database
    ||
|| `use_tls`
| Settings for configuring a TLS connection between {{ ydb-short-name }} and the LDAP server
    ||
|| `enable`
| Determines whether an attempt will be made to establish a TLS connection using the [request `StartTls`](../../security/authentication.md#starttls). When setting this parameter to `true`, it is necessary to disable the use of the connection scheme `ldaps` by setting the parameter `ldap_authentication.scheme` to `ldap`
    ||
|| `ca_cert_file`
| Path to the certificate authority file
    ||
|| `cert_require`
| The level of requirements for the LDAP server certificate.

Possible values:

- `NEVER` - {{ ydb-short-name }} does not request a certificate or any certificate passes the verification.
- `ALLOW` - {{ ydb-short-name }} requires the LDAP server to provide a certificate. If the provided certificate cannot be trusted, the TLS session will still be established.
- `TRY` - {{ ydb-short-name }} requires the LDAP server to provide a certificate. If the provided certificate cannot be trusted, the establishment of a TLS connection is terminated.
- `DEMAND` and `HARD` — These requirements are equivalent to the `TRY` parameter.

Default value: `DEMAND`
    ||
|| `cert_file`
| The path to the client certificate file. Used as authentication information for [service account](../../security/authentication.md#ldap-service-account-auth).
    ||
|| `key_file`
| Path to the client certificate key file
    ||
|| `scheme`
| LDAP server connection scheme.

Possible values:

- `ldap` — {{ ydb-short-name }} will connect to the LDAP server without any encryption. Passwords will be sent to the LDAP server in plain text.
- `ldaps` — {{ ydb-short-name }} will establish an encrypted connection with the LDAP server via the TLS protocol from the very first request. To successfully establish a connection via the `ldaps` scheme, it is necessary to disable the use of [the `StartTls`](../../security/authentication.md#starttls) request in the `ldap_authentication.use_tls.enable: false` section and fill in the information about the `ldap_authentication.use_tls.ca_cert_file` certificate and the certificate requirement level `ldap_authentication.use_tls.cert_require`.
- If any other value is used, the default value will be taken - `ldap`.

Default value: `ldap`
    ||
|| `requested_group_attribute`
| Group inverse membership attribute. By default `memberOf`
    ||
|| `extended_settings.enable_nested_groups_search`
| The flag determines whether a request will be made to obtain the entire tree of groups that include the user's immediate groups.

Possible values:

- `true` — {{ ydb-short-name }} requests information about all groups that the user's immediate groups belong to. Requests about all parent groups can take a long time.
- `false` — {{ ydb-short-name }} requests a flat list of user groups. Such a request does not retrieve information about possible nested parent groups.

Default value: `false`
    ||
|| `extended_settings.enable_sasl_external_bind`
| The flag determines whether [service account authentication](../../security/authentication.md#ldap-service-account-auth) will be performed using the SASL protocol with the EXTERNAL mechanism.

Possible values:

- `true` - The SASL protocol with the EXTERNAL mechanism (authentication via a client TLS certificate within mTLS) will be used to authenticate the service account. The authentication information is the client certificate specified in the parameters `use_tls.cert_file` and `use_tls.key_file`. The parameters `bind_dn` and `bind_password` are not set in this case.
- `false` - The simple bind method will be used to authenticate the service account. It is necessary to specify the parameters `bind_dn` and `bind_password`.

Default value: `false`
    ||
|| `host`
| The hostname of the LDAP server. This is a deprecated parameter; the `hosts` parameter should be used instead
    ||
|| `ldap_authentication_domain`
| A user name suffix that allows distinguishing users from the LDAP directory from users authenticated via other providers.

Default value: `ldap`
    ||
|#

## Client certificate authentication configuration {#certificate-auth-config}

{{ ydb-short-name }} supports [client certificate authentication](../../security/authentication.md#client-certificate). The rules for checking certificates are set in the section [client_certificate_authorization](client_certificate_authorization.md). Additionally, the section `auth_config` can specify a suffix for the usernames authenticated by certificate:

#|
|| Parameter | Description ||
|| `certificate_authentication_domain`
| A user name suffix that allows distinguishing users authenticated with a client certificate from users authenticated by other methods.

Default value: `cert` (that is, the default SID suffix is `@cert`)
    ||
|#

## Authentication configuration using an external IdP {#external-idp-auth-config}

{{ ydb-short-name }} supports [JWT token authentication of an external identity provider using OpenID Connect](../../security/authentication.md#external-idp). To enable authentication, you need to add the `external_idp_config` section to `auth_config`.

Example configuration:

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
| The expected value of the field `iss` (issuer), which identifies the issuer of the JWT token, and the base URL for OIDC Discovery. A required parameter; must start with `https://` and must not end with the character `/`. The values of `issuer` in the Discovery document and `iss` in the JWT must exactly match the specified value.

Default value: empty string
    ||
|| `external_idp_config.audience`
| The expected value of the `aud` (audience) field, which identifies the recipient of the JWT token. If the parameter is not set, the token recipient is not verified.

Default value: empty string
    ||
|| `external_idp_config.allowed_clock_skew`
| The acceptable time discrepancy when checking JWT fields related to time: `exp` sets the token expiration time, `nbf` — the start time of its validity, `iat` — the issuance time.

Default value: `30s`
    ||
|| `external_idp_config.subject_claim_name`
| The name of the JWT string field from which the user's SID is formed. If the field is missing or has a different type, the `sub` field is used.

Default value: `sub`
    ||
|| `external_idp_config.groups_claim_name`
| The name of the JWT field with an array of user groups. Only string elements are extracted from the array.

Default value: `groups`
    ||
|| `external_idp_config.discovery_periodic_settings.success_refresh_period`
| The period for updating the Discovery document after a successful request.

Default value: `1h`
    ||
|| `external_idp_config.discovery_periodic_settings.min_error_refresh_period`
| The minimum interval before re-requesting the Discovery document after an error.

Default value: `1s`
    ||
|| `external_idp_config.discovery_periodic_settings.max_error_refresh_period`
| Maximum interval before re-requesting the Discovery document after an error.

Default value: `5m`
    ||
|| `external_idp_config.discovery_periodic_settings.request_timeout`
| Discovery document request timeout.

Default value: `15s`
    ||
|| `external_idp_config.jwks_periodic_settings.success_refresh_period`
| JWKS update period after a successful request.

Default value: `1h`
    ||
|| `external_idp_config.jwks_periodic_settings.min_error_refresh_period`
| The minimum interval before re-requesting JWKS after an error.

Default value: `1s`
    ||
|| `external_idp_config.jwks_periodic_settings.max_error_refresh_period`
| Maximum interval before re-requesting JWKS after an error.

Default value: `5m`
    ||
|| `external_idp_config.jwks_periodic_settings.request_timeout`
| JWKS request timeout.

Default value: `15s`
    ||
|| `external_idp_config.jwks_cache_settings.timeout`
| The maximum cache age for JWKS. If updating JWKS fails, the keys are deleted after this period expires.

Default value: `2h`
    ||
|| `external_idp_authentication_domain`
| A user name suffix that allows distinguishing users of an external IdP from users authenticated via other providers. The same suffix is added to the names of groups obtained from the JWT token.

Default value: `sso`
    ||
|#

{% note warning %}

A third-party IAM provider and an external IdP via the OIDC protocol use tokens of type `Bearer`. If the `use_access_service` parameter is enabled, the IAM provider takes precedence and intercepts all such tokens. Therefore, simultaneous use of authentication via an IAM provider and an external IdP via the OIDC protocol is not supported.

{% endnote %}

## Authentication configuration using a third-party IAM provider {#iam-auth-config}

{{ ydb-short-name }} supports user authentication using the [Yandex Identity and Access Management (IAM)](https://yandex.cloud/en/services/iam) service, which is used in Yandex Cloud, or another service compatible with it via API. To configure IAM authentication, you need to define the following parameters:

#|
|| Parameter | Description ||
|| `use_access_service`
| The flag allows user authentication in Yandex Cloud via IAM using AccessService.

Default value: `false`
    ||
|| `access_service_endpoint`
| The address to which requests are sent to AccessService (IAM).

Default value: `as.private-api.cloud.yandex.net:4286`
    ||
|| `use_access_service_tls`
| The flag enables the use of TLS connections between {{ ydb-short-name }} and AccessService.

Default value: `true`
    ||
|| `access_service_domain`
| The suffix of the «user source» in [SID](../../concepts/glossary.md#access-sid) for users coming to {{ ydb-short-name }} from Yandex Cloud IAM.

Default value: `as` ("access service")
    ||
|| `path_to_root_ca`
| The path to the certificate authority file used for interacting with AccessService.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
    ||
|| `access_service_grpc_keep_alive_time_ms`
| The time period, in milliseconds, after which {{ ydb-short-name }} sends a keepalive ping to the IAM server to maintain the connection.

Default value: `10000`
    ||
|| `access_service_grpc_keep_alive_timeout_ms`
| The time period for waiting for a response from the IAM server to a keepalive ping, in milliseconds. If no response is received from the IAM server after the waiting period, {{ ydb-short-name }} closes the connection.

Default value: `1000`
    ||
|| `use_access_service_api_key`
| The flag allows the use of IAM API keys. An API key is a secret key issued in Yandex Cloud IAM for simplified authorization of service accounts in the Yandex Cloud API. It is used when it is not possible to automatically request an IAM token.

Default value: `false`
    ||
|#

## Authentication results caching settings {#caching-auth-results}

To reduce the number of [authentication token checks](../../security/authentication.md#token-validation), each node {{ ydb-short-name }} caches the verification results in [user tokens](../../concepts/glossary.md#user-token). See more in the article [{#T}](../../security/caching-authentication-results.md).

The lifetime and other aspects of user token operation are configured using the following parameters. The values of time parameters are set by a number with a unit suffix: `ms` — milliseconds, `s` — seconds, `m` — minutes, `h` — hours, `d` — days. For example, `300ms`, `30s`, `10m`, `1h` or `2d`.

#|
|| `refresh_period`
| Determines how often the node {{ ydb-short-name }} scans user tokens in the cache to check if they have reached the time limits specified in the parameters `refresh_time`, `life_time` and `expire_time`, after which the token needs to be updated or deleted. The shorter the specified interval for checking user tokens, the higher the CPU load.

Default value: `1s`
    ||
|| `refresh_time`
| The maximum interval between a successful user token update and the next update attempt. The specific update time is selected in the range from `refresh_time/2` to `refresh_time`.

For example, after the first request with a valid authentication token, the node creates a user token. After a randomly selected interval ranging from `refresh_time/2` to `refresh_time`, the node checks the authentication token again. After a successful check, the cycle repeats. In case of a retriable error, the node rechecks taking into account the parameters `min_error_refresh_time` and `max_error_refresh_time`, and in case of a permanent error, it stops using the previously created user token.

The parameter applies to updated authentication tokens, such as login and password tokens and external identity provider tokens.

Default value: `1h`
    ||
|| `life_time`
| The period for which a user token is stored in the node cache {{ ydb-short-name }} from the moment of its last use. If there are no requests from the user for whom the token was created to the node {{ ydb-short-name }} within the specified period, the node removes this user token from its cache.

Default value: `1h`
    ||
|| `expire_time`
| The period during which the result of a successful verification is valid for most types of authentication tokens. After a successful update, the countdown starts again. When the period expires, the entry is deleted from the cache regardless of `life_time`.

For login and password access tokens and external identity provider tokens, the validity period of the authentication token itself is used. For requests signed with an access key, a separate parameter `as_signature_expire_time` is used.

Default value: `24h`
    ||
|| `as_signature_expire_time`
| The period of validity of the query check result authenticated using the access key signature.

Default value: `1m`
    ||
|| `min_error_refresh_time`
| Initial interval between repeated checks after a recoverable error in updating the user token.

After a retriable error, the first recheck is performed immediately. If it also results in a retriable error, the delay before the next check is randomly selected in the range from `min_error_refresh_time/2` to `min_error_refresh_time`. After each subsequent retriable error, the current interval is doubled, but does not exceed the difference `max_error_refresh_time - min_error_refresh_time`. For each current interval `D`, the actual delay is randomly selected in the range from `D/2` to `D`.

{% note warning %}

It is not recommended to set the parameter value to `0`, as immediate retries create excessive load.

{% endnote %}

Default value: `1s`
    ||
|| `max_error_refresh_time`
| Limits the increase in the interval between repeated checks after recoverable errors in updating the user token. Does not limit the total duration of repeated checks.

Default value: `1m`
    ||
|#

Example for authentication by login and password:

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

After creating a user token, the next scheduled check is performed after a randomly selected interval ranging from `30m` to `1h` (`refresh_time`). This condition is checked once every `1s` (`refresh_period`). After a successful check, the interval is selected again. If a retriable error occurs, the first recheck is performed immediately. If it also results in a retriable error, the next check is performed after a randomly selected interval ranging from `500ms` to `1s` (`min_error_refresh_time`). Then the current interval is doubled, but does not exceed `59s` (`max_error_refresh_time - min_error_refresh_time`). The actual delay is randomly selected each time within the range of half to the full current interval.

For an authentication token obtained via login and password, the `expire_time` parameter does not apply: the validity period of such a token is set by `login_token_expire_time`, so it becomes invalid after `12h`. The cached entry stops being used at the next verification after this condition, but it can be deleted earlier due to the lack of requests within `2h` (`life_time`) or a persistent error.

Example for requests with an access key signature:

```yaml
auth_config:
  refresh_period: "1s"
  life_time: "30m"
  as_signature_expire_time: "1h"
```

The user token for the request signed with the access key is not updated regularly. The entry is considered valid `1h` (`as_signature_expire_time`) from the moment of successful verification and is deleted during the next cache check. With the specified values, it will be deleted no later than after `1h + 1s` (`as_signature_expire_time + refresh_period`). The entry may be deleted earlier due to the absence of requests for `30m` (`life_time`).

## Node registration token configuration {#node-registration-token}

{{ ydb-short-name }} allows you to configure the type of authentication for database nodes when they are registered in the cluster. This type is configured via the `node_registration_token` parameter in the `auth_config` section.

#|
|| Parameter | Description ||
|| `node_registration_token`
| Determines the type of authentication for database nodes when they are registered in the cluster {{ ydb-short-name }}.

Possible values:

- Empty string (`""`) — node authentication via TLS certificates is used. In this case, nodes must use certificates to authenticate when registering in the cluster. For more information about setting up node authentication via certificates, see the section [Node authentication and authorization for database nodes](../../devops/configuration-management/configuration-v1/node-authorization.md).
- "root@builtin" is an authentication mode via a special debugging token. This mode is planned to be removed in future releases and is not recommended for use: to ensure the security of the cluster, it is recommended to use the node authentication mode via TLS certificates by setting the parameter to an empty value.

    ||
|#

Example of a section `auth_config` with node registration settings by certificate:

```yaml
auth_config:
  #...
  node_registration_token: ""
  #...
```
