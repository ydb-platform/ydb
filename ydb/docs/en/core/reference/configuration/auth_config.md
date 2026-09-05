# auth_config

{{ ydb-short-name }} allows using various methods of user authentication in the system. Authentication and authentication provider settings are specified in the `auth_config` section of the {{ ydb-short-name }} configuration file.

## Local user authentication configuration {{ ydb-short-name }} {#local-auth-config}

For more information about authentication of [local users](../../concepts/glossary.md#access-user), see the section on [login and password authentication](../../security/authentication.md#static-credentials). To configure authentication of local users by login and password, specify the following parameters in the `auth_config` section:

#|
|| Parameter | Description ||
|| use_login_provider
| Flag enables authentication of local users by auth tokens obtained as a result of login and password entry. The login procedure in {{ ydb-short-name }} is the exchange of login and password for an authentication token.

Possible values:

- `true` — enables authentication of local users by authentication tokens.
- `false` — disables authentication of local users by authentication tokens.

Default value: `true`

{% note info %}

For the ability to create and authenticate local users, the `use_login_provider` and `enable_login_authentication` parameters must be set to `true`. Otherwise, local users will not be able to authenticate in {{ ydb-short-name }}.

{% endnote %}


    ||


|| enable_login_authentication
| Flag enables creation of local users and obtaining an authentication token for them in exchange for login and password.

Possible values:

- `true` — enables creation of local users and obtaining an authentication token for them.
- `false` — disables creation of local users and obtaining an authentication token for them.

Default value: `true`
||
|| domain_login_only
| Flag defines the boundaries of access rights of local users in the {{ ydb-short-name }} cluster.

Possible values:

- `true` — local users {{ ydb-short-name }} exist at the cluster level and can be granted access rights to multiple [databases](../../concepts/glossary.md#database).
- `false` — local users can exist both at the cluster level and at the level of each individual database. The boundaries of access rights of local users created at the database level are limited to the database in which they were created.

Default value: `true`
||
|| login_token_expire_time
| Lifetime of the authentication token created in exchange for the login and password of a local user.

Default value: `12h`
||
|#

### User lockout configuration on incorrect password {#account-lockout}

{{ ydb-short-name }} allows you to prevent a user from authenticating if they have made several failed password attempts. To configure user lockout conditions, fill in the `account_lockout` section.

Example of the `account_lockout` section:


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
|| attempt_threshold
| The number of incorrect password attempts after which the user account is temporarily locked. If the user enters the wrong password the specified number of times in a row, they are prohibited from authenticating for the time specified in the `attempt_reset_duration` parameter.

If the parameter is set to `0`, the number of incorrect password attempts is unlimited. After successful authentication (entering the correct username and password), the counter of failed attempts is reset to 0.

Default value: `4`
||
|| attempt_reset_duration
| The period of time during which the user is considered locked. During this period, the user will not be able to authenticate to the system even if they enter the correct username and password. The lockout period starts from the moment of the last incorrect password attempt.

If a zero value is specified (`"0s"` — an entry equivalent to 0 seconds), the user will be locked for an unlimited time. In this case, you can unlock the user using the [ALTER USER ...  LOGIN](../../yql/reference/syntax/alter-user.md) command.

The minimum lockout time interval is 1 second.

Supported units of measurement:

- Seconds. `30s`
- Minutes. `20m`
- Hours. `5h`
- Days. `3d`

Combining units of measurement in a single line is not allowed. For example, the following entry is incorrect: `1d12h`. Such an entry should be replaced with an equivalent one, for example `36h`.

Default value: `1h`
||
|#

### Configuring password complexity requirements {#password-complexity}

{{ ydb-short-name }} allows you to authenticate users by login and password. For more details, see the section [login and password authentication](../../security/authentication.md#static-credentials). To enhance security, {{ ydb-short-name }} provides the ability to configure the complexity of passwords used by [local users](../../concepts/glossary.md#access-user). To configure password requirements, you need to describe the `password_complexity` section.

Example of the `password_complexity` section:


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
|| min_length
| Minimum password length.

Default value: `0` (unlimited)
||
|| min_lower_case_count
| Minimum number of lowercase letters in the password.

Default value: `0` (unlimited)
||
|| min_upper_case_count
| Minimum number of uppercase letters in the password.

Default value: `0` (unlimited)
||
|| min_numbers_count
| Minimum number of digits in the password.

Default value: `0` (unlimited)
||
|| min_special_chars_count
| Minimum number of special characters in the password from those specified in the parameter `special_chars`.

Default value: `0` (unlimited)
||
|| special_chars
| List of special characters allowed when setting a password.

Valid values: `!@#$%^&*()_+{}\|<>?=`

Default value: empty string (allows using all valid special characters)
||
|| can_contain_username
| Flag determines whether the username can be included in the password.

Default value: `false`
||
|#

{% note info %}

Any changes to the password policy do not affect existing user passwords, so there is no need to change existing passwords; they will be accepted as is.

{% endnote %}

## LDAP authentication configuration {#ldap-auth-config}

One way to authenticate users in {{ ydb-short-name }} is to use an [LDAP](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol) directory. More about this type of authentication is described in the section about [using an LDAP directory](../../security/authentication.md#ldap). To configure LDAP authentication, you need to describe the `ldap_authentication` section.

Example of the `ldap_authentication` section:


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
|| hosts
| List of host names on which the LDAP server runs
||
|| port
| Port for connecting to the LDAP server
||
|| base_dn
| Root of the subtree in the LDAP directory from which the user record search will start
||
|| bind_dn
| Distinguished Name (DN) of the service account on behalf of which the user record search is performed
||
|| bind_password
| Password of the service account on behalf of which the user record search is performed. Not set when `extended_settings.enable_sasl_external_bind: true`
||
|| search_filter
| Filter for searching for a user record in the LDAP directory. The filter string may contain the character sequence *$username*, which will be replaced with the username requested for authentication in the database
||
|| use_tls
| Settings for configuring the TLS connection between {{ ydb-short-name }} and the LDAP server
||
|| enable
| Determines whether an attempt will be made to establish a TLS connection with [ using the request `StartTls`](../../security/authentication.md#starttls). When setting this parameter to `true`, you must disable the use of the connection scheme `ldaps` by assigning the parameter `ldap_authentication.scheme` the value `ldap`
||
|| ca_cert_file
| Path to the certificate authority certificate file
||
|| cert_require
| Level of requirements for the LDAP server certificate.

Possible values:

- `NEVER` - {{ ydb-short-name }} does not request a certificate, or any certificate passes verification.
- `ALLOW` - {{ ydb-short-name }} requires that the LDAP server provide a certificate. If the provided certificate cannot be trusted, the TLS session will still be established.
- `TRY` - {{ ydb-short-name }} requires that the LDAP server provide a certificate. If the provided certificate cannot be trusted, the TLS connection establishment is terminated.
- `DEMAND` and `HARD` — These requirements are equivalent to the parameter `TRY`.

Default value: `DEMAND`
||
|| cert_file
| Path to the client certificate file. Used as authentication information for the [service account](../../security/authentication.md#ldap-service-account-auth).
||
|| key_file
| Path to the client certificate key file
||
|| scheme
| LDAP server connection scheme.

Possible values:

- `ldap` — {{ ydb-short-name }} will connect to the LDAP server without any encryption. Passwords will be sent to the LDAP server in plain text.
- `ldaps` — {{ ydb-short-name }} will establish an encrypted connection to the LDAP server over TLS from the very first request. To successfully establish a connection using the `ldaps` scheme, you need to disable the use of [the `StartTls` query](../../security/authentication.md#starttls) in the `ldap_authentication.use_tls.enable: false` section and fill in the certificate information `ldap_authentication.use_tls.ca_cert_file` and the certificate requirement level `ldap_authentication.use_tls.cert_require`.
- If any other value is used, the default value `ldap` is taken.

Default value: `ldap`
||
|| requested_group_attribute
| Reverse group membership attribute. Default: `memberOf`
||
|| extended_settings.enable_nested_groups_search
| Flag that determines whether a query will be executed to retrieve the entire tree of groups that include the user's direct groups.

Possible values:

- `true`: {{ ydb-short-name }} requests information about all groups that include the user's direct groups. Queries about all parent groups can take a long time.
- `false`: {{ ydb-short-name }} requests a flat list of the user's groups. Such a query does not retrieve information about possible nested parent groups.

Default value: `false`
||
|| extended_settings.enable_sasl_external_bind
| Flag that determines whether [service account authentication](../../security/authentication.md#ldap-service-account-auth) will be performed using the SASL protocol with the EXTERNAL mechanism.

Possible values:

- `true` - For service account authentication, the SASL protocol with the EXTERNAL mechanism will be used (authentication using a client TLS certificate within mTLS). The client certificate specified in the `use_tls.cert_file` parameters and `use_tls.key_file` are used as authentication information. In this case, the `bind_dn` and `bind_password` parameters are not set.
- `false` - For service account authentication, the simple bind method is used. You must specify the `bind_dn` and `bind_password` parameters.

Default value: `false`
||
|| host
| Host name where the LDAP server runs. This is a deprecated parameter; instead, the `hosts` parameter should be used.
||
|| ldap_authentication_domain
| User name suffix that allows distinguishing users from the LDAP directory from users authenticated using other providers.

Default value: `ldap`
||
|#

## Configuration of client certificate authentication {#certificate-auth-config}

{{ ydb-short-name }} supports [client certificate authentication](../../security/authentication.md#client-certificate). The rules for certificate verification are set in the [client_certificate_authorization](client_certificate_authorization.md) section. Additionally, in the `auth_config` section, a suffix for user names authenticated by certificate may be specified:

#|
|| Parameter | Description ||
|| certificate_authentication_domain
| User name suffix that allows distinguishing users authenticated by client certificate from users authenticated by other methods.

Default value: `cert` (that is, the default SID suffix is `@cert`)
||
|#

## Configuration of authentication using an external IdP {#external-idp-auth-config}

{{ ydb-short-name }} supports [authentication by JWT tokens of an external identity provider using OpenID Connect](../../security/authentication.md#external-idp). To enable authentication, add the `external_idp_config` section to `auth_config`.

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
|| external_idp_config.issuer
| Expected value of the `iss` field (issuer), which identifies the issuer of the JWT token, and the base URL for OIDC Discovery. Required parameter; must start with `https://` and must not end with the `/` character. The `issuer` values in the Discovery document and `iss` in the JWT must exactly match the specified value.

Default value: empty string
||
|| external_idp_config.audience
| Expected value of the `aud` field (audience), which identifies the recipient of the JWT token. If the parameter is not set, the token recipient is not checked.

Default value: empty string
||
|| external_idp_config.allowed_clock_skew
| Allowed clock skew when checking time-related JWT fields: `exp` specifies the token expiration time, `nbf` the token start time, `iat` the issuance time.

Default value: `30s`
||
|| external_idp_config.subject_claim_name
| Name of the string JWT field from which the user SID is formed. If the field is missing or has a different type, the `sub` field is used.

Default value: `sub`
||
|| external_idp_config.groups_claim_name
| Name of the JWT field with an array of user groups. Only string elements are extracted from the array.

Default value: `groups`
||
|| external_idp_config.discovery_periodic_settings.success_refresh_period
| Period for refreshing the Discovery document after a successful request.

Default value: `1h`
||
|| external_idp_config.discovery_periodic_settings.min_error_refresh_period
| Minimum interval before re-requesting the Discovery document after an error.

Default value: `1s`
||
|| external_idp_config.discovery_periodic_settings.max_error_refresh_period
| Maximum interval before re-requesting the Discovery document after an error.

Default value: `5m`
||
|| external_idp_config.discovery_periodic_settings.request_timeout
| Timeout for requesting the Discovery document.

Default value: `15s`
||
|| external_idp_config.jwks_periodic_settings.success_refresh_period
| Period for refreshing JWKS after a successful request.

Default value: `1h`
||
|| external_idp_config.jwks_periodic_settings.min_error_refresh_period
| Minimum interval before re-requesting JWKS after an error.

Default value: `1s`
||
|| external_idp_config.jwks_periodic_settings.max_error_refresh_period
| Maximum interval before re-requesting JWKS after an error.

Default value: `5m`
||
|| external_idp_config.jwks_periodic_settings.request_timeout
| JWKS request timeout.

Default value: `15s`
||
|| external_idp_config.jwks_cache_settings.timeout
| Maximum age of the JWKS cache. If the JWKS update fails, the keys are removed after this period expires.

Default value: `2h`
||
|| external_idp_authentication_domain
| Username suffix that distinguishes external IdP users from users authenticated by other providers. The same suffix is added to group names obtained from the JWT token.

Default value: `sso`
||
|#

{% note warning %}

A third-party IAM provider and an external IdP using the OIDC protocol use tokens of type `Bearer`. If the `use_access_service` parameter is enabled, the IAM provider takes priority and intercepts all such tokens. Therefore, simultaneous use of authentication via an IAM provider and an external IdP using the OIDC protocol is not supported.

{% endnote %}

## Configuring authentication with a third-party IAM provider {#iam-auth-config}

{{ ydb-short-name }} supports user authentication using the [Yandex Identity and Access Management (IAM)](https://yandex.cloud/en/services/iam) service, which is used in Yandex Cloud, or another service compatible with it via API. To configure IAM authentication, define the following parameters:

#|
|| Parameter | Description ||
|| use_access_service
| Flag enables user authentication in Yandex Cloud via IAM using AccessService.

Default value: `false`
||
|| access_service_endpoint
| Address to which requests are sent to AccessService (IAM).

Default value: `as.private-api.cloud.yandex.net:4286`
||
|| use_access_service_tls
| Flag enables the use of TLS connections between {{ ydb-short-name }} and AccessService.

Default value: `true`
||
|| access_service_domain
| Suffix of the 'user source' in [SID](../../concepts/glossary.md#access-sid) for users coming to {{ ydb-short-name }} from Yandex Cloud IAM.

Default value: `as` ("access service")
||
|| path_to_root_ca
| Path to the certificate authority file used for interaction with AccessService.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
||
|| access_service_grpc_keep_alive_time_ms
| Time period, in milliseconds, after which {{ ydb-short-name }} sends a keepalive ping to the IAM server to keep the connection alive.

Default value: `10000`
||
|| access_service_grpc_keep_alive_timeout_ms
| Timeout period for waiting for a response from the IAM server to a keepalive ping, in milliseconds. If no response is received from the IAM server within the timeout, {{ ydb-short-name }} closes the connection.

Default value: `1000`
||
|| use_access_service_api_key
| The flag enables the use of IAM API keys. An API key is a secret key issued in Yandex Cloud IAM for simplified authorization of service accounts in the Yandex Cloud API. It is used when it is not possible to automatically request an IAM token.

Default value: `false`
||
|#

## Authentication result caching settings {#caching-auth-results}

To reduce the number of [authentication token checks](../../security/authentication.md#token-validation), each {{ ydb-short-name }} node caches the results of checking [user tokens](../../concepts/glossary.md#user-token). For more information, see the article caching-authentication-results.

The lifetime and other aspects of user token operation are configured using the following parameters. Time parameter values are specified as a number with a unit suffix: `ms` — milliseconds, `s` — seconds, `m` — minutes, `h` — hours, `d` — days. For example, `300ms`, `30s`, `10m`, `1h`, or `2d`.

#|
|| refresh_period
| Defines how often the {{ ydb-short-name }} node scans user tokens in the cache for reaching the time limits specified in the `refresh_time`, `life_time`, and `expire_time` parameters, after which the token must be refreshed or deleted. The shorter the specified user token check interval, the higher the CPU load.

Default value: `1s`
||
|| refresh_time
| Maximum interval between a successful user token refresh and the next refresh attempt. The specific refresh time is chosen in the range from `refresh_time/2` to `refresh_time`.

For example, after the first request with a valid authentication token, the node creates a user token. After an interval randomly chosen in the range from `refresh_time/2` to `refresh_time`, the node checks the authentication token again. After a successful check, the cycle repeats. On a retryable error, the node repeats the check taking into account the `min_error_refresh_time` and `max_error_refresh_time` parameters, and on a permanent error it stops using the previously created user token.

The parameter applies to refreshable authentication tokens, for example, to login and password tokens and tokens of an external identity provider.

Default value: `1h`
||
|| life_time
| The period of storing the user token in the {{ ydb-short-name }} node cache since its last use. If requests from the user for whom the token was created have not arrived at the {{ ydb-short-name }} node within the specified period, the node removes this user token from its cache.

Default value: `1h`
||
|| expire_time
| The validity period of a successful check result for most types of authentication tokens. After a successful refresh, the countdown restarts. After the period expires, the record is removed from the cache regardless of `life_time`.

For login and password tokens and tokens of an external identity provider, the expiration time of the authentication token itself is used. For requests signed with an access key, a separate parameter `as_signature_expire_time` is used.

Default value: `24h`
||
|| as_signature_expire_time
| The validity period of the check result for a request authenticated using an access key signature.

Default value: `1m`
||
|| min_error_refresh_time
| The initial interval between repeated checks after a retryable user token refresh error.

After a retryable error, the first retry check is performed immediately. If it also ends with a retryable error, the delay before the next check is chosen randomly in the range from `min_error_refresh_time/2` to `min_error_refresh_time`. After each subsequent retryable error, the current interval doubles, but does not exceed the difference `max_error_refresh_time - min_error_refresh_time`. For each current interval `D`, the actual delay is chosen randomly in the range from `D/2` to `D`.

{% note warning %}

It is not recommended to set the parameter value to `0`, as immediate retries create excessive load.

{% endnote %}

Default value: `1s`
||
|| max_error_refresh_time
| Limits the increase of the interval between retry checks after retryable errors of user token refresh. Does not limit the total duration of retry checks.

Default value: `1m`
||
|#

Example for login and password authentication:


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


After a user token is created, the next scheduled check is performed after an interval randomly selected in the range from `30m` to `1h` (`refresh_time`). This condition is checked once every `1s` (`refresh_period`). After a successful check, the interval is selected anew. If a retryable error occurs, the first retry check is performed immediately. If it also ends with a retryable error, the next check is performed after an interval randomly selected in the range from `500ms` to `1s` (`min_error_refresh_time`). Then the current interval is doubled, but does not exceed `59s` (`max_error_refresh_time - min_error_refresh_time`). The actual delay is each time randomly selected in the range from half to the full current interval.

For an authentication token obtained by login and password, the `expire_time` parameter does not apply: the validity period of such a token is set by `login_token_expire_time`, so it becomes invalid after `12h`. The cached entry stops being used at the nearest check after this condition, but may be deleted earlier due to lack of queries for `2h` (`life_time`) or a persistent error.

Example for queries signed with an access key:


```yaml
auth_config:
  refresh_period: "1s"
  life_time: "30m"
  as_signature_expire_time: "1h"
```


The user token for a query signed with an access key is not updated on a schedule. The entry is considered valid for `1h` (`as_signature_expire_time`) from the moment of successful verification and is deleted at the nearest cache check. With the specified values, it will be deleted no later than after `1h + 1s` (`as_signature_expire_time + refresh_period`). The entry may be deleted earlier due to lack of queries for `30m` (`life_time`).

## Node registration token configuration {#node-registration-token}

{{ ydb-short-name }} allows you to configure the authentication type of database nodes when they register in the cluster. This type is configured via the `node_registration_token` parameter of the `auth_config` section.

#|
|| Parameter | Description ||
|| node_registration_token
| Defines the authentication type of database nodes when they register in the cluster {{ ydb-short-name }}.

Possible values:

- Empty string (`""`) — the authentication mode for nodes via TLS certificates is used. In this case, nodes must use certificates for authentication when registering in the cluster. For more details on configuring node authentication via certificates, see the section [Authentication and authorization of database nodes](../../devops/configuration-management/configuration-v1/node-authorization.md).
- "root@builtin" — authentication mode via a special debug token. This mode is planned to be removed in future releases and is not recommended for use: to ensure cluster security, it is recommended to use the node authentication mode via TLS certificates by setting the parameter to an empty value.

  ||
  |#

Example of the `auth_config` section with node registration configured by certificate:


```yaml
auth_config:
  #...
  node_registration_token: ""
  #...
```
