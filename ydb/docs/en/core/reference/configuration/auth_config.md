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

For the ability to create and authenticate local users, the `use_login_provider` and `enable_login_authentication` parameters must have the value `true`. Otherwise, local users will not be able to authenticate in {{ ydb-short-name }}.

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

- `true` — local users {{ ydb-short-name }} exist at the cluster level and can be assigned access rights to multiple [databases](../../concepts/glossary.md#database).
- `false` — local users can exist both at the cluster level and at the level of each individual database. The access rights boundaries of local users created at the database level are limited to the database in which they were created.

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

If a zero value is specified (`"0s"` — a record equivalent to 0 seconds), the user will be locked for an unlimited time. In this case, the lock can be removed using the [ALTER USER ...  LOGIN](../../yql/reference/syntax/alter-user.md) command.

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

{{ ydb-short-name }} allows you to authenticate users by login and password. For more details, see the section [login and password authentication](../../security/authentication.md#static-credentials). To enhance security, {{ ydb-short-name }} provides the ability to configure the complexity of passwords used by [local users](../../concepts/glossary.md#access-user). To configure password requirements, describe the `password_complexity` section.

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

Default value: 0 (unlimited)
||
|| min_lower_case_count
| Minimum number of lowercase letters in the password.

Default value: 0 (unlimited)
||
|| min_upper_case_count
| Minimum number of uppercase letters in the password.

Default value: 0 (unlimited)
||
|| min_numbers_count
| Minimum number of digits in the password.

Default value: 0 (unlimited)
||
|| min_special_chars_count
| Minimum number of special characters in the password from those specified in the `special_chars` parameter.

Default value: 0 (unlimited)
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

One way to authenticate users in {{ ydb-short-name }} is to use an [LDAP](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol) directory. More about this type of authentication is described in the section on [using an LDAP directory](../../security/authentication.md#ldap). To configure LDAP authentication, you need to describe the `ldap_authentication` section.

Example of the `ldap_authentication` section:


```yaml
auth_config:
  ...
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
  ...
```


#|
|| Parameter | Description ||
|| `hosts`
| List of host names on which the LDAP server runs
||
|| `port`
| Port for connecting to the LDAP server
||
|| `base_dn`
| Root of the subtree in the LDAP directory from which the user record search will be performed
||
|| `bind_dn`
| Distinguished Name (DN) of the service account on whose behalf the user record search is performed
||
|| `bind_password`
| Password of the service account on whose behalf the user record search is performed. Not set when `extended_settings.enable_sasl_external_bind: true`
||
|| `search_filter`
| Filter for searching the user record in the LDAP directory. The filter string may contain the character sequence *$username*, which will be replaced with the user name requested for authentication in the database
||
|| `use_tls`
| Settings for configuring the TLS connection between {{ ydb-short-name }} and the LDAP server
||
|| `enable`
| Determines whether an attempt will be made to establish a TLS connection with [ using the request `StartTls`](../../security/authentication.md#starttls). When setting this parameter to `true`, you must disable the use of the connection scheme `ldaps` by setting the parameter `ldap_authentication.scheme` to `ldap`
||
|| `ca_cert_file`
| Path to the certificate authority file
||
|| `cert_require`
| Level of requirements for the LDAP server certificate.

Possible values:

- `NEVER` - {{ ydb-short-name }} does not request a certificate, or any certificate passes verification.
- `ALLOW` - {{ ydb-short-name }} requires the LDAP server to provide a certificate. If the provided certificate cannot be trusted, the TLS session will still be established.
- `TRY` - {{ ydb-short-name }} requires the LDAP server to provide a certificate. If the provided certificate cannot be trusted, the TLS connection is terminated.
- `DEMAND` and `HARD` — These requirements are equivalent to the `TRY` parameter.

Default value: `DEMAND`
||
|| `cert_file`
| Path to the client certificate file. Used as authentication information for [service account](../../security/authentication.md#ldap-service-account-auth).
||
|| `key_file`
| Path to the client certificate key file
||
|| `scheme`
| LDAP server connection scheme.

Possible values:

- `ldap` — {{ ydb-short-name }} will connect to the LDAP server without any encryption. Passwords will be sent to the LDAP server in plain text.
- `ldaps` — {{ ydb-short-name }} will establish an encrypted connection to the LDAP server over TLS from the very first request. To successfully establish a connection using the `ldaps` scheme, you need to disable the use of [query `StartTls`](../../security/authentication.md#starttls) in the `ldap_authentication.use_tls.enable: false` section and fill in the certificate information `ldap_authentication.use_tls.ca_cert_file` and the certificate requirement level `ldap_authentication.use_tls.cert_require`.
- If any other value is used, the default value `ldap` will be taken.

Default value: `ldap`
||
|| `requested_group_attribute`
| Attribute of reverse group membership. By default `memberOf`
||
|| `extended_settings.enable_nested_groups_search`
| Flag determines whether a query will be executed to retrieve the entire tree of groups that include the user's immediate groups.

Possible values:

- `true` — {{ ydb-short-name }} requests information about all groups that include the user's immediate groups. Queries about all parent groups can take a long time.
- `false` — {{ ydb-short-name }} requests a flat list of the user's groups. Such a query does not retrieve information about possible nested parent groups.

Default value: `false`
||
|| `extended_settings.enable_sasl_external_bind`
| Flag determines whether [service account authentication](../../security/authentication.md#ldap-service-account-auth) will be performed using the SASL protocol with the EXTERNAL mechanism.

Possible values:

- `true` - The SASL protocol with the EXTERNAL mechanism (authentication using a client TLS certificate within mTLS) will be used for service account authentication. The client certificate specified in the `use_tls.cert_file` and `use_tls.key_file` parameters is used as authentication information. The `bind_dn` and `bind_password` parameters are not set in this case.
- `false` - The simple bind method will be used for service account authentication. The `bind_dn` and `bind_password` parameters must be specified.

Default value: `false`
||
|| `host`
| Host name where the LDAP server runs. This is a deprecated parameter; the `hosts` parameter should be used instead.
||
|| `ldap_authentication_domain`
| User name suffix that allows distinguishing users from the LDAP directory from users authenticated using other providers.

Default value: `ldap`
||
|#

## Configuring client certificate authentication {#certificate-auth-config}

{{ ydb-short-name }} supports [client certificate authentication](../../security/authentication.md#client-certificate). Certificate verification rules are set in the [client_certificate_authorization](client_certificate_authorization.md) section. Additionally, a suffix for user names of users authenticated by certificate may be specified in the `auth_config` section.

#|
|| Parameter | Description ||
|| `certificate_authentication_domain`
| User name suffix that allows distinguishing users authenticated by client certificate from users authenticated by other methods.

Default value: `cert` (that is, the default SID suffix is `@cert`).
||
|#

## Configuring authentication using a third-party IAM provider {#iam-auth-config}

{{ ydb-short-name }} supports user authentication using the [Yandex Identity and Access Management (IAM)](https://yandex.cloud/en/services/iam) service, which is used in Yandex Cloud, or another service compatible with it via API. To configure IAM authentication, you need to define the following parameters:

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
| Suffix of the “user source” in [SID](../../concepts/glossary.md#access-sid) for users coming to {{ ydb-short-name }} from Yandex Cloud IAM.

Default value: `as` ("access service")
||
|| path_to_root_ca
| Path to the certificate authority file used to interact with AccessService.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
||
|| access_service_grpc_keep_alive_time_ms
| Time period, in milliseconds, after which {{ ydb-short-name }} sends a keepalive ping to the IAM server to keep the connection alive.

Default value: `10000`
||
|| access_service_grpc_keep_alive_timeout_ms
| Time period to wait for a response from the IAM server to a keepalive ping, in milliseconds. If no response is received from the IAM server within the timeout, {{ ydb-short-name }} closes the connection.

Default value: `1000`
||
|| use_access_service_api_key
| Flag enables the use of IAM API keys. An API key is a secret key issued in Yandex Cloud IAM for simplified authorization of service accounts in the Yandex Cloud API. It is used when it is not possible to automatically request an IAM token.

Default value: `false`
||
|#

## Authentication result caching settings

During authentication, the user session receives an authentication token that is passed with every request to the {{ ydb-short-name }} cluster. Since {{ ydb-short-name }} is a distributed system, user requests will ultimately be processed on one or more {{ ydb-short-name }} nodes. Each {{ ydb-short-name }} node, having received a request from the user, verifies the authentication token and, if the check succeeds, generates a **user token** that is valid only within the current {{ ydb-short-name }} node and is used to authorize the actions requested by the user. Subsequent requests with the same authentication token to the same {{ ydb-short-name }} node no longer require authentication token verification and are executed under the user token.

The lifetime and other important aspects of the user token operation are configured in the {{ ydb-short-name }} configuration using the following parameters:

#|
|| refresh_period
| Determines how often the {{ ydb-short-name }} node scans user tokens in the cache for reaching the time limits specified in the `refresh_time`, `life_time`, and `expire_time` parameters, after which the token must be refreshed or deleted. The shorter the specified user token check interval, the higher the CPU load.

Default value: `1s`
||
|| refresh_time
| Determines the time elapsed since the last refresh when the {{ ydb-short-name }} node will attempt to refresh the user token. The specific refresh time will fall within the range from `refresh_time/2` to `refresh_time`.

Default value: `1h`
||
|| life_time
| The period of storing the user token in the {{ ydb-short-name }} node cache since its last use. If requests from the user for whom the token was created have not arrived at the {{ ydb-short-name }} node within the specified period, the node removes this user token from its cache.

Default value: `1h`
||
|| expire_time
| The expiration period of the user token, after which the token is removed from the {{ ydb-short-name }} node cache. The removal occurs regardless of the period specified in the `life_time` parameter.

{% note warning %}

If a third-party system has successfully authenticated on the {{ ydb-short-name }} node and sends requests to the same node more frequently than the `life_time` interval, {{ ydb-short-name }} will only reliably detect a possible removal or change of the user account privileges after the `expire_time` period expires.

{% endnote %}

The shorter the specified period, the more often the {{ ydb-short-name }} node re-authenticates users and updates their privileges. However, too frequent re-authentication of users slows down the {{ ydb-short-name }}, especially for external users. Setting this parameter in seconds negates the cache for user tokens.

Default value: `24h`
||
|| min_error_refresh_time
| The minimum period after which the attempt to refresh a user token is repeated if an error (temporary failure) occurred while obtaining it.

Together with the `max_error_refresh_time` parameter, it defines the boundaries for selecting the delay before retrying to refresh a user token that was obtained with an error. Each subsequent delay increases until it reaches the value of `max_error_refresh_time`. Attempts to refresh the user token continue until a successful refresh or until the end of the `expire_time` period.

{% note warning %}

It is not recommended to set the parameter value to `0`, as immediate retries create excessive load.

{% endnote %}

Default value: `1s`
||
|| max_error_refresh_time
| The maximum period before which the attempt to refresh a user token is repeated if an error (temporary failure) occurred while obtaining it.

Together with the `min_error_refresh_time` parameter, it defines the boundaries for selecting the delay before retrying to refresh a user token that was obtained with an error. Each subsequent delay increases until it reaches the value of `max_error_refresh_time`. Attempts to refresh the user token continue until a successful refresh or until the end of the `expire_time` period.

Default value: `1m`
||
|#

## Node registration token configuration {#node-registration-token}

{{ ydb-short-name }} allows you to configure the authentication type of database nodes when they register in the cluster. This type is configured through the `node_registration_token` parameter of the `auth_config` section.

#|
|| Parameter | Description ||
|| node_registration_token
| Defines the authentication type of database nodes when they register in the {{ ydb-short-name }} cluster.

Possible values:

- Empty string (`""`) — the authentication mode for nodes via TLS certificates is used. In this case, nodes must use certificates for authentication when registering in the cluster. For more details on configuring node authentication via certificates, see the section [Authentication and authorization of database nodes](../../devops/configuration-management/configuration-v1/node-authorization.md).
- "root@builtin" is an authentication mode using a special debug token. This mode is planned to be removed in future releases and is not recommended for use: to ensure cluster security, it is recommended to use node authentication via TLS certificates by setting the parameter to an empty value.

||
|#

Example of the `auth_config` section with certificate-based node registration configuration:


```yaml
auth_config:
  ...
  node_registration_token: ""
  ...
```
