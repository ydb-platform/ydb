# auth_config

{{ ydb-short-name }} allows using various user authentication methods in the system. Authentication and authentication provider settings are specified in the `auth_config` section of the {{ ydb-short-name }} configuration file.

## Configuring Local {{ ydb-short-name }} User Authentication {#local-auth-config}

For more information about authentication of [local users](../../concepts/glossary.md#access-user), see the section on [login and password authentication](../../security/authentication.md#static-credentials). To configure authentication of local users by login and password, specify the following parameters in the `auth_config` section:

#|
|| Parameter | Description ||
|| use_login_provider
| The flag enables authentication of local users using auth tokens obtained through login and password authentication. The login procedure in {{ ydb-short-name }} is an exchange of login and password for an authentication token.

Possible values:

- `true` — enables authentication of local users using authentication tokens.
- `false` — disables authentication of local users using authentication tokens.

Default value: `true`

{% note info %}

For the ability to create and authenticate local users, the `use_login_provider` and `enable_login_authentication` parameters must be set to `true`. Otherwise, local users will not be able to authenticate in {{ ydb-short-name }}.

{% endnote %}


    ||


|| enable_login_authentication
| The flag enables creation of local users and obtaining an authentication token for them in exchange for login and password.

Possible values:

- `true` — enables creation of local users and obtaining an authentication token for them.
- `false` — disables creation of local users and obtaining an authentication token for them.

Default value: `true`
||
|| domain_login_only
| Determines the scope of local user access rights in a {{ ydb-short-name }} cluster.

Valid values:

- `true` — local users exist in a {{ ydb-short-name }} cluster and can be granted rights to access multiple [databases](../../concepts/glossary.md#database).
- `false` — local users can exist either at the cluster or database level. The scope of access rights for local users created at the database level is limited to the database, in which they are created.

Default value: `true`
||
|#

Example of the `12h` section:

### Configuring user lockout on incorrect password {#account-lockout}

{{ ydb-short-name }} allows you to prevent a user from authenticating if they have made several failed password attempts. To configure user lockout conditions, fill in the `account_lockout` section.

#|
|| Parameter | Description ||
|| attempt_threshold
| Specifies the number of failed attempts to enter the correct password for a user account, after which the account is blocked for a period specified by the `account_lockout` parameter.


```yaml
auth_config:
  #...
  account_lockout:
    attempt_threshold: 4
    attempt_reset_duration: "1h"
  #...
```


If `attempt_reset_duration`, the number of attempts to enter the correct password is unlimited. After successful authentication (correct username and password), the counter for failed attempts is reset to 0.

Default value: `0`
||
|| attempt_reset_duration
| Specifies the period that a locked-out account remains locked before automatically becoming unlocked. This period starts after the last failed attempt.

Default value: `4`
||
|| attempt_reset_duration
| The period of time during which the user is considered locked out. During this period, the user will not be able to authenticate in the system even if they enter the correct username and password. The lockout period starts from the moment of the last incorrect password attempt.

If a zero value is specified (`"0s"` — an entry equivalent to 0 seconds), the user will be locked out for an unlimited time. In this case, you can remove the lockout using the [ALTER USER ...  LOGIN](../../yql/reference/syntax/alter-user.md) command.

The minimum lockout duration is 1 second.

Supported time units:

- Seconds: `30s`
- Minutes: `20m`
- Hours: `5h`
- Days: `3d`

It is not allowed to combine time units in one entry. For example, the entry `1d12h` is incorrect. It should be replaced with an equivalent, such as `36h`.

Default value: `1h`
||
|#

### Configuring Password Complexity Requirements {#password-complexity}

{{ ydb-short-name }} allows authenticating users by login and password. For more details, see the section [authentication by login and password](../../security/authentication.md#static-credentials). To improve security, {{ ydb-short-name }} provides the ability to configure the complexity of passwords used by [local users](../../concepts/glossary.md#access-user). To configure password requirements, you need to describe the `password_complexity` section.

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
| Specifies the minimum password length.

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

Valid values: `special_chars`

Default value: 0 (unlimited)
||
|| special_chars
| List of special characters allowed when setting a password.

Default value: empty (any of the `!@#$%^&*()_+{}\|<>?=` characters are allowed)
||
|| can_contain_username
| Indicates whether passwords can include a username.

Default value: empty string (allows using all valid special characters)
||
|| can_contain_username
| Flag determines whether the username can be included in the password.

Default value: `false`
||
|#

{% note info %}

Any changes to the password policy do not affect existing user passwords, so it is not necessary to change current passwords; they will be accepted as they are.

{% endnote %}

## Configuring LDAP Authentication {#ldap-auth-config}

One of the ways to authenticate users in {{ ydb-short-name }} is to use an [LDAP](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol) directory. For more details about this type of authentication, see the section on [using an LDAP directory](../../security/authentication.md#ldap). To configure LDAP authentication, you need to describe the `ldap_authentication` section.

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
| Root of the subtree in the LDAP directory from which the user record search will start
||
|| `bind_dn`
| Distinguished Name (DN) of the service account on whose behalf the user record search is performed
||
|| `bind_password`
| Password of the service account on whose behalf the user record search is performed. Not set when `extended_settings.enable_sasl_external_bind: true`
||
|| `search_filter`
| Filter for searching for a user record in the LDAP directory. The filter string may contain the *$username* character sequence, which will be replaced with the username requested for authentication in the database
||
|| `use_tls`
| Settings for configuring the TLS connection between {{ ydb-short-name }} and the LDAP server
||
|| `enable`
| Determines whether an attempt will be made to establish a TLS connection [using the `StartTls` request](../../security/authentication.md#starttls). When setting this parameter to `true`, you must disable the use of the `ldaps` connection scheme by setting the `ldap_authentication.scheme` parameter to `ldap`
||
|| `ca_cert_file`
| Path to the certificate authority file
||
|| `cert_require`
| Level of requirements for the LDAP server certificate.

Possible values:

- `NEVER` - {{ ydb-short-name }} does not request a certificate or accepts any presented certificate.
- `ALLOW` - {{ ydb-short-name }} requests a certificate from the LDAP server but will establish the TLS session even if the certificate is not trusted.
- `TRY` - {{ ydb-short-name }} requires a certificate from the LDAP server and terminates the connection if it is not trusted.
- `DEMAND` and `HARD` — These requirements are equivalent to the `TRY` parameter.

Default value: `DEMAND`
||
|| `cert_file`
| Path to the client certificate file. Used as authentication information for the [service account](../../security/authentication.md#ldap-service-account-auth).
||
|| `key_file`
| Path to the client certificate key file
||
|| `scheme`
| LDAP server connection scheme.

Possible values:

- `ldap` — {{ ydb-short-name }} will connect to the LDAP server without any encryption. Passwords will be sent to the LDAP server in plain text.
- `ldaps` — {{ ydb-short-name }} will establish an encrypted connection with the LDAP server over TLS from the very first request. To successfully establish a connection using the `ldaps` scheme, you need to disable the use of the [query `StartTls`](../../security/authentication.md#starttls) in the `ldap_authentication.use_tls.enable: false` section and fill in the certificate `ldap_authentication.use_tls.ca_cert_file` information and the certificate requirement level `ldap_authentication.use_tls.cert_require`.
- Any other value defaults to `ldap`.

Default value: `ldap`
||
|| `requested_group_attribute`
| Specifies the attribute used for reverse group membership. The default is `memberOf`.
||
|| `extended_settings.enable_nested_groups_search`
| Indicates whether to perform a request to retrieve the full hierarchy of groups to which the user's direct groups belong.

Possible values:

- `true` — {{ ydb-short-name }} requests information about all groups to which the user's direct groups belong. It might take a long time to traverse the entire hierarchy of nested parent groups.
- `false` — {{ ydb-short-name }} requests a flat list of groups, to which the user belongs. This request does not traverse possible nested parent groups.

Default value: `false`
||
|| `extended_settings.enable_sasl_external_bind`
| The flag determines whether [service account authentication](../../security/authentication.md#ldap-service-account-auth) will be performed using the SASL protocol with the EXTERNAL mechanism.

Possible values:

- `true` - The SASL protocol with the EXTERNAL mechanism will be used for service account authentication (authentication by client TLS certificate within mTLS). The client certificate specified in the `use_tls.cert_file` parameters and `use_tls.key_file` is used as authentication information. In this case, the `bind_dn` and `bind_password` parameters are not set.
- `false` - The simple bind method will be used for service account authentication. The `bind_dn` and `bind_password` parameters must be specified.

Default value: `false`
||
|| `host`
| The hostname where the LDAP server runs. This is a deprecated parameter; the `hosts` parameter should be used instead.
||
|| `ldap_authentication_domain`
| The username suffix that distinguishes users from the LDAP directory from users authenticated by other providers.

Default value: `ldap`
||
|#

## Client certificate authentication configuration {#certificate-auth-config}

{{ ydb-short-name }} supports [client certificate authentication](../../security/authentication.md#client-certificate). Certificate verification rules are set in the [client_certificate_authorization](client_certificate_authorization.md) section. Additionally, the `auth_config` section may specify a suffix for usernames of users authenticated by certificate:

#|
|| Parameter | Description ||
|| `certificate_authentication_domain`
| The username suffix that distinguishes users authenticated by client certificate from users authenticated by other methods.

Default value: `cert` (that is, the default SID suffix is `@cert`).
||
|#

## Configuring Third-Party IAM Authentication {#iam-auth-config}

{{ ydb-short-name }} supports user authentication using the [Yandex Identity and Access Management (IAM)](https://yandex.cloud/en/services/iam) service, which is used in Yandex Cloud, or another service compatible with it via API. To configure IAM authentication, the following parameters must be defined:

#|
|| Parameter | Description ||
|| use_access_service
| Indicates whether to allow authentication in Yandex Cloud using IAM AccessService.

Default value: `false` ("access service")
||
|| path_to_root_ca
| Specifies the path to the certification authority's certificate file that is used to interact with AccessService.

Default value: `as.private-api.cloud.yandex.net:4286`
||
|| use_access_service_tls
| The flag enables the use of TLS connections between {{ ydb-short-name }} and AccessService.

Default value: `true`
||
|| access_service_domain
| The suffix of the “user source” in [SID](../../concepts/glossary.md#access-sid) for users coming to {{ ydb-short-name }} from Yandex Cloud IAM.

Default value: `as`
||
|| access_service_grpc_keep_alive_time_ms
| Specifies the period of time, in milliseconds, after which a keepalive ping is sent on the transport to IAM AccessService.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
||
|| access_service_grpc_keep_alive_time_ms
| The time period, in milliseconds, after which {{ ydb-short-name }} sends a keepalive ping to the IAM server to keep the connection alive.

Default value: `10000`
||
|| access_service_grpc_keep_alive_timeout_ms
| Timeout period for waiting for a response from the IAM server to a keepalive ping, in milliseconds. If no response is received from the IAM server within the timeout, {{ ydb-short-name }} closes the connection.

Default value: `1000`
||
|| use_access_service_api_key
| Indicates whether to use IAM API keys. The API key is a secret key created in Yandex Cloud IAM for simplified authorization of service accounts with the Yandex Cloud API. Use API keys if requesting an IAM token automatically is not an option.

Default value: `false`
||
|#

## Configuring Caching for Authentication Results

During authentication, the user session receives an authentication token, which is passed with every request to the {{ ydb-short-name }} cluster. Since {{ ydb-short-name }} is a distributed system, user requests will eventually be processed on one or more {{ ydb-short-name }} nodes. Each {{ ydb-short-name }} node, upon receiving a request from the user, verifies the authentication token, and if the verification succeeds, generates a **user token** that is valid only within the current {{ ydb-short-name }} node and is used to authorize the actions requested by the user. Subsequent requests with the same authentication token to the same {{ ydb-short-name }} node no longer require authentication token verification and are executed under the user token.

The lifetime and other important aspects of the user token operation are configured in the {{ ydb-short-name }} configuration using the following parameters:

#|
|| refresh_period
| Specifies how often a {{ ydb-short-name }} node scans cached user tokens to find the ones that need to be refreshed because the `refresh_time`, `life_time` or `expire_time` interval elapses. The lower this parameter value, the higher the CPU load.

Default value: `1s`
||
|| refresh_time
| Specifies the time interval since the last user token update after which a {{ ydb-short-name }} node updates the user token again. The actual update will occur within the range from `refresh_time/2` to `refresh_time`.

Default value: `1h`
||
|| life_time
| Specifies the time interval for keeping a user token in {{ ydb-short-name }} node cache since its last use. If a {{ ydb-short-name }} node does not receive queries from a user within the specified time interval, the node deletes the user token from its cache.

Default value: `1h`
||
|| expire_time
| Specifies the time period, after which a user token is deleted from {{ ydb-short-name }} node cache. Deletion occurs regardless of the `life_time` interval.

{% note warning %}

If a third-party system has successfully authenticated in the {{ ydb-short-name }} node and regularly (more often than the `life_time` interval) sends requests to the same node, {{ ydb-short-name }} will detect the possible deletion or change in the user account privileges only after the `expire_time` interval elapses.

{% endnote %}

The shorter this time period, the more often {{ ydb-short-name }} nodes re-authenticate users and refresh their privileges. However, excessive user re-authentication slows down {{ ydb-short-name }}, especially so for external users. Setting this parameter to seconds negates the effect of caching user tokens.

Default value: `24h`
||
|| min_error_refresh_time
| Specifies minimum period of time that must elapse since a failed attempt (temporary failure) to refresh a user token before retrying the attempt.

Together with the `max_error_refresh_time`, determines the possible interval for a delay before retrying a failed attempt to refresh a user token. Each subsequent delay is increased till it reaches the `max_error_refresh_time` value. Retries continue until a user token is refreshed or the `expire_time` period elapses.

{% note warning %}

Setting this parameter to `0` is not recommended, because instant retries results in excessive load.

{% endnote %}

Default value: `1s`
||
|| max_error_refresh_time
| Specifies the maximum time interval that can elapse since a failed attempt (temporary failure) to refresh a user token before retrying the attempt.

Together with the `min_error_refresh_time`, determines the possible interval for a delay before retrying a failed attempt to refresh a user token. Each subsequent delay is increased till it reaches the `max_error_refresh_time` value. Retries continue until a user token is refreshed or the `expire_time` period elapses.

Default value: `1m`
||
|#

## Node registration token configuration {#node-registration-token}

{{ ydb-short-name }} allows you to configure the authentication method for database nodes when they register with a cluster. This is done via the `node_registration_token` parameter in the `auth_config` section.

#|
|| Parameter | Description ||
|| node_registration_token
| Defines the authentication type for database nodes when they are registered in the {{ ydb-short-name }} cluster.

Possible values:

- Empty string (`""`) — nodes authenticate using TLS certificates when registering with the cluster. In this mode, nodes must use certificates for authentication during the registration process. For details on configuring node authentication with certificates, see [Database node authentication and authorization](../../devops/configuration-management/configuration-v1/node-authorization.md).
- "root@builtin" is an authentication mode using a special debug token. This mode is planned to be removed in future releases and is not recommended for use: to ensure cluster security, it is recommended to use node authentication via TLS certificates by setting the parameter to an empty value.

||
|#

Example of an `auth_config` section with TLS certificate authentication enabled:


```yaml
auth_config:
  ...
  node_registration_token: ""
  ...
```
