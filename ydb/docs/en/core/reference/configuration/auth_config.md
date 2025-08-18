# auth_config

{{ ydb-short-name }} supports various user authentication methods. The configuration for authentication providers is specified in the `auth_config` section.

## Configuring Local {{ ydb-short-name }} User Authentication {#local-auth-config}

For more information about the authentication of [local {{ ydb-short-name }} users](../../concepts/glossary.md#access-user), see [{#T}](../../security/authentication.md#static-credentials). To configure authentication by username and password, define the following parameters in the `auth_config` section:

#|
|| Parameter | Description ||
|| use_login_provider
| Indicates whether to allow the authentication of local users with an [authentication token](../../concepts/glossary.md#auth-token) that is obtained after entering a username and password.

Default value: `true`
    ||
|| enable_login_authentication
| Indicates whether to allow adding local users to {{ ydb-short-name }} databases and generating authentication tokens after a local user enters a username and password.

Default value: `true`
    ||
|| domain_login_only
| Determines the scope of local user access rights in a {{ ydb-short-name }} cluster.

Valid values:

- `true` — local users exist in a {{ ydb-short-name }} cluster and can be granted rights to access multiple [databases](../../concepts/glossary.md#database).

- `false` — local users can exist either at the cluster or database level. The scope of access rights for local users created at the database level is limited to the database, in which they are created.

Default value: `true`
    ||
|| login_token_expire_time
| Specifies the expiration time of the authentication token created when a local user logs in to {{ ydb-short-name }}.

Default value: `12h`
    ||
|#

### Configuring User Lockout

You can configure {{ ydb-short-name }} to lock a user account out after a specified number of failed attempts to enter the correct password. To configure user lockout, define the `account_lockout` subsection inside the `auth_config` section.

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
| Specifies the number of failed attempts to enter the correct password for a user account, after which the account is blocked for a period specified by the `attempt_reset_duration` parameter.

If `attempt_threshold = 0`, the number of attempts to enter the correct password is unlimited. After successful authentication (correct username and password), the counter for failed attempts is reset to 0.

Default value: `4`
    ||
|| attempt_reset_duration
| Specifies the period that a locked-out account remains locked before automatically becoming unlocked. This period starts after the last failed attempt.

During this period, the user will not be able to authenticate in the system even if the correct username and password are entered.

If this parameter is set to zero ("0s" - a notation equivalent of 0 seconds), user accounts will be locked indefinitely. In this case you can unlock the account using the [ALTER USER ...  LOGIN](../../yql/reference/syntax/alter-user.md) command.

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

{{ ydb-short-name }} allows local users to authenticate using a login and password. For more information, see [authentication by login and password](../../security/authentication.md#static-credentials). To enhance security in {{ ydb-short-name }}, configure complexity requirements for the passwords of [local users](../../concepts/glossary.md#access-user) in the `password_complexity` subsection inside the `auth_config` section.

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

Default value: `0` (no requirements)
    ||
|| min_lower_case_count
| Specifies the minimum number of lowercase letters that a password must contain.

Default value: `0` (no requirements)
    ||
|| min_upper_case_count
| Specifies the minimum number of uppercase letters that a password must contain.

Default value: `0` (no requirements)
    ||
|| min_numbers_count
| Specifies the minimum number of digits that a password must contain.

Default value: `0` (no requirements)
    ||
|| min_special_chars_count
| Specifies the minimum number of special characters from the `special_chars` list that a password must contain.

Default value: `0` (no requirements)
    ||
|| special_chars
| Specifies a list of special characters that are allowed in a password.

Valid values: `!@#$%^&*()_+{}\|<>?=`

Default value: empty (any of the `!@#$%^&*()_+{}\|<>?=` characters are allowed)
    ||
|| can_contain_username
| Indicates whether passwords can include a username.

Default value: `false`
    ||
|#

{% note info %}

Any changes to the password policy do not affect existing user passwords, so it is not necessary to change current passwords; they will be accepted as they are.

{% endnote %}

## Configuring LDAP Authentication {#ldap-auth-config}

One of the user authentication methods in {{ ydb-short-name }} is using an LDAP directory. For more details, see [Interacting with the LDAP directory](../../security/authentication.md#ldap-auth-provider). To configure LDAP authentication, define the `ldap_authentication` section inside the `auth_config` section.

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
    use_tls:
      enable: true
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  ldap_authentication_domain: "ldap"
  scheme: "ldap"
  requested_group_attribute: "memberOf"
  extended_settings:
      enable_nested_groups_search: true

  refresh_time: "1h"
  #...
```

#|
|| Parameter | Description ||
|| `hosts`
| Specifies a list of hostnames where the LDAP server is running.
    ||
|| `port`
| Specifies the port used to connect to the LDAP server.
    ||
|| `base_dn`
| Specifies the root of the subtree in the LDAP directory from which the user entry search begins.
    ||
|| `bind_dn`
| Specifies the Distinguished Name (DN) of the service account used to search for the user entry.
    ||
|| `bind_password`
| Specifies the password for the service account used to search for the user entry.
    ||
|| `search_filter`
| Specifies a filter for searching the user entry in the LDAP directory. The filter string can include the sequence *$username*, which is replaced with the username requested for authentication in the database.
    ||
|| `use_tls`
| Configuration settings for the TLS connection between {{ ydb-short-name }} and the LDAP server.
    ||
|| `enable`
| Indicates whether a TLS connection [using the `StartTls` request](../../security/authentication.md#starttls) will be attempted. When set to `true`, the `ldaps` connection scheme should be disabled by setting `ldap_authentication.scheme` to `ldap`.
    ||
|| `ca_cert_file`
| Specifies the path to the certification authority's certificate file.
    ||
|| `cert_require`
| Specifies the certificate requirement level for the LDAP server.

Possible values:

- `NEVER` - {{ ydb-short-name }} does not request a certificate or accepts any presented certificate.
- `ALLOW` - {{ ydb-short-name }} requests a certificate from the LDAP server but will establish the TLS session even if the certificate is not trusted.
- `TRY` - {{ ydb-short-name }} requires a certificate from the LDAP server and terminates the connection if it is not trusted.
- `DEMAND`/`HARD` - These are equivalent to `TRY` and are the default setting, with the value set to `DEMAND`.
    ||
|| `ldap_authentication_domain`
| Specifies an identifier appended to the username to distinguish LDAP directory users from those authenticated using other providers.

Default value: `ldap`
    ||
|| `scheme`
| Specifies the connection scheme to the LDAP server.

Possible values:

- `ldap` - Connects without encryption, sending passwords in plain text.
- `ldaps` - Connects using TLS encryption from the first request. To use `ldaps`, disable the [`StartTls` request](../../security/authentication.md#starttls) by setting `ldap_authentication.use_tls.enable` to `false`, and provide certificate details in `ldap_authentication.use_tls.ca_cert_file` and set the certificate requirement level in `ldap_authentication.use_tls.cert_require`.
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
|| `host`
| Specifies the hostname of the LDAP server. This parameter is deprecated and should be replaced with the `hosts` parameter.
    ||
|#

## Configuring Third-Party IAM Authentication {#iam-auth-config}

{{ ydb-short-name }} supports Yandex Identity and Access Management (IAM) used in Yandex Cloud for user authentication. To configure IAM authentication, define the following parameters:

#|
|| Parameter | Description ||
|| use_access_service
| Indicates whether to allow authentication in Yandex Cloud using IAM AccessService.

Default value: `false`
    ||
|| access_service_endpoint
| Specifies an IAM AccessService address, to which {{ ydb-short-name }} sends requests.

Default value: `as.private-api.cloud.yandex.net:4286`
    ||
|| use_access_service_tls
| Indicates whether to use TLS connections between {{ ydb-short-name }} and AccessService.

Default value: `true`
    ||
|| access_service_domain
| Specifies an identifier appended to the username in [SID](../../concepts/glossary.md#access-sid) to distinguish Yandex Cloud IAM users from those authenticated using other providers.

Default value: `as` ("access service")
    ||
|| path_to_root_ca
| Specifies the path to the certification authority's certificate file that is used to interact with AccessService.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
    ||
|| access_service_grpc_keep_alive_time_ms
| Specifies the period of time, in milliseconds, after which a keepalive ping is sent on the transport to IAM AccessService.

Default value: `10000`
    ||
|| access_service_grpc_keep_alive_timeout_ms
| Specifies the amount of time, in milliseconds, that {{ ydb-short-name }} waits for the acknowledgement of the keepalive ping from IAM AccessService. If {{ ydb-short-name }} does not receive an acknowledgment within this time, it will close the connection.

Default value: `1000`
    ||
|| use_access_service_api_key
| Indicates whether to use IAM API keys. The API key is a secret key created in Yandex Cloud IAM for simplified authorization of service accounts with the Yandex Cloud API. Use API keys if requesting an IAM token automatically is not an option.

Default value: `false`
    ||
|#

## Configuring Caching for Authentication Results

During the authentication process, a user session receives an authentication token, which is transmitted along with each request to the cluster {{ydb-short-name }}. Since {{ydb-short-name }} is a distributed system, user requests will eventually be processed on one or more {{ydb-short-name }} nodes. After receiving a request from the user, a {{ydb-short-name }} node verifies the authentication token. If successful, the node generates a **user token**, which is valid only inside the current node and is used to authorize the actions requested by the user. Subsequent requests with the same authentication token to the same node do not require verification of the authentication token.

To configure the life cycle and other important aspects of managing user tokens, define the following parameters:

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

If a third-party system has successfully authenticated in the {{ydb-short-name }} node and regularly (more often than the `life_time` interval) sends requests to the same node, {{ydb-short-name }} will detect the possible deletion or change in the user account privileges only after the `expire_time` interval elapses.

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
