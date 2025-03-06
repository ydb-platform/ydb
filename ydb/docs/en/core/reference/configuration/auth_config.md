# `auth_config` section

{{ ydb-short-name }} supports various user authentication methods. The configuration for authentication providers is specified in the `auth_config` section.

## Configuring internal {{ ydb-short-name }} user authentication {#local-auth-config}

Internal {{ ydb-short-name }} users are added directly to a {{ ydb-short-name }} database without using third-party directory services. For more information about this authentication method, see [{#T}](../../security/authentication.md#static-credentials). To configure authentication by user name and password, define the following parameters in the `auth_config` section.

#|
|| Parameter | Description ||
|| UseLoginProvider
| Indicates whether to allow authentication of internal users with an auth-token that is received after entering a user name and password.

Default value: `true`
    ||
|| EnableLoginAuthentication
| Indicates whether to allow adding internal users to {{ ydb-short-name }} databases and generating auth-tokens after an internal user enters a user name and password.

Default value: `true`
    ||
|| DomainLoginOnly
| Determines the databases, to which internal users are added.

Valid values:

- `true` – internal users are added only to the root database.
- `false` – internal users are added to the root and to tenant databases.

Default value: `true`
    ||
|| LoginTokenExpireTime
| Specifies the expiration time of the authentication token that is created when an internal user logs in to {{ ydb-short-name }}.

Default value: `12h`
    ||
|#

### Configuring user lockout

You can configure {{ ydb-short-name }} to lock a user account out after the specified number of failed attempts to enter a valid password. To configure user lockout, define the `AccountLockout` section inside the `auth_config` section:

#|
|| Parameter | Description ||
|| AttemptThreshold
| Specifies the number of failed attempts to enter a valid password for a user account, after which the user account is temporarily blocked.

Default value: `4`
    ||
|| AttemptResetDuration
| Specifies the period of time that a locked-out account remains locked before automatically becoming unlocked.

Default value: `1h`
    ||
|#

### Configuring password complexity requirements

To configure password complexity requirements, define the following parameters in the `PasswordComplexity` subsection inside the `auth_config` section.

#|
|| Parameter | Description ||
|| MinLength
| Specifies the minimum password length
    ||
|| MinLowerCaseCount
| Specifies the minimum number of lower case letters that a password must contain
    ||
|| MinUpperCaseCount
| Specifies the minimum number of upper case letters that a password must contain
    ||
|| MinNumbersCount
| Specifies the minimum number of digits that a password must contain
    ||
|| MinSpecialCharsCount
| Specifies the minimum number of special characters that a password must contain
    ||
|| SpecialChars
| Specifies a list of special characters that are allowed in a password
    ||
|| CanContainUsername
| Indicates whether passwords can include a user name
    ||
|#

## Configuring third-party IAM authentication {#iam-auth-config}

{{ ydb-short-name }} supports Yandex Identity and Access Management (IAM) used in Yandex Cloud for user authentication. To configure IAM authentication, define the following parameters:

#|
|| Parameter | Description ||
|| UseAccessService
| Indicates whether to allow IAM AccessService authentication.

Default value: `false`
    ||
|| AccessServiceEndpoint
| Specifies an IAM AccessService address, to which {{ ydb-short-name }} sends requests.

Default value: `as.private-api.cloud.yandex.net:4286`
    ||
|| UserAccountServiceEndpoint
| Specifies an IAM AccessService address, to which {{ ydb-short-name }} sends requests to access user accounts.

Default value: `api-adapter.private-api.cloud.yandex.net:8443`
    ||
|| ServiceAccountServiceEndpoint
| Specifies an IAM AccessService address, to which {{ ydb-short-name }} sends requests to access service accounts.

Default value: `api-adapter.private-api.cloud.yandex.net:8443`
    ||
|| UseAccessServiceTLS
| Indicates whether to use TLS connections between {{ ydb-short-name }} and IAM AccessService сервером.

Default value: `true`
    ||
|| AccessServiceDomain
| Specifies an identifier appended to the username to distinguish AccessService directory users from those authenticated using other providers.

Default value: `as`
    ||
|| PathToRootCA
| Specifies the path to the certification authority's certificate file.

Default value: `/etc/ssl/certs/YandexInternalRootCA.pem`
    ||
|| AccessServiceGrpcKeepAliveTimeMs
| Specifies the period of time, in milliseconds, after which a keepalive ping is sent on the transport to IAM AccessService.

Default value: `10000`
    ||
|| AccessServiceGrpcKeepAliveTimeoutMs
| Specifies the amount of time, in milliseconds, that {{ ydb-short-name }} waits for the acknowledgement of the keepalive ping from IAM AccessService. If {{ ydb-short-name }} does not receive an acknowledgment within this time, it will close the connection.

Default value: `1000`
    ||
|| UseAccessServiceApiKey
| Indicates whether to use API keys. The API key is a secret key only used for simplified authorization of service accounts with the Yandex Cloud API. Use API keys if requesting an IAM token automatically is not an option.

Default value: `false`
    ||
|#

## Configuring LDAP authentication {#ldap-auth-config}

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

| Parameter | Description |
| --- | --- |
| `hosts` | Specifies a list of hostnames where the LDAP server is running. |
| `port` | Specifies the port used to connect to the LDAP server. |
| `base_dn` | Specifies the root of the subtree in the LDAP directory from which the user entry search begins. |
| `bind_dn` | Specifies the Distinguished Name (DN) of the service account used to search for the user entry. |
| `bind_password` | Specifies the password for the service account used to search for the user entry. |
| `search_filter` | Specifies a filter for searching the user entry in the LDAP directory. The filter string can include the sequence *$username*, which is replaced with the username requested for authentication in the database. |
| `use_tls` | Configuration settings for the TLS connection between {{ ydb-short-name }} and the LDAP server. |
| `enable` | Indicates whether a TLS connection [using the `StartTls` request](../../security/authentication.md#starttls) will be attempted. When set to `true`, the `ldaps` connection scheme should be disabled by setting `ldap_authentication.scheme` to `ldap`. |
| `ca_cert_file` | Specifies the path to the certification authority's certificate file. |
| `cert_require` | Specifies the certificate requirement level for the LDAP server.<br>Possible values:<ul><li>`NEVER` - {{ ydb-short-name }} does not request a certificate or accepts any presented certificate.</li><li>`ALLOW` - {{ ydb-short-name }} requests a certificate from the LDAP server but will establish the TLS session even if the certificate is not trusted.</li><li>`TRY` - {{ ydb-short-name }} requires a certificate from the LDAP server and terminates the connection if it is not trusted.</li><li>`DEMAND`/`HARD` - These are equivalent to `TRY` and are the default setting, with the value set to `DEMAND`.</li></ul> |
| `ldap_authentication_domain` | Specifies an identifier appended to the username to distinguish LDAP directory users from those authenticated using other providers. The default value is `ldap`. |
| `scheme` | Specifies the connection scheme to the LDAP server.<br>Possible values:<ul><li>`ldap` - Connects without encryption, sending passwords in plain text. This is the default value.</li><li>`ldaps` - Connects using TLS encryption from the first request. To use `ldaps`, disable the [`StartTls` request](../../security/authentication.md#starttls) by setting `ldap_authentication.use_tls.enable` to `false`, and provide certificate details in `ldap_authentication.use_tls.ca_cert_file` and set the certificate requirement level in `ldap_authentication.use_tls.cert_require`.</li><li>Any other value defaults to `ldap`.</li></ul> |
| `requested_group_attribute` | Specifies the attribute used for reverse group membership. The default is `memberOf`. |
| `extended_settings.enable_nested_groups_search` | Indicates whether to perform a request to retrieve the full hierarchy of groups to which the user's direct groups belong. |
| `host` | Specifies the hostname of the LDAP server. This parameter is deprecated and should be replaced with the `hosts` parameter. |

## Configuring token life cycle

Parameters for configuring the token life cycle are applicable to all authentication methods.

#|
|| RefreshPeriod
| Specifies the time interval for detecting expired tokens

Default value: `1s`
    ||
|| RefreshTime
| Specifies the time interval for refreshing user information. The actual update will occur within the range from `refresh_time/2` to `refresh_time`.

Default value: `1h`
    ||
|| LifeTime
| Specifies the time interval for keeping a token in cache since its last use.

Default value: `1h`
    ||
|| ExpireTime
| Specifies the time period, after which a token expires and is deleted from cache.

Default value: `24h`
    ||
|| MinErrorRefreshTime
| Specifies minimum period of time that must elapse since a failed attempt to refresh a token before retrying the attempt.

Default value: `1s`
    ||
|| MaxErrorRefreshTime
| Specifies the maximum time interval that can elapse since a failed attempt to refresh a token before retrying the attempt.

Default value: `1m`
    ||
|#