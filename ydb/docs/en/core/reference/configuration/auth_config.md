# Section `auth_config`

{{ ydb-short-name }} supports various user authentication methods. The configuration for authentication providers is specified in the `auth_config` section.

## Configuring LDAP authentication {#ldap-auth-config}

One of the user authentication methods in {{ ydb-short-name }} is with an LDAP directory. More details about this type of authentication can be found in the section on [interacting with the LDAP directory](../../security/authentication.md#ldap-auth-provider). To configure LDAP authentication, the `ldap_authentication` section must be defined.

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
| `hosts` | A list of hostnames where the LDAP server is running. |
| `port` | The port used to connect to the LDAP server. |
| `base_dn` | The root of the subtree in the LDAP directory from which the user entry search begins. |
| `bind_dn` | The Distinguished Name (DN) of the service account used to search for the user entry. |
| `bind_password` | The password for the service account used to search for the user entry. |
| `search_filter` | A filter for searching the user entry in the LDAP directory. The filter string can include the sequence *$username*, which is replaced with the username requested for authentication in the database. |
| `use_tls` | Configuration settings for the TLS connection between {{ ydb-short-name }} and the LDAP server. |
| `enable` | Determines if a TLS connection [using the `StartTls` request](../../security/authentication.md#starttls) will be attempted. When set to `true`, the `ldaps` connection scheme should be disabled by setting `ldap_authentication.scheme` to `ldap`. |
| `ca_cert_file` | The path to the certification authority's certificate file. |
| `cert_require` | Specifies the certificate requirement level for the LDAP server.<br>Possible values:<ul><li>`NEVER` - {{ ydb-short-name }} does not request a certificate or accepts any presented certificate.</li><li>`ALLOW` - {{ ydb-short-name }} requests a certificate from the LDAP server but will establish the TLS session even if the certificate is not trusted.</li><li>`TRY` - {{ ydb-short-name }} requires a certificate from the LDAP server and terminates the connection if it is not trusted.</li><li>`DEMAND`/`HARD` - These are equivalent to `TRY` and are the default setting, with the value set to `DEMAND`.</li></ul> |
| `ldap_authentication_domain` | An identifier appended to the username to distinguish LDAP directory users from those authenticated using other providers. The default value is `ldap`. |
| `scheme` | The connection scheme to the LDAP server.<br>Possible values:<ul><li>`ldap` - Connects without encryption, sending passwords in plain text. This is the default value.</li><li>`ldaps` - Connects using TLS encryption from the first request. To use `ldaps`, disable the [`StartTls` request](../../security/authentication.md#starttls) by setting `ldap_authentication.use_tls.enable` to `false`, and provide certificate details in `ldap_authentication.use_tls.ca_cert_file` and set the certificate requirement level in `ldap_authentication.use_tls.cert_require`.</li><li>Any other value defaults to `ldap`.</li></ul> |
| `requested_group_attribute` | The attribute used for reverse group membership. The default is `memberOf`. |
| `extended_settings.enable_nested_groups_search` | A flag indicating whether to perform a request to retrieve the full hierarchy of groups to which the user's direct groups belong. |
| `host` | The hostname of the LDAP server. This parameter is deprecated and should be replaced with the `hosts` parameter. |
| `refresh_time` | Specifies the interval for refreshing user information. The actual update will occur within the range from `refresh_time/2` to `refresh_time`. |
