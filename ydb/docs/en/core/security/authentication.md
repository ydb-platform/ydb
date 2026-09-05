# Authentication

After a network connection is successfully established, the server accepts requests from the client for processing. Authentication information is passed as an [authentication token](../concepts/glossary.md#auth-token) or a [client certificate](../concepts/glossary.md#client-certificate). Based on it, the server determines the client's [SID](../concepts/glossary.md#access-sid) and checks its permissions to execute the request.

{% note info %}

An authentication client is a user who undergoes the authentication procedure when accessing {{ ydb-short-name }}. Examples of clients include applications that use the [SDK](../reference/ydb-sdk/index.md) or [CLI](../reference/ydb-cli/index.md).

{% endnote %}

The following authentication types are supported:

* [Anonymous](#anonymous) authentication.
* Authentication using [login and password](#static-credentials).
* Authentication using an [LDAP directory](#ldap).
* Authentication using a [client certificate](#client-certificate).
* [Authentication using a third-party IAM provider](#iam), such as [Yandex Identity and Access Management](https://yandex.cloud/en/docs/iam/).

## Anonymous authentication {#anonymous}

By default, {{ ydb-short-name }} allows requests to be executed without specifying authentication data, such as a username or [token](../concepts/glossary.md#auth-token). Access rights checking ([authorization](authorization.md)) is also not performed in this case.

{% note warning %}

Anonymous authentication should only be used for evaluation purposes for local databases that have no network access.

{% endnote %}

The `enforce_user_token_requirement` flag in the [authentication mode settings](../reference/configuration/security_config.md#security-auth) of {{ ydb-short-name }} is responsible for disabling anonymous authentication.

Depending on the authentication mode settings, the actual authentication may not be anonymous:

- A token missing from requests may be replaced with a default token
- A token explicitly specified in requests may be checked according to the appropriate rules

Then requests will be executed non-anonymously, and access rights checking will also be performed.

Depending on the [access level settings](../reference/configuration/security_config.md#security-access-levels), anonymous requests may also perform actions in the system that require administrative access level.

## Authentication by login and password {#static-credentials}

Authentication by login and password through the {{ ydb-short-name }} server is available only for [local users](../concepts/glossary.md#access-user). External user authentication involves servers of external systems.

This type of access implies that each database user has a login and password.
A user login may contain only lowercase Latin letters, digits, and the `@` character.
Various [criteria](#password-complexity) for password complexity may be set.

The user login and password hash are stored in a table inside the authentication component. The password is hashed using the [Argon2](https://en.wikipedia.org/wiki/Argon2) method. Only the system administrator has access to this table.

In response to login and password, an [authentication token](../concepts/glossary.md#auth-token) is returned. The default token lifetime is 12 hours. For token rotation, the client, for example, the [SDK](../reference/ydb-sdk/index.md), independently contacts the authentication service. Using a token speeds up the authentication process and increases security.

The authentication process using a login and password includes the following steps:

1. The client accesses the database and passes the user's login and password to the authentication service {{ ydb-short-name }}.
2. The service verifies the authentication data, and upon successful matching, creates a token and returns it to the client.
3. The client accesses the database, passing the token as authentication information.

To enable login and password authentication, you need to ensure that the parameters `use_login_provider` and `enable_login_authentication` are set to the default value `true` in the [configuration](../reference/configuration/auth_config.md). Additionally, to disable anonymous authentication, you need to set the value of the parameter [`enforce_user_token_requirement` to `true`](../reference/configuration/security_config.md).

For information about managing roles and users, see [{#T}](../security/authorization.md).

### Password complexity {#password-complexity}

{{ ydb-short-name }} allows you to configure password complexity requirements. If a password provided via the `CREATE USER` or `ALTER USER` commands does not meet the complexity criteria, the command execution will fail.
By default, no restrictions are imposed on passwords: a password of any length is accepted, including an empty string; the password may contain any number of digits and letters in any case, as well as special characters from the list `!@#$%^&*()_+{}|<>?=`. To set password complexity restrictions, you need to fill in the `password_complexity` section in the [configuration](../reference/configuration/auth_config.md#password-complexity).

### Forced user lock/unlock

There is another way to prevent a user from authenticating: forced blocking by a cluster or database administrator. Administrators can unblock both users who were forcibly blocked and users who were blocked due to exceeding the limit on the number of incorrect password attempts. For detailed information about forced blocking and unblocking of users, see the description of the [`ALTER USER LOGIN/NOLOGIN`](../yql/reference/syntax/alter-user.md) command.

### Password brute-force protection

{{ ydb-short-name }} provides protection against password brute force by a user. A user will be considered blocked if they exceed the number of incorrect password attempts. After the specified time has elapsed, they will be able to authenticate again.

{% note info %}

This mechanism applies only to users served by {{ ydb-short-name }} itself, the so-called built-in users. Users served by external authentication sources, such as LDAP servers, are not subject to the password brute-force protection mechanism.

{% endnote %}

By default, the user is given 4 attempts to enter the correct password. Otherwise, authentication will be blocked for them for one hour. You can configure user blocking criteria in the [configuration](../reference/configuration/auth_config.md#account-lockout).

If necessary, the cluster or database administrator can [unblock](../yql/reference/syntax/alter-user.md) the user ahead of time.

Information about the user's lock status and the number of incorrect password attempts can be obtained from the [system view](../dev/system-views.md#users) of the user.

## Authentication using an LDAP directory {#ldap}

{{ ydb-short-name }} integrates interaction with the [LDAP directory](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol). The LDAP directory is an external service relative to {{ ydb-short-name }} and is used for authenticating and authorizing database users. Before using this authentication and authorization method, you must have a deployed LDAP service and configured network access between it and the {{ ydb-short-name }} servers.

Examples of supported LDAP directory implementations: [OpenLdap](https://openldap.org/), [Active Directory](https://azure.microsoft.com/en-us/products/active-directory/).

### Authentication

Authentication using the LDAP protocol is similar to the authentication process by login and password. The only difference is that the LDAP directory plays the role of the authentication component. The LDAP directory is used to verify the login/password pair and to determine the groups to which the user belongs.

{% note info %}

Since the LDAP directory is an external independent service, {{ ydb-short-name }} cannot manage user accounts in the directory. For successful authentication, the user must already exist in the LDAP directory. Using the `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, `DROP GROUP` commands will not affect the list of users and groups in the directory. For information about managing accounts, refer to the documentation of the LDAP directory being used.

{% endnote %}

Currently, {{ ydb-short-name }} supports only one method of LDAP user authentication — *search+bind*: after receiving the login and password, a service *bind* is performed on behalf of [service account](#ldap-service-account-auth) (login and password in `bind_dn` or `bind_password`, or certificate and SASL EXTERNAL — see [configuration](../reference/configuration/auth_config.md#ldap-auth-config)), then a search for the user record and a second *bind* on behalf of the user.

{% note info %}

A service account is a separate account in the LDAP directory that applications or services use to connect to LDAP and perform required operations.

{% endnote %}

Credentials of the service account for connecting to LDAP are specified in the configuration settings: use the `bind_dn` and `bind_password` parameters, or configure [mTLS](../concepts/glossary.md#mtls) (for details, see the section [Service account authentication](#ldap-service-account-auth)).

Next, the authentication process proceeds as follows:

1. {{ ydb-short-name }} connects to LDAP on behalf of the service account.
2. After a successful connection, a search is performed for the user attempting to authenticate. The search runs across the entire subtree specified in the configuration parameter `base_dn` and according to the filter set in the parameter `search_filter`.
3. If the user is found, {{ ydb-short-name }} performs a second bind operation — this time on behalf of the found user, using their password.
4. The final result — successful or unsuccessful authentication — is determined by the outcome of the second bind (on behalf of the user).

Thus, {{ ydb-short-name }} does not store user passwords and fully relies on the LDAP authentication mechanism.

As a result of successful verification of the user's login and password in the LDAP directory, {{ ydb-short-name }} returns an [authentication token](../concepts/glossary.md#auth-token). This token is then used instead of the login and password. Using the token speeds up the authentication process and improves security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

#### Service account authentication {#ldap-service-account-auth}

A service account can be authenticated in two main ways:

* Using a login and password.  \nIn this case, you need to specify the login (`bind_dn`) and password (`bind_password`) in the configuration. These parameters will be used to connect to the LDAP server on behalf of the service account.
* Using mTLS (mutual TLS) via the SASL EXTERNAL mechanism.  \nIn this option, certificates are used for authentication instead of a login and password. This allows you to avoid storing the service account password in the configuration — you only need to specify the certificate file (`use_tls.cert_file`) and private key file (`use_tls.key_file`), and also enable a special flag (`extended_settings.enable_sasl_external_bind`). For detailed configuration information, see the section [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config).

### Token verification

After user authentication in the system, a token is generated, which is checked before executing the requested operation. During token verification, it is determined on behalf of which user the action is requested in the system and in which groups they belong. For users from the LDAP directory, the token does not contain group information, so after token verification, another request is made to the LDAP server to obtain the list of groups the user belongs to.

Groups, like the user themselves, are subjects of operations on database schema objects. To differentiate access to various database resources, subjects can be assigned access rights. And according to the list of assigned rights, subjects will be authorized to perform certain operations.

The process of obtaining the list of user groups from the LDAP directory is similar to the actions performed during authentication. First, a *bind* operation is performed for the service user whose credentials are recorded in the parameters `bind_dn` and `bind_password` of the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file. After successful authentication, a search is performed for the user record for which the token was previously generated. The search is also performed in accordance with the parameter `search_filter`. If the user still exists, the returned result of the *search* operation will be a list of values of the attribute recorded in the parameter `requested_group_attribute`. If this parameter is empty, the attribute of reverse group membership will be `memberOf`. The attribute `memberOf` stores the unique names (Distinguished Name, DN) of the groups the user belongs to.

#### Obtaining groups

By default, {{ ydb-short-name }} searches only for the groups in which the user is directly a member. By enabling the `extended_settings.enable_nested_groups_search` flag of the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section, {{ ydb-short-name }} will attempt to obtain groups at all nesting levels, not only those the user belongs to directly. If {{ ydb-short-name }} is configured to work with Active Directory, an Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN) will be used to search for all nested groups. This rule allows obtaining all nested groups with a single request. For LDAP servers based on OpenLDAP, group search will be performed by recursive graph traversal, which generally requires multiple requests. For both Active Directory and OpenLDAP, group search will be performed only for the subtree whose root is taken from the configuration parameter `base_dn`.

{% note info %}

In the current implementation, the group names that {{ ydb-short-name }} will operate with match the values written in the `memberOf` attribute. They can be long and hard to read.

Example:


```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

In the section of the configuration file that describes authentication information, you can configure how often information about a user and their groups is updated. This value is controlled by the `refresh_time` parameter. For more information about configuration files, see the section on [cluster configuration](../reference/configuration/auth_config.md#auth-config).

{% endnote %}

{% note warning %}

Note that currently {{ ydb-short-name }} cannot track group renames made on the LDAP server side. As a result, a group with a new name will not have the same permissions that the group with the old name had.

{% endnote %}

### LDAP users and LDAP groups in {{ ydb-short-name }}

Since {{ ydb-short-name }} allows different user authentication methods, when working with user and group names it is often useful to distinguish where exactly the user was authenticated. For all authentication types except login and password authentication, group and user names are appended with a suffix of the form `@<auth-domain>`.

For LDAP users, *auth-domain* is set in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) `ldap_authentication_domain`. By default it has the value `ldap`, so all names of users authenticated via the LDAP directory and the names of groups they belong to will look as follows in {{ ydb-short-name }}:

- `user1@ldap`
- `group1@ldap`
- `group2@ldap`

{% note warning %}

To distinguish that the entered login must be a login of a user from the LDAP directory rather than a login of a local {{ ydb-short-name }} user, you need to add the `@ldap` suffix to it.

Below are examples of authenticating user `user1` using the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md):

* Authenticating a user from the LDAP directory: `ydb --user user1@ldap -p ydb_profile scheme ls`
* Authenticating a user with the internal {{ ydb-short-name }} mechanism: `ydb --user user1 -p ydb_profile scheme ls`

{% endnote %}

### TLS connection {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or an unencrypted connection. An encrypted connection to the LDAP server is established using the TLS protocol. This method is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically. The [`ldaps`](#ldaps) connection scheme is used
* Using the LDAP protocol extension [`StartTls`](#starttls)

When using an unencrypted connection, all data transmitted in requests to the LDAP server is sent in plain text, including passwords. This connection method is easier to start using and is better suited for experiments or testing.

#### LDAPS

For {{ ydb-short-name }} to automatically establish an encrypted connection to the LDAP server, set the value `ldaps` in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) **scheme**. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the `ldaps` scheme. The LDAP server must be configured to accept TLS connections on the specified ports.

#### LDAP protocol extension `StartTls` {#starttls}

`StartTls` is an extension of the LDAP protocol used to encrypt messages over TLS. It allows sending some messages in encrypted form and others in plaintext within a single connection to the LDAP server. A message with this extension is sent from {{ ydb-short-name }} to the LDAP server to initiate a TLS connection. In the case of {{ ydb-short-name }}, enabling and disabling a TLS connection within a single connection is not supported. Therefore, when using the `StartTls` extension, after establishing an encrypted connection, {{ ydb-short-name }} will send all further messages to the LDAP server in encrypted form. One of the advantages of using this extension instead of the `ldaps` scheme (with appropriate LDAP server configuration) is the ability to establish a TLS connection on an unencrypted port. The extension is enabled in [the `use_tls` section](../reference/configuration/auth_config.md#ldap-auth-config) of the configuration file.

## Client certificate authentication {#client-certificate}

{{ ydb-short-name }} can authenticate a client based on the client certificate data obtained during TLS connection establishment. The verification is performed at the application protocol level (gRPC and others), when the server already accepts requests over an open connection.

This method is suitable, for example, in corporate scenarios with centralized certificate issuance.

### How it works

1. The client establishes a TLS connection with the {{ ydb-short-name }} server, providing the client certificate (and the trust chain).
2. When processing a request, the server extracts the certificate from the TLS context.
3. The server uses the certificate for authentication and checks it against the rules of the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section.
4. As a result of successful certificate verification, the client is assigned a security identifier [SID](../concepts/glossary.md#access-sid), which has all the [rights](../concepts/glossary.md#access-right) assigned to the corresponding identifier.

Certificate authentication is used only for requests without an [authentication token](../concepts/glossary.md#auth-token). If the client passes an authentication token — for example, in the `Authorization` header for HTTP or through SDK/CLI mechanisms for IAM, login, and password — the token takes precedence. In this case, the certificate is transmitted at the TLS level but is not used for authentication.

### SID formation

{% note info %}

Client certificate verification for [device authentication](#device-auth) and user authentication by client certificate are different mechanisms. Device authentication restricts the network perimeter without forming a SID; user authentication by client certificate forms a SID and groups for [authorization](./authorization.md).

{% endnote %}

Successful certificate authentication creates a user SID with the suffix `@<domain>`, where `<domain>` is the [parameter value](../reference/configuration/auth_config.md#certificate-auth-config) `certificate_authentication_domain` in the `auth_config` section (default: `cert`). The name is formed from all attributes of the certificate's Subject field in `Имя=Значение,...@<domain>` notation. The attribute order matches the order of fields in the certificate. Example:


```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```


### Getting groups

If the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section specifies `client_certificate_definitions` blocks, the certificate is accepted if it matches at least one of them. For each matching block, the client is included in the groups from `member_groups`. If `member_groups` is not specified, the default group is used — `default_group` (default value: `DefaultClientAuth@cert`).

### Server configuration

Certificate verification and group assignment rules are set in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section of the cluster's static configuration. To enable client certificate requests during TLS handshake over gRPC, set the `request_client_certificate: true` parameter.

### Client configuration

For more information about configuring [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), see the section [TLS connection parameters](../reference/ydb-cli/connect.md#tls).

## Certificate-based device authentication {#device-auth}

Device authentication is the verification of the [client certificate](../concepts/glossary.md#client-certificate) when establishing a TLS connection. [SID](../concepts/glossary.md#access-sid) is not generated in this case. If a certificate is presented, the chain of trust to the CA is verified. An untrusted certificate causes the connection to be rejected before application requests are processed. Whether a certificate must be presented depends on the interface (see the section [Usage in {{ ydb-short-name }}](#device-auth-interfaces)).

### Why device authentication is needed {#device-auth-motivation}

Device authentication solves the following tasks in {{ ydb-short-name }}:

1. Cluster isolation: restrict the set of hosts and applications that can establish a TLS connection with {{ ydb-short-name }} nodes.
2. Protection against configuration errors: prevent connections to foreign {{ ydb-short-name }} clusters, for example, if the [node-broker](../devops/configuration-management/configuration-v1/node-authorization.md) parameter is incorrect, a dynamic node will not connect to a foreign cluster, whereas with regular TLS such a connection could be established.
3. Complicating attacks on the application layer: a process on a foreign host without a suitable certificate does not get access to the cluster API, even if a network route to the port exists.

After device authentication, user or application [authentication](./authentication.md) may be required to access data. It can be performed not only using a verified client certificate, but also using other authentication methods in {{ ydb-short-name }}, for example, by [login and password](./authentication.md#static-credentials).

### How it works {#device-auth-how-it-works}

1. The client establishes a TLS connection with the {{ ydb-short-name }} server and, if the connection interface requires it, presents a client certificate.
2. If a certificate is presented, the server verifies it at the TLS level: the chain of trust to the configured certificate authority (CA), expiration date, and so on. Additional rules for matching certificate fields (for example, `require_same_issuer`, Subject, and SAN) are applied during [client certificate authentication](./authentication.md#client-certificate).
3. If verification succeeds, the connection is opened. If it fails, it is rejected.

### Usage in {{ ydb-short-name }} {#device-auth-interfaces}

Device authentication is optional and configured independently: the mechanism can be enabled on some ports and disabled on others.

- **Interconnect** — when TLS is enabled in the [interconnect_config](../reference/configuration/tls.md#interconnect) section, [Interconnect](../concepts/glossary.md#actor-system-interconnect) requires a client certificate.
- **Kafka API**: when mTLS is enabled, requires a client certificate; only the trust chain to the CA is checked, a connection without a certificate or with an untrusted certificate is not established. The server configuration is described in the [kafka_proxy_config](../reference/configuration/kafka_proxy_config.md) section, and the client connection in the [Device authentication via mTLS](../reference/kafka-api/auth.md#device-auth) section.
- **gRPC** and **YDB Monitoring**: you can enable a client certificate request for device authentication, and also separately enable its mandatory verification (an untrusted certificate is always rejected). The gRPC configuration is described in the [grpc_config](../reference/configuration/tls.md#grpc) and [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) sections, and the client connection in the [TLS connection parameters](../reference/ydb-cli/connect.md#tls) section; the YDB Monitoring configuration is described in the [monitoring_config](../reference/configuration/monitoring_config.md#tls) section.

## Authentication using a third-party IAM provider {#iam}

* **Access Token**: a fixed token is set as a parameter for the client (SDK or CLI) and is passed in requests.
* **Refresh Token**: an [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account is set as a parameter for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token passed in requests.
* **Service Account Key**: the attributes of a service account and a signing key are set as parameters for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token passed in requests.
* **Metadata**: the client (SDK or CLI) periodically accesses a local service to rotate (obtain the next) token passed in requests.
* **OAuth 2.0 token exchange**: the client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), which is then passed in requests to the {{ ydb-short-name }} API.

Any holder of a valid token can gain access to perform operations, so the main task of the security system is to ensure the secrecy of the token and prevent its compromise.

Authentication modes with token rotation, **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the mode with a fixed token, **Access Token**, because only short-lived secrets are transmitted over the network to the {{ ydb-short-name }} server.

Maximum security and performance are achieved when using the **Metadata** mode, since it eliminates the need to handle secrets during application deployment and lets you access IAM and cache the token ahead of time, before the application starts.

When choosing an authentication mode among those supported by the server and environment, follow these recommendations:

* **Anonymous** is typically used on self-deployed local {{ ydb-short-name }} clusters that are not accessible over the network.
* **Access Token** is used when the server does not support other modes or for configuration and debugging purposes. It does not require client interactions with IAM. However, if IAM supports an API for token rotation, the fixed tokens typically issued by such IAM have a short lifetime, which forces you to manually renew them in IAM on a regular basis.
* **Refresh Token** can be used for one-off manual operations under a personal account, for example, those related to maintaining data in the database, running ad-hoc CLI operations, or launching applications from a workstation. Such a token can be obtained manually in IAM once for a long period and stored in an environment variable on your personal workstation for automatic use when you run the CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to run in environments that support the **Metadata** mode, when testing them outside such environments (for example, on a workstation). It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be limited.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

A token to be specified in parameters can be obtained from the IAM system associated with a specific {{ ydb-short-name }} installation. In particular, for the {{ ydb-short-name }} service in {{ yandex-cloud }}, Yandex.Passport OAuth and {{ yandex-cloud }} service accounts are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems for the organization may be used.

When using modes that involve the {{ ydb-short-name }} client accessing IAM, you can additionally specify an IAM URL that provides a token issuance API. By default, existing SDKs and CLIs attempt to access the IAM API {{ yandex-cloud }} hosted on `iam.api.cloud.yandex.net:443`.
