# Authentication

After a network connection is successfully established, the server accepts requests from the client for processing. Authentication information is transmitted as an [authentication token](../concepts/glossary.md#auth-token) or a [client certificate](../concepts/glossary.md#client-certificate). Based on this information, the server determines the client's [SID](../concepts/glossary.md#access-sid) and checks its access rights to execute the request.

{% note info %}

An authentication client is a user who undergoes authentication when accessing {{ ydb-short-name }}. Examples of clients include applications using the [SDK](../reference/ydb-sdk/index.md) or [CLI](../reference/ydb-cli/index.md).

{% endnote %}

The following authentication modes are supported:

* [Anonymous](#anonymous) authentication.
* [Anonymous](#static-credentials) authentication.
* Authentication by [username and password](#ldap).
* [LDAP](#client-certificate) authentication.
* [Authentication through a third-party IAM provider](#iam), for example, [Yandex Identity and Access Management](https://yandex.cloud/en/docs/iam/).

## Anonymous authentication {#anonymous}

By default, {{ ydb-short-name }} allows executing queries without specifying authentication credentials such as a username or [token](../concepts/glossary.md#auth-token). Access control checks ([authorization](authorization.md)) are also not performed.

{% note warning %}

However, if a user or token is specified, the corresponding authentication mode will work with subsequent authorization.

{% endnote %}

The `enforce_user_token_requirement` flag in the [authentication mode settings](../reference/configuration/security_config.md#security-auth) {{ ydb-short-name }} is responsible for disabling anonymous authentication mode.

Anonymous authentication should be used only for informational purposes for local databases that are not accessible over the network.

- This access type implies that each database user has a username and password.
- A token explicitly specified in queries can be validated according to appropriate rules.

Then queries will be executed not anonymously, and permission checking will also be performed.

The username and hashed password are stored in a table inside the authentication component. The password is hashed using the [Argon2](../reference/configuration/security_config.md#security-access-levels) method. Only the system administrator has access to this table.

## Login and password authentication {#static-credentials}

Login and password authentication via the {{ ydb-short-name }} server is only available for [local users](../concepts/glossary.md#access-user). Authentication of external users involves servers of external systems.

This type of access implies that each database user has a login and password.
A user login can only contain lowercase Latin letters, digits, and the `@` character.
Various [criteria](#password-complexity) for password complexity can be set.

A token is returned in response to the username and password. Tokens have a default lifetime of 12 hours. To rotate tokens, the client, such as the [SDK](https://en.wikipedia.org/wiki/Argon2), independently sends requests to the authentication service. Tokens accelerate authentication and enhance security.

In response to the login and password, an [authentication token](../concepts/glossary.md#auth-token) is returned. The default token lifetime is 12 hours. For token rotation, the client, for example, [SDK](../reference/ydb-sdk/index.md), independently contacts the authentication service. Using a token speeds up the authentication process and improves security.

Authentication by username and password includes the following steps:

1. The client accesses the database and presents their username and password to the {{ ydb-short-name }} authentication service.
2. The service validates authentication data. If the data matches, it generates a token and returns it to the authentication service.
3. The client accesses the database, presenting their token as authentication data.

To enable authentication by username and password, you must ensure that the parameters `use_login_provider` and `enable_login_authentication` are set to the default value `true` in [the configuration](../reference/configuration/auth_config.md). Additionally, to disable anonymous authentication, you must set the value of the parameter [`enforce_user_token_requirement` to `true`](../reference/configuration/security_config.md).

To learn how to manage roles and users, see [{#T}](../security/authorization.md).

### Password complexity {#password-complexity}

{{ ydb-short-name }}allows you to configure password complexity requirements. If a password provided via the `CREATE USER` or `ALTER USER` commands does not meet the complexity criteria, the command execution will fail.
By default, no restrictions are imposed on passwords: a password of any length is accepted, including an empty string; the password may contain any number of digits and letters in any case, as well as special characters from the list `!@#$%^&*()_+{}|<>?=`. To set password complexity restrictions, fill in the `password_complexity` section in the [configuration](../reference/configuration/auth_config.md#password-complexity).

### Password brute-force protection

There is another way to prevent a user from authenticating — forced blocking by a cluster or database administrator. Administrators can unblock both users who were forcibly blocked and users who became blocked due to exceeding the limit on the number of incorrect password attempts. Detailed information about forced blocking and unblocking of users can be found in the description of the [`ALTER USER LOGIN/NOLOGIN`](../yql/reference/syntax/alter-user.md) command.

### Manual user lockout

{{ ydb-short-name }} provides protection against password brute‑forcing by the user. A user will be considered blocked if they exceed the number of incorrect password attempts. After the specified time has elapsed, they will be able to authenticate again.

{% note info %}

This mechanism is only applicable to users served by {{ ydb-short-name }} itself, the so-called built-in users. Users served by external authentication sources, such as LDAP servers, are not subject to the password brute-force protection mechanism.

{% endnote %}

By default, the user has 4 attempts to enter the correct password. Otherwise, authentication will be blocked for them for one hour. You can configure user lockout criteria in the [configuration](../reference/configuration/auth_config.md#account-lockout).

If necessary, a cluster or database administrator can [unlock](../yql/reference/syntax/alter-user.md) a user early.

Information about the user's lock status and the number of incorrect password attempts can be found in the [system view](../dev/system-views.md#system-view) of the user.

## LDAP directory integration {#ldap}

{{ ydb-short-name }} integrates interaction with an [LDAP directory](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol). The LDAP directory is an external service relative to {{ ydb-short-name }} and is used for authenticating and authorizing database users. Before using this authentication and authorization method, you must have a deployed LDAP service and configured network access between it and the {{ ydb-short-name }} servers.

Examples of supported LDAP implementations include [OpenLDAP](https://openldap.org/) and [Active Directory](https://azure.microsoft.com/en-us/products/active-directory/).

### Authentication through a third-party IAM provider

**Service Account Key**: Service account attributes and a signature key set as parameters for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.

{% note info %}

Since the LDAP directory is an external independent service, {{ ydb-short-name }} cannot manage user accounts in the directory. For successful authentication, the user must already exist in the LDAP directory. Using the `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, `DROP GROUP` commands will not affect the list of users and groups in the directory. Information on managing user accounts should be found in the documentation of the LDAP directory being used.

{% endnote %}

Currently, {{ ydb-short-name }} supports only one method of LDAP user authentication — *search+bind*: after receiving the login and password, a service *bind* is performed on behalf of a [service account](#ldap-service-account-auth) (login and password in `bind_dn` / `bind_password` or certificate and SASL EXTERNAL — see [configuration](../reference/configuration/auth_config.md#ldap-auth-config)), then a search for the user record and a second *bind* on behalf of the user.

{% note info %}

**Metadata**: Client (SDK or CLI) periodically accesses a local service to rotate a token (obtain a new one) to pass in requests.

{% endnote %}

Service account credentials for connecting to LDAP are specified in the configuration settings: use the `bind_dn` and `bind_password` parameters, or configure [mTLS](../concepts/glossary.md#mtls) (for details, see the [Service account authentication](#ldap-service-account-auth) section).

Any owner of a valid token can get access to perform operations; therefore, the principal objective of the security system is to ensure that a token remains private and to protect it from being compromised.

1. Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.
2. After a successful connection, the system searches for the user attempting to authenticate. The search is performed across the entire subtree specified in the `base_dn` configuration parameter and using the filter specified in the `search_filter` parameter.
3. **You would normally use Anonymous** on self-deployed local {{ ydb-short-name }} clusters that are inaccessible over the network.
4. Authentication using the LDAP protocol is similar to the static credentials authentication process (using a login and password). The difference is that the LDAP directory acts as the authentication component. The LDAP directory is used solely to verify the login/password pair.

Once the user entry is found, {{ ydb-short-name }} performs another *bind* operation using the found user's entry and the password provided earlier. The success of this second *bind* operation determines whether the user authentication is successful.

After successful verification of the user's login and password in the LDAP directory, {{ ydb-short-name }} returns an [authentication token](../concepts/glossary.md#auth-token). This token is then used instead of the login and password. Using the token speeds up the authentication process and improves security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

#### Token verification {#ldap-service-account-auth}

After a user is authenticated in the system, a token is generated and verified before executing the requested operation. During the token verification process, the system determines on whose behalf the action is being requested and identifies the groups the user belongs to. For users from the LDAP directory, the token does not include information about group memberships. Therefore, after the token is verified, an additional query is made to the LDAP server to retrieve the list of groups the user is a member of.

* Using a login and password.  
  In this case, you need to specify the login (`bind_dn`) and password (`bind_password`) in the configuration. These parameters will be used to connect to the LDAP server on behalf of a service account.
* Using mTLS (mutual TLS) via the SASL EXTERNAL mechanism.
  In this option, certificates are used for authentication instead of a login and password. This allows you to avoid storing the service account password in the configuration — you just need to specify the certificate file (`use_tls.cert_file`) and private key file (`use_tls.key_file`), and also enable a special flag (`extended_settings.enable_sasl_external_bind`). For detailed setup information, see the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section.

### Token verification

Groups, like users, are entities that can have assigned access rights to perform operations on database schema objects and other resources. These assigned rights determine which operations a user is authorized to perform.

Groups, like the user themselves, are subjects that perform operations on database schema objects. To differentiate access to various database resources, subjects can be assigned access rights. And according to the list of assigned rights, subjects will be authorized to perform certain operations.

The process of retrieving a user's group list from an LDAP directory is similar to the actions performed during authentication. First, a *bind* operation is performed for the service user whose credentials are written in the `bind_dn` and `bind_password` parameters of the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file. After successful authentication, a search is performed for the user record for which the token was previously generated. The search is also performed according to the `search_filter` parameter. If the user still exists, the result of the *search* operation will be a list of values of the attribute written in the `requested_group_attribute` parameter. If this parameter is empty, the reverse group membership attribute will be `memberOf`. The `memberOf` attribute stores the Distinguished Names (DN) of the groups the user belongs to.

#### Group search

By default, {{ ydb-short-name }} searches only for groups in which the user is a direct member. By enabling the `extended_settings.enable_nested_groups_search` flag in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section, {{ ydb-short-name }} will attempt to retrieve groups at all nesting levels, not just those the user directly belongs to. If {{ ydb-short-name }} is configured to work with Active Directory, the Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN) will be used to search for all nested groups. This rule allows retrieving all nested groups with a single query. For LDAP servers based on OpenLDAP, group search will be performed by recursive graph traversal, which generally requires multiple queries. For both Active Directory and OpenLDAP, group search will be performed only for the subtree whose root is taken from the configuration parameter `base_dn`.

{% note info %}

In the current implementation, the group names that {{ ydb-short-name }} will operate with match the values stored in the `memberOf` attribute. They may be long and hard to read.

Example:


```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

In the section of the configuration file that describes authentication information, you can configure the refresh frequency for user and group information. This value is controlled by the `refresh_time` parameter. For more information about configuration files, see the [cluster configuration](../reference/configuration/auth_config.md#auth-config) section.

{% endnote %}

{% note warning %}

It should be noted that currently, {{ ydb-short-name }} does not have the capability to track group renaming on the LDAP server side. Consequently, a group with a new name will not retain the rights assigned to the group under its previous name.

{% endnote %}

### LDAP users and groups in {{ ydb-short-name }}

Since {{ ydb-short-name }} supports different user authentication methods, when working with user and group names, it is often useful to distinguish where exactly the user was authenticated. For all authentication types except login/password authentication, group and user names are appended with a suffix of the form `@<auth-domain>`.

For LDAP users, the *auth-domain* is set in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) `ldap_authentication_domain`. By default, it has the value `ldap`, so all user names authenticated via the LDAP directory and the names of groups they belong to will have the following form in {{ ydb-short-name }}:

- `user1@ldap`
- `group1@ldap`
- `group2@ldap`

{% note warning %}

To distinguish that the entered login should be a login of a user from the LDAP directory rather than a login of a local {{ ydb-short-name }} user, you need to append the suffix `@ldap` to it.

Below are examples of authenticating the user `user1` using the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md):

* Authentication of a user from the LDAP directory: `ydb --user user1@ldap -p ydb_profile scheme ls`
* Authentication of a user using the internal {{ ydb-short-name }} mechanism: `ydb --user user1 -p ydb_profile scheme ls`

{% endnote %}

### TLS connection {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or unencrypted connection. An encrypted connection with the LDAP server is established using the TLS protocol, which is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically via the [`ldaps`](#ldaps) connection scheme.
* Using the [`StartTls`](#starttls) LDAP protocol extension*.

When using an unencrypted connection, all data transmitted in requests to the LDAP server, including passwords, will be sent in plain text. This method is easier to set up and is more suited for experimentation or testing purposes.

#### LDAPS

To have {{ ydb-short-name }} automatically establish an encrypted connection with the LDAP server, the **scheme** value in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) should be set to `ldaps`. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the `ldaps` scheme. The LDAP server must be configured to accept TLS connections on the specified ports.

#### LDAP protocol extension `StartTls` {#starttls}

`StartTls` is an LDAP protocol extension used for encrypting messages over TLS. It allows some messages to be transmitted in encrypted form and others in plain text within a single connection to the LDAP server. A message with this extension is sent from {{ ydb-short-name }} to the LDAP server to initiate a TLS connection. In the case of {{ ydb-short-name }}, enabling and disabling a TLS connection within a single connection is not supported. Therefore, when using the `StartTls` extension, after establishing an encrypted connection, {{ ydb-short-name }} will send all subsequent messages to the LDAP server in encrypted form. One advantage of using this extension instead of the `ldaps` scheme (with appropriate LDAP server configuration) is the ability to establish a TLS connection on an unencrypted port. The extension is enabled in the [`use_tls` section](../reference/configuration/auth_config.md#ldap-auth-config) of the configuration file.

## Client authentication by certificate {#client-certificate}

{{ ydb-short-name }} can authenticate a client using the client certificate received during TLS connection establishment. Verification is performed at the application protocol level (gRPC, etc.), when the server already accepts requests over an open connection.

This method is suitable, for example, in corporate scenarios with centralized certificate issuance.

### How it works

1. The client establishes a TLS connection with the {{ ydb-short-name }} server, passing the client certificate (and chain of trust).
2. When processing a request, the server extracts the certificate from the TLS context.
3. The server uses the certificate for authentication and validates it according to the rules in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section.
4. Upon successful certificate verification, the client is assigned a security identifier [SID](../concepts/glossary.md#access-sid), which has all the [rights](../concepts/glossary.md#access-right) assigned to the corresponding identifier.

Certificate authentication is only used for requests without an [authentication token](../concepts/glossary.md#auth-token). If the client passes an authentication token — for example, in the `Authorization` header for HTTP or through SDK/CLI mechanisms for IAM, login, and password — the token takes precedence. In this case, the certificate is transmitted at the TLS level but is not used for authentication.

### SID formation

{% note info %}

Checking the client certificate during [device authentication](#device-auth) and user authentication by client certificate are different mechanisms. Device authentication restricts the network perimeter without forming a SID; user authentication by client certificate forms a SID and groups for [authorization](./authorization.md).

{% endnote %}

Successful certificate authentication creates a user SID with the suffix `@<domain>`, where `<domain>` is the [parameter value](../reference/configuration/auth_config.md#iam-auth-config) `certificate_authentication_domain` in the `auth_config` section (default: `cert`). The name is formed from all attributes of the certificate's Subject field in `Name=Value,...@<domain>` notation. The attribute order corresponds to the field order in the certificate. Example:


```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```


### Obtaining groups

If the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section specifies `client_certificate_definitions` blocks, the certificate is accepted if it matches at least one of them. For each matching block, the client is included in the groups from `member_groups`. If `member_groups` is not specified, the default group is used — `default_group` (default value: `DefaultClientAuth@cert`).

### Server configuration

Certificate validation rules and group assignment are specified in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section of the cluster static configuration. To enable client certificate requests during TLS handshake over gRPCs, set the `request_client_certificate: true` parameter.

### Client configuration

For more details on configuring [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), see the section [TLS connection parameters](../reference/ydb-cli/connect.md#activated-profile).

## Device authentication by certificate {#device-auth}

Device authentication is the verification of the [client certificate](../concepts/glossary.md#client-certificate) during TLS connection establishment; [SID](../concepts/glossary.md#access-sid) is not formed. If a certificate is presented, the chain of trust to the CA is checked; an untrusted certificate leads to connection rejection before processing application requests. The requirement to present a certificate depends on the interface (see the section [Usage in {{ ydb-short-name }}](#device-auth-interfaces)).

### Purpose of device authentication {#device-auth-motivation}

Device authentication solves the following tasks in {{ ydb-short-name }}:

1. Cluster isolation — limit the set of hosts and applications that can establish a TLS connection with {{ ydb-short-name }} nodes.
2. Protection against configuration errors — prevent connections to foreign {{ ydb-short-name }} clusters, for example, if the [node-broker](../devops/deployment-options/manual/node-authorization.md) parameter is incorrect, a dynamic node will not connect to a foreign cluster, whereas with regular TLS such a connection could be established.
3. Complicating application-level attacks — a process on a foreign host without a suitable certificate does not get access to the cluster API, even if a network route to the port exists.

After passing device authentication, [user or application authentication](./authentication.md) may be required to access data. It can be performed not only by the verified client certificate, but also using other authentication methods in {{ ydb-short-name }}, for example, by [login and password](./authentication.md#static-credentials).

### How it works {#device-auth-how-it-works}

1. The client establishes a TLS connection with the {{ ydb-short-name }} server and, if the connection interface requires it, presents a client certificate.
2. If a certificate is presented, the server verifies it at the TLS level: the trust chain to the configured certificate authority (CA), expiration date, and so on. Additional rules for matching certificate fields (for example, `require_same_issuer`, Subject, and SAN) are applied during [client certificate authentication](./authentication.md#client-certificate).
3. If the verification succeeds, the connection is opened; if it fails, it is rejected.

### Usage in {{ ydb-short-name }} {#device-auth-interfaces}

Device authentication is optional and configured independently: the mechanism can be enabled on some ports and disabled on others.

- **Interconnect** — when TLS is enabled in the [interconnect_config](../reference/configuration/tls.md#interconnect) section, [Interconnect](../concepts/glossary.md#actor-system-interconnect) requires a client certificate.
- **gRPC** — you can enable client certificate request for device authentication, and also separately enable mandatory verification (an untrusted certificate is always rejected). Server configuration is described in the [grpc_config](../reference/configuration/tls.md#grpc) and [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) sections, and client connection is described in the [TLS connection parameters](../reference/ydb-cli/connect.md#activated-profile) section.
- **Kafka API** — when mTLS is enabled, it requires a client certificate; only the trust chain to the CA is verified, a connection without a certificate or with an untrusted certificate is not established. Server configuration is described in the [kafka_proxy_config](../reference/configuration/kafka_proxy_config.md) section, and client connection is described in the [Device authentication via mTLS](../reference/kafka-api/auth.md#mtls-auth) section.

## Authentication using a third-party IAM provider {#iam}

* **Access Token** — a fixed token is set as a parameter for the client (SDK or CLI) and is passed in requests.
* **Refresh Token** — an [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account is set as a parameter for the client (SDK or CLI), based on which the client periodically contacts the IAM API in the background to rotate (obtain the next) token that is passed in requests.
* **Service Account Key** — the attributes of a service account and a signing key are set as parameters for the client (SDK or CLI), based on which the client periodically contacts the IAM API in the background to rotate (obtain the next) token that is passed in requests.
* **Metadata** — the client (SDK or CLI) periodically contacts a local service to rotate (obtain the next) token that is passed in requests.
* **OAuth 2.0 token exchange** — the client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), which is then passed in {{ ydb-short-name }} API requests.

Any holder of a valid token can gain access to perform operations, so the main task of the security system is to ensure token secrecy and prevent its compromise.

Authentication modes with token rotation, **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the fixed token mode **Access Token**, because only short-lived secrets are transmitted over the network to the {{ ydb-short-name }} server.

Maximum security and performance are ensured when using the **Metadata** mode, as it eliminates the need to work with secrets when deploying an application, and also allows you to contact IAM and cache the token in advance, before starting the application.

When choosing an authentication mode among those supported by the server and environment, follow these recommendations:

* **Anonymous** is typically used on self-deployed local {{ ydb-short-name }} clusters that are not accessible over the network.
* **Access Token** is used when other modes are not supported on the server side or for configuration/debugging purposes. It does not require client interactions with IAM. However, if the IAM supports an API for token rotation, the fixed tokens issued by such IAM typically have a short lifetime, which forces you to manually renew them in IAM on a regular basis.
* **Refresh Token** can be used for one-time manual operations under a personal account, for example, related to database data maintenance, performing ad-hoc operations in the CLI, or launching applications from a workstation. Such a token can be obtained manually in IAM once for a long period and stored in an environment variable on a personal workstation for automatic use when starting the CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to run in environments that support the **Metadata** mode, when testing them outside such environments (for example, on a workstation). It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be limited.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to be specified in the parameters can be obtained from the IAM system associated with a particular {{ ydb-short-name }} installation. In particular, for the {{ ydb-short-name }} service in {{ yandex-cloud }}, Yandex.Passport OAuth and {{ yandex-cloud }} service accounts are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems of the given organization may be used.

When using modes that involve the {{ ydb-short-name }} client accessing IAM, an IAM URL providing a token issuance API can additionally be specified. By default, existing SDKs and CLI attempt to access the {{ yandex-cloud }} IAM API hosted on `iam.api.cloud.yandex.net:443`.
