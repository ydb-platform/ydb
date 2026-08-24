# Authentication

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

{% note info %}

An authentication client refers to a user undergoing the authentication process when accessing {{ ydb-short-name }}. Examples of clients include the SDK or CLI.

{% endnote %}

The following authentication modes are supported:

* [Anonymous](#anonymous) authentication.
* Authentication by [username and password](#static-credentials).
* [LDAP](#ldap-auth-provider) authentication.
* [Authentication through a third-party IAM provider](#iam), for example, [Yandex Identity and Access Management](https://yandex.cloud/en/docs/iam/).

## Anonymous authentication {#anonymous}

Anonymous authentication allows you to connect to {{ ydb-short-name }} without specifying any credentials like username and password. This type of access should be used only for educational purposes in local databases that cannot be accessed over the network.

However, if a user or token is specified, the corresponding authentication mode will work with subsequent authorization.

{% note warning %}

Anonymous authentication should be used only for informational purposes for local databases that are not accessible over the network.

{% endnote %}

To enable anonymous authentication, use `false` in the `enforce_user_token_requirement` key of the cluster's [configuration file](../reference/configuration/auth_config.md#auth).

## Authenticating by username and password {#static-credentials}

Authentication by username and password using the YDB server is available only to [local users](../concepts/glossary.md#access-user). Authentication of external users involves third-party servers.

This access type implies that each database user has a username and password.

Only digits and lowercase Latin letters can be used in usernames. [Password complexity requirements](#password-complexity) can be configured.

The username and hashed password are stored in a table inside the authentication component. The password is hashed using the [Argon2](https://en.wikipedia.org/wiki/Argon2) method. Only the system administrator has access to this table.

A token is returned in response to the username and password. Tokens have a default lifetime of 12 hours. To rotate tokens, the client, such as the [SDK](../reference/ydb-sdk/index.md), independently sends requests to the authentication service. Tokens accelerate authentication and enhance security.

Authentication by username and password includes the following steps:

1. The client accesses the database and presents their username and password to the {{ ydb-short-name }} authentication service.
1. The service validates authentication data. If the data matches, it generates a token and returns it to the authentication service.
1. The client accesses the database, presenting their token as authentication data.

To enable authentication by username and password, ensure that the `use_login_provider` and `enable_login_authentication` parameters are set to the default value of `true` in the [configuration file](../reference/configuration/auth_config.md). Besides, to disable anonymous authentication, set the [`enforce_user_token_requirement` parameter](../reference/configuration/security_config.md) to `true`.

To learn how to manage roles and users, see [{#T}](../security/authorization.md).

### Password complexity {#password-complexity}

{{ ydb-short-name }} allows configuring requirements for password complexity. If a password specified in the `CREATE USER` or `ALTER USER` command does not meet complexity requirements, the command will result in an error. By default, {{ ydb-short-name }} has no password complexity requirements. A password of any length is accepted, including an empty string. A password can contain any number of digits and uppercase or lowercase letters, as well as special characters from the `!@#$%^&*()_+{}|<>?=` list. To set requirements for password complexity, define parameters in the `password_complexity` section in the [configuration](../reference/configuration/auth_config.md#password-complexity).

### Password brute-force protection

{{ ydb-short-name }} provides password brute-force protection. A user is locked out after exceeding a specified number of failed attempts to enter a password. After a certain period, the user will be unlocked and able to log in again.

By default, a user has four attempts to enter a password. If a user fails to enter the correct password in four attempts, the user will be locked out for an hour. You can change these lockout settings in the `auth_config` section of the [configuration](../reference/configuration/auth_config.md#account-lockout).

If necessary, a {{ ydb-short-name }} cluster or database administrator can [unlock](../yql/reference/syntax/alter-user.md) a user before the lockout period expires.

### Manual user lockout

{{ ydb-short-name }} provides another method for disabling authentication for a user, manual user lockout by a {{ ydb-short-name }} cluster or database administrator. An administrator can unlock user accounts that were previously locked manually or automatically after exceeding the number of failed attempts to enter the correct password. For more information about manual user lockout, see the [`ALTER USER LOGIN/NOLOGIN`](../yql/reference/syntax/alter-user.md) command description.

## LDAP directory integration {#ldap-auth-provider}

{{ ydb-short-name }} supports authentication and authorization via an [LDAP directory](https://en.wikipedia.org/wiki/LDAP). To use this feature, an LDAP directory service must be deployed and accessible from the {{ ydb-short-name }} servers.

Examples of supported LDAP implementations include [OpenLDAP](https://openldap.org/) and [Active Directory](https://azure.microsoft.com/en-us/products/active-directory/).

## Authentication through a third-party IAM provider {#iam}

* **Anonymous**: Empty token passed in a request.
* **Access Token**: Fixed token set as a parameter for the client (SDK or CLI) and passed in requests.
* **Refresh Token**: [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a user's personal account set as a parameter for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Service Account Key**: Service account attributes and a signature key set as parameters for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Metadata**: Client (SDK or CLI) periodically accesses a local service to rotate a token (obtain a new one) to pass in requests.
* **OAuth 2.0 token exchange** - The client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), then it uses the access token in {{ ydb-short-name }} API requests.

Any owner of a valid token can get access to perform operations; therefore, the principal objective of the security system is to ensure that a token remains private and to protect it from being compromised.

Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.

The highest level of security and performance is provided when using the **Metadata** mode, since it eliminates the need to work with secrets when deploying an application and allows accessing the IAM system and caching a token in advance, before running the application.

When choosing the authentication mode among those supported by the server and environment, follow the recommendations below:

* **You would normally use Anonymous** on self-deployed local {{ ydb-short-name }} clusters that are inaccessible over the network.
* **You would use Access Token** when other modes are not supported on server side or for setup/debugging purposes. It does not require that the client access IAM. However, if the IAM system supports an API for token rotation, fixed tokens issued by this IAM usually have a short validity period, which makes it necessary to update them manually in the IAM system on a regular basis.
* **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to DB data maintenance, performing ad-hoc operations in the CLI, or running applications from a workstation. You can manually obtain this token from IAM once to have it last a long time and save it in an environment variable on a personal workstation to use automatically and with no additional authentication parameters on CLI launch.
* **Service Account Key** is mainly used for applications designed to run in environments where the **Metadata** mode is supported, when testing them outside these environments (for example, on a workstation). It can also be used for applications outside these environments, working as an analog of **Refresh Token** for service accounts. Unlike a personal account, service account access objects and roles can be restricted.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to specify in request parameters can be obtained in the IAM system that the specific {{ ydb-short-name }} deployment is associated with. In particular, {{ ydb-short-name }} in {{ yandex-cloud }} uses Yandex.Passport OAuth and {{ yandex-cloud }} service accounts. When using {{ ydb-short-name }} in a corporate context, a company's standard centralized authentication system may be used.

When using modes in which the {{ ydb-short-name }} client accesses the IAM system, the IAM URL that provides an API for issuing tokens can be set additionally. By default, existing SDKs and CLIs attempt to access the {{ yandex-cloud }} IAM API hosted at `iam.api.cloud.yandex.net:443`.

### Authentication

Authentication using the LDAP protocol is similar to the static credentials authentication process (using a login and password). The difference is that the LDAP directory acts as the authentication component. The LDAP directory is used solely to verify the login/password pair.

{% note info %}

Since the LDAP directory is an external, independent service, {{ ydb-short-name }} cannot manage user accounts within it. For successful authentication, the user must already exist in the LDAP directory. The commands `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, and `DROP GROUP` do not affect the list of users and groups in the LDAP directory. Information on managing accounts should be found in the documentation for the specific LDAP directory implementation in use.

{% endnote %}

Currently, {{ ydb-short-name }} supports only one method of LDAP authentication, known as the `search+bind` method, which involves several steps. Upon receiving the username and password of the user being authenticated, a *bind* operation is performed using the credentials of a special service account specified in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section. These credentials are defined by the **bind_dn** and **bind_password** configuration parameters. After the service account is successfully authenticated, a search is conducted in the LDAP directory for the user attempting to authenticate in the system. The *search* operation is performed across the entire subtree rooted at the location specified by the **base_dn** configuration parameter and uses the filter defined in the **search_filter** configuration parameter.

Once the user entry is found, {{ ydb-short-name }} performs another *bind* operation using the found user's entry and the password provided earlier. The success of this second *bind* operation determines whether the user authentication is successful.

After successful authentication, a token is generated. This token is then used in place of the username and password, speeding up the authentication process and enhancing security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

### Token verification

After a user is authenticated in the system, a token is generated and verified before executing the requested operation. During the token verification process, the system determines on whose behalf the action is being requested and identifies the groups the user belongs to. For users from the LDAP directory, the token does not include information about group memberships. Therefore, after the token is verified, an additional query is made to the LDAP server to retrieve the list of groups the user is a member of.

Groups, like users, are entities that can have assigned access rights to perform operations on database schema objects and other resources. These assigned rights determine which operations a user is authorized to perform.

The process of retrieving a user's group list from an LDAP directory is similar to the steps taken during authentication. First, a *bind* operation is performed using the service user credentials specified by the **bind_dn** and **bind_password** parameters in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file. After successful authentication, a search is conducted for the user entry associated with the previously generated token. This search uses the **search_filter** parameter. If the user still exists, the result of the *search* operation will be a list of attribute values specified by the **requested_group_attribute** parameter. If this parameter is not set, the *memberOf* attribute is used as the default for reverse group membership. The *memberOf* attribute contains the distinguished names (DNs) of the groups to which the user belongs.

#### Group search

By default, {{ ydb-short-name }} only searches for groups in which the user is a direct member. However, by enabling the **extended_settings.enable_nested_groups_search** flag in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section, {{ ydb-short-name }} will attempt to retrieve groups at all levels of nesting, not just those the user directly belongs to. If {{ ydb-short-name }} is configured to work with Active Directory, the Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN) will be used to find all nested groups. This rule allows for the retrieval of all nested groups with a single query. For LDAP servers based on OpenLDAP, group searches will be conducted using recursive graph traversal, which generally requires multiple queries. In both Active Directory and OpenLDAP configurations, the group search is performed only within the subtree specified by the **base_dn** parameter.

{% note info %}

In the current implementation, the group names that {{ ydb-short-name }} uses match the values stored in the *memberOf* attribute. These names can be long and difficult to read.

Example:

```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

In the configuration file section that specifies authentication information, the refresh rate for user and group information can be set using the **refresh_time** parameter. For more detailed information about configuration files, refer to the [cluster configuration](../reference/configuration/auth_config.md#auth-config) section.

{% endnote %}

{% note warning %}

It should be noted that currently, {{ ydb-short-name }} does not have the capability to track group renaming on the LDAP server side. Consequently, a group with a new name will not retain the rights assigned to the group under its previous name.

{% endnote %}

### LDAP users and groups in {{ ydb-short-name }}

Since {{ ydb-short-name }} supports various methods of user authentication (login and password authentication, IAM provider usage, LDAP directory), it is often helpful to identify the specific source of authentication when handling user and group names. For all authentication types except login and password, a suffix in the format `<username>@<domain>` is appended to user and group names.

For LDAP users, the `<domain>` is determined by the **ldap_authentication_domain** configuration parameter in the [configuration section](../reference/configuration/auth_config.md#ldap-auth-config). By default, this parameter is set to `ldap`, so all usernames authenticated through the LDAP directory, as well as their corresponding group names in {{ ydb-short-name }}, will follow this format:

- `user1@ldap`
- `group1@ldap`
- `group2@ldap`

{% note warning %}

To indicate that the entered login should be recognized as a username from the LDAP directory, rather than for login and password authentication, you need to append the LDAP authentication domain suffix. This suffix is specified through the **ldap_authentication_domain** configuration parameter.

Below are examples of authenticating the user `user1` using the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md):

* Authentication of a user from the LDAP directory: `ydb --user user1@ldap -p ydb_profile scheme ls`
* Authentication of a user using the internal {{ ydb-short-name }} mechanism: `ydb --user user1 -p ydb_profile scheme ls`

{% endnote %}

### TLS connection {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or unencrypted connection. An encrypted connection with the LDAP server is established using the TLS protocol, which is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically via the [`ldaps`](#ldaps) connection scheme.
* Using the [`StartTls`](#starttls) LDAP protocol extension*.

When using an unencrypted connection, all data transmitted in requests to the LDAP server, including passwords, will be sent in plain text. This method is easier to set up and is more suited for experimentation or testing purposes.

#### LDAPS {#ldaps}

To have {{ ydb-short-name }} automatically establish an encrypted connection with the LDAP server, the **scheme** value in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) should be set to `ldaps`. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the `ldaps` scheme. The LDAP server must be configured to accept TLS connections on the specified ports.

#### LDAP protocol extension `StartTls` {#starttls}

<<<<<<< HEAD
`StartTls` is an LDAP protocol extension that enables message encryption using the TLS protocol. It allows a combination of encrypted and plain-text message transmission within a single connection to the LDAP server. {{ ydb-short-name }} sends a `StartTls` request to the LDAP server to initiate a TLS connection. In {{ ydb-short-name }}, enabling or disabling TLS within an active session is not supported. Therefore, once an encrypted connection is established using `StartTls`, all subsequent messages sent to the LDAP server will be encrypted. One advantage of using this extension, provided the LDAP server is appropriately configured, is the capability to initiate a TLS connection over an unencrypted port. The extension can be enabled in the [`use_tls` section](../reference/configuration/auth_config.md#ldap-auth-config) of the configuration file.
=======
`StartTls` is an extension of the LDAP protocol used to encrypt messages over TLS. It allows transmitting some messages in encrypted form and others in plain text within a single connection to the LDAP server. A message with this extension is sent from {{ ydb-short-name }} to the LDAP server to initiate a TLS connection. In the case of {{ ydb-short-name }}, it is not possible to enable and disable a TLS connection within a single connection. Therefore, when using the `StartTls` extension, after establishing an encrypted connection, {{ ydb-short-name }} will send all further messages to the LDAP server in encrypted form. One of the advantages of using this extension instead of the `ldaps` scheme (with appropriate LDAP server configuration) is the ability to establish a TLS connection on an unencrypted port. The extension is enabled in the [`use_tls` section](../reference/configuration/auth_config.md#ldap-auth-config) of the configuration file.

## Client authentication by certificate {#client-certificate}

{{ ydb-short-name }} can authenticate a client using the client certificate data received during TLS connection establishment. The verification is performed at the application protocol level (gRPC, etc.), when the server already accepts requests over an open connection.

This method is suitable, for example, in corporate scenarios with centralized certificate issuance.

### How it works

1. The client establishes a TLS connection with the {{ ydb-short-name }} server, passing the client certificate (and the trust chain).
2. When processing a request, the server extracts the certificate from the TLS context.
3. The server uses the certificate for authentication and validates it against the rules of the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section.
4. As a result of successful certificate verification, the client is assigned a security identifier [SID](../concepts/glossary.md#access-sid), which has all the [rights](../concepts/glossary.md#access-right) assigned to the corresponding identifier.

Certificate authentication is only applied to requests without an [authentication token](../concepts/glossary.md#auth-token). If the client passes an authentication token — for example, in the `Authorization` header for HTTP or through SDK/CLI mechanisms for IAM, login, and password — the token takes priority. In this case, the certificate is passed at the TLS level but is not used for authentication.

### SID formation

{% note info %}

Client certificate verification during [device authentication](#device-auth) and user authentication by client certificate are different mechanisms. Device authentication restricts the network perimeter without forming a SID; user authentication by client certificate forms a SID and groups for [authorization](./authorization.md).

{% endnote %}

Successful certificate authentication creates a user SID with the suffix `@<domain>`, where `<domain>` is the [parameter value](../reference/configuration/auth_config.md#iam-auth-config) of `certificate_authentication_domain` in the `auth_config` section (default: `cert`). The name is formed from all attributes of the certificate's Subject field in `Name=Value,...@<domain>` notation. The order of attributes corresponds to the order of fields in the certificate. Example:


```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```


### Getting groups

If the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section specifies `client_certificate_definitions` blocks, the certificate is accepted if it matches at least one of them. For each matching block, the client is included in the groups from `member_groups`. If `member_groups` is not specified, the default group is used — `default_group` (default value: `DefaultClientAuth@cert`).

### Server configuration

Certificate verification rules and group assignment are set in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section of the cluster static configuration. To enable client certificate requests during TLS handshake over gRPCs, set the `request_client_certificate: true` parameter.

### Client configuration

For more details on configuring the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), see the [TLS connection parameters](../reference/ydb-cli/connect.md#activated-profile) section.

## Device authentication by certificate {#device-auth}

Device authentication is the verification of the [client certificate](../concepts/glossary.md#client-certificate) during TLS connection establishment; no [SID](../concepts/glossary.md#access-sid) is formed in this case. If a certificate is presented, the trust chain to the CA is verified; an untrusted certificate causes the connection to be rejected before application requests are processed. Whether certificate presentation is mandatory depends on the interface (see the [Usage in {{ ydb-short-name }}](#device-auth-interfaces) section).

### Why device authentication is needed {#device-auth-motivation}

Device authentication addresses the following tasks in {{ ydb-short-name }}:

1. Cluster isolation — limit the set of hosts and applications that can establish a TLS connection with {{ ydb-short-name }} nodes.
2. Protection against configuration errors — prevent connections to foreign {{ ydb-short-name }} clusters. For example, with an incorrect [node-broker](../devops/configuration-management/configuration-v1/node-authorization.md) parameter, a dynamic node will not connect to a foreign cluster, whereas with regular TLS such a connection could be established.
3. Complicating application-level attacks — a process on a foreign host without a suitable certificate does not get access to the cluster API, even if a network route to the port exists.

After passing device authentication, [user or application authentication](./authentication.md) may be required to access data. It can be performed not only using the verified client certificate, but also through other authentication methods in {{ ydb-short-name }}, for example, by [login and password](./authentication.md#static-credentials).

### How it works {#device-auth-how-it-works}

1. The client establishes a TLS connection with the {{ ydb-short-name }} server and, if the connection interface requires it, presents a client certificate.
2. If a certificate is presented, the server verifies it at the TLS level: the trust chain to the configured certificate authority (CA), expiration date, and so on. Additional rules for matching certificate fields (for example, `require_same_issuer`, Subject, and SAN) are applied during [client certificate authentication](./authentication.md#client-certificate).
3. If verification succeeds, the connection is opened; if it fails, it is rejected.

### Usage in {{ ydb-short-name }} {#device-auth-interfaces}

Device authentication is optional and configured independently: the mechanism can be enabled on some ports and disabled on others.

- **Interconnect** — when TLS is enabled in the [interconnect_config](../reference/configuration/tls.md#interconnect) section, [Interconnect](../concepts/glossary.md#actor-system-interconnect) requires a client certificate.
- **gRPC** — you can enable client certificate request for device authentication, and also separately enable mandatory verification (an untrusted certificate is always rejected). Server configuration is described in the [grpc_config](../reference/configuration/tls.md#grpc) and [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) sections, and client connection — in the [TLS connection parameters](../reference/ydb-cli/connect.md#activated-profile) section.
- **Kafka API** — when mTLS is enabled, it requires a client certificate; only the trust chain to the CA is verified, a connection without a certificate or with an untrusted certificate is not established. Server configuration is described in the [kafka_proxy_config](../reference/configuration/kafka_proxy_config.md) section, and client connection — in the [Device authentication via mTLS](../reference/kafka-api/auth.md#mtls-auth) section.

## Authentication using a third-party IAM provider {#iam}

* **Access Token** — a fixed token is set as a parameter for the client (SDK or CLI) and is passed in requests.
* **Refresh Token** — an [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account is set as a parameter for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token that is passed in requests.
* **Service Account Key** — the attributes of a service account and a signing key are set as parameters for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token that is passed in requests.
* **Metadata** — the client (SDK or CLI) periodically accesses a local service to rotate (obtain the next) token that is passed in requests.
* **OAuth 2.0 token exchange** — the client (SDK or CLI) exchanges a token of another type for an access token according to the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), which is then passed in {{ ydb-short-name }} API requests.

Any holder of a valid token can get access to perform operations, so the main task of the security system is to ensure the secrecy of the token and prevent its compromise.

Authentication modes with token rotation **Refresh Token** and **Service Account Key** provide a higher level of security compared to the mode with a fixed token **Access Token**, because only short-lived secrets are transmitted over the network to the {{ ydb-short-name }} server.

Maximum security and performance are ensured when using the **Metadata** mode, as it eliminates the need to work with secrets when deploying an application, and also allows you to access the IAM and cache the token in advance, before starting the application.

When choosing an authentication mode among those supported by the server and environment, you should follow these recommendations:

* **Anonymous** is usually used on self-deployed local {{ ydb-short-name }} clusters that are not accessible over the network.
* **Access Token** is used when there is no support for other modes on the server side or for configuration/debugging purposes. It does not require client interactions with the IAM. However, if the IAM supports an API for token rotation, then the fixed tokens usually issued by such an IAM have a short lifetime, which forces you to manually renew them in the IAM regularly.
* **Refresh Token** can be used for performing one-off manual operations under a personal account, for example, related to data maintenance in the database, performing ad-hoc operations in the CLI, or launching applications from a workstation. Such a token can be obtained manually in IAM once for a long period and saved in an environment variable on a personal workstation for automatic use when starting the CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to run in environments that support the **Metadata** mode, when testing them outside such environments (for example, on a workstation). It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be limited.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to be specified in the parameters can be obtained in the IAM system associated with a specific {{ ydb-short-name }} installation. In particular, for the {{ ydb-short-name }} service in {{ yandex-cloud }}, Yandex.Passport OAuth and {{ yandex-cloud }} service accounts are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems for that organization may be used.

When using modes that involve the {{ ydb-short-name }} client accessing IAM, an IAM URL that provides a token issuance API can additionally be specified. By default, existing SDKs and CLI attempt to access the {{ yandex-cloud }} IAM API hosted on `iam.api.cloud.yandex.net:443`.
>>>>>>> 0f7797315fa (docs: complete maintenance overview links (#50848))
