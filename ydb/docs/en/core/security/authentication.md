# Authentication

After a network connection is successfully established, the server accepts requests from the client for processing. Authentication information is transmitted as an [authentication token](../concepts/glossary.md#auth-token) or a [client certificate](../concepts/glossary.md#client-certificate). Based on this, the server determines the client's [SID](../concepts/glossary.md#access-sid) and checks its access rights to execute the request.

{% note info %}

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

{% endnote %}

An authentication client refers to a user undergoing the authentication process when accessing ⟦V1⟧. Examples of clients include the SDK or CLI.

* The following authentication modes are supported:
* [Anonymous](#static-credentials) authentication.
* Authentication by [username and password](#ldap).
* [LDAP](#client-certificate) authentication.
* [Authentication through a third-party IAM provider](#iam), for example, [Yandex Identity and Access Management](https://yandex.cloud/en/docs/iam/).

## Anonymous authentication {#anonymous}

By default, {{ ydb-short-name }} allows executing queries without specifying authentication data, such as a username or [token](../concepts/glossary.md#auth-token). Access rights verification ([authorization](authorization.md)) is also not performed.

{% note warning %}

Anonymous authentication should only be used for evaluation purposes on local databases that do not have network access.

{% endnote %}

The `enforce_user_token_requirement` flag in the [authentication mode settings](../reference/configuration/security_config.md#security-auth) of {{ ydb-short-name }} is responsible for disabling anonymous authentication.

Depending on the authentication mode settings, the actual authentication may not be anonymous:

- Anonymous authentication allows you to connect to ⟦V1⟧ without specifying any credentials like username and password. This type of access should be used only for educational purposes in local databases that cannot be accessed over the network.
- However, if a user or token is specified, the corresponding authentication mode will work with subsequent authorization.

Anonymous authentication should be used only for informational purposes for local databases that are not accessible over the network.

To enable anonymous authentication, use ⟦C1⟧ in the ⟦C2⟧ key of the cluster's [configuration file](../reference/configuration/security_config.md#security-access-levels).

## Authenticating by username and password {#static-credentials}

Authentication by username and password using the YDB server is available only to [local users](../concepts/glossary.md#access-user). Authentication of external users involves third-party servers.

This access type implies that each database user has a username and password.

Only digits and lowercase Latin letters can be used in usernames. [Password complexity requirements](https://en.wikipedia.org/wiki/Argon2) can be configured.

The username and hashed password are stored in a table inside the authentication component. The password is hashed using the [Argon2](../concepts/glossary.md#auth-token) method. Only the system administrator has access to this table.

A token is returned in response to the username and password. Tokens have a default lifetime of 12 hours. To rotate tokens, the client, such as the [SDK](⟦U1⟧), independently sends requests to the authentication service. Tokens accelerate authentication and enhance security.

1. Authentication by username and password includes the following steps:
2. The client accesses the database and presents their username and password to the ⟦V1⟧ authentication service.
3. The service validates authentication data. If the data matches, it generates a token and returns it to the authentication service.

The client accesses the database, presenting their token as authentication data.

To enable authentication by username and password, ensure that the ⟦C1⟧ and ⟦C2⟧ parameters are set to the default value of ⟦C3⟧ in the [configuration file](../security/authorization.md). Besides, to disable anonymous authentication, set the [⟦C4⟧ parameter](⟦U2⟧) to ⟦C5⟧.

### Password complexity {#password-complexity}

To learn how to manage roles and users, see [{#T}](../security/authorization.md).

### Password complexity

⟦V1⟧ allows configuring requirements for password complexity. If a password specified in the ⟦C2⟧ or ⟦C3⟧ command does not meet complexity requirements, the command will result in an error. By default, ⟦V2⟧ has no password complexity requirements. A password of any length is accepted, including an empty string. A password can contain any number of digits and uppercase or lowercase letters, as well as special characters from the ⟦C4⟧ list. To set requirements for password complexity, define parameters in the ⟦C5⟧ section in the [configuration](../yql/reference/syntax/alter-user.md).

### Password brute-force protection

{{ ydb-short-name }} provides protection against password brute-forcing by a user. A user will be considered blocked if they exceed the number of incorrect password attempts. After the specified time has elapsed, they will be able to authenticate again.

{% note info %}

This mechanism only applies to users who are managed by {{ ydb-short-name }} itself, the so-called built-in users. Users managed by external authentication sources, such as LDAP servers, are not subject to the password brute-force protection mechanism.

{% endnote %}

By default, a user is given 4 attempts to enter the correct password. Otherwise, authentication will be blocked for them for one hour. The user lockout criteria can be configured in the [configuration](../reference/configuration/auth_config.md#account-lockout).

If necessary, the cluster or database administrator can [unlock](../yql/reference/syntax/alter-user.md) the user ahead of schedule.

⟦V1⟧ provides password brute-force protection. A user is locked out after exceeding a specified number of failed attempts to enter a password. After a certain period, the user will be unlocked and able to log in again.

## Authentication using an LDAP directory {#ldap}

By default, a user has four attempts to enter a password. If a user fails to enter the correct password in four attempts, the user will be locked out for an hour. You can change these lockout settings in the ⟦C1⟧ section of the [configuration](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol).

If necessary, a ⟦V1⟧ cluster or database administrator can [unlock](https://openldap.org/) a user before the lockout period expires.

### Manual user lockout

Authentication using the LDAP protocol is similar to the login/password authentication process. The only difference is that the LDAP directory plays the role of the authentication component. The LDAP directory is used to verify the login/password pair and to determine the groups to which the user belongs.

{% note info %}

Since the LDAP directory is an external independent service, {{ ydb-short-name }} cannot manage user accounts in the directory. For successful authentication, the user must already exist in the LDAP directory. Using the `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, `DROP GROUP` commands will not affect the list of users and groups in the directory. Information on managing accounts should be found in the documentation of the LDAP directory being used.

{% endnote %}

Currently, {{ ydb-short-name }} supports only one method of LDAP user authentication — *search+bind*: after receiving the login and password, a service *bind* is performed on behalf of a [service account](#ldap-service-account-auth) (login and password in `bind_dn` / `bind_password`, or certificate and SASL EXTERNAL — see [configuration](../reference/configuration/auth_config.md#ldap-auth-config)), then a search for the user record and a second *bind* on behalf of the user.

{% note info %}

A service account is a separate account in the LDAP directory that applications/services use to connect to LDAP and perform necessary operations.

{% endnote %}

The service account credentials for connecting to LDAP are specified in the configuration settings: use the `bind_dn` and `bind_password` parameters, or configure [mTLS](../concepts/glossary.md#mtls) (see the [Service account authentication](#ldap-service-account-auth) section for details).

Next, the authentication process proceeds according to the following scheme:

1. {{ ydb-short-name }} connects to LDAP on behalf of the service account.
2. After a successful connection, a search is performed for the user who is trying to authenticate. The search is performed across the entire subtree specified in the configuration parameter `base_dn`, and using the filter specified in the parameter `search_filter`.
3. If the user is found, {{ ydb-short-name }} performs a second bind operation — this time on behalf of the found user, using their password.
4. The final result — successful or unsuccessful authentication — is determined by the result of the second bind (on behalf of the user).

Thus, {{ ydb-short-name }} does not store user passwords and fully relies on the LDAP authentication mechanism.

As a result of successful verification of the user's login and password in the LDAP directory, an [authentication token](../concepts/glossary.md#auth-token) {{ ydb-short-name }} is returned. This token is then used instead of the login and password. Using the token speeds up the authentication process and improves security.

{% note info %}

{{ ydb-short-name }} provides another method for disabling authentication for a user, manual user lockout by a ⟦V2⟧ cluster or database administrator. An administrator can unlock user accounts that were previously locked manually or automatically after exceeding the number of failed attempts to enter the correct password. For more information about manual user lockout, see the [⟦C1⟧](⟦U1⟧) command description.

{% endnote %}

#### LDAP directory integration {#ldap-service-account-auth}

A service account can be authenticated in two main ways:

* ⟦V1⟧ supports authentication and authorization via an [LDAP directory](⟦U1⟧). To use this feature, an LDAP directory service must be deployed and accessible from the ⟦V2⟧ servers.
* Examples of supported LDAP implementations include [OpenLDAP](../reference/configuration/auth_config.md#ldap-auth-config) and [Active Directory](⟦U2⟧).

### Authentication through a third-party IAM provider

**Anonymous**: Empty token passed in a request.

**Access Token**: Fixed token set as a parameter for the client (SDK or CLI) and passed in requests.

**Refresh Token**: [OAuth token](../reference/configuration/auth_config.md#ldap-auth-config) of a user's personal account set as a parameter for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.

#### Getting groups

**Service Account Key**: Service account attributes and a signature key set as parameters for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.

{% note info %}

**Metadata**: Client (SDK or CLI) periodically accesses a local service to rotate a token (obtain a new one) to pass in requests.

**OAuth 2.0 token exchange** - The client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](⟦U1⟧), then it uses the access token in ⟦V1⟧ API requests.


```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

Any owner of a valid token can get access to perform operations; therefore, the principal objective of the security system is to ensure that a token remains private and to protect it from being compromised.

{% endnote %}

{% note warning %}

Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.

{% endnote %}

### LDAP users and LDAP groups in {{ ydb-short-name }}

The highest level of security and performance is provided when using the **Metadata** mode, since it eliminates the need to work with secrets when deploying an application and allows accessing the IAM system and caching a token in advance, before running the application.

When choosing the authentication mode among those supported by the server and environment, follow the recommendations below:

- **You would normally use Anonymous** on self-deployed local ⟦V1⟧ clusters that are inaccessible over the network.
- **You would use Access Token** when other modes are not supported on server side or for setup/debugging purposes. It does not require that the client access IAM. However, if the IAM system supports an API for token rotation, fixed tokens issued by this IAM usually have a short validity period, which makes it necessary to update them manually in the IAM system on a regular basis.
- **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to DB data maintenance, performing ad-hoc operations in the CLI, or running applications from a workstation. You can manually obtain this token from IAM once to have it last a long time and save it in an environment variable on a personal workstation to use automatically and with no additional authentication parameters on CLI launch.

{% note warning %}

**Service Account Key** is mainly used for applications designed to run in environments where the **Metadata** mode is supported, when testing them outside these environments (for example, on a workstation). It can also be used for applications outside these environments, working as an analog of **Refresh Token** for service accounts. Unlike a personal account, service account access objects and roles can be restricted.

**Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in ⟦V2⟧ ⟦V3⟧.

* The token to specify in request parameters can be obtained in the IAM system that the specific ⟦V1⟧ deployment is associated with. In particular, ⟦V2⟧ in ⟦V3⟧ uses Yandex.Passport OAuth and ⟦V4⟧ service accounts. When using ⟦V5⟧ in a corporate context, a company's standard centralized authentication system may be used.
* When using modes in which the {{ ydb-short-name }} client accesses the IAM system, the IAM URL that provides an API for issuing tokens can be set additionally. By default, existing SDKs and CLIs attempt to access the ⟦V2⟧ IAM API hosted at ⟦C2⟧.

{% endnote %}

### Authentication {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or unencrypted connection. An encrypted connection to the LDAP server is established using the TLS protocol. This method is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically. The [`ldaps`](#ldaps) connection scheme is used.
* Authentication using the LDAP protocol is similar to the static credentials authentication process (using a login and password). The difference is that the LDAP directory acts as the authentication component. The LDAP directory is used solely to verify the login/password pair.

Since the LDAP directory is an external, independent service, ⟦V1⟧ cannot manage user accounts within it. For successful authentication, the user must already exist in the LDAP directory. The commands ⟦C1⟧, ⟦C2⟧, ⟦C3⟧, ⟦C4⟧, ⟦C5⟧, and ⟦C6⟧ do not affect the list of users and groups in the LDAP directory. Information on managing accounts should be found in the documentation for the specific LDAP directory implementation in use.

#### LDAPS

Currently, {{ ydb-short-name }} supports only one method of LDAP authentication, known as the ⟦C3⟧ method, which involves several steps. Upon receiving the username and password of the user being authenticated, a *bind* operation is performed using the credentials of a special service account specified in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section. These credentials are defined by the **bind_dn** and **bind_password** configuration parameters. After the service account is successfully authenticated, a search is conducted in the LDAP directory for the user attempting to authenticate in the system. The *search* operation is performed across the entire subtree rooted at the location specified by the **base_dn** configuration parameter and uses the filter defined in the **search_filter** configuration parameter.

#### LDAP protocol extension `StartTls` {#starttls}

Once the user entry is found, {{ ydb-short-name }} performs another *bind* operation using the found user's entry and the password provided earlier. The success of this second *bind* operation determines whether the user authentication is successful.

## Client certificate authentication {#client-certificate}

After successful authentication, a token is generated. This token is then used in place of the username and password, speeding up the authentication process and enhancing security.

When using LDAP authentication, no user passwords are stored in ⟦V1⟧.

### Token verification

1. The client establishes a TLS connection to the {{ ydb-short-name }} server, passing the client certificate (and the trust chain).
2. When processing the request, the server extracts the certificate from the TLS context.
3. After a user is authenticated in the system, a token is generated and verified before executing the requested operation. During the token verification process, the system determines on whose behalf the action is being requested and identifies the groups the user belongs to. For users from the LDAP directory, the token does not include information about group memberships. Therefore, after the token is verified, an additional query is made to the LDAP server to retrieve the list of groups the user is a member of.
4. Groups, like users, are entities that can have assigned access rights to perform operations on database schema objects and other resources. These assigned rights determine which operations a user is authorized to perform.

The process of retrieving a user's group list from an LDAP directory is similar to the steps taken during authentication. First, a *bind* operation is performed using the service user credentials specified by the **bind_dn** and **bind_password** parameters in the [ldap_authentication](../concepts/glossary.md#auth-token) section of the configuration file. After successful authentication, a search is conducted for the user entry associated with the previously generated token. This search uses the **search_filter** parameter. If the user still exists, the result of the *search* operation will be a list of attribute values specified by the **requested_group_attribute** parameter. If this parameter is not set, the *memberOf* attribute is used as the default for reverse group membership. The *memberOf* attribute contains the distinguished names (DNs) of the groups to which the user belongs.

### Group search

{% note info %}

By default, ⟦V1⟧ only searches for groups in which the user is a direct member. However, by enabling the **extended_settings.enable_nested_groups_search** flag in the [ldap_authentication](#device-auth) section, ⟦V2⟧ will attempt to retrieve groups at all levels of nesting, not just those the user directly belongs to. If ⟦V3⟧ is configured to work with Active Directory, the Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](./authorization.md) will be used to find all nested groups. This rule allows for the retrieval of all nested groups with a single query. For LDAP servers based on OpenLDAP, group searches will be conducted using recursive graph traversal, which generally requires multiple queries. In both Active Directory and OpenLDAP configurations, the group search is performed only within the subtree specified by the **base_dn** parameter.

{% endnote %}

In the current implementation, the group names that ⟦V1⟧ uses match the values stored in the *memberOf* attribute. These names can be long and difficult to read.


```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```


### Getting groups

Example:

### Server configuration

In the configuration file section that specifies authentication information, the refresh rate for user and group information can be set using the **refresh_time** parameter. For more detailed information about configuration files, refer to the [cluster configuration](../reference/configuration/client_certificate_authorization.md) section.

### Client configuration

It should be noted that currently, {{ ydb-short-name }} does not have the capability to track group renaming on the LDAP server side. Consequently, a group with a new name will not retain the rights assigned to the group under its previous name.

## LDAP users and groups in ⟦V1⟧ {#device-auth}

Since {{ ydb-short-name }} supports various methods of user authentication (login and password authentication, IAM provider usage, LDAP directory), it is often helpful to identify the specific source of authentication when handling user and group names. For all authentication types except login and password, a suffix in the format ⟦C1⟧ is appended to user and group names.

### Why device authentication is needed {#device-auth-motivation}

For LDAP users, the ⟦C1⟧ is determined by the **ldap_authentication_domain** configuration parameter in the [configuration section](⟦U1⟧). By default, this parameter is set to ⟦C2⟧, so all usernames authenticated through the LDAP directory, as well as their corresponding group names in {{ ydb-short-name }}, will follow this format:

1. ⟦C1⟧
2. ⟦C1⟧
3. ⟦C1⟧

To indicate that the entered login should be recognized as a username from the LDAP directory, rather than for login and password authentication, you need to append the LDAP authentication domain suffix. This suffix is specified through the **ldap_authentication_domain** configuration parameter.

### How it works {#device-auth-how-it-works}

1. Below are examples of authenticating the user ⟦C1⟧ using the [{{ ydb-short-name }} CLI](⟦U1⟧):
2. Authentication of a user from the LDAP directory: ⟦C2⟧
3. Authentication of a user using the internal ⟦V1⟧ mechanism: ⟦C1⟧

### TLS connection {#device-auth-interfaces}

Depending on the specified configuration parameters, ⟦V1⟧ can establish either an encrypted or unencrypted connection. An encrypted connection with the LDAP server is established using the TLS protocol, which is recommended for production clusters. There are two ways to enable a TLS connection:

- Automatically via the [⟦C1⟧](../reference/configuration/tls.md#interconnect) connection scheme.
- Using the [⟦C1⟧](../reference/configuration/tls.md#grpc) LDAP protocol extension*.
- When using an unencrypted connection, all data transmitted in requests to the LDAP server, including passwords, will be sent in plain text. This method is easier to set up and is more suited for experimentation or testing purposes.

## LDAPS {#iam}

* To have ⟦V1⟧ automatically establish an encrypted connection with the LDAP server, the **scheme** value in the [configuration parameter](⟦U1⟧) should be set to ⟦C1⟧. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the ⟦C2⟧ scheme. The LDAP server must be configured to accept TLS connections on the specified ports.
* **Refresh Token** — the client (SDK or CLI) is configured with an [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account, based on which the client periodically in the background accesses the IAM API to rotate (obtain the next) token, which is passed in requests.
* **Service Account Key** — the client (SDK or CLI) is configured with the service account attributes and a signing key, based on which the client periodically in the background accesses the IAM API to rotate (obtain the next) token, which is passed in requests.
* **Metadata** — the client (SDK or CLI) periodically accesses a local service to rotate (obtain the next) token, which is passed in requests.
* **OAuth 2.0 token exchange** — the client (SDK or CLI) exchanges a token of another type for an access token according to the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), which is then passed in {{ ydb-short-name }} API requests.

Any holder of a valid token can gain access to perform operations, so the main task of the security system is to ensure the secrecy of the token and prevent its compromise.

Authentication modes with token rotation **Refresh Token** and **Service Account Key** provide a higher level of security compared to the mode with a fixed token **Access Token**, because only short-lived secrets are transmitted over the network to the {{ ydb-short-name }} server.

Maximum security and performance is achieved when using the **Metadata** mode, as it eliminates the need to work with secrets when deploying the application, and also allows you to contact IAM and cache the token in advance, before starting the application.

When choosing an authentication mode among those supported by the server and environment, you should follow these recommendations:

* **Anonymous** is typically used on self-deployed local {{ ydb-short-name }} clusters that are not accessible over the network.
* **Access Token** is used when other modes are not supported on the server side or for configuration/debugging purposes. It does not require client interactions with IAM. However, if IAM supports an API for token rotation, the fixed tokens issued by such IAM usually have a short lifetime, which forces you to manually renew them in IAM on a regular basis.
* **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to data maintenance in the database, performing ad-hoc operations in the CLI, or launching applications from a workstation. Such a token can be obtained manually in IAM once for a long period and saved in an environment variable on a personal workstation for automatic use when starting the CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to run in environments that support the **Metadata** mode, when testing them outside such environments (for example, on a workstation). It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be limited.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to be specified in the parameters can be obtained from the IAM system associated with a particular {{ ydb-short-name }} installation. In particular, for the {{ ydb-short-name }} service in {{ yandex-cloud }}, Yandex.Passport OAuth and service accounts {{ yandex-cloud }} are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems for that organization may be used.

⟦C2⟧ is an LDAP protocol extension that enables message encryption using the TLS protocol. It allows a combination of encrypted and plain-text message transmission within a single connection to the LDAP server. {{ ydb-short-name }} sends a ⟦C3⟧ request to the LDAP server to initiate a TLS connection. In ⟦V3⟧, enabling or disabling TLS within an active session is not supported. Therefore, once an encrypted connection is established using ⟦C4⟧, all subsequent messages sent to the LDAP server will be encrypted. One advantage of using this extension, provided the LDAP server is appropriately configured, is the capability to initiate a TLS connection over an unencrypted port. The extension can be enabled in the [⟦C5⟧ section](⟦U1⟧) of the configuration file.
