# Authentication

After successfully establishing a network connection, the server accepts requests from the client for processing. Authentication information is transmitted in the form of [an authentication token](../concepts/glossary.md#auth-token) or [a client certificate](../concepts/glossary.md#client-certificate). Based on this, the server determines the client's [SID](../concepts/glossary.md#access-sid) and checks their access to perform the request.

{% note info %}

An authentication client refers to a user undergoing an authentication procedure when accessing {{ ydb-short-name }}. Examples of clients include applications that use [SDK](../reference/ydb-sdk/index.md) or [CLI](../reference/ydb-cli/index.md).

{% endnote %}

The following types of authentication are supported:

* [Anonymous](#anonymous) authentication.
* Authentication via [username and password](#static-credentials).
* Authentication using [LDAP directory](#ldap).
* Authentication using [an external identity provider via the OpenID Connect protocol](#external-idp).
* Authentication using [client certificate](#client-certificate).
* [Authentication using a third-party IAM provider](#iam), for example [Yandex Identity and Access Management](https://yandex.cloud/ru/docs/iam/).

## Anonymous authentication {#anonymous}

By default, {{ ydb-short-name }} allows executing queries without specifying authentication data, such as a username or [token](../concepts/glossary.md#auth-token). Access rights verification ([authorization](authorization.md)) is also not performed.

{% note warning %}

Anonymous authentication should only be used for familiarization purposes for local databases that do not have network access.

{% endnote %}

The flag `enforce_user_token_requirement` in the [authentication mode settings](../reference/configuration/auth_config.md#security-auth) {{ ydb-short-name }} is responsible for turning off anonymous authentication mode.

Depending on the authentication mode settings, real authentication may not be anonymous:

- A token missing from the requests can be replaced with a default token
- A token explicitly specified in the requests can be checked according to the appropriate rules

Then the requests will not be executed anonymously, and rights verification will also be performed.

Depending on [access level settings](../reference/configuration/security_config.md#security-access-levels), anonymous requests can also perform actions in the system that require administrative level access.

## Authentication by login and password {#static-credentials}

Authentication by login and password through the server {{ ydb-short-name }} is available only for [local users](../concepts/glossary.md#access-user). External user authentication involves external system servers.

This type of access implies that each database user has a login and a password.
The user's login can only contain lowercase letters of the Latin alphabet, numbers, and the symbol `@`.
It is possible to set various [criteria](#password-complexity) for password complexity.

The user login and password hash are stored in a table within the authentication component. The password is hashed using the [Argon2](https://ru.wikipedia.org/wiki/Argon2) method. Only the system administrator has access to this table.

In response to the login and password, an [authentication token](../concepts/glossary.md#auth-token) is returned. The default token lifetime is 12 hours. For token rotation, the client, for example, [SDK](../reference/ydb-sdk/index.md), independently accesses the authentication service. Using a token speeds up the authentication process and increases security.

The login and password authentication process includes the following steps:

1. The client accesses the database and sends the user's login and password to the authentication service {{ ydb-short-name }}.
1. The service checks authentication data, creates a token upon successful matching, and returns it to the client.
1. The client accesses the database, providing a token as authentication information.

To enable authentication by login and password, you need to make sure that the parameters `use_login_provider` and `enable_login_authentication` are set to the default value `true` in [the configuration](../reference/configuration/auth_config.md). In addition, to disable anonymous authentication, you need to set the value of the parameter [`enforce_user_token_requirement` to `true`](../reference/configuration/security_config.md).

Read about managing roles and users in [{#T}](../security/authorization.md).

### Password complexity {#password-complexity}

{{ ydb-short-name }} allows you to configure password complexity requirements. If the password provided via the `CREATE USER` or `ALTER USER` commands does not meet the complexity criteria, the command will fail.
By default, there are no restrictions on passwords: a password of any length, including an empty string, is accepted; the password can contain an arbitrary number of digits and letters in any case, as well as special characters from the `!@#$%^&*()_+{}|<>?=` list. To set password complexity restrictions, you need to fill in the `password_complexity` section in the [configuration](../reference/configuration/auth_config.md#password-complexity).


### Forced user blocking/unblocking

There is another way to prevent a user from authenticating — forced blocking by the cluster or database administrator. Administrators can unlock both users who have been forcibly blocked and users who have been blocked due to exceeding the limit on the number of incorrect password entry attempts. Detailed information about forced blocking and unlocking users can be found in the description of the command [`ALTER USER LOGIN/NOLOGIN`](../yql/reference/syntax/alter-user.md).

### Password brute-force protection

{{ ydb-short-name }} provides protection against password brute-force by a user. A user will be considered blocked if they exceed the number of attempts to enter an incorrect password. After the specified time has elapsed, they will again be able to authenticate.

{% note info %}

This mechanism is applicable only to users who are serviced by {{ ydb-short-name }} itself, for so-called embedded users. Users serviced by external authentication sources, such as LDAP servers, are not subject to the password brute-force protection mechanism.

{% endnote %}

By default, a user is given 4 attempts to enter the correct password. Otherwise, authentication will be denied for them for one hour. You can configure the criteria for blocking a user in [configuration](../reference/configuration/auth_config.md#account-lockout).

If necessary, the cluster or database administrator can [unblock](../yql/reference/syntax/alter-user.md) the user prematurely.

Information about the user blocking status and the number of incorrect password entry attempts can be found in the [system view](../dev/system-views.md#информация-о-пользователях-users) of the user.


## Authentication using an LDAP directory {#ldap}

The {{ ydb-short-name }} integrates interaction with the [LDAP directory](https://ru.wikipedia.org/wiki/LDAP). The LDAP directory is external to the {{ ydb-short-name }} service and is used for user authentication and authorization in the database. Before using this method of authentication and authorization, you must have an LDAP service deployed and network access configured between it and the {{ ydb-short-name }} servers.

Examples of supported LDAP directory implementations: [OpenLdap](https://openldap.org/), [Active Directory](https://azure.microsoft.com/en-us/products/active-directory/).

### Authentication

Authentication using the LDAP protocol is similar to the process of authentication via login and password. The only difference is that the LDAP directory plays the role of the authentication component. The LDAP directory is used to verify the login/password pair and to determine the groups to which the user belongs.

{% note info %}

Since the LDAP directory is an external independent service, {{ ydb-short-name }} does not have the ability to manage user accounts in the directory. For successful authentication, the user must already be created in the LDAP directory. Using the commands `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, `DROP GROUP` will not affect the list of users and groups in the directory. Information on managing user accounts must be sought in the documentation of the LDAP directory being used.

{% endnote %}

At the moment, {{ ydb-short-name }} supports only one method of LDAP user authentication — *search+bind*: after receiving the username and password, a utility *bind* is performed on behalf of the [service account](#ldap-service-account-auth) (username and password in `bind_dn` / `bind_password` or certificate and SASL EXTERNAL — see [configuration](../reference/configuration/auth_config.md#ldap-auth-config)), followed by a search for the user record and a second *bind* on behalf of the user.

{% note info %}

A service account is understood as a separate account in the LDAP directory, which applications/services use to connect to LDAP and perform the necessary operations.

{% endnote %}

The service account credentials for connecting to LDAP are specified in the configuration settings: use the `bind_dn` and `bind_password` parameters, or configure [mTLS](../concepts/glossary.md#mtls) (for more details, see the [Service account authentication](#ldap-service-account-auth) section).

Next, the authentication process follows the following scheme:

1. {{ ydb-short-name }} connects to LDAP on behalf of the service account.
2. After successful connection, a search for the user attempting to authenticate is performed. The search is conducted across the entire subtree specified in the configuration parameter `base_dn` and using the filter set in the parameter `search_filter`.
3. If the user is found, {{ ydb-short-name }} performs the bind operation again — on behalf of the found user, using their password.
4. The final result — successful or unsuccessful authentication — is determined by the result of the second bind (on behalf of the user).

Thus, {{ ydb-short-name }} does not store user passwords and relies entirely on the LDAP authentication mechanism.

As a result of successful verification of the user's login and password in the LDAP directory, an [authentication token](../concepts/glossary.md#auth-token) {{ ydb-short-name }} is returned. This token is then used instead of the login and password. Using the token speeds up the authentication process and increases security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

#### Authentication of the service account {#ldap-service-account-auth}

A service account can be authenticated in two main ways:

* Using a username and password.
  In this case, the configuration needs to specify the username (`bind_dn`) and password (`bind_password`). These parameters will be used to connect to the LDAP server on behalf of the service account.

* Using mTLS (mutual TLS) via the SASL EXTERNAL mechanism.
  In this option, certificates are used for authentication instead of a username and password. This allows you not to store the service account password in the configuration — it is enough to specify the certificate files (`use_tls.cert_file`) and the private key (`use_tls.key_file`), as well as enable a special flag (`extended_settings.enable_sasl_external_bind`). For detailed setup information, see [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config).

### Token verification {#token-validation}

After user authentication, an [authentication token](../concepts/glossary.md#auth-token) is generated, which is sent with requests to {{ ydb-short-name }}. When the token is verified, the node determines the user on whose behalf the request is made and the groups to which they belong. Depending on the authentication method, a cryptographic verification of the token may be performed on the {{ ydb-short-name }} side or a request may be made to an external authentication system. For example, for a user from an LDAP directory, the token does not contain information about groups, so the node makes another request to the LDAP server to obtain the user's group list. Network requests increase the time required to verify the authentication token and process the request, as well as the load on the external system. Therefore, {{ ydb-short-name }} nodes [cache the verification results](./caching-authentication-results.md).

Groups, like the user themselves, are subjects performing operations on database schema objects. Access rights can be assigned to subjects to control access to different database resources. Subjects will be authorized to perform certain operations in accordance with the list of assigned rights.

The process of obtaining a list of user groups from the LDAP directory is similar to the actions performed during authentication. First, the *bind* operation is performed for the service user, whose credentials are recorded in the parameters `bind_dn` and `bind_password` of the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file. After successful authentication, a search is performed for the user for whom the token was previously generated. The search is also performed in accordance with the `search_filter` parameter. If the user still exists, then the result of the *search* operation will be a list of values of the attribute recorded in the `requested_group_attribute` parameter. If this parameter is empty, then the attribute for reverse group membership will be `memberOf`. The `memberOf` attribute stores the unique names (Distinguished Name, DN) of the groups to which the user belongs.

#### Getting groups

By default, {{ ydb-short-name }} searches only for those groups that the user is directly a member of. By enabling the `extended_settings.enable_nested_groups_search` flag, the {{ ydb-short-name }} section [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) will attempt to retrieve groups at all levels of nesting, not just those that the user is directly part of. If {{ ydb-short-name }} is configured to work with Active Directory, a rule specific to Active Directory [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN) will be used to find all nested groups. This rule allows you to retrieve all nested groups with a single request. For OpenLDAP-based LDAP servers, group search will be performed by recursively traversing the graph, which generally requires multiple requests. For both Active Directory and OpenLDAP, group search will be performed only for the subtree whose root is taken from the `base_dn` configuration parameter.

{% note info %}

In the current implementation, the group names that {{ ydb-short-name }} will operate with match the values recorded in the `memberOf` attribute. They can be long and difficult to read.

Example:

```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

The frequency of updating information about the user and their groups is set by the parameter [`auth_config.refresh_time`](../reference/configuration/auth_config.md#caching-auth-results). For more details, see the article about [caching authentication results](./caching-authentication-results.md#refreshing-user-tokens).

{% endnote %}

{% note warning %}

It should be noted that at the moment {{ ydb-short-name }} does not have the ability to track group renames made on the LDAP server side. Thus, a group with a new name will not have the same rights as the group with the previous name.

{% endnote %}

### LDAP users and LDAP groups in {{ ydb-short-name }}

Since {{ ydb-short-name }} allows using different methods of user authentication, it is often useful to distinguish where exactly the user was authenticated when working with user and group names. For all types of authentication, except for login and password authentication, group and user names are supplemented with a suffix of the form `@<auth-domain>`.

For LDAP users *auth-domain* is set in [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) `ldap_authentication_domain`. By default, it has the value `ldap`, so all usernames authenticated via the LDAP directory and the names of the groups they belong to in {{ ydb-short-name }} will have the following form:

- `user1@ldap`
- `group1@ldap`
- `group2@ldap`

{% note warning %}

To distinguish that the entered login should be a user login from the LDAP directory, not a local user login {{ ydb-short-name }}, you need to add the suffix `@ldap` to it.

Below are examples of user authentication `user1` using [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md):

* User authentication from LDAP directory: `ydb --user user1@ldap -p ydb_profile scheme ls`
* User authentication with the internal mechanism {{ ydb-short-name }}: `ydb --user user1 -p ydb_profile scheme ls`

{% endnote %}

### TLS connection {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or unencrypted connection. An encrypted connection with the LDAP server is established using the TLS protocol. This method is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically. The connection scheme is used [`ldaps`](#ldaps)
* Using the LDAP protocol extension [`StartTls`](#starttls)

When using an unencrypted connection, all data transmitted in requests to the LDAP server will be sent in plain text, including passwords. This type of connection is easier to start using and is more suitable for experiments or testing.

#### LDAPS

In order for {{ ydb-short-name }} to automatically establish an encrypted connection with the LDAP server, it is necessary to set the value `ldaps` in the [configuration parameter](../reference/configuration/auth_config.md#ldap-auth-config) **scheme**. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the `ldaps` scheme. The LDAP server must be configured to accept TLS connections on the specified ports.

#### Extension of the LDAP protocol `StartTls` {#starttls}

`StartTls` is an LDAP protocol extension used to encrypt messages over the TLS protocol. It allows some messages to be transmitted in encrypted form and others in plain text within a single connection to the LDAP server. A message with this extension is sent from {{ ydb-short-name }} to the LDAP server to initiate a TLS connection. In the case of {{ ydb-short-name }}, it is not possible to enable and disable the TLS connection within a single connection. Therefore, when using the `StartTls` extension, after establishing an encrypted connection, {{ ydb-short-name }} will send all further messages to the LDAP server in encrypted form. One of the advantages of using this extension instead of the `ldaps` scheme (with the corresponding LDAP server configuration) is the ability to establish a TLS connection on an unencrypted port. The extension is included in the [ `use_tls` ](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file.

## Authentication using an external IdP via the OpenID Connect protocol {#external-idp}

{{ ydb-short-name }} can authenticate users via [JWT tokens](https://www.rfc-editor.org/rfc/rfc7519) issued by an external [identity provider](https://csrc.nist.gov/glossary/term/identity_provider) (Identity Provider, IdP) that supports the [OpenID Connect](https://openid.net/developers/how-connect-works/) (OIDC) protocol. The provider is responsible for user authentication and issuing the token, while {{ ydb-short-name }} verifies the signature and claims about the subject and the conditions of the token's validity and generates the user's SID and their groups.

Obtaining and updating the JWT token are performed on the client side and by the IdP. {{ ydb-short-name }} does not redirect the user to the IdP login page and does not exchange `authorization code` for tokens. The client passes the already obtained JWT token as a Bearer token with each request.

### Principle of operation

1. The client authenticates with an external IdP and receives a signed JWT token.
2. The client sends a token in a request to {{ ydb-short-name }} with type `Bearer`.
3. The node {{ ydb-short-name }} requests the OIDC Discovery document at `<issuer>/.well-known/openid-configuration`, where `<issuer>` is the configured provider URL and the token issuer identifier. For example, for `issuer: https://idp.example.com`, the request is sent to `https://idp.example.com/.well-known/openid-configuration`. The value of the `issuer` field in the document must exactly match the configured provider address.
4. From the `jwks_uri` field of the Discovery document, a URL is extracted, from which {{ ydb-short-name }} periodically retrieves a set of public keys [JWKS](https://www.rfc-editor.org/rfc/rfc7517).
5. The public key is selected based on the fields `alg` (algorithm, signature algorithm) and `kid` (key ID, key identifier) in the JWT header. {{ ydb-short-name }} verifies the token signature, issuer, recipient, and expiration dates.
6. From the token fields, the user ID and list of groups are extracted. An configured authentication domain suffix is added to them, and the resulting SIDs are used for [authorization](./authorization.md).

The Discovery URL, `issuer` and `jwks_uri` must use the `https://` scheme. If the Discovery document or JWKS is temporarily unavailable, {{ ydb-short-name }} retries requests with an increasing interval. The obtained keys are cached and periodically updated to support key rotation on the IdP side. After the cache lifetime expires, outdated keys are deleted; until JWKS is successfully updated, new token verifications result in a temporary error.

### Requirements for token and keys

The JWT token must have a correct compact serialization format and contain the fields `alg` and `kid` in the header. Only asymmetric signature algorithms are supported:

- RSA PKCS#1: `RS256`, `RS384`, `RS512`;
- RSA-PSS: `PS256`, `PS384`, `PS512`;
- ECDSA: `ES256`, `ES384`, `ES512`.

Symmetric algorithms of the `HS*` family are not supported. The public key in JWKS must have matching `kty` (key type) and `kid` and contain `x5c` (X.509 certificate chain, a chain of X.509 certificates). The public key is extracted from the first `x5c` certificate; JWKs containing only RSA or EC parameters without `x5c` are skipped.

The following fields are required for successful authentication:

- `alg` and `kid` in the JWT header;
- statement `iss` (issuer, token issuer) matching the value of `issuer` in the configuration;
- non-empty string user identifier. It is obtained using the statement specified by the parameter `subject_claim_name`. If this statement is missing or has a different type, the standard statement `sub` (subject, subject identifier) is used.

The remaining fields to be checked are optional:

- the assertion `aud` (audience, token recipient) is checked if the `audience` parameter is set in the configuration; it is recommended to always specify the expected audience so that tokens issued for other services are not accepted;
- when checking time-related assertions such as `exp` (expiration time, expiration time), `nbf` (not before, start time) and `iat` (issued at, issue time), the permissible time difference specified by [the `allowed_clock_skew`](../reference/configuration/auth_config.md#external-idp-auth-config) parameter is taken into account. If `exp` is missing, {{ ydb-short-name }} sets the authentication result expiration time to 10 minutes;
- the statement specified by the parameter `groups_claim_name` contains a list of user groups. It must be an array; only string elements are extracted from the array. If the statement is missing or has a different type, the group list is considered empty.

### SID formation

A suffix `@<auth-domain>` is added to the user identifier and each group from the JWT. The value `<auth-domain>` is set by the parameter `external_idp_authentication_domain`; the default value is `sso`.

For example, with `sub: user1`, `groups: [admins, developers]` and the default domain, the following SIDs will be generated:

- user `user1@sso`;
- groups `admins@sso` and `developers@sso`.

{{ ydb-short-name }} uses a list of groups from the token without additional requests to the IdP and without exposing nested groups. It is not possible to manage users and groups of an external IdP using the commands `CREATE USER`, `ALTER USER`, `CREATE GROUP`, and `ALTER GROUP`. Permissions are assigned to the formed SIDs using the methods described in the section [{#T}](./authorization.md).

### Server setup

Authentication through an external IdP is enabled in [authentication configuration](../reference/configuration/auth_config.md#external-idp-auth-config) if there is a `external_idp_config` section.

## Client authentication by certificate {#client-certificate}

{{ ydb-short-name }} can authenticate a client using client certificate data obtained during the establishment of a TLS connection. The verification is performed at the application protocol level (gRPC, etc.) when the server is already accepting requests over an open connection.

This method is suitable, for example, in corporate scenarios with centralized certificate issuance.

### Principle of operation

1. The client establishes a TLS connection with the server {{ ydb-short-name }}, transmitting a client certificate (and the chain of trust).
2. When processing the request, the server retrieves the certificate from the TLS context.
3. The server uses a certificate for authentication and verifies it according to the rules of the section [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md).
4. As a result of a successful certificate check, the client is assigned a security identifier [SID](../concepts/glossary.md#access-sid), which has all the [rights](../concepts/glossary.md#access-right) assigned to the corresponding identifier.

Certificate authentication is only applied to requests without [an authentication token](../concepts/glossary.md#auth-token). If the client provides an authentication token — for example, in the `Authorization` header for HTTP or through SDK/CLI mechanisms for IAM, login, and password — the token takes precedence. In this case, the certificate is transmitted at the TLS level, but is not used for authentication.

### SID formation

{% note info %}

Checking the client certificate during [device authentication](#device-auth) and user authentication using a client certificate are different mechanisms. Device authentication restricts the network perimeter without generating a SID; user authentication using a client certificate generates a SID and groups for [authorization](./authorization.md).

{% endnote %}

Successful authentication with a certificate creates a user SID with the suffix `@<domain>`, where `<domain>` is [the value of the parameter](../reference/configuration/auth_config.md#certificate-auth-config) `certificate_authentication_domain` in the section `auth_config` (default: `cert`). The name is formed from all the attributes of the Subject field of the certificate in the notation `Имя=Значение,...@<domain>`. The order of the attributes corresponds to the order of the fields in the certificate. Example:

```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```

### Getting groups

If blocks `client_certificate_definitions` are specified in the section [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md), the certificate is accepted if it matches at least one of them. For each matching block, the client is added to the groups from `member_groups`. If `member_groups` is not specified, the default group is used — `default_group` (default value: `DefaultClientAuth@cert`).

### Server setup

The rules for certificate verification and group assignment are specified in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section of the cluster's static configuration. To enable client certificate requests during the TLS-handshake over gRPCs, set the `request_client_certificate: true` parameter.

### Client setup

Learn more about configuring [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) in the [TLS connection parameters](../reference/ydb-cli/connect.md#tls) section.

## Device authentication by certificate {#device-auth}

Device authentication — verification of the [client certificate](../concepts/glossary.md#client-certificate) when establishing a TLS connection; [SID](../concepts/glossary.md#access-sid) is not generated in this case. If a certificate is presented, the chain of trust to the CA is verified; an untrusted certificate leads to the rejection of the connection before processing application requests. The requirement to present a certificate depends on the interface (see section [Usage in {{ ydb-short-name }}](#device-auth-interfaces)).

### Why is device authentication needed {#device-auth-motivation}

Device authentication solves the following tasks in {{ ydb-short-name }}:

1. Cluster isolation — limit the range of hosts and applications that can establish a TLS connection with the nodes {{ ydb-short-name }}.

2. Protection against configuration errors — to prevent connecting to foreign clusters {{ ydb-short-name }}, for example, if the [node-broker](../devops/configuration-management/configuration-v1/node-authorization.md) parameter is incorrect, the dynamic node will not connect to a foreign cluster, whereas with regular TLS such a connection could be established.

3. Complicating attacks on the application layer — a process on an external host without a suitable certificate does not gain access to the cluster API, even if a network route to the port exists.

After device authentication, user or application [authentication](./authentication.md) may be required to access data. It can be carried out not only with a verified client certificate, but also using other authentication methods in {{ ydb-short-name }}, for example, with [username and password](./authentication.md#static-credentials).

### The principle of operation {#device-auth-how-it-works}

1. The client establishes a TLS connection with the server {{ ydb-short-name }} and, if the connection interface requires it, presents a client certificate.
2. If a certificate is presented, the server checks it at the TLS level: the chain of trust to the configured certification authority (CA), the validity period, etc. Additional rules for matching certificate fields (for example, `require_same_issuer`, Subject and SAN) are applied during [client authentication by certificate](./authentication.md#client-certificate).
3. If the check is successful, the connection is opened; if it is unsuccessful, it is rejected.

### Use in {{ ydb-short-name }} {#device-auth-interfaces}

Device authentication is optional and can be configured independently: the mechanism can be enabled on some ports and disabled on others.

- **Interconnect** — when TLS is enabled in the [interconnect_config](../reference/configuration/tls.md#interconnect) [Interconnect](../concepts/glossary.md#actor-system-interconnect) section, a client certificate is required.

- **Kafka API** — when mTLS is enabled, it requires a client certificate; only the trust chain to the CA is checked, a connection without a certificate or with an untrusted certificate is not established. Server configuration is described in the section [kafka_proxy_config](../reference/configuration/kafka_proxy_config.md), and client connection is described in the section [Device authentication via mTLS](../reference/kafka-api/auth.md#device-auth).

- **gRPC** and **YDB Monitoring** — you can enable the request for a client certificate for device authentication, and also separately enable its mandatory verification (an untrusted certificate is always rejected). The gRPC configuration is described in the sections [grpc_config](../reference/configuration/tls.md#grpc) and [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md), and the client connection is described in the [TLS connection parameters](../reference/ydb-cli/connect.md#tls) section; the YDB Monitoring configuration is described in the [monitoring_config](../reference/configuration/monitoring_config.md#tls) section.

## Authentication using a third-party IAM provider {#iam}

* **Access Token** — a fixed token is set as a parameter for the client (SDK or CLI) and is passed in requests.
* **Refresh Token** — a parameter for the client (SDK or CLI) that sets the [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account, based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token passed in requests.
* **Service Account Key** — the client (SDK or CLI) is configured with the service account attributes and the signing key, based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token, which is passed in requests.
* **Metadata** — the client (SDK or CLI) periodically accesses the local service to rotate (get the next) token, which is passed in requests.
* **OAuth 2.0 token exchange** — the client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth token exchange protocol 2.0](https://www.rfc-editor.org/rfc/rfc8693), which is then passed to {{ ydb-short-name }} API requests.

Any holder of a valid token can gain access to perform operations, so the main task of the security system is to ensure the secrecy of the token and prevent its compromise.

Authentication modes with token rotation **Refresh Token** and **Service Account Key** provide a higher level of security compared to the mode with a fixed token **Access Token**, as only short-lived secrets are transmitted to the server {{ ydb-short-name }} over the network.

Maximum security and performance are ensured when using the **Metadata** mode, as it eliminates the need to work with secrets when deploying the application and allows you to access IAM and cache the token in advance, before launching the application.

When selecting an authentication mode among those supported by the server and environment, the following recommendations should be followed:

* **Anonymous** is usually applied on self-deployed local clusters {{ ydb-short-name }} that are not accessible over the network.
* **Access Token** is used when the server does not support other modes or for configuration/debugging purposes. It does not require client interaction with IAM. However, if IAM supports an API for token rotation, the fixed tokens issued by such IAM usually have a short lifespan, which requires them to be manually updated in IAM regularly.
* **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, those related to data maintenance in the database, performing ad-hoc operations in the CLI, or running applications from a workstation. Such a token can be obtained manually in IAM once for a long time and stored in an environment variable on a personal workstation for automatic use when launching the CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to operate in environments that support **Metadata** mode, when testing them outside such environments (for example, on a workstation). It can also be used for applications outside such environments, working as an analogue of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be restricted.
* **Metadata** is used when deploying applications to the cloud. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to be specified in the parameters can be obtained in the IAM system associated with a specific installation {{ ydb-short-name }}. In particular, for the {{ ydb-short-name }} service in {{ yandex-cloud }}, Yandex.Passport OAuth and service accounts {{ yandex-cloud }} are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems of the organization may be applied.

When using modes that involve the client {{ ydb-short-name }} accessing IAM, an additional IAM URL can be specified, which provides a token issuance API. By default, existing SDKs and CLIs attempt to access the IAM API {{ yandex-cloud }} hosted at `iam.api.cloud.yandex.net:443`.
