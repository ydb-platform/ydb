# Authentication

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

The following authentication modes are supported:

* Anonymous access is enabled by default and available immediately when you [install the cluster](../devops/index.md).
* [Authentication through a third-party IAM provider](#iam), for example, [Yandex Identity and Access Management]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/){% endif %}.
* Authentication by [username and password](#static-credentials).

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

{% include [overlay/auth_choose.md](_includes/connect_overlay/auth_choose.md) %}

## Authenticating by username and password {#static-credentials}

Authentication by username and password includes the following steps:

1. The client accesses the database and presents their username and password to the {{ ydb-short-name }} authentication service.

   You can only use lower case Latin letters and digits in usernames. No restrictions apply to passwords (empty passwords can be used).

1. The authentication service passes authentication data to the {{ ydb-short-name }} authentication component.
1. The component validates authentication data. If the data matches, it generates a token and returns it to the authentication service.

   Tokens accelerate authentication and strengthen security. Tokens have a default lifetime of 12 hours. YDB SDK rotates tokens by accessing the authentication service.

   The username and hashed password are stored in the table inside the authentication component. The password is hashed by the [Argon2]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Argon2){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Argon2){% endif %} method. In authentication mode, only the system administrator can use a username/password pair to access the table.

1. The authentication system returns the token to the client.
1. The client accesses the database, presenting their token as authentication data.

To enable username/password authentication, use `true` in the `enforce_user_token_requirement` key of the cluster's [configuration file](../reference/configuration/index.md#auth).

To learn how to manage roles and users, see [{#T}](../security/access-management.md).

## Interaction with the LDAP directory {#ldap-auth-provider}

The {{ ydb-short-name }} has integrated interaction with an [LDAP directory](https://en.wikipedia.org/wiki/LDAP). The LDAP directory is an external service in relation to {{ ydb-short-name }} and is used for the authentication and authorization of database users. Before using this method of authentication and authorization, you must have a deployed LDAP service and configured network access between it and the {{ ydb-short-name }} servers.

Examples of supported LDAP directory implementations: [OpenLdap](https://openldap.org/), [Active Directory](https://azure.microsoft.com/en-us/products/active-directory/).

## Authentication

Authentication using the LDAP protocol is similar to the static credentials authentication process (using login and password). The difference lies in the fact that the LDAP directory acts as the authentication component. The LDAP directory is used only to verify the login/password pair.

{% note info %}

Since the LDAP directory is an external independent service, {{ ydb-short-name }} cannot manage user accounts in the directory. For successful authentication, a user must already be created in the LDAP directory. Using the commands `CREATE USER`, `CREATE GROUP`, `ALTER USER`, `ALTER GROUP`, `DROP USER`, `DROP GROUP` will not affect the list of users and groups in the directory. Information about managing accounts should be sought in the documentation of the LDAP directory being used.

{% endnote %}

Currently, {{ ydb-short-name }} supports only one method of LDAP authentication, known as the search+bind method, which consists of several steps. After receiving the username and password of the user to be authenticated, a *bind* operation is performed using credentials of a special service account predefined in the [ldap_authentication](../reference/configuration/index.md#ldap-auth-config) section. The credentials of this service account are specified through the **bind_dn** and **bind_password** configuration parameters. After the service user is successfully authenticated, a search is conducted in the LDAP directory for the user attempting to authenticate in the system. The *search* operation is performed across the entire subtree, the root of which is specified by the **base_dn** configuration parameter. The search within the subtree is carried out according to the filter defined in the **search_filter** configuration parameter. Once the user entry is found, {{ ydb-short-name }} performs another *bind* operation on behalf of this found user, using the previously provided password. The success of the user authentication is determined by the result of this second *bind* operation.

After successful user authentication, a token is generated. This token is then used instead of the username and password. Using the token speeds up the authentication process and enhances security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

### Token verification

After a user is authenticated in the system, a token is generated, which is verified before executing the requested operation. During the token verification process, it is determined on whose behalf the action is being requested in the system and which groups the user belongs to. For users from the LDAP directory, the token does not contain information about groups. Therefore, after the token is verified, an additional query is made to the LDAP server to obtain the list of groups the user belongs to.

Groups, like users themselves, are entities that perform operations on database schema objects. To manage access to different database resources, access rights can be assigned to these entities. Based on the list of assigned rights, entities will be authorized to perform various operations.

The process of retrieving a user's group list from an LDAP directory is similar to the actions performed during authentication. First, a *bind* operation is executed for the service user, whose credentials are specified in the **bind_dn** and **bind_password** parameters of the [ldap_authentication](../reference/configuration/index.md#ldap-auth-config) section in the configuration file. After successful authentication, a search is conducted for the user's entry for whom the token was previously generated. This search is also carried out according to the **search_filter** parameter. If the user still exists, the result of the *search* operation will be a list of attribute values specified in the **requested_group_attribute** parameter. If this parameter is empty, the attribute for reverse group membership will be *memberOf*. The *memberOf* attribute stores the distinguished names (DNs) of the groups to which the user belongs.

#### Group search

By default, {{ ydb-short-name }} searches only for those groups in which the user is directly a member. By enabling the flag **extended_settings.enable_nested_groups_search** in the [ldap_authentication](../reference/configuration/index.md#ldap-auth-config) section, {{ ydb-short-name }} will attempt to retrieve groups at all levels of nesting, not just those the user directly belongs to. If {{ ydb-short-name }} is configured to work with Active Directory, the Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN) will be used to find all nested groups. This rule allows all nested groups to be retrieved with a single query. For LDAP servers based on OpenLDAP, the group search will be performed with a recursive traversal of the graph, which generally requires multiple queries. For both Active Directory and OpenLDAP, the group search will be performed only for the subtree rooted at the configuration parameter **base_dn**.

{% note info %}

In the current implementation, the group names that {{ ydb-short-name }} operates with match the values recorded in the *memberOf* attribute. These names can be lengthy and difficult to read.

Example:

```text
cn=Developers,ou=Groups,dc=mycompany,dc=net@ldap
```

{% endnote %}

{% note info %}

In the configuration file section that describes authentication information, you can set the refresh rate for the user and group information. This is controlled by the **refresh_time** parameter. For more detailed information about configuration files, refer to the [cluster configuration](../reference/configuration/index.md#auth-config) section.

{% endnote %}

{% note warning %}

It should be noted that currently, {{ ydb-short-name }} does not have the capability to track groups renaming made on the LDAP server side. As a result, a group with a new name will not possess the same rights as the group with the previous name.

{% endnote %}

### LDAP users and groups in {{ ydb-short-name }}

Since {{ ydb-short-name }} allows for various methods of user authentication (login and password authentication, using an IAM provider, LDAP directory), it is often useful to distinguish where exactly the user authenticated when handling user and group names. For all types of authentication except for login and password authentication, user and group names are appended with a suffix in the format <username>@<domain>.

For LDAP users, the *domain* is specified in the [configuration parameter](../reference/configuration/index.md#ldap-auth-config) **ldap_authentication_domain**. By default, it is set to `ldap` so all usernames authenticated through the LDAP directory, as well as the group names they belong to in {{ ydb-short-name }}, will have the following format:

- `user1@ldap`
- `group1@ldap`
- `group2@ldap`

{% note warning %}

To distinguish that the entered login should be a username from the LDAP directory rather than for login and password authentication, you need to add the LDAP authentication domain suffix to it. The suffix is specified through the **ldap_authentication_domain** configuration parameter.

Below are examples of authenticating the user `user1` using the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md):

* Authentication of a user from the LDAP directory: `ydb --user user1@ldap -p ydb_profile scheme ls`
* Authentication of a user using the internal {{ ydb-short-name }} mechanism: `ydb --user user1 -p ydb_profile scheme ls`

{% endnote %}

### TLS connection {#ldap-tls}

Depending on the specified configuration parameters, {{ ydb-short-name }} can establish either an encrypted or unencrypted connection. An encrypted connection with the LDAP server is established using the TLS protocol. This method is recommended for production clusters. There are two ways to enable a TLS connection:

* Automatically. The [`ldaps`](#ldaps) connection scheme is used.
* Using the LDAP protocol extension [`StartTls`](#starttls).

When using an unencrypted connection, all data transmitted in requests to the LDAP server will be sent in plain text, including passwords. This connection method is easier to start using and is more suitable for experimentation or testing.

#### LDAPS {#ldaps}

In order to {{ ydb-short-name }} automatically establish an encrypted connection with the LDAP server, the **scheme** value in the [configuration parameter](../reference/configuration/index.md#ldap-auth-config) should be set to `ldaps`. The TLS handshake will be initiated on the port specified in the configuration. If no port is specified, the default port 636 will be used for the `ldaps` scheme. The LDAP server must be configured to accept TLS connections on the specified ports.

#### LDAP protocol extension `StartTls` {#starttls}

`StartTls` is an LDAP protocol extension used for encrypting messages via the TLS protocol. It allows some messages to be transmitted in encrypted form and others in plain text within a single connection to the LDAP server. The message with this extension is sent from {{ ydb-short-name }} to the LDAP server to initiate a TLS connection. In the case of {{ ydb-short-name }}, enabling and disabling a TLS connection within a single session is not supported. Therefore, when using the `StartTls` extension, after establishing an encrypted connection, {{ ydb-short-name }} will send all subsequent messages to the LDAP server in encrypted form. One of the advantages of using this extension instead of the `ldaps` scheme (with the appropriate LDAP server configuration) is the ability to establish a TLS connection on an unencrypted port. The extension is enabled in the [`use_tls` section](../reference/configuration/index.md#ldap-auth-config) of the configuration file.
