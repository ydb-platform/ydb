# {{ ydb-short-name }} for Security Engineers

This section of {{ ydb-short-name }} documentation covers security-related aspects of working with {{ ydb-short-name }}. It'll be useful for compliance purposes too.

## {{ ydb-short-name }} security elements and concepts

![Eagle-view diagram](./_assets/security-overview.png)

The {{ ydb-short-name }} security system operates with the following concepts:

- **Access subjects**:

  - **Users**. {{ ydb-short-name }} supports both internal [users](./authorization.md#user) and external users from third-party directory services, such as LDAP and IAM systems.
  - **Groups**. {{ ydb-short-name }} allows grouping users into named collections. The list of users in a group can be modified later. A group can be empty.
- **Access objects** in {{ ydb-short-name }} are scheme objects (tables, views, etc) for which access rights are configured.
- **Access rights** in {{ ydb-short-name }} are used to determine the list of permitted operations with access objects for a given user or group.

  Access rights represent permission for an access subject to perform a specific set of operations (create, drop, select, update, etc) in a cluster or database on a specific access object.

  Access rights can be granted to a user or a group. When a user is added to a group, the user gets the access rights that were granted to the group. When a user is removed from a group, the user loses the access rights of the group.

  For more information about access rights, see [{#T}](./authorization.md#right).
- **Access levels** in {{ ydb-short-name }} are used to determine the list of additional cluster management operations permitted for a given user or group. {{ ydb-short-name }} uses hierarchical access levels: database, viewer, monitoring, and administration. Higher levels automatically include all lower level privileges. In addition, there are two special non-hierarchical lists: bootstrap (for initial cluster bootstrap) and register node (for dynamic node registration).

  For more information about access levels, see [{#T}](./authorization.md#access-level-lists).
- **[Device authentication](./authentication.md#device-auth)**. When opening a TLS connection, {{ ydb-short-name }} can check the [client certificate](../concepts/glossary.md#client-certificate) and thereby restrict the network perimeter. Such authentication is optional and is configured for supported interfaces. After passing device authentication, [user authentication](./authentication.md) may still be required to access data.
- **[Authentication](./authentication.md) and [authorization](./authorization.md)**. The access control system in {{ ydb-short-name }} provides data protection in a {{ ydb-short-name }} cluster. Due to the access system, only authorized [access subjects](../concepts/glossary.md#access-subject) (users and groups) can work with data. Access to data can be restricted.

  - **Authentication**. When accessing the {{ ydb-short-name }} cluster, [users](../concepts/glossary.md#access-user) undergo [authentication](./authentication.md) — a verification process that confirms the user's identity. {{ ydb-short-name }} supports various authentication mechanisms; their detailed description can be found in the corresponding [authentication](./authentication.md) section.

    It is important to note that regardless of the mechanism used, as a result of successful authentication, users receive an identifier (SID) and an authentication token.

    - The identifier in the form of [SID](./authorization.md#sid) is used to identify the user in {{ ydb-short-name }}. For example, for local users, the SID is the user's login. For external users, the SID also contains information about the user's source. The user's SID can also be found in [system views](../dev/system-views.md#auth) that describe the current security settings.
    - The authentication token is used by {{ ydb-short-name }} nodes to authorize user access before processing user requests.

      The user can then use the received authentication token repeatedly when making requests to the {{ ydb-short-name }} cluster. For more information about the authentication token and related configuration parameters, see [{#T}](../reference/configuration/auth_config.md).
  - **Authorization**. Based on the authentication data, a user then goes through [authorization](./authorization.md) — a process that verifies whether a user has sufficient [access rights](../concepts/glossary.md#access-right) and [access levels](../concepts/glossary.md#access-level) to perform user operations.
- **Audit logs**. Actions aimed at changing security settings are additionally logged in a separate journal called the [audit log](./audit-log.md). This journal will primarily be of interest to those responsible for information security. The audit log includes actions such as creating or deleting access objects, creating or deleting users, changing passwords, granting or revoking access rights, etc.
- **Encryption**. {{ ydb-short-name }} is a distributed system typically running on a cluster, often spanning multiple datacenters. To protect user data, {{ ydb-short-name }} provides the following technologies:

  - [encryption in transit](./encryption/data-in-transit.md) to secure data transmitted between a client and {{ ydb-short-name }}, and between nodes of the {{ ydb-short-name }} cluster.
  - [data encryption at rest](./encryption/data-at-rest.md).
