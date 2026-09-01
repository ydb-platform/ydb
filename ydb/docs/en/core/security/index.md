# {{ ydb-short-name }} for Security Engineers

This section of the {{ ydb-short-name }} documentation covers security-related aspects of working with {{ ydb-short-name }}. It will also be useful for compliance purposes.

## {{ ydb-short-name }} security elements and concepts

![Eagle-view diagram](./_assets/security-overview.png)

The {{ ydb-short-name }} security system operates with the following concepts:

- **Access subjects**:

  - **Users**. {{ ydb-short-name }} supports both internal [users](./authorization.md#user) and external users from third-party directory services, such as LDAP and IAM systems.
  - **Groups**. {{ ydb-short-name }} allows you to combine users into named sets. The composition of users in a group can be changed later. A group can also be empty — containing no users.
- **Access objects**. In {{ ydb-short-name }}, access objects are schema objects (tables, system views, etc.) for which access rights are configured.
- **Access rights**. **Access rights** in {{ ydb-short-name }} define the list of allowed operations on access objects for a specific user or group.

  Access rights are the ability to perform certain actions (create, delete, read, update, etc.) on an access object.

  Access rights can be granted to a specific user or a group. A user who is added to a group is granted the rights previously assigned to that group for the duration of their membership in the group.

  For more information about access rights, see [{#T}](./authorization.md#right).
- **Access levels**. **Access levels** in {{ ydb-short-name }} define access to additional cluster management capabilities for a specific user or group. {{ ydb-short-name }} uses hierarchical access levels: database, viewer, monitoring, and administration. Higher levels automatically include all lower ones. Additionally, there are two special non-hierarchical lists: bootstrap (for initial cluster initialization) and register node (for registering dynamic nodes).

  For more information about access levels, see [{#T}](./authorization.md#access-level-lists).
- **[Authentication](./authentication.md) and [authorization](./authorization.md)**. The access control system in {{ ydb-short-name }} provides a mechanism for protecting data in the {{ ydb-short-name }} cluster. Only authenticated [access subjects](../concepts/glossary.md#access-subject) (users and groups) can work with data, and access to data can be restricted.

  - **[Device authentication](./authentication.md#device-auth)**. When opening a TLS connection, {{ ydb-short-name }} can verify the [client certificate](../concepts/glossary.md#client-certificate) and thus restrict the network perimeter. Such authentication is optional and configurable for supported interfaces. After device authentication, [user authentication](./authentication.md) may still be required to access data.
  - **Client authentication**. When accessing the {{ ydb-short-name }} cluster, [users](../concepts/glossary.md#access-user) undergo [authentication](./authentication.md) — a verification process that confirms the user's identity. {{ ydb-short-name }} supports various authentication mechanisms; their detailed description can be found in the corresponding [authentication](./authentication.md) section.

    It is important to note that regardless of the mechanism used, upon successful authentication users receive an identifier (SID) and an authentication token.

    - The identifier in the form of a [SID](./authorization.md#user) is used to identify a user in {{ ydb-short-name }}. For example, for local users, the SID is the user's login. For external users, the SID also contains information about the user's origin. The user's SID can also be found in [system views](../dev/system-views.md#auth) that describe current security settings.
    - The authentication token is used by {{ ydb-short-name }} nodes to authorize user access before processing their requests.

      The user can then use the received authentication token repeatedly when making requests to the {{ ydb-short-name }} cluster. For more information about the authentication token and related configuration parameters, see [{#T}](../reference/configuration/auth_config.md).
  - **Authorization**. Based on the authentication data, a user then goes through [authorization](./authorization.md) — a process that verifies whether a user has sufficient [access rights](../concepts/glossary.md#access-right) and [access levels](../concepts/glossary.md#access-level) to perform user operations.
- **Audit logs**. Actions aimed at changing security settings are additionally logged in a separate journal called the [audit log](./audit-log.md). This journal will primarily be of interest to those responsible for information security. The audit log captures actions such as creating or deleting access objects, creating or deleting users, changing passwords, granting or revoking access rights, etc.
- **Encryption**. {{ ydb-short-name }} is a distributed system typically running on clusters located in multiple data centers. To protect user data, {{ ydb-short-name }} provides the following mechanisms:

  - [encryption in transit](./encryption/data-in-transit.md) to secure data transmitted between a client and {{ ydb-short-name }}, and between nodes of the {{ ydb-short-name }} cluster.
  - [data encryption at rest](./encryption/data-at-rest.md).
