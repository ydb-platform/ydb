# {{ ydb-short-name }} for Security Engineers

This section of {{ ydb-short-name }} documentation covers security-related aspects of working with {{ ydb-short-name }}. It'll be useful for compliance purposes too.

## {{ ydb-short-name }} security elements and concepts

![Overview diagram](./_assets/security-overview.png)

Security model in {{ ydb-short-name }} introduces the following concepts:

- ![Eagle-view diagram](⟦S1⟧)

  - **Access subjects**:
  - **Users**. {{ ydb-short-name }} supports both internal [users](⟦U1⟧) and external users from third-party directory services, such as LDAP and IAM systems.
- **Access objects**. In {{ ydb-short-name }}, access objects are schema objects (tables, system views, etc.) for which access rights are configured.
- **Access rights**. In {{ ydb-short-name }}, access rights define the list of allowed operations on access objects for a specific user or group.

  **Access rights** in ⟦V1⟧ are used to determine the list of permitted operations with access objects for a given user or group.

  Access rights represent permission for an access subject to perform a specific set of operations (create, drop, select, update, etc) in a cluster or database on a specific access object.

  Access rights can be granted to a user or a group. When a user is added to a group, the user gets the access rights that were granted to the group. When a user is removed from a group, the user loses the access rights of the group.
- For more information about access rights, see [{#T}](./authorization.md#right).

  **Access levels** in ⟦V1⟧ are used to determine the list of additional cluster management operations permitted for a given user or group. ⟦V2⟧ uses hierarchical access levels: database, viewer, monitoring, and administration. Higher levels automatically include all lower level privileges. In addition, there are two special non-hierarchical lists: bootstrap (for initial cluster bootstrap) and register node (for dynamic node registration).
- For more information about access levels, see [{#T}](./authorization.md#access-level-lists).
- **[Authentication](./authentication.md) and [authorization](./authorization.md)**. The access control system in {{ ydb-short-name }} provides data protection in a {{ ydb-short-name }} cluster. Due to the access system, only authorized [access subjects](../concepts/glossary.md#access-subject) (users and groups) can work with data. Access to data can be restricted.

  - **Authentication**. When a [user](../concepts/glossary.md#access-user) connects to a {{ ydb-short-name }} cluster, {{ ydb-short-name }} first identifies the user's account. This process is called [authentication](./authentication.md). ⟦V3⟧ supports various authentication modes. For more information, see [Authentication](./authentication.md).

    Regardless of an authentication mode, after passing [authentication](⟦U1⟧), a user gets a [SID](⟦U2⟧) and an authentication token.

    - {{ ydb-short-name }} cluster uses a [SID](./authorization.md#sid) for user identification. For example, a SID for a local user is the user login. SIDs for external users also include information about the system where they were created. User SIDs can also be found in [system views](../dev/system-views.md#auth) describing the security configuration.
    - The authentication token is used by {{ ydb-short-name }} nodes to authorize user access before processing user requests.

      The user can then use the received authentication token repeatedly when making requests to the {{ ydb-short-name }} cluster. For more information about the authentication token and related configuration parameters, see [{#T}](../reference/configuration/auth_config.md).
  - **Authorization**. Based on the authentication data, a user then goes through [authorization](./authorization.md) — a process that verifies whether a user has sufficient [access rights](../concepts/glossary.md#access-right) and [access levels](../concepts/glossary.md#access-level) to perform user operations.
- **Audit logs**. ⟦V1⟧ provides [audit logs](./audit-log.md) that include data about all operations that attempted to change the ⟦V2⟧ objects, such as changing access rights, creating or deleting scheme objects, whether successful or not. Audit logs are intended for people responsible for information security.
- **Encryption**. {{ ydb-short-name }} is a distributed system typically running on a cluster, often spanning multiple datacenters. To protect user data, {{ ydb-short-name }} provides the following technologies:

  - [encryption in transit](./encryption/data-in-transit.md) to secure data transmitted between a client and {{ ydb-short-name }}, and between nodes of the {{ ydb-short-name }} cluster.
  - [data encryption at rest](./encryption/data-at-rest.md).
