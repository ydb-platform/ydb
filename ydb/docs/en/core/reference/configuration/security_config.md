# Section `security_config`

The `domains_config.security_config` section defines [authentication](../../security/authentication.md) modes, initial configuration of local [users](../../concepts/glossary.md#access-user) and [groups](../../concepts/glossary.md#access-group), and their [access rights](../../concepts/glossary.md#access-right).

```yaml
domains_config:
  ...
  security_config:
    # authentication mode configuration
    enforce_user_token_requirement: false
    enforce_user_token_check_requirement: false
    default_user_sids: <authentication token for anonymous requests>
    all_authenticated_users: <group name for all authenticated users>
    all_users_group: <group name for all users>

    # initial security configuration
    default_users: <default user list>
    default_groups: <default group list>
    default_access: <default access rights on the cluster scheme root>

    # access list configuration
    viewer_allowed_sids: <list of SIDs that are allowed to view the cluster state>
    monitoring_allowed_sids: <list of SIDs that are allowed to monitor and change the cluster state>
    administration_allowed_sids: <list of SIDs that are allowed cluster administration>
```

[//]: # (TODO: wait for pull/9387, dynamic_node_registration to add info about "register_dynamic_node_allowed_sids: <список SID'ов с правами подключения динамических нод в кластер>")

## Configuring authentication mode {#security-auth}

#|
|| Parameter | Description ||
|| `enforce_user_token_requirement` | Enforces user [authentication](../../security/authentication.md) mode.

- `enforce_user_token_requirement: true` — User authentication is mandatory. Requests to {{ ydb-short-name }} must include an [auth token](../../concepts/glossary.md#auth-token).

    Requests to {{ ydb-short-name }} undergo authentication and authorization.

- `enforce_user_token_requirement: false` — User authentication is optional. Requests to {{ ydb-short-name }} are not required to include an [auth token](../../concepts/glossary.md#auth-token).

    Requests without an auth token are processed in [anonymous mode](../../security/authentication.md#anonymous) without authorization.

    Requests with an auth token undergo authentication and authorization. But if an authentication error occurs, requests are still processed in anonymous mode.

    When `enforce_user_token_check_requirement: true`, requests with authentication errors are blocked.

[//]: # (TODO: добавить про ошибки проверки права на доступ к базе данных, когда появится место для ссылки)

If the `default_user_sids` parameter is defined and not empty (see the description below), its value is used instead of the missing auth token. In this case, authentication and authorization is performed for the [access subject](../../concepts/glossary.md#access-subject) defined in `default_user_sids`.

Default value: `false`.
    ||
|| `enforce_user_token_check_requirement` | Forbids to ignore authentication errors in the `enforce_user_token_requirement: false` mode.

Default value: `false`.
    ||
|| `default_user_sids` | Specifies a list of [SIDs](../../concepts/glossary.md#access-sid) for authenticating incoming requests without an [auth token](../../concepts/glossary.md#auth-token).

`default_user_sids` acts as an auth token for anonymous requests. The first element in the list must be a user SID. The following element must be the SIDs of groups to which the user belongs.

If the `default_user_sids` list is not empty, mandatory authentication mode (`enforce_user_token_requirement: true`) can be used for anonymous requests. This mode can be used in some {{ ydb-short-name }} testing scenarios or for educational purposes in local {{ ydb-short-name }} installations.

Default value: empty.
    ||
|| `all_authenticated_users` | Specifies the name of the virtual [group](../../concepts/glossary.md#access-group) that includes all authenticated [users](../../concepts/glossary.md#access-user).

This virtual group is created automatically by {{ ydb-short-name }}. You cannot delete this virtual group, list, or change its members.
You can use this group to grant [access rights](../../concepts/glossary.md#access-right) on [scheme objects](../../concepts/glossary.md#scheme-object).

Default value: `all-users@well-known`.
    ||
|| `all_users_group` | Specifies the name of the [group](../../concepts/glossary.md#access-group) that includes all local [users](../../concepts/glossary.md#access-user).

If `all_users_group` is not empty, all local users upon creation will be added to the group with this name. This group must exist, when new users are added.

The `all_users_group` parameter is used during initialization of the [built-in security](../../security/builtin-security.md).

Default value: empty.
    ||
|#

## Bootstrapping security {#security-bootstrap}

The `default_users`, `default_groups`, `default_access` parameters affect the initial {{ ydb-short-name }} cluster configuration that occurs when {{ ydb-short-name }} starts for the first time. During subsequent runs initial configuration is not repeated, and these parameters are ignored.

See [{#T}](../../security/builtin-security.md) and the related [`domains_config`](index.md#domains-config) parameters.

#|
|| Parameter | Description ||
|| `default_users` | The list of [users](../../concepts/glossary.md#access-user) to be created when the {{ ydb-short-name }} cluster is started for the first time.

The list of login-password pairs. The first user in the list is a superuser.

{% note info %}

Passwords are specified in clear form, so it's too dangerous to keep using them for a long time. You must change these passwords in {{ ydb-short-name }} after the first start. For example, use the [`ALTER USER`](../../yql/reference/syntax/alter-user.md) statement.

[//]: # (TODO: добавить про возможность блокировки этих стартовых пользователей, когда такое описание появится)

{% endnote %}

Example:

```yaml
default_users:
- name: root
  password: <...>
- name: user1
  password: <...>
```

Errors in the `default_users` list (duplicate logins) are logged, but do not affect {{ ydb-short-name }} cluster startup.

    ||
|| `default_groups` | The list of [groups](../../concepts/glossary.md#access-group) to be created when the {{ ydb-short-name }} cluster is started for the first time.

The list of groups and their members.

Example:

```yaml
default_groups:
- name: ADMINS
  members: root
- name: USERS
  members:
  - ADMINS
  - root
  - user1
```

The order of groups is this list matters: groups are created in the order of their appearance in the `default_groups` parameter. Group members must exist by the time the group is created. Inexistent users will not be added to the group.

Failures to add users to groups are logged, but do not affect {{ ydb-short-name }} cluster startup.

    ||
|| `default_access` | The list of [access rights](../../concepts/glossary.md#access-right) to be granted on the cluster scheme root.

The list of access rights is specified in the [short access control notation](../../security/short-access-control-notation.md).

Example:

```yaml
default_access:
- +(CDB|DDB|GAR):ADMINS
- +(ConnDB):USERS
```

    ||
|#

Errors in the access right entries are logged, but do not affect {{ ydb-short-name }} cluster startup. Access rights with errors will not be granted.

[//]: # (TODO: требуется доработка, сейчас ошибка в формате приводит к падению процесса)

## Configuring administrative and other privileges {#security-access-levels}

Access control in {{ ydb-short-name }} is divided into two segments:

- [access control lists](../../concepts/glossary.md#access-control-list) for [scheme objects](../../concepts/glossary.md#scheme-object)
- [access level lists](../../concepts/glossary.md#access-level-list) to define additional privileges or restrictions

Both segments are used in combination: a [subject](../../concepts/glossary.md#access-subject) is granted the privilege to perform an action if both segments allow it. The action is not allowed if it is not granted in one of the segments.

Access levels are defined by the `viewer_allowed_sids`, `monitoring_allowed_sids`, and `administration_allowed_sids` lists in the cluster configuration. Access levels of subjects affect their privileges to manage [scheme objects](../../concepts/glossary.md#scheme-object) as well as privileges that are not related to scheme objects.

[//]: # (TODO: добавить ссылку на справку по viewer api и требуемым правам, когда она появится)

#|
|| Parameter | Description ||
|| `viewer_allowed_sids` | The list of [SIDs](../../concepts/glossary.md#access-sid) with the viewer access level.

This level allows viewing the cluster state that is closed for public access (most pages in Embedded UI ([YDB Monitoring](../embedded-ui/ydb-monitoring.md))), no changes are allowed.
    ||
|| `monitoring_allowed_sids` | The list of [SIDs](../../concepts/glossary.md#access-sid) with the operator access level.

This level grants additional privileges to monitor and change the cluster state. For example, perform a backup, restore a database, or execute YQL-statements in the Embedded UI.
    ||
|| `administration_allowed_sids` | The list of [SIDs](../../concepts/glossary.md#access-sid) with the administrator access level.

This level grants privileges to administrate the {{ ydb-short-name }} cluster and its databases.
    ||
|#

[//]: # (TODO: wait for pull/9387, dinamic_node_registration to add info about `register_dynamic_node_allowed_sids`)

{% note warning %}

The access level lists are empty by default.

An empty list grants its access level to any user (including anonymous users).

If all three lists are empty, any user has the administrative access level.

For secure {{ ydb-short-name }} deployment make sure to plan the access model beforehand and define the group lists before the cluster is started for the first time.

{% endnote %}

The access level lists can include the SIDs of [users](../../concepts/glossary.md#access-user) or [user groups](../../concepts/glossary.md#access-group). A user belongs to the access level list if the list includes the SID of the user or the SID of the group to which the user or its subgroup (recursively) belongs.
It's recommended to add user groups and separate service accounts to the `*_allowed_sids` access level lists. This way, granting access levels to individual users will not require changing the {{ ydb-short-name }} cluster configuration.

You can treat access level lists as layers of additional privileges:

- An access subject that is not added to any of the access level lists can view publicly available information about the cluster (for example, [a list of databases on the cluster](../embedded-ui/ydb-monitoring.md#tenant_list_page) or [a list of cluster nodes](../embedded-ui/ydb-monitoring.md#node_list_page)).
- Each of the `viewer_allowed_sids`, `monitoring_allowed_sids`, `administration_allowed_sids` lists adds privileges to the access subject. For the maximum level of privileges an access subject must be added to all of the three access level lists.
- Adding an access subject to the `monitoring_allowed_sids` or `administration_allowed_sids` list without adding it to `viewer_allowed_sids` makes no sense.

For example:

- An operator (the SID of the user or of the group to which the user belongs) must be added to `viewer_allowed_sids` and to `monitoring_allowed_sids`.
- An administrator must be added to `viewer_allowed_sids`, `monitoring_allowed_sids`, and `administration_allowed_sids`.