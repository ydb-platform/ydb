# Audit event attributes

Audit log event attributes are divided into two groups:

* Common attributes present in many *audit event sources* and always carry the same meaning.
* Attributes specific to the source that generates the event.

In this section, you will find a reference guide to the attributes in audit events. It covers both common attributes and source-specific ones. For each source, its UID, recorded operations, and configuration requirements are also provided.

## Common attributes {#common-attributes}

The table below lists the common attributes.

#|
|| **Attribute**          | **Description**                                                                                                                                                | **Optional/Required** ||
|| `subject`              | Event source [SID](../../concepts/glossary.md#access-sid) if mandatory authentication is enabled, or `{none}` otherwise.                                         | Required ||
|| `sanitized_token`      | A partially masked authentication token that was used to execute the request. Can be used to link related events while keeping the original credentials hidden. If authentication was not performed, the value will be `{none}`. | Required ||
|| `operation`            | Operation name (for example, `ALTER DATABASE`, `CREATE TABLE`).                                                                                              | Required ||
|| `component`            | Unique identifier (UID) of the *audit event source*.                                                                                                          | Required ||
|| `status`               | Operation completion status.<br/>Possible values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li><li>`IN-PROCESS`: The operation is in progress.</li></ul> | Required ||
|| `reason`               | Error message.                                                                                                                                                | Optional ||
|| `request_id`           | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context. | Optional ||
|| `remote_address`       | IP address (IPv4 or IPv6) of the client that delivered the request. Can be a comma-separated list of addresses, where the enumeration indicates the chain of addresses the request passed through. Port numbers may be included after a colon (e.g., `192.0.2.1:54321`) | Optional ||
|| `detailed_status`      | A refined or specific status delivered by a {{ ydb-short-name }} *audit event source*. This field may be used by an audit source to record additional information about the operation's status. | Optional ||
|| `database`             | Database path (for example, `/Root/db`).                                                                                                                     | Optional ||
|| `cloud_id`             | Cloud identifier of the {{ ydb-short-name }} database. The value is taken from the user-attributes of the database, usually set by the control plane. | Optional ||
|| `folder_id`            | Folder identifier of the {{ ydb-short-name }} cluster or database. The value is taken from the user-attributes of the database, usually set by the control plane. | Optional ||
|| `resource_id`          | Resource identifier of the {{ ydb-short-name }} database. The value is taken from the user-attributes of the database, usually set by the control plane. | Optional ||
|#

## Audit sources attributes

### Schemeshard {#schemeshard}

**UID:** `schemeshard`.
**Logged operations:** Schema operations triggered by DDL queries, ACL modifications, and user management operations.
**How to enable:** Only [basic audit configuration](../../reference/configuration/audit_config.md#enabling-audit-log) required.

The table below lists additional attributes specific to the `Schemeshard` source.

#|
|| **Attribute**                            | **Description**                                                                                                                                                     | **Optional/Required** ||
|| **Common schemeshard attributes**        | **>**                                                                                                                                                              |  ||
|| `tx_id`                                  | Unique transaction ID. This ID can be used to differentiate events related to different operations.                                                                 | Required ||
|| `paths`                                  | List of paths in the database that are changed by the operation (for example, `[/my_dir/db/table-a, /my_dir/db/table-b]`).                                         | Optional ||
|| **Ownership and permission attributes**  | **>**                                                                                                                                                              |  ||
|| `new_owner`                              | SID of the new owner of the object when ownership is transferred.                                                                                                   | Optional ||
|| `acl_add`                                | List of added permissions in [short notation](../../security/short-access-control-notation.md) (for example, `[+R:someuser]`).                                                  | Optional ||
|| `acl_remove`                             | List of revoked permissions in [short notation](../../security/short-access-control-notation.md) (for example, `[-R:someuser]`).                                                | Optional ||
|| **User attributes**                    | **>**                                                                                                                                                              |  ||
|| `user_attrs_add`                         | List of user attributes added when creating objects or updating attributes (for example, `[attr_name1: A, attr_name2: B]`).                                      | Optional ||
|| `user_attrs_remove`                      | List of user attributes removed when creating objects or updating attributes (for example, `[attr_name1, attr_name2]`).                                          | Optional ||
|| **Login/Auth specific**                  | **>**                                                                                                                                                              |  ||
|| `login_user`                             | User name recorded by login operations.                                                                                                                            | Optional ||
|| `login_group`                            | Group name is filled in for user group modification operations.                                                                                                    | Optional ||
|| `login_member`                           | User name, filled in when a user's group membership changes.                                                                            | Optional ||
|| `login_user_change`                      | Changes applied to user settings, such as password changes or user block/unblock actions. Filled if such changes are made. | Optional ||
|| `login_user_level`                       | Indicates whether the user had full administrator privileges at the time of the event: the value is `admin` for such users; for all others, this attribute is not used.                                  | Optional ||
|| **Import/Export operation attributes**   | **>**                                                                                                                                                              |  ||
|| `id`                                     | Unique identifier for export or import operations.                                                                                                                 | Optional ||
|| `uid`                                    | User-defined label for operations.                                                                                                                                 | Optional ||
|| `start_time`                             | Operation start time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format.                                                                                | Optional ||
|| `end_time`                               | Operation end time in ISO 8601 format.                                                                                                                             | Optional ||
|| `last_login`                             | User's last successful login time in ISO 8601 format.                                                                                                              | Optional ||
|| **Export-specific**                      | **>**                                                                                                                                                              |  ||
|| `export_type`                            | Export destination. Possible values: `yt`, `s3`.                                                                                                                   | Optional ||
|| `export_item_count`                      | Number of exported schema objects.                                                                                                                                  | Optional ||
|| `export_yt_prefix`                       | [YTsaurus](https://ytsaurus.tech/) destination path prefix.                                                                                                        | Optional ||
|| `export_s3_bucket`                       | S3 bucket used for exports.                                                                                                                                        | Optional ||
|| `export_s3_prefix`                       | S3 destination prefix.                                                                                                                                             | Optional ||
|| **Import-specific**                      | **>**                                                                                                                                                              |  ||
|| `import_type`                            | Import source type. It's always `s3`.                                                                                                                              | Optional ||
|| `import_item_count`                      | Number of imported items.                                                                                                                                          | Optional ||
|| `import_s3_bucket`                       | S3 bucket used for imports.                                                                                                                                        | Optional ||
|| `import_s3_prefix`                       | S3 source prefix.                                                                                                                                                  | Optional ||
|#

### gRPC services {#grpc-proxy}

**UID:** `grpc-proxy`.
**Logged operations:** All non-internal gRPC requests.
**How to enable:** Requires specifying log classes in audit configuration.
**Log classes:** Depends on the RPC request type: `Ddl`, `Dml`, `Operations`, `ClusterAdmin`, `DatabaseAdmin`, or other classes.
**Log phases:** `Received`, `Completed`.

The table below lists additional attributes specific to the `gRPC services` source.

#|
|| **Attribute**              | **Description**                                                                                         | **Optional/Required** ||
|| **Common gRPC attributes** | **>**                                                                                                  |  ||
|| `grpc_method`              | RPC method name.                                                                                       | Optional ||
|| `request`                  | Sanitized representation of the incoming protobuf request in single-line format. Sensitive fields are masked as ***.                                                      | Optional ||
|| `start_time`               | Operation start time in ISO 8601 format.                                                               | Required ||
|| `end_time`                 | Operation end time in ISO 8601 format.                                                                 | Optional ||
|| **Transaction attributes** | **>**                                                                                                  |  ||
|| `tx_id`                    | Transaction identifier.                                                                                | Optional ||
|| `begin_tx`                 | Flag set to `1` when the request starts a new transaction.                                             | Optional ||
|| `commit_tx`                | Shows whether the request commits the transaction. Possible values: `1`, `0`.                   | Optional ||
|| **Request fields**         | **>**                                                                                                  |  ||
|| `query_text`               | [YQL](../../yql/reference/index.md) query text formatted for logging (single-line, max 1024 characters).                                                 | Optional ||
|| `prepared_query_id`        | Identifier of a prepared query.                                                                        | Optional ||
|| `program_text`             | [MiniKQL program](../../concepts/glossary.md#minikql) sent with the request.                              | Optional ||
|| `schema_changes`           | Description of schema modifications requested in the operation. Example format `{"delta":[{"delta_type":"AddTable","table_id":5555,"table_name":"MyAwesomeTable"}]}`                                      | Optional ||
|| `table`                    | Full table path.                                                                                       | Optional ||
|| `row_count`                | Number of rows processed by a [bulk upsert](../../recipes/ydb-sdk/bulk-upsert.md) request.                | Optional ||
|| `tablet_id`                | Tablet identifier (present only for the gRPC TabletService).                                          | Optional ||
|#

### gRPC connection {#grpc-connection}

**UID:** `grpc-conn`.
**Logged operations:** Only connection (connect) events are tracked.
**How to enable:** Enable the `enable_grpc_audit` [feature flag](../../reference/configuration/feature_flags.md).

*This source uses only common attributes.*

### gRPC authentication {#grpc-login}

**UID:** `grpc-login`.
**Logged operations:** gRPC authentication.
**How to enable:** Requires specifying log classes in [audit configuration](../../reference/configuration/audit_config.md#audit-log-configuration).
**Log classes:** `Login`.
**Log phases:** `Completed`.

The table below lists additional attributes specific to the `gRPC authentication` source.

#|
|| **Attribute**      | **Description**                                                                      | **Required/Optional** ||
|| `login_user`       | User name.                                                                          | Required ||
|| `login_user_level` | Privilege level of the user recorded by audit events. This attribute only uses the `admin` value. | Optional ||
|#

### Monitoring service {#monitoring}

**UID:** `monitoring`.
**Logged operations:** HTTP requests handled by the monitoring service.
**How to enable:** Requires specifying log classes in [audit configuration](../../reference/configuration/audit_config.md#audit-log-configuration).
**Log classes:** `ClusterAdmin`.
**Log phases:** `Received`, `Completed`.

The table below lists additional attributes specific to the `Monitoring service` source.

#|
|| **Attribute**  | **Description**                                                      | **Required/Optional** ||
|| `method`       | HTTP request method. For example `POST`, `GET`.                      | Required             ||
|| `url`          | Request path without query parameters.                               | Required             ||
|| `params`       | Raw query parameters.                                                | Optional             ||
|| `body`         | Request body (truncated to 2 MB with the `TRUNCATED_BY_YDB` suffix). | Optional             ||
|#

### Heartbeat {#heartbeat}

**UID:** `audit`.
**Logged operations:** Periodic audit [heartbeat](../../reference/configuration/audit_config.md#heartbeat-settings) messages.
**How to enable:** Enable this source by specifying log classes in [audit configuration](../../reference/configuration/audit_config.md#audit-log-configuration).
**Log classes:** `AuditHeartbeat`.
**Log phases:** `Completed`.

The table below lists additional attributes specific to the `Heartbeat` source.

#|
|| **Attribute**  | **Description**                                 | **Required/Optional** ||
|| `node_id`      | Node identifier of the node that sent the heartbeat.       | Required             ||
|#

### BlobStorage Controller {#bsc}

**UID:** `bsc`.
**Logged operations:** Configuration replacement requests (`TEvControllerReplaceConfigRequest`) emitted by the console.
**How to enable:** Only [basic audit configuration](../../reference/configuration/audit_config.md#enabling-audit-log) required.

The table below lists additional attributes specific to the `BlobStorage Controller` source.

#|
|| **Attribute**    | **Description**                                                                             | **Required/Optional** ||
|| `old_config`     | Snapshot of the previous [BlobStorage Controller configuration](../../reference/configuration/blob_storage_config.md) in YAML format. | Optional ||
|| `new_config`     | Snapshot of the configuration that replaced the previous one. | Optional ||
|#

### Distconf {#distconf}

**UID:** `distconf`.
**Logged operations:** [Distributed configuration](../../concepts/glossary.md#distributed-configuration) changes.
**How to enable:** Only [basic audit configuration](../../reference/configuration/audit_config.md#enabling-audit-log) required.

The table below lists additional attributes specific to the `Distconf` source.

#|
|| **Attribute**    | **Description**                                                                                                                         | **Required/Optional** ||
|| `old_config`     | Snapshot of the configuration that was active before the distributed update was accepted. Distconf serializes it in YAML.                | Required             ||
|| `new_config`     | Snapshot of the configuration that Distconf committed after the change.                                                                 | Required             ||
|#

### Web login {#web-login}

**UID:** `web-login`.
**Logged operations:** Tracks interactions with the {{ ydb-short-name }} web console authentication widget.
**How to enable:** Only [basic audit configuration](../../reference/configuration/audit_config.md#enabling-audit-log) required.

*This source uses only common attributes.*

### Console {#console}

**UID:** `console`.
**Logged operations:** Database lifecycle operations and dynamic configuration changes.
**How to enable:** Only [basic audit configuration](../../reference/configuration/audit_config.md#enabling-audit-log) required.

The table below lists additional attributes specific to the `Console` source.

#|
|| **Attribute**    | **Description**                                                                              | **Required/Optional** ||
|| `old_config`     | Snapshot of the configuration (in YAML format) that was in effect before the console request was applied.     | Optional             ||
|| `new_config`     | Snapshot of the configuration that the console applied.                                      | Optional             ||
|#
