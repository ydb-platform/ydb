# Audit log

_An audit_ log is a stream of records that document security-relevant operations performed within the {{ ydb-short-name }} cluster. Unlike technical logs, which help detect failures and troubleshoot issues, the audit log provides data relevant to security. It serves as a source of information that answers the questions: who did what, when, and from where.

Each record in the audit log is called an [audit event](../concepts/glossary.md#audit-events). An audit event captures a single security-relevant action.

Every event includes a set of attributes that describe different aspects of the event. The specific set of attributes present in a given event depends on its event source: all audit events include the [common attributes](../reference/audit-log-attributes.md#common-attributes), and some sources add additional, source-specific attributes (see [Audit event sources overview](#audit-event-sources-overview)).

Examples of typical audit log events:

* Data access through DML requests.
* Schema or configuration management operations.
* Changes to permissions or access-control settings.
* Administrative user actions.

See the full list of possible log classes in the [Log classes](#log-classes) section.

The `audit_config` section in the [cluster configuration](../reference/configuration/index.md) defines which audit logs are collected, how they need to be serialized and where they are delivered. See the [audit log configuration](../reference/configuration/audit_config.md#audit-log-configuration) section for details.

## Log classes {#log-classes}

Audit events are grouped into **log classes** that represent broad categories of operations. You can enable or disable logging for each class in [configuration](../reference/configuration/audit_config.md#log-class-config) and, if necessary, tailor the configuration per class. The available log classes are:

#|
|| **Log class**      | **Description** ||
|| `ClusterAdmin`     | Cluster administration requests. ||
|| `DatabaseAdmin`    | Database administration requests. ||
|| `Login`            | Login requests. ||
|| `NodeRegistration` | Node registration. ||
|| `Ddl`              | [DDL requests](https://en.wikipedia.org/wiki/Data_definition_language). ||
|| `Dml`              | [DML requests](https://en.wikipedia.org/wiki/Data_manipulation_language). ||
|| `Operations`       | Asynchronous remote procedure call (RPC) operations that require polling to track the result. ||
|| `ExportImport`     | Export and import data operations. ||
|| `Acl`              | Access Control List (ACL) operations. ||
|| `AuditHeartbeat`   | Synthetic heartbeat messages that confirm audit logging remains operational. ||
|| `Default`          | Default settings for any component that doesn't have a configuration entry. ||
|#

At the moment, not all audit event sources categorize events into log classes. For most of them, the [basic configuration](../reference/configuration/audit_config.md#enabling-audit-log) is sufficient to capture their events. See the [Audit event sources overview](#audit-event-sources-overview) section for details.

## Log phases {#log-phases}

Some audit event sources divide the request processing into stages. **Logging phase** indicates the processing stages at which audit logging records events. Specifying logging phases is useful when you need fine-grained visibility into request execution and want to capture events before and after critical processing steps. The available log phases are:

#|
|| **Log phase**    | **Description** ||
|| `Received`       | A request is received and the initial checks and authentication are made. The `status` attribute is set to `IN-PROCESS`. </br>This phase is disabled by default; you must include `Received` in `log_class_config.log_phase` to enable it. ||
|| `Completed`      | A request is completely finished. The `status` attribute is set to `SUCCESS` or `ERROR`. This phase is enabled by default when `log_class_config.log_phase` is not set. ||
|#

## Audit log destinations {#stream-destinations}

**Audit log destination** is a target where the audit log stream can be delivered.

You can currently configure the following destinations for the audit log:

* A file on each {{ ydb-short-name }} cluster node.
* The standard error stream, `stderr`.
* An agent for delivering [Unified Agent](https://yandex.cloud/en/docs/monitoring/concepts/data-collection/unified-agent/) metrics.

You can use any of the listed destinations or their combinations. See the [audit log configuration](../reference/configuration/audit_config.md#audit-log-configuration) for details.

If you forward the stream to a file, file-system permissions control access to the audit log. Saving the audit log to a file is recommended for production installations.

For test installations, forward the audit log to the standard error stream (`stderr`). Further stream processing depends on the {{ ydb-short-name }} cluster [logging](../devops/observability/logging.md) settings.

## Audit event sources overview {#audit-event-sources-overview}

The table below summarizes the built-in audit event sources. Use it to identify which source emits the events you need and how to enable those events.

#|
|| **Source / UID**                                     | **What it records** | **Source attributes** | **Configuration requirements** ||
|| [Schemeshard](../concepts/glossary.md#scheme-shard) </br>`schemeshard`       | Schema operations, ACL modifications, and user management actions. | [Attributes](../reference/audit-log-attributes.md#schemeshard) | Included in the [basic audit configuration](../reference/configuration/audit_config.md#enabling-audit-log). ||
|| [gRPC services](../concepts/glossary.md#grpc-proxy) </br>`grpc-proxy`       | Non-internal requests handled by {{ ydb-short-name }} gRPC endpoints. | [Attributes](../reference/audit-log-attributes.md#grpc-proxy) | Enable the relevant [log classes](../reference/configuration/audit_config.md#log-class-config) and optional [log phases](#log-phases). ||
|| [gRPC connection](../concepts/glossary.md#grpc-proxy) </br>`grpc-conn` | Client connection and disconnection events. | [Attributes](../reference/audit-log-attributes.md#grpc-connection) | Enable the [`enable_grpc_audit`](../reference/configuration/feature_flags.md) feature flag. ||
|| [gRPC authentication](../concepts/glossary.md#grpc-proxy) </br>`grpc-login` | gRPC authentication attempts. | [Attributes](../reference/audit-log-attributes.md#grpc-authentication) | Enable the `Login` class in [`log_class_config`](../reference/configuration/audit_config.md#log-class-config). ||
|| Monitoring service </br>`monitoring`  | HTTP requests handled by the [monitoring endpoint](../reference/configuration/tls.md#http). | [Attributes](../reference/audit-log-attributes.md#monitoring) | Enable the `ClusterAdmin` class in [`log_class_config`](../reference/configuration/audit_config.md#log-class-config). ||
|| Heartbeat </br>`audit`                 | Synthetic heartbeat events proving that audit logging is alive. | [Attributes](../reference/audit-log-attributes.md#heartbeat) | Enable the `AuditHeartbeat` class in [`log_class_config`](../reference/configuration/audit_config.md#log-class-config) and optionally adjust [heartbeat settings](../reference/configuration/audit_config.md#heartbeat-settings). ||
|| [BlobStorage Controller](../concepts/glossary.md#distributed-storage) </br>`bsc`            | Console-driven BlobStorage Controller configuration changes. | [Attributes](../reference/audit-log-attributes.md#bsc) | Included in the [basic audit configuration](../reference/configuration/audit_config.md#enabling-audit-log). ||
|| [Distconf](../concepts/glossary.md#distributed-configuration) </br>`distconf`                | Distributed configuration updates. | [Attributes](../reference/audit-log-attributes.md#distconf) | Included in the [basic audit configuration](../reference/configuration/audit_config.md#enabling-audit-log). ||
|| Web login </br>`web-login`             | Interactions with the web console authentication widget. | [Attributes](../reference/audit-log-attributes.md#web-login) | Included in the [basic audit configuration](../reference/configuration/audit_config.md#enabling-audit-log). ||
|| Console </br>`console`                   | Database lifecycle operations and dynamic configuration changes. | [Attributes](../reference/audit-log-attributes.md#console) | Included in the [basic audit configuration](../reference/configuration/audit_config.md#enabling-audit-log). ||
|#

## Examples {#examples}

The following tabs show the same audit log event written using different [backend settings](../reference/configuration/audit_config.md#backend-settings).

{% list tabs %}

- JSON

    The `JSON` format produces entries like:

    ```json
    2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}", "detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    2025-11-03T17:41:44.203214Z: {"component":"monitoring","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- TXT

    The `TXT` format produces entries like:

    ```txt
    2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
    2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
    2025-11-03T18:07:39.056211Z: component=grpc-proxy, tx_id=281474976775656, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject=serviceaccount@as, database=/my_dir/db1, operation=ExecuteQueryRequest, query_text=SELECT * FROM `my_row_table`; status=SUCCESS, detailed_status=StatusSuccess, begin_tx=1, commit_tx=1, end_time=2025-11-03T18:07:39.056204Z, grpc_method=Ydb.Query.V1.QueryService/ExecuteQuery, sanitized_token=xxxxxxxx.**, start_time=2025-11-03T18:07:39.054863Z
    2025-11-03T17:41:44.203214Z: component=monitoring, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, operation=HTTP REQUEST, method=POST, url=/viewer/query, params=base64=false&schema=multipart, body={"query":"SELECT * FROM `my_row_table`;","database":"/local","action":"execute-query","syntax":"yql_v1"}, status=IN-PROCESS, reason=Execute
    ```

- JSON_LOG_COMPATIBLE

    The `JSON_LOG_COMPATIBLE` format produces entries like:

    ```json
    {"@timestamp":"2023-03-14T10:41:36.485788Z","@log_type":"audit","paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    {"@timestamp":"2023-03-13T20:07:30.927210Z","@log_type":"audit","reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    {"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as","sanitized_token":"xxxxxxxx.**"}
    {"@timestamp":"2025-11-03T17:41:44.203214Z","@log_type":"audit","component":"monitoring","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- Envelope JSON

    The JSON envelope template `{"message": %message%, "source": "ydb-audit-log"}` produces entries like:

    ```json
    {"message":"2023-03-14T10:41:36.485788Z: {\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"281474976775658\",\"database\":\"/my_dir/db1\",\"remote_address\":\"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAccepted\",\"operation\":\"MODIFY ACL\",\"component\":\"schemeshard\",\"acl_add\":\"[+(ConnDB):subject:-]\"}\n","source":"ydb-audit-log"}
    {"message":"2023-03-13T20:07:30.927210Z: {\"reason\":\"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)\",\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"844424930216970\",\"database\":\"/my_dir/db1\",\"remote_address\":\"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAlreadyExists\",\"operation\":\"CREATE DIRECTORY\",\"component\":\"schemeshard\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T18:07:39.056211Z: {\"@log_type\":\"audit\",\"begin_tx\":1,\"commit_tx\":1,\"component\":\"grpc-proxy\",\"database\":\"/my_dir/db1\",\"detailed_status\":\"SUCCESS\",\"end_time\":\"2025-11-03T18:07:39.056204Z\",\"grpc_method\":\"Ydb.Query.V1.QueryService/ExecuteQuery\",\"operation\":\"ExecuteQueryRequest\",\"query_text\":\"SELECT * FROM `my_row_table`;\",\"remote_address\":\"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx\",\"sanitized_token\":\"xxxxxxxx.**\",\"start_time\":\"2025-11-03T18:07:39.054863Z\",\"status\":\"SUCCESS\",\"subject\":\"serviceaccount@as\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T17:41:44.203214Z: {\"component\":\"monitoring\",\"remote_address\":\"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx\",\"operation\":\"HTTP REQUEST\",\"method\":\"POST\",\"url\":\"/viewer/query\",\"params\":\"base64=false&schema=multipart\",\"body\":\"{\\\"query\\\":\\\"SELECT * FROM `my_row_table`;\\\",\\\"database\\\":\\\"/local\\\",\\\"action\\\":\\\"execute-query\\\",\\\"syntax\\\":\\\"yql_v1\\\"}\",\"status\":\"IN-PROCESS\",\"reason\":\"Execute\"}\n","source":"ydb-audit-log"}
    ```

- Pretty-JSON

    The pretty-JSON formatting shown below is for illustration purposes only; the audit log does not support pretty or indented JSON output.

    ```json
    {
      "paths": "[/my_dir/db1/some_dir]",
      "tx_id": "281474976775658",
      "database": "/my_dir/db1",
      "remote_address": "xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx",
      "status": "SUCCESS",
      "subject": "{none}",
      "detailed_status": "StatusAccepted",
      "operation": "MODIFY ACL",
      "component": "schemeshard",
      "acl_add": "[+(ConnDB):subject:-]"
    }
    ```

{% endlist %}
