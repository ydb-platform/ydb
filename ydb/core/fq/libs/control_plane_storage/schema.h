#pragma once

namespace NFq {

// tables
#define QUERIES_TABLE_NAME "queries"
#define PENDING_SMALL_TABLE_NAME "pending_small"
#define CONNECTIONS_TABLE_NAME "connections"
#define BINDINGS_TABLE_NAME "bindings"
#define RESULT_SETS_TABLE_NAME "result_sets"
#define IDEMPOTENCY_KEYS_TABLE_NAME "idempotency_keys"
#define JOBS_TABLE_NAME "jobs"
#define NODES_TABLE_NAME "nodes"
#define QUOTAS_TABLE_NAME "quotas"
#define TENANTS_TABLE_NAME "tenants"
#define TENANT_ACKS_TABLE_NAME "tenant_acks"
#define MAPPINGS_TABLE_NAME "mappings"
#define COMPUTE_DATABASES_TABLE_NAME "compute_databases"

// columns
#define SCOPE_COLUMN_NAME "scope"
#define VISIBILITY_COLUMN_NAME "visibility"
#define AUTOMATIC_COLUMN_NAME "automatic"
#define USER_COLUMN_NAME "user"
#define REVISION_COLUMN_NAME "revision"
#define META_REVISION_COLUMN_NAME "meta_revision"
#define INTERNAL_COLUMN_NAME "internal"
#define GENERATION_COLUMN_NAME "generation"
#define RETRY_COUNTER_COLUMN_NAME "retry_counter"
#define RETRY_COUNTER_UPDATE_COLUMN_NAME "retry_counter_updated_at"
#define IS_RESIGN_QUERY_COLUMN_NAME "is_resign_query"

#define STATUS_COLUMN_NAME "status"
#define QUERY_TYPE_COLUMN_NAME "query_type"
#define EXECUTE_MODE_COLUMN_NAME "execute_mode"
#define LAST_JOB_ID_COLUMN_NAME "last_job_id"
#define RESULT_SETS_EXPIRE_AT_COLUMN_NAME "result_sets_expire_at"

#define QUERY_ID_COLUMN_NAME "query_id"
#define QUERY_COLUMN_NAME "query"

#define CONNECTION_ID_COLUMN_NAME "connection_id"
#define CONNECTION_COLUMN_NAME "connection"
#define CONNECTION_TYPE_COLUMN_NAME "connection_type"
#define NAME_COLUMN_NAME "name"

#define BINDING_ID_COLUMN_NAME "binding_id"
#define BINDING_COLUMN_NAME "binding"

#define IDEMPOTENCY_KEY_COLUMN_NAME "idempotency_key"
#define EXPIRE_AT_COLUMN_NAME "expire_at"
#define RESPONSE_COLUMN_NAME "response"

#define RESULT_ID_COLUMN_NAME "result_id"
#define RESULT_SET_ID_COLUMN_NAME "result_set_id"
#define ROW_ID_COLUMN_NAME "row_id"
#define RESULT_SET_COLUMN_NAME "result_set"

#define JOB_COLUMN_NAME "job"
#define JOB_ID_COLUMN_NAME "job_id"

#define TENANT_COLUMN_NAME "tenant"
#define INSTANCE_ID_COLUMN_NAME "instance_id"
#define NODE_ID_COLUMN_NAME "node_id"
#define ACTIVE_WORKERS_COLUMN_NAME "active_workers"
#define MEMORY_LIMIT_COLUMN_NAME "memory_limit"
#define MEMORY_ALLOCATED_COLUMN_NAME "memory_allocated"
#define INTERCONNECT_PORT_COLUMN_NAME "interconnect_port"
#define NODE_ADDRESS_COLUMN_NAME "node_address"
#define DATA_CENTER_COLUMN_NAME "data_center"

#define HOST_NAME_COLUMN_NAME "hostname"
#define OWNER_COLUMN_NAME "owner"
#define LAST_SEEN_AT_COLUMN_NAME "last_seen_at"
#define ASSIGNED_UNTIL_COLUMN_NAME "assigned_until"
#define RETRY_RATE_COLUMN_NAME "retry_rate"

#define SUBJECT_TYPE_COLUMN_NAME "subject_type"
#define SUBJECT_ID_COLUMN_NAME "subject_id"
#define METRIC_NAME_COLUMN_NAME "metric_name"
#define METRIC_LIMIT_COLUMN_NAME "metric_limit"
#define LIMIT_UPDATED_AT_COLUMN_NAME "limit_updated_at"
#define METRIC_USAGE_COLUMN_NAME "metric_usage"
#define USAGE_UPDATED_AT_COLUMN_NAME "usage_updated_at"

#define VTENANT_COLUMN_NAME "vtenant"
#define NODE_COLUMN_NAME "node"
#define COMMON_COLUMN_NAME "common"
#define STATE_COLUMN_NAME "state"
#define STATE_TIME_COLUMN_NAME "state_time"

#define CREATED_AT_COLUMN_NAME "created_at"
#define LAST_ACCESS_AT_COLUMN_NAME "last_access_at"

} // namespace NFq
