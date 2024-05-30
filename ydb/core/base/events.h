#pragma once
#include "defs.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>

#include <ydb/core/fq/libs/events/event_ids.h>

namespace NKikimr {

struct TKikimrEvents : TEvents {
    enum EEventSpaceKikimr {
        /* WARNING:
           Please mind that you should never change the order
           for the following keywords, you should consider
           issues about "rolling update".
        */
        ES_KIKIMR_ES_BEGIN = ES_USERSPACE,  //4096
        ES_STATESTORAGE, //4097
        ES_DEPRECATED_4098, //4098
        ES_BLOBSTORAGE, //4099
        ES_HIVE, //4100
        ES_TABLETBASE, //4101
        ES_TABLET, //4102
        ES_TABLETRESOLVER,
        ES_LOCAL,
        ES_DEPRECATED_4105,
        ES_TX_PROXY, // generic proxy commands 4106
        ES_TX_COORDINATOR,
        ES_TX_MEDIATOR,
        ES_TX_PROCESSING, // 4109
        ES_DEPRECATED_4110,
        ES_DEPRECATED_4111,
        ES_DEPRECATED_4112,
        ES_TX_DATASHARD,
        ES_DEPRECATED_4114,
        ES_TX_USERPROXY, // user proxy interface
        ES_SCHEME_CACHE,
        ES_TX_PROXY_REQ,
        ES_TABLET_PIPE,
        ES_DEPRECATED_4118,
        ES_TABLET_COUNTERS_AGGREGATOR,
        ES_DEPRECATED_4121,
        ES_PROXY_BUS, //4122
        ES_BOOTSTRAPPER,
        ES_TX_MEDIATORTIMECAST,
        ES_DEPRECATED_4125,
        ES_DEPRECATED_4126,
        ES_DEPRECATED_4127,
        ES_DEPRECATED_4128,
        ES_DEPRECATED_4129,
        ES_DEPRECATED_4130,
        ES_DEPRECATED_4131,
        ES_KEYVALUE, //4132
        ES_MSGBUS_TRACER,
        ES_RTMR_TABLET,
        ES_FLAT_EXECUTOR,
        ES_NODE_WHITEBOARD,
        ES_FLAT_TX_SCHEMESHARD, // 4137
        ES_PQ,
        ES_YQL_KIKIMR_PROXY,
        ES_PQ_META_CACHE,
        ES_DEPRECATED_4141,
        ES_PQ_L2_CACHE, //4142
        ES_TOKEN_BUILDER,
        ES_TICKET_PARSER,
        ES_KQP = NYql::NDq::TDqEvents::ES_DQ_COMPUTE_KQP_COMPATIBLE, // 4145
        ES_BLACKBOX_VALIDATOR,
        ES_SELF_PING,
        ES_PIPECACHE,
        ES_PQ_PROXY,
        ES_CMS,
        ES_NODE_BROKER,
        ES_TX_ALLOCATOR, //4152
        // reserve event space for each RTMR process
        ES_RTMR_STORAGE,
        ES_RTMR_PROXY,
        ES_RTMR_PUSHER,
        ES_RTMR_HOST,
        ES_RESOURCE_BROKER,
        ES_VIEWER,
        ES_SUB_DOMAIN,
        ES_GRPC_PROXY_STATUS, //OLD
        ES_SQS,
        ES_BLOCKSTORE, //4162
        ES_RTMR_ICBUS,
        ES_TENANT_POOL,
        ES_USER_REGISTRY,
        ES_TVM_SETTINGS_UPDATER,
        ES_PQ_CLUSTERS_UPDATER,
        ES_TENANT_SLOT_BROKER,
        ES_GRPC_CALLS,
        ES_CONSOLE,
        ES_KESUS_PROXY,
        ES_KESUS,
        ES_CONFIGS_DISPATCHER,
        ES_IAM_SERVICE,
        ES_FOLDER_SERVICE,
        ES_GRPC_MON,
        ES_QUOTA, // must be in sync with ydb/core/quoter/public/quoter.h
        ES_COORDINATED_QUOTA,
        ES_ACCESS_SERVICE,
        ES_USER_ACCOUNT_SERVICE,
        ES_PQ_PROXY_NEW,
        ES_GRPC_STREAMING,
        ES_SCHEME_BOARD,
        ES_FLAT_TX_SCHEMESHARD_PROTECTED,
        ES_GRPC_REQUEST_PROXY,
        ES_EXPORT_SERVICE,
        ES_TX_ALLOCATOR_CLIENT,
        ES_PQ_CLUSTER_TRACKER,
        ES_NET_CLASSIFIER,
        ES_SYSTEM_VIEW,
        ES_TENANT_NODE_ENUMERATOR,
        ES_SERVICE_ACCOUNT_SERVICE,
        ES_INDEX_BUILD,
        ES_BLOCKSTORE_PRIVATE,
        ES_YT_WRAPPER,
        ES_S3_WRAPPER,
        ES_FILESTORE,
        ES_FILESTORE_PRIVATE,
        ES_YDB_METERING,
        ES_IMPORT_SERVICE, // 4200
        ES_TX_OLAPSHARD,
        ES_TX_COLUMNSHARD,
        ES_CROSSREF,
        ES_SCHEME_BOARD_MON,
        ES_YQL_ANALYTICS_PROXY = NFq::TEventIds::ES_YQL_ANALYTICS_PROXY,
        ES_BLOB_CACHE,
        ES_LONG_TX_SERVICE,
        ES_TEST_SHARD,
        ES_DATASTREAMS_PROXY,
        ES_IAM_TOKEN_SERVICE,
        ES_HEALTH_CHECK,
        ES_DQ = NYql::NDq::TDqEvents::ES_DQ_COMPUTE, // 4212
        ES_YQ, // 4213
        ES_CHANGE_EXCHANGE_DATASHARD,
        ES_DATABASE_SERVICE, //4215
        ES_SEQUENCESHARD, // 4216
        ES_SEQUENCEPROXY, // 4217
        ES_CLOUD_STORAGE,
        ES_CLOUD_STORAGE_PRIVATE,
        ES_FOLDER_SERVICE_ADAPTER,
        ES_PQ_PARTITION_WRITER,
        ES_YDB_PROXY,
        ES_REPLICATION_CONTROLLER,
        ES_HTTP_PROXY,
        ES_BLOB_DEPOT,
        ES_DATASHARD_LOAD,
        ES_METADATA_PROVIDER,
        ES_INTERNAL_REQUEST,
        ES_BACKGROUND_TASKS,
        ES_TIERING,
        ES_METADATA_INITIALIZER,
        ES_YDB_AUDIT_LOG,
        ES_METADATA_MANAGER,
        ES_METADATA_SECRET,
        ES_TEST_LOAD,
        ES_GRPC_CANCELATION,
        ES_DISCOVERY,
        ES_EXT_INDEX,
        ES_CONVEYOR,
        ES_KQP_SCAN_EXCHANGE,
        ES_IC_NODE_CACHE,
        ES_DATA_OPERATIONS,
        ES_KAFKA,
        ES_STATISTICS,
        ES_LDAP_AUTH_PROVIDER,
        ES_DB_METADATA_CACHE,
        ES_TABLE_CREATOR,
        ES_PQ_PARTITION_CHOOSER,
        ES_GRAPH,
        ES_REPLICATION_WORKER,
        ES_CHANGE_EXCHANGE,
        ES_S3_FILE_QUEUE,
        ES_NEBIUS_ACCESS_SERVICE,
        ES_REPLICATION_SERVICE,
        ES_BACKUP_SERVICE,
        ES_TX_BACKGROUND,
        ES_SS_BG_TASKS,
        ES_LIMITER
    };
};

}
