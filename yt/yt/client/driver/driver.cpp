#include "driver.h"

#include "authentication_commands.h"
#include "admin_commands.h"
#include "bundle_controller_commands.h"
#include "chaos_commands.h"
#include "command.h"
#include "config.h"
#include "cypress_commands.h"
#include "distributed_table_commands.h"
#include "etc_commands.h"
#include "file_commands.h"
#include "journal_commands.h"
#include "queue_commands.h"
#include "scheduler_commands.h"
#include "table_commands.h"
#include "transaction_commands.h"
#include "internal_commands.h"
#include "proxy_discovery_cache.h"
#include "query_commands.h"
#include "flow_commands.h"

#include <yt/yt/client/api/client_cache.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/api/rpc_proxy/connection_impl.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/tvm/tvm_base.h>


namespace NYT::NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NElection;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;
using namespace NFormats;
using namespace NSecurityClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NHiveClient;
using namespace NTabletClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TCommandDescriptor& descriptor, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("name").Value(descriptor.CommandName)
            .Item("input_type").Value(descriptor.InputType)
            .Item("output_type").Value(descriptor.OutputType)
            .Item("is_volatile").Value(descriptor.Volatile)
            .Item("is_heavy").Value(descriptor.Heavy)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TDriverRequest::TDriverRequest()
    : ResponseParametersConsumer(GetNullYsonConsumer())
{ }

TDriverRequest::TDriverRequest(THolderPtr holder)
    : ResponseParametersConsumer(GetNullYsonConsumer())
    , Holder_(std::move(holder))
{ }

void TDriverRequest::Reset()
{
    Holder_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor IDriver::GetCommandDescriptor(const TString& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    YT_VERIFY(descriptor);
    return *descriptor;
}

TCommandDescriptor IDriver::GetCommandDescriptorOrThrow(const TString& commandName) const
{
    auto descriptor = FindCommandDescriptor(commandName);
    if (!descriptor) {
        THROW_ERROR_EXCEPTION("Unknown command %Qv", commandName);
    }
    return *descriptor;
}

////////////////////////////////////////////////////////////////////////////////


class TDriver
    : public IDriver
{
public:
    TDriver(TDriverConfigPtr config, IConnectionPtr connection)
        : Config_(std::move(config))
        , Connection_(std::move(connection))
        , ClientCache_(New<TClientCache>(Config_->ClientCache, Connection_))
        , RootClient_(ClientCache_->Get(
            GetRootAuthenticationIdentity(),
            TClientOptions::FromAuthenticationIdentity(GetRootAuthenticationIdentity())))
        , ProxyDiscoveryCache_(CreateProxyDiscoveryCache(
            Config_->ProxyDiscoveryCache,
            RootClient_))
        , StickyTransactionPool_(CreateStickyTransactionPool(Logger()))
    {
        // Register all commands.
#define REGISTER(command, name, inDataType, outDataType, isVolatile, isHeavy, version) \
            if (version == Config_->ApiVersion) { \
                RegisterCommand<command>( \
                    TCommandDescriptor{name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy}); \
            }
#define REGISTER_ALL(command, name, inDataType, outDataType, isVolatile, isHeavy) \
            RegisterCommand<command>( \
                TCommandDescriptor{name, EDataType::inDataType, EDataType::outDataType, isVolatile, isHeavy}); \

        REGISTER    (TStartTransactionCommand,             "start_tx",                        Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TPingTransactionCommand,              "ping_tx",                         Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TCommitTransactionCommand,            "commit_tx",                       Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAbortTransactionCommand,             "abort_tx",                        Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TStartTransactionCommand,             "start_transaction",               Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TPingTransactionCommand,              "ping_transaction",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TCommitTransactionCommand,            "commit_transaction",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortTransactionCommand,             "abort_transaction",               Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TGenerateTimestampCommand,            "generate_timestamp",              Null,       Structured, false, false);

        REGISTER_ALL(TCreateCommand,                       "create",                          Null,       Structured, true,  false);
        REGISTER_ALL(TGetCommand,                          "get",                             Null,       Structured, false, false);
        REGISTER_ALL(TListCommand,                         "list",                            Null,       Structured, false, false);
        REGISTER_ALL(TLockCommand,                         "lock",                            Null,       Structured, true,  false);

        REGISTER    (TUnlockCommand,                       "unlock",                          Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUnlockCommand,                       "unlock",                          Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TCopyCommand,                         "copy",                            Null,       Structured, true,  false);
        REGISTER_ALL(TMoveCommand,                         "move",                            Null,       Structured, true,  false);
        REGISTER_ALL(TLinkCommand,                         "link",                            Null,       Structured, true,  false);
        REGISTER_ALL(TExistsCommand,                       "exists",                          Null,       Structured, false, false);

        REGISTER    (TConcatenateCommand,                  "concatenate",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemoveCommand,                       "remove",                          Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TSetCommand,                          "set",                             Structured, Null,       true,  false, ApiVersion3);

        REGISTER    (TConcatenateCommand,                  "concatenate",                     Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveCommand,                       "remove",                          Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TSetCommand,                          "set",                             Structured, Structured, true,  false, ApiVersion4);
        REGISTER    (TMultisetAttributesCommand,           "multiset_attributes",             Structured, Structured, true,  false, ApiVersion4);
        REGISTER    (TExternalizeCommand,                  "externalize",                     Null,       Null,       true,  false, ApiVersion4);
        REGISTER    (TInternalizeCommand,                  "internalize",                     Null,       Null,       true,  false, ApiVersion4);

        REGISTER    (TWriteFileCommand,                    "write_file",                      Binary,     Null,       true,  true,  ApiVersion3);
        REGISTER    (TWriteFileCommand,                    "write_file",                      Binary,     Structured, true,  true,  ApiVersion4);
        REGISTER_ALL(TReadFileCommand,                     "read_file",                       Null,       Binary,     false, true );

        REGISTER_ALL(TGetFileFromCacheCommand,             "get_file_from_cache",             Null,       Structured, false, false);
        REGISTER_ALL(TPutFileToCacheCommand,               "put_file_to_cache",               Null,       Structured, true,  false);

        REGISTER    (TWriteTableCommand,                   "write_table",                     Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TWriteTableCommand,                   "write_table",                     Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER_ALL(TGetTableColumnarStatisticsCommand,   "get_table_columnar_statistics",   Null,       Structured, false, false);
        REGISTER_ALL(TReadTableCommand,                    "read_table",                      Null,       Tabular,    false, true );
        REGISTER_ALL(TReadBlobTableCommand,                "read_blob_table",                 Null,       Binary,     false, true );
        REGISTER_ALL(TLocateSkynetShareCommand,            "locate_skynet_share",             Null,       Structured, false, true );

        REGISTER_ALL(TPartitionTablesCommand,              "partition_tables",                Null,       Structured, false, false);

        REGISTER    (TInsertRowsCommand,                   "insert_rows",                     Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TLockRowsCommand,                     "lock_rows",                       Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TDeleteRowsCommand,                   "delete_rows",                     Tabular,    Null,       true,  true , ApiVersion3);
        REGISTER    (TTrimRowsCommand,                     "trim_rows",                       Null,       Null,       true,  true , ApiVersion3);

        REGISTER    (TInsertRowsCommand,                   "insert_rows",                     Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER    (TLockRowsCommand,                     "lock_rows",                       Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER    (TDeleteRowsCommand,                   "delete_rows",                     Tabular,    Structured, true,  true , ApiVersion4);
        REGISTER    (TTrimRowsCommand,                     "trim_rows",                       Null,       Structured, true,  true , ApiVersion4);

        REGISTER_ALL(TExplainQueryCommand,                 "explain_query",                   Null,       Structured, false, true );
        REGISTER_ALL(TSelectRowsCommand,                   "select_rows",                     Null,       Tabular,    false, true );
        REGISTER_ALL(TLookupRowsCommand,                   "lookup_rows",                     Tabular,    Tabular,    false, true );
        REGISTER_ALL(TPullRowsCommand,                     "pull_rows",                       Null,       Tabular,    false, true );

        REGISTER    (TEnableTableReplicaCommand,           "enable_table_replica",            Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TDisableTableReplicaCommand,          "disable_table_replica",           Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAlterTableReplicaCommand,            "alter_table_replica",             Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TEnableTableReplicaCommand,           "enable_table_replica",            Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TDisableTableReplicaCommand,          "disable_table_replica",           Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAlterTableReplicaCommand,            "alter_table_replica",             Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TGetInSyncReplicasCommand,            "get_in_sync_replicas",            Tabular,    Structured, false, true );

        REGISTER    (TMountTableCommand,                   "mount_table",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUnmountTableCommand,                 "unmount_table",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemountTableCommand,                 "remount_table",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TFreezeTableCommand,                  "freeze_table",                    Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUnfreezeTableCommand,                "unfreeze_table",                  Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TReshardTableCommand,                 "reshard_table",                   Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAlterTableCommand,                   "alter_table",                     Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TMountTableCommand,                   "mount_table",                     Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUnmountTableCommand,                 "unmount_table",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemountTableCommand,                 "remount_table",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TFreezeTableCommand,                  "freeze_table",                    Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUnfreezeTableCommand,                "unfreeze_table",                  Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TReshardTableCommand,                 "reshard_table",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAlterTableCommand,                   "alter_table",                     Null,       Structured, true,  false, ApiVersion4);

        REGISTER    (TGetTablePivotKeysCommand,            "get_table_pivot_keys",            Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TGetTabletInfosCommand,               "get_tablet_infos",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TGetTabletErrorsCommand,              "get_tablet_errors",               Null,       Structured, true,  false, ApiVersion4);

        REGISTER    (TCreateTableBackupCommand,            "create_table_backup",             Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRestoreTableBackupCommand,           "restore_table_backup",            Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TCreateTableBackupCommand,            "create_table_backup",             Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRestoreTableBackupCommand,           "restore_table_backup",            Null,       Structured, true,  false, ApiVersion4);


        REGISTER_ALL(TReshardTableAutomaticCommand,        "reshard_table_automatic",         Null,       Structured, true,  false);
        REGISTER_ALL(TBalanceTabletCellsCommand,           "balance_tablet_cells",            Null,       Structured, true,  false);

        REGISTER    (TUpdateChaosTableReplicaProgressCommand, "update_replication_progress",  Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TAlterReplicationCardCommand,         "alter_replication_card",          Null,       Structured, false, false, ApiVersion4);

        REGISTER    (TMergeCommand,                        "merge",                           Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TEraseCommand,                        "erase",                           Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TMapCommand,                          "map",                             Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TSortCommand,                         "sort",                            Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TReduceCommand,                       "reduce",                          Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TJoinReduceCommand,                   "join_reduce",                     Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TMapReduceCommand,                    "map_reduce",                      Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TRemoteCopyCommand,                   "remote_copy",                     Null,       Structured, true,  false, ApiVersion3);

        REGISTER    (TStartOperationCommand,               "start_op",                        Null,       Structured, true,  false, ApiVersion3);
        REGISTER    (TAbortOperationCommand,               "abort_op",                        Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TSuspendOperationCommand,             "suspend_op",                      Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TResumeOperationCommand,              "resume_op",                       Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TCompleteOperationCommand,            "complete_op",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TUpdateOperationParametersCommand,    "update_op_parameters",            Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TStartOperationCommand,               "start_operation",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortOperationCommand,               "abort_operation",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TSuspendOperationCommand,             "suspend_operation",               Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TResumeOperationCommand,              "resume_operation",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TCompleteOperationCommand,            "complete_operation",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUpdateOperationParametersCommand,    "update_operation_parameters",     Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TParseYPathCommand,                   "parse_ypath",                     Null,       Structured, false, false);

        REGISTER    (TAddMemberCommand,                    "add_member",                      Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TRemoveMemberCommand,                 "remove_member",                   Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TAddMemberCommand,                    "add_member",                      Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveMemberCommand,                 "remove_member",                   Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TCheckPermissionCommand,              "check_permission",                Null,       Structured, false, false);
        REGISTER_ALL(TCheckPermissionByAclCommand,         "check_permission_by_acl",         Null,       Structured, false, false);

        REGISTER    (TTransferAccountResourcesCommand,     "transfer_account_resources",      Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TTransferPoolResourcesCommand,        "transfer_pool_resources",         Null,       Structured, true,  false, ApiVersion4);

        REGISTER    (TWriteJournalCommand,                 "write_journal",                   Tabular,    Null,       true,  true,  ApiVersion3);
        REGISTER    (TWriteJournalCommand,                 "write_journal",                   Tabular,    Structured, true,  true,  ApiVersion4);
        REGISTER_ALL(TReadJournalCommand,                  "read_journal",                    Null,       Tabular,    false, true );
        REGISTER    (TTruncateJournalCommand,              "truncate_journal",                Null,       Null,       true,  false, ApiVersion4);

        REGISTER_ALL(TGetJobInputCommand,                  "get_job_input",                   Null,       Binary,     false, true );
        REGISTER_ALL(TGetJobInputPathsCommand,             "get_job_input_paths",             Null,       Structured, false, true );
        REGISTER_ALL(TGetJobStderrCommand,                 "get_job_stderr",                  Null,       Binary,     false, true );
        REGISTER_ALL(TGetJobFailContextCommand,            "get_job_fail_context",            Null,       Binary,     false, true );
        REGISTER_ALL(TGetJobSpecCommand,                   "get_job_spec",                    Null,       Structured, false, true );
        REGISTER_ALL(TListOperationsCommand,               "list_operations",                 Null,       Structured, false, false);
        REGISTER_ALL(TListJobsCommand,                     "list_jobs",                       Null,       Structured, false, false);
        REGISTER_ALL(TGetJobCommand,                       "get_job",                         Null,       Structured, false, false);
        REGISTER_ALL(TPollJobShellCommand,                 "poll_job_shell",                  Null,       Structured, true,  false);
        REGISTER_ALL(TGetOperationCommand,                 "get_operation",                   Null,       Structured, false, false);

        REGISTER    (TDumpJobContextCommand,               "dump_job_context",                Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAbandonJobCommand,                   "abandon_job",                     Null,       Null,       true,  false, ApiVersion3);
        REGISTER    (TAbortJobCommand,                     "abort_job",                       Null,       Null,       true,  false, ApiVersion3);

        REGISTER    (TDumpJobContextCommand,               "dump_job_context",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbandonJobCommand,                   "abandon_job",                     Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortJobCommand,                     "abort_job",                       Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TDumpJobProxyLogCommand,              "dump_job_proxy_log",              Null,       Structured, true,  false, ApiVersion4);

        REGISTER_ALL(TGetVersionCommand,                   "get_version",                     Null,       Structured, false, false);
        REGISTER_ALL(TGetSupportedFeaturesCommand,         "get_supported_features",          Null,       Structured, false, false);

        REGISTER_ALL(TExecuteBatchCommand,                 "execute_batch",                   Null,       Structured, true,  false);

        REGISTER    (TDiscoverProxiesCommand,              "discover_proxies",                Null,       Structured, false, false, ApiVersion4);

        REGISTER_ALL(TBuildSnapshotCommand,                "build_snapshot",                  Null,       Structured, true,  false);
        REGISTER_ALL(TBuildMasterSnapshotsCommand,         "build_master_snapshots",          Null,       Structured, true,  false);
        REGISTER_ALL(TGetMasterConsistentStateCommand,     "get_master_consistent_state",     Null,       Structured, true,  false);
        REGISTER_ALL(TExitReadOnlyCommand,                 "exit_read_only",                  Null,       Structured, true,  false);
        REGISTER_ALL(TMasterExitReadOnlyCommand,           "master_exit_read_only",           Null,       Structured, true,  false);
        REGISTER_ALL(TDiscombobulateNonvotingPeersCommand, "discombobulate_nonvoting_peers",  Null,       Structured, true,  false);
        REGISTER_ALL(TSwitchLeaderCommand,                 "switch_leader",                   Null,       Structured, true,  false);
        REGISTER_ALL(TResetStateHashCommand,               "reset_state_hash",                Null,       Structured, true,  false);
        REGISTER_ALL(THealExecNodeCommand,                 "heal_exec_node",                  Null,       Structured, true,  false);

        REGISTER_ALL(TSuspendCoordinatorCommand,           "suspend_coordinator",             Null,       Structured, true,  false);
        REGISTER_ALL(TResumeCoordinatorCommand,            "resume_coordinator",              Null,       Structured, true,  false);
        REGISTER_ALL(TMigrateReplicationCardsCommand,      "migrate_replication_cards",       Null,       Structured, true,  false);
        REGISTER_ALL(TSuspendChaosCellsCommand,            "suspend_chaos_cells",             Null,       Structured, true,  false);
        REGISTER_ALL(TResumeChaosCellsCommand,             "resume_chaos_cells",              Null,       Structured, true,  false);

        REGISTER_ALL(TSuspendTabletCellsCommand,           "suspend_tablet_cells",            Null,       Structured, true,  false);
        REGISTER_ALL(TResumeTabletCellsCommand,            "resume_tablet_cells",             Null,       Structured, true,  false);

        REGISTER    (TAddMaintenanceCommand,               "add_maintenance",                 Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveMaintenanceCommand,            "remove_maintenance",              Null,       Structured, true,  false, ApiVersion4);
        REGISTER_ALL(TDisableChunkLocationsCommand,        "disable_chunk_locations",         Null,       Structured, true,  false);
        REGISTER_ALL(TDestroyChunkLocationsCommand,        "destroy_chunk_locations",         Null,       Structured, true,  false);
        REGISTER_ALL(TResurrectChunkLocationsCommand,      "resurrect_chunk_locations",       Null,       Structured, true,  false);
        REGISTER_ALL(TRequestRestartCommand,               "request_restart",                 Null,       Structured, true,  false);

        REGISTER_ALL(TSetUserPasswordCommand,              "set_user_password",               Null,       Structured, true,  false);
        REGISTER_ALL(TIssueTokenCommand,                   "issue_token",                     Null,       Structured, true,  false);
        REGISTER_ALL(TRevokeTokenCommand,                  "revoke_token",                    Null,       Structured, true,  false);
        REGISTER_ALL(TListUserTokensCommand,               "list_user_tokens",                Null,       Structured, false, false);

        REGISTER    (TRegisterQueueConsumerCommand,        "register_queue_consumer",         Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TUnregisterQueueConsumerCommand,      "unregister_queue_consumer",       Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TListQueueConsumerRegistrationsCommand, "list_queue_consumer_registrations", Null,   Structured, false, false, ApiVersion4);
        REGISTER    (TPullQueueCommand,                    "pull_queue",                      Null,       Tabular,    false, true , ApiVersion4);
        // COMPAT(nadya73): for compatibility with old versions of clients.
        REGISTER    (TPullQueueConsumerCommand,            "pull_consumer",                   Null,       Tabular,    false, true , ApiVersion4);
        REGISTER    (TPullQueueConsumerCommand,            "pull_queue_consumer",             Null,       Tabular,    false, true , ApiVersion4);
        // COMPAT(nadya73): for compatibility with old versions of clients.
        REGISTER    (TAdvanceQueueConsumerCommand,         "advance_consumer",                Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAdvanceQueueConsumerCommand,         "advance_queue_consumer",          Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TCreateQueueProducerSessionCommand,   "create_queue_producer_session",   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TRemoveQueueProducerSessionCommand,   "remove_queue_producer_session",   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TPushQueueProducerCommand,            "push_queue_producer",             Null,       Structured, true,  false, ApiVersion4);

        REGISTER    (TStartQueryCommand,                   "start_query",                     Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TAbortQueryCommand,                   "abort_query",                     Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TGetQueryCommand,                     "get_query",                       Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TListQueriesCommand,                  "list_queries",                    Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TGetQueryResultCommand,               "get_query_result",                Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TReadQueryResultCommand,              "read_query_result",               Null,       Tabular,    false, true , ApiVersion4);
        REGISTER    (TAlterQueryCommand,                   "alter_query",                     Null,       Tabular,    true,  false, ApiVersion4);
        REGISTER    (TGetQueryTrackerInfoCommand,          "get_query_tracker_info",          Null,       Structured, false, false, ApiVersion4);

        REGISTER_ALL(TGetBundleConfigCommand,              "get_bundle_config",               Null,       Structured, false, false);
        REGISTER_ALL(TSetBundleConfigCommand,              "set_bundle_config",               Structured, Null,       true,  false);

        REGISTER    (TGetPipelineSpecCommand,              "get_pipeline_spec",               Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TSetPipelineSpecCommand,              "set_pipeline_spec",               Structured, Structured, true,  false, ApiVersion4);
        REGISTER    (TRemovePipelineSpecCommand,           "remove_pipeline_spec",            Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TGetPipelineDynamicSpecCommand,       "get_pipeline_dynamic_spec",       Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TSetPipelineDynamicSpecCommand,       "set_pipeline_dynamic_spec",       Structured, Structured, true,  false, ApiVersion4);
        REGISTER    (TRemovePipelineDynamicSpecCommand,    "remove_pipeline_dynamic_spec",    Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TStartPipelineCommand,                "start_pipeline",                  Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TStopPipelineCommand,                 "stop_pipeline",                   Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TPausePipelineCommand,                "pause_pipeline",                  Null,       Structured, true,  false, ApiVersion4);
        REGISTER    (TGetPipelineStateCommand,             "get_pipeline_state",              Null,       Structured, false, false, ApiVersion4);
        REGISTER    (TGetFlowViewCommand,                  "get_flow_view",                   Null,       Structured, false, false, ApiVersion4);

        if (Config_->EnableInternalCommands) {
            REGISTER_ALL(TReadHunksCommand,                "read_hunks",                      Null,       Structured, false, true );
            REGISTER_ALL(TWriteHunksCommand,               "write_hunks",                     Null,       Structured, true,  true );
            REGISTER_ALL(TLockHunkStoreCommand,            "lock_hunk_store",                 Null,       Structured, true,  true );
            REGISTER_ALL(TUnlockHunkStoreCommand,          "unlock_hunk_store",               Null,       Structured, true,  true );
            REGISTER_ALL(TGetConnectionConfigCommand,      "get_connection_config",           Null,       Structured, false, false);
            REGISTER_ALL(TIssueLeaseCommand,               "issue_lease",                     Null,       Structured, true,  false);
            REGISTER_ALL(TRevokeLeaseCommand,              "revoke_lease",                    Null,       Structured, true,  false);
            REGISTER_ALL(TReferenceLeaseCommand,           "reference_lease",                 Null,       Structured, true,  false);
            REGISTER_ALL(TUnreferenceLeaseCommand,         "unreference_lease",               Null,       Structured, true,  false);
            REGISTER_ALL(TStartDistributedWriteSessionCommand,    "start_distributed_write_session",    Null,    Structured, true, false);
            REGISTER_ALL(TFinishDistributedWriteSessionCommand,   "finish_distributed_write_session",   Null,    Null,       true, false);
            REGISTER_ALL(TParticipantWriteTableCommand,           "participant_write_table",            Tabular, Structured, true, true );
        }

#undef REGISTER
#undef REGISTER_ALL
    }

    TFuture<void> Execute(const TDriverRequest& request) override
    {
        auto traceContext = CreateTraceContextFromCurrent("Driver");
        TTraceContextGuard guard(std::move(traceContext));

        auto it = CommandNameToEntry_.find(request.CommandName);
        if (it == CommandNameToEntry_.end()) {
            return MakeFuture(TError(
                "Unknown command %Qv",
                request.CommandName));
        }

        const auto& entry = it->second;
        TAuthenticationIdentity identity(
            request.AuthenticatedUser,
            request.UserTag.value_or(""));

        YT_VERIFY(entry.Descriptor.InputType == EDataType::Null || request.InputStream);
        YT_VERIFY(entry.Descriptor.OutputType == EDataType::Null || request.OutputStream);

        YT_LOG_DEBUG("Command received (RequestId: %" PRIx64 ", Command: %v, User: %v)",
            request.Id,
            request.CommandName,
            identity.User);

        auto options = TClientOptions::FromAuthenticationIdentity(identity);
        options.Token = request.UserToken;
        options.ServiceTicketAuth = request.ServiceTicket
            ? std::make_optional(New<NAuth::TServiceTicketFixedAuth>(*request.ServiceTicket))
            : std::nullopt;

        auto client = ClientCache_->Get(identity, options);

        auto context = New<TCommandContext>(
            this,
            std::move(client),
            RootClient_,
            Config_,
            entry.Descriptor,
            request);

        return BIND(&TDriver::DoExecute, entry.Execute, context)
            .AsyncVia(Connection_->GetInvoker())
            .Run();
    }

    std::optional<TCommandDescriptor> FindCommandDescriptor(const TString& commandName) const override
    {
        auto it = CommandNameToEntry_.find(commandName);
        return it == CommandNameToEntry_.end() ? std::nullopt : std::make_optional(it->second.Descriptor);
    }

    const std::vector<TCommandDescriptor> GetCommandDescriptors() const override
    {
        std::vector<TCommandDescriptor> result;
        result.reserve(CommandNameToEntry_.size());
        for (const auto& [name, entry] : CommandNameToEntry_) {
            result.push_back(entry.Descriptor);
        }
        return result;
    }

    void ClearMetadataCaches() override
    {
        ClientCache_->Clear();
        Connection_->ClearMetadataCaches();
    }

    IStickyTransactionPoolPtr GetStickyTransactionPool() override
    {
        return StickyTransactionPool_;
    }

    IProxyDiscoveryCachePtr GetProxyDiscoveryCache() override
    {
        return ProxyDiscoveryCache_;
    }

    IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    void Terminate() override
    {
        // TODO(ignat): find and eliminate reference loop.
        // Reset of the connection should be sufficient to release this connection.
        // But there is some reference loop and it does not work.

        ClearMetadataCaches();

        // Release the connection with entire thread pools.
        if (Connection_) {
            Connection_->Terminate();
            ClientCache_.Reset();
            ProxyDiscoveryCache_.Reset();
            Connection_.Reset();
        }
    }

private:
    const TDriverConfigPtr Config_;
    IConnectionPtr Connection_;
    TClientCachePtr ClientCache_;
    const IClientPtr RootClient_;
    IProxyDiscoveryCachePtr ProxyDiscoveryCache_;

    class TCommandContext;
    using TCommandContextPtr = TIntrusivePtr<TCommandContext>;
    using TExecuteCallback = TCallback<void(ICommandContextPtr)>;

    const IStickyTransactionPoolPtr StickyTransactionPool_;

    struct TCommandEntry
    {
        TCommandDescriptor Descriptor;
        TExecuteCallback Execute;
    };

    THashMap<TString, TCommandEntry> CommandNameToEntry_;


    template <class TCommand>
    void RegisterCommand(const TCommandDescriptor& descriptor)
    {
        TCommandEntry entry;
        entry.Descriptor = descriptor;
        entry.Execute = BIND_NO_PROPAGATE([] (ICommandContextPtr context) {
            TCommand command;
            command.Execute(context);
        });
        YT_VERIFY(CommandNameToEntry_.emplace(descriptor.CommandName, entry).second);
    }

    static void DoExecute(TExecuteCallback executeCallback, TCommandContextPtr context)
    {
        const auto& request = context->Request();

        if (request.LoggingTags) {
            Logger().WithRawTag(*request.LoggingTags);
        }

        NTracing::TChildTraceContextGuard commandSpan(ConcatToString(TStringBuf("Driver:"), request.CommandName));
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("user", request.AuthenticatedUser);
            traceContext->AddTag("request_id", request.Id);
        });

        YT_LOG_DEBUG("Command started (RequestId: %" PRIx64 ", Command: %v, User: %v)",
            request.Id,
            request.CommandName,
            request.AuthenticatedUser);

        TError result;
        try {
            executeCallback.Run(context);
        } catch (const std::exception& ex) {
            result = TError(ex);
        }

        if (result.IsOK()) {
            YT_LOG_DEBUG("Command completed (RequestId: %" PRIx64 ", Command: %v, User: %v)",
                request.Id,
                request.CommandName,
                request.AuthenticatedUser);
        } else {
            YT_LOG_DEBUG(result, "Command failed (RequestId: %" PRIx64 ", Command: %v, User: %v)",
                request.Id,
                request.CommandName,
                request.AuthenticatedUser);
        }

        context->Finish();

        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    class TCommandContext
        : public ICommandContext
    {
    public:
        TCommandContext(
            IDriverPtr driver,
            IClientPtr client,
            IClientPtr rootClient,
            TDriverConfigPtr config,
            TCommandDescriptor descriptor,
            const TDriverRequest& request)
            : Driver_(std::move(driver))
            , Client_(std::move(client))
            , RootClient_(std::move(rootClient))
            , Config_(std::move(config))
            , Descriptor_(std::move(descriptor))
            , Request_(std::move(request))
        { }

        const TDriverConfigPtr& GetConfig() const override
        {
            return Config_;
        }

        const IClientPtr& GetClient() const override
        {
            return Client_;
        }

        const IClientPtr& GetRootClient() const override
        {
            return RootClient_;
        }

        IInternalClientPtr GetInternalClientOrThrow() const override
        {
            auto internalClient = DynamicPointerCast<IInternalClient>(Client_);
            if (!internalClient) {
                THROW_ERROR_EXCEPTION("Client does not support internal API");
            }
            return internalClient;
        }

        const IDriverPtr& GetDriver() const override
        {
            return Driver_;
        }

        const TDriverRequest& Request() const override
        {
            return Request_;
        }

        const TFormat& GetInputFormat() override
        {
            if (!InputFormat_) {
                InputFormat_ = ConvertTo<TFormat>(Request_.Parameters->GetChildOrThrow("input_format"));
            }
            return *InputFormat_;
        }

        const TFormat& GetOutputFormat() override
        {
            if (!OutputFormat_) {
                OutputFormat_ = ConvertTo<TFormat>(Request_.Parameters->GetChildOrThrow("output_format"));
            }
            return *OutputFormat_;
        }

        TYsonString ConsumeInputValue() override
        {
            YT_VERIFY(Request_.InputStream);
            auto syncInputStream = CreateSyncAdapter(CreateCopyingAdapter(Request_.InputStream));

            auto producer = CreateProducerForFormat(
                GetInputFormat(),
                Descriptor_.InputType,
                syncInputStream.get());

            return ConvertToYsonString(producer);
        }

        void ProduceOutputValue(const TYsonString& yson) override
        {
            YT_VERIFY(Request_.OutputStream);
            auto syncOutputStream = CreateBufferedSyncAdapter(Request_.OutputStream);

            auto consumer = CreateConsumerForFormat(
                GetOutputFormat(),
                Descriptor_.OutputType,
                syncOutputStream.get());

            Serialize(yson, consumer.get());

            consumer->Flush();
            syncOutputStream->Flush();
        }

        void Finish()
        {
            Request_.Reset();
        }

    private:
        const IDriverPtr Driver_;
        const IClientPtr Client_;
        const IClientPtr RootClient_;
        const TDriverConfigPtr Config_;
        const TCommandDescriptor Descriptor_;

        TDriverRequest Request_;

        std::optional<TFormat> InputFormat_;
        std::optional<TFormat> OutputFormat_;
    };
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(
    IConnectionPtr connection,
    TDriverConfigPtr config)
{
    YT_VERIFY(connection);
    YT_VERIFY(config);

    return New<TDriver>(
        std::move(config),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
