#pragma once

#include "public.h"
#include "connection.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/journal_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/query_tracker_client/public.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/node_tracker_client/public.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/chunk_stripe_statistics.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/client/table_client/columnar_statistics.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/chunk_client/config.h>
#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/driver/private.h>

#include <yt/yt/client/ypath/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/attribute_filter.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkloadDescriptor
{
    EUserWorkloadCategory Category = EUserWorkloadCategory::Interactive;
    int Band = 0;

    operator TWorkloadDescriptor() const;
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer);
void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, NYTree::INodePtr node);
void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

struct TTimeoutOptions
{
    std::optional<TDuration> Timeout;
};

struct TDetailedProfilingInfo
    : public TRefCounted
{
    bool EnableDetailedTableProfiling = false;
    NYPath::TYPath TablePath;
    TDuration MountCacheWaitTime;

    int WastedSubrequestCount = 0;

    std::vector<TErrorCode> RetryReasons;
};

DEFINE_REFCOUNTED_TYPE(TDetailedProfilingInfo)

struct TMultiplexingBandOptions
{
    NRpc::EMultiplexingBand MultiplexingBand = NRpc::EMultiplexingBand::Default;
};

struct TTabletRangeOptions
{
    std::optional<int> FirstTabletIndex;
    std::optional<int> LastTabletIndex;
};

struct TTransactionalOptions
{
    //! Ignored when queried via transaction.
    NObjectClient::TTransactionId TransactionId;
    bool Ping = false;
    bool PingAncestors = false;
    //! For internal use only.
    //! Setting it to |true| may result in loss of consistency.
    bool SuppressTransactionCoordinatorSync = false;
    //! For internal use only.
    //! Setting it to |true| may result in loss of consistency .
    bool SuppressUpstreamSync = false;
};

struct TSuppressableAccessTrackingOptions
{
    bool SuppressAccessTracking = false;
    bool SuppressModificationTracking = false;
    bool SuppressExpirationTimeoutRenewal = false;
};

struct TMutatingOptions
{
    NRpc::TMutationId MutationId;
    bool Retry = false;

    NRpc::TMutationId GetOrGenerateMutationId() const;
};

struct TMasterReadOptions
{
    EMasterChannelKind ReadFrom = EMasterChannelKind::Follower;
    bool DisablePerUserCache = false;
    TDuration ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(15);
    TDuration ExpireAfterFailedUpdateTime = TDuration::Seconds(15);
    std::optional<int> CacheStickyGroupSize;
    bool EnableClientCacheStickiness = false;

    // When staleness bound is non-zero, master cache is allowed to
    // return successful expired response, with staleness not exceeding the bound.
    // This allows non-blocking master cache responses, with async on-demand updates.
    TDuration SuccessStalenessBound;
};

struct TSerializableMasterReadOptions
    : public TMasterReadOptions
    , public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TSerializableMasterReadOptions);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("read_from", &TThis::ReadFrom)
            .Default(TMasterReadOptions{}.ReadFrom);

        registrar.BaseClassParameter("disable_per_user_cache", &TThis::DisablePerUserCache)
            .Default(TMasterReadOptions{}.DisablePerUserCache);

        registrar.BaseClassParameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
            .Default(TMasterReadOptions{}.ExpireAfterSuccessfulUpdateTime);

        registrar.BaseClassParameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
            .Default(TMasterReadOptions{}.ExpireAfterFailedUpdateTime);

        registrar.BaseClassParameter("cache_sticky_group_size", &TThis::CacheStickyGroupSize)
            .Default(TMasterReadOptions{}.CacheStickyGroupSize);

        registrar.BaseClassParameter("enable_client_cache_stickiness", &TThis::EnableClientCacheStickiness)
            .Default(TMasterReadOptions{}.EnableClientCacheStickiness);

        registrar.BaseClassParameter("success_staleness_bound", &TThis::SuccessStalenessBound)
            .Default(TMasterReadOptions{}.SuccessStalenessBound);
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableMasterReadOptions)

struct TPrerequisiteRevisionConfig
    : public NYTree::TYsonStruct
{
    NYTree::TYPath Path;
    NHydra::TRevision Revision;

    REGISTER_YSON_STRUCT(TPrerequisiteRevisionConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
        registrar.Parameter("revision", &TThis::Revision);
    }
};

DEFINE_REFCOUNTED_TYPE(TPrerequisiteRevisionConfig)

struct TPrerequisiteOptions
{
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;
    std::vector<TPrerequisiteRevisionConfigPtr> PrerequisiteRevisions;
};

struct TSyncReplicaCacheOptions
{
    std::optional<TDuration> CachedSyncReplicasTimeout;
};

struct TMountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    NTabletClient::TTabletCellId CellId = NTabletClient::NullTabletCellId;
    std::vector<NTabletClient::TTabletCellId> TargetCellIds;
    bool Freeze = false;
};

struct TUnmountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    bool Force = false;
};

struct TRemountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TFreezeTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TUnfreezeTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TReshardTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    std::optional<bool> Uniform;
    std::optional<bool> EnableSlicing;
    std::optional<double> SlicingAccuracy;
};

struct TReshardTableAutomaticOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    bool KeepActions = false;
};

struct TAlterTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTransactionalOptions
{
    std::optional<NTableClient::TTableSchema> Schema;
    std::optional<NTableClient::TMasterTableSchemaId> SchemaId;
    std::optional<bool> Dynamic;
    std::optional<NTabletClient::TTableReplicaId> UpstreamReplicaId;
    std::optional<NTableClient::ETableSchemaModification> SchemaModification;
    std::optional<NChaosClient::TReplicationProgress> ReplicationProgress;
};

struct TTrimTableOptions
    : public TTimeoutOptions
{ };

struct TAlterTableReplicaOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    std::optional<bool> Enabled;
    std::optional<NTabletClient::ETableReplicaMode> Mode;
    std::optional<bool> PreserveTimestamps;
    std::optional<NTransactionClient::EAtomicity> Atomicity;
    std::optional<bool> EnableReplicatedTableTracker;
};

struct TGetTablePivotKeysOptions
    : public TTimeoutOptions
{
    bool RepresentKeyAsList = false;
};

struct TGetInSyncReplicasOptions
    : public TTimeoutOptions
    , public TSyncReplicaCacheOptions
{
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
};

struct TGetTabletInfosOptions
    : public TTimeoutOptions
{
    bool RequestErrors = false;
};

struct TTabletInfo
{
    struct TTableReplicaInfo
    {
        NTabletClient::TTableReplicaId ReplicaId;
        NTransactionClient::TTimestamp LastReplicationTimestamp;
        NTabletClient::ETableReplicaMode Mode;
        i64 CurrentReplicationRowIndex;
        i64 CommittedReplicationRowIndex;
        TError ReplicationError;
    };

    //! Currently only provided for ordered tablets.
    //! Indicates the total number of rows added to the tablet (including trimmed ones).
    // TODO(babenko): implement for sorted tablets
    i64 TotalRowCount = 0;

    //! Only makes sense for ordered tablet.
    //! Contains the number of front rows that are trimmed and are not guaranteed to be accessible.
    i64 TrimmedRowCount = 0;

    //! Only makes sense for replicated tablets.
    //! Contains the number of rows that are yet to be committed.
    i64 DelayedLocklessRowCount = 0;

    //! Mostly makes sense for ordered tablets.
    //! Contains the barrier timestamp of the tablet cell containing the tablet, which lags behind the current timestamp.
    //! It is guaranteed that all transactions with commit timestamp not exceeding the barrier
    //! are fully committed; e.g. all their added rows are visible (and are included in TTabletInfo::TotalRowCount).
    NTransactionClient::TTimestamp BarrierTimestamp;

    //! Contains maximum timestamp of committed transactions.
    NTransactionClient::TTimestamp LastWriteTimestamp;

    //! Only makes sense for replicated tablets.
    std::optional<std::vector<TTableReplicaInfo>> TableReplicaInfos;

    //! Set if `RequestErrors` is present in command parameters.
    std::vector<TError> TabletErrors;
};

struct TGetTabletErrorsOptions
    : public TTimeoutOptions
{
    std::optional<i64> Limit;
};

struct TGetTabletErrorsResult
{
    bool Incomplete = false;
    THashMap<NTabletClient::TTabletId, std::vector<TError>> TabletErrors;
    THashMap<NTabletClient::TTableReplicaId, std::vector<TError>> ReplicationErrors;
};

struct TBalanceTabletCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    bool KeepActions = false;
};

struct TGetReplicationCardOptions
    : public TTimeoutOptions
    , public NChaosClient::TReplicationCardFetchOptions
{
    bool BypassCache = false;
};

struct TUpdateChaosTableReplicaProgressOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    NChaosClient::TReplicationProgress Progress;
};

struct TAlterReplicationCardOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    NTabletClient::TReplicatedTableOptionsPtr ReplicatedTableOptions;
    std::optional<bool> EnableReplicatedTableTracker;
    std::optional<NChaosClient::TReplicationCardCollocationId> ReplicationCardCollocationId;
};

struct TAddMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCheckPermissionOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{
    std::optional<std::vector<TString>> Columns;
    std::optional<bool> Vital;
};

struct TCheckPermissionResult
{
    TError ToError(
        const TString& user,
        NYTree::EPermission permission,
        const std::optional<TString>& columns = {}) const;

    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    std::optional<TString> ObjectName;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<TString> SubjectName;
};

struct TCheckPermissionResponse
    : public TCheckPermissionResult
{
    std::optional<std::vector<TCheckPermissionResult>> Columns;
};

struct TCheckPermissionByAclOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TPrerequisiteOptions
{
    bool IgnoreMissingSubjects = false;
};

struct TCheckPermissionByAclResult
{
    TError ToError(const TString& user, NYTree::EPermission permission) const;

    NSecurityClient::ESecurityAction Action;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<TString> SubjectName;
    std::vector<TString> MissingSubjects;
};

struct TTransferAccountResourcesOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TTransferPoolResourcesOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TTransactionStartOptions
    : public TMutatingOptions
{
    std::optional<TDuration> Timeout;

    //! If not null then the transaction must use this externally provided id.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTransactionId Id;

    NTransactionClient::TTransactionId ParentId;
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;

    std::optional<TInstant> Deadline;

    bool AutoAbort = true;
    bool Sticky = false;

    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = true;

    NYTree::IAttributeDictionaryPtr Attributes;

    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;

    //! If not null then the transaction must use this externally provided start timestamp.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;

    //! For master transactions only; disables generating start timestamp.
    bool SuppressStartTimestampGeneration = false;

    //! Only for master transactions.
    //! Indicates the master cell the transaction will be initially started at and controlled by
    //! (chosen automatically by default).
    NObjectClient::TCellTag CoordinatorMasterCellTag = NObjectClient::InvalidCellTag;

    //! Only for master transactions.
    //! Indicates the cells the transaction will be replicated to at the start. None by default,
    //! but usually the transaction will be able to be replicated at a later time on demand.
    std::optional<NObjectClient::TCellTagList> ReplicateToMasterCellTags;

    //! Only for master transactions.
    //! By default, all master transactions are Cypress expect for some
    //! system ones (e.g. store flusher transactions).
    bool StartCypressTransaction = true;
};

struct TTransactionAttachOptions
{
    bool AutoAbort = false;
    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = false;

    //! If non-empty, assumes that the transaction is sticky and specifies address of the transaction manager.
    //! Throws if the transaction is not sticky actually.
    //! Only supported by RPC proxy client for now. Ignored by other clients.
    TString StickyAddress;
};

struct TTransactionCommitOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    //! If not null, then this particular cell will be the coordinator.
    NObjectClient::TCellId CoordinatorCellId;

    //! If |true| then two-phase-commit protocol is executed regardless of the number of participants.
    bool Force2PC = false;

    //! Eager: coordinator is committed first, success is reported immediately; the participants are committed afterwards.
    //! Lazy: all the participants must successfully commit before coordinator commits; only after this success is reported.
    ETransactionCoordinatorCommitMode CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Eager;

    //! Early: coordinator is prepared first and committed first.
    //! Late: coordinator is prepared last and after commit timestamp generation; prepare and commit
    //! are executed in single mutation.
    ETransactionCoordinatorPrepareMode CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Early;

    //! At non-coordinating participants, Transaction Manager will synchronize with
    //! these cells before running prepare.
    std::vector<NObjectClient::TCellId> CellIdsToSyncWithBeforePrepare;

    //! If |true| then all participants will use the commit timestamp provided by the coordinator.
    //! If |false| then the participants will use individual commit timestamps based on their cell tag.
    bool InheritCommitTimestamp = true;

    //! If |true| then the coordinator will generate a non-null prepare timestamp (which is a lower bound for
    //! the upcoming commit timestamp) and send it to all the participants.
    //! If |false| then no prepare timestamp is generated and null value is provided to the participants.
    //! The latter is useful for async replication that does not involve any local write operations
    //! and also relies on ETransactionCoordinatorCommitMode::Lazy transactions whose commit may be delayed
    //! for an arbitrary period of time in case of replica failure.
    bool GeneratePrepareTimestamp = true;

    //! If non-null then the coordinator will fail the commit if generated commit timestamp
    //! exceeds |MaxAllowedCommitTimestamp|.
    NTransactionClient::TTimestamp MaxAllowedCommitTimestamp = NTransactionClient::NullTimestamp;

    //! Cell ids of additional 2PC participants.
    //! Used to implement cross-cluster commit via RPC proxy.
    std::vector<NObjectClient::TCellId> AdditionalParticipantCellIds;
};

struct TTransactionPingOptions
{
    bool EnableRetries = false;
};

struct TTransactionCommitResult
{
    //! NullTimestamp for all cases when CommitTimestamps are empty.
    //! NullTimestamp when the primary cell did not participate in to transaction.
    NHiveClient::TTimestamp PrimaryCommitTimestamp = NHiveClient::NullTimestamp;
    //! Empty for non-atomic transactions (timestamps are fake).
    //! Empty for empty tablet transactions (since the commit is essentially no-op).
    //! May contain multiple items for cross-cluster commit.
    NHiveClient::TTimestampMap CommitTimestamps;
};

struct TTransactionAbortOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    bool Force = false;
};

struct TTabletReadOptionsBase
    : public TSyncReplicaCacheOptions
{
    NHydra::EPeerKind ReadFrom = NHydra::EPeerKind::Leader;
    std::optional<TDuration> RpcHedgingDelay;

    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

    NTransactionClient::TTimestamp RetentionTimestamp = NTransactionClient::NullTimestamp;

    EReplicaConsistency ReplicaConsistency = EReplicaConsistency::None;
};

struct TTabletReadOptions
    : public TTimeoutOptions
    , public TTabletReadOptionsBase
{ };

struct TLookupRequestOptions
{
    NTableClient::TColumnFilter ColumnFilter;
    bool KeepMissingRows = false;
    bool EnablePartialResult = false;
    std::optional<bool> UseLookupCache;
    TDetailedProfilingInfoPtr DetailedProfilingInfo;
    NTableClient::TTableSchemaPtr FallbackTableSchema;
    NTabletClient::TTableReplicaId FallbackReplicaId;
};

struct TLookupRowsOptionsBase
    : public TTabletReadOptions
    , public TLookupRequestOptions
    , public TMultiplexingBandOptions
{ };

struct TLookupRowsOptions
    : public TLookupRowsOptionsBase
{ };

struct TVersionedLookupRowsOptions
    : public TLookupRowsOptionsBase
{
    NTableClient::TRetentionConfigPtr RetentionConfig;
};

struct TMultiLookupSubrequest
{
    NYPath::TYPath Path;
    NTableClient::TNameTablePtr NameTable;
    TSharedRange<NTableClient::TLegacyKey> Keys;

    // NB: Other options from TLookupRowsOptions that are absent from TLookupRequestOptions are
    // common and included in TMultiLookupOptions.
    TLookupRequestOptions Options;
};

struct TMultiLookupOptions
    : public TTimeoutOptions
    , public TTabletReadOptionsBase
    , public TMultiplexingBandOptions
{ };

struct TSelectRowsOptionsBase
    : public TTabletReadOptions
    , public TSuppressableAccessTrackingOptions
{
    //! Limits range expanding.
    ui64 RangeExpansionLimit = 200000;
    //! Limits maximum parallel subqueries.
    int MaxSubqueries = std::numeric_limits<int>::max();
    //! Path in Cypress with UDFs.
    std::optional<TString> UdfRegistryPath;
    //! If |true| then logging is more verbose.
    bool VerboseLogging = false;
};

struct TSelectRowsOptions
    : public TSelectRowsOptionsBase
{
    //! If null then connection defaults are used.
    std::optional<i64> InputRowLimit;
    //! If null then connection defaults are used.
    std::optional<i64> OutputRowLimit;
    //! Combine independent joins in one.
    bool UseMultijoin = true;
    //! Allow queries without any condition on key columns.
    bool AllowFullScan = true;
    //! Allow queries with join condition which implies foreign query with IN operator.
    bool AllowJoinWithoutIndex = false;
    //! Execution pool.
    std::optional<TString> ExecutionPool;
    //! If |true| then incomplete result would lead to a failure.
    bool FailOnIncompleteResult = true;
    //! Enables generated code caching.
    bool EnableCodeCache = true;
    //! Used to prioritize requests.
    TUserWorkloadDescriptor WorkloadDescriptor;
    //! Memory limit per execution node.
    size_t MemoryLimitPerNode = std::numeric_limits<size_t>::max();
    //! Info on detailed profiling.
    TDetailedProfilingInfoPtr DetailedProfilingInfo;
    //! YSON map with placeholder values for parameterized queries.
    NYson::TYsonString PlaceholderValues;
    //! Expected schemas for tables in a query (used for replica fallback in replicated tables).
    using TExpectedTableSchemas = THashMap<NYPath::TYPath, NTableClient::TTableSchemaPtr>;
    TExpectedTableSchemas ExpectedTableSchemas;
};

struct TExplainQueryOptions
    : public TSelectRowsOptionsBase
{
    bool VerboseOutput = false;
};

struct TPullRowsOptions
    : public TTabletReadOptions
{
    NChaosClient::TReplicaId UpstreamReplicaId;
    THashMap<NTabletClient::TTabletId, i64> StartReplicationRowIndexes;
    i64 TabletRowsPerRead = 1000;
    bool OrderRowsByTimestamp = false;

    NChaosClient::TReplicationProgress ReplicationProgress;
    NTransactionClient::TTimestamp UpperTimestamp = NTransactionClient::NullTimestamp;
    NTableClient::TTableSchemaPtr TableSchema;
};

struct TPullRowsResult
{
    THashMap<NTabletClient::TTabletId, i64> EndReplicationRowIndexes;
    i64 RowCount = 0;
    i64 DataWeight = 0;
    NChaosClient::TReplicationProgress ReplicationProgress;
    ITypeErasedRowsetPtr Rowset;
    bool Versioned = true;
};

struct TGetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    // NB(eshcherbin): Used in profiling Orchid.
    NYTree::IAttributeDictionaryPtr Options;
    NYTree::TAttributeFilter Attributes;
    std::optional<i64> MaxSize;
};

struct TSetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
};

struct TMultisetAttributesNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    NYTree::TAttributeFilter Attributes;
    std::optional<i64> MaxSize;
};

struct TCreateObjectOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool IgnoreExisting = false;
    bool Sync = true;
    NYTree::IAttributeDictionaryPtr Attributes;
};

struct TCreateNodeOptions
    : public TCreateObjectOptions
    , public TTransactionalOptions
{
    bool Recursive = false;
    bool LockExisting = false;
    bool Force = false;
    bool IgnoreTypeMismatch = false;
};

struct TLockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Waitable = false;
    std::optional<TString> ChildKey;
    std::optional<TString> AttributeKey;
};

struct TLockNodeResult
{
    NCypressClient::TLockId LockId;
    NCypressClient::TNodeId NodeId;
    NHydra::TRevision Revision = NHydra::NullRevision;
};

struct TUnlockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCopyNodeOptionsBase
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveOwner = false;
    bool PreserveExpirationTime = false;
    bool PreserveExpirationTimeout = false;
    bool PreserveAcl = false;
    bool PessimisticQuotaCheck = true;
};

struct TCopyNodeOptions
    : public TCopyNodeOptionsBase
{
    bool IgnoreExisting = false;
    bool LockExisting = false;
};

struct TMoveNodeOptions
    : public TCopyNodeOptionsBase
{
    TMoveNodeOptions()
    {
        // COMPAT(babenko): YT-11903, consider dropping this override
        PreserveCreationTime = true;
    }
};

struct TLinkNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    //! Attributes of a newly created link node.
    NYTree::IAttributeDictionaryPtr Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
    bool LockExisting = false;
    bool Force = false;
};

struct TConcatenateNodesOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{
    NChunkClient::TFetcherConfigPtr ChunkMetaFetcherConfig;

    bool UniqualizeChunks = false;
};

struct TNodeExistsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{ };

struct TExternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

struct TInternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

struct TFileReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> Offset;
    std::optional<i64> Length;
    TFileReaderConfigPtr Config;
};

struct TFileWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    bool ComputeMD5 = false;
    TFileWriterConfigPtr Config;
};

struct TGetFileFromCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
};

struct TPutFileToCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
    bool PreserveExpirationTimeout = false;
    int RetryCount = 10;
};

struct TJournalReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> FirstRowIndex;
    std::optional<i64> RowCount;
    TJournalReaderConfigPtr Config;
};

struct TJournalWriterPerformanceCounters
{
    TJournalWriterPerformanceCounters() = default;
    explicit TJournalWriterPerformanceCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TEventTimer GetBasicAttributesTimer;
    NProfiling::TEventTimer BeginUploadTimer;
    NProfiling::TEventTimer GetExtendedAttributesTimer;
    NProfiling::TEventTimer GetUploadParametersTimer;
    NProfiling::TEventTimer EndUploadTimer;
    NProfiling::TEventTimer OpenSessionTimer;
    NProfiling::TEventTimer CreateChunkTimer;
    NProfiling::TEventTimer AllocateWriteTargetsTimer;
    NProfiling::TEventTimer StartNodeSessionTimer;
    NProfiling::TEventTimer ConfirmChunkTimer;
    NProfiling::TEventTimer AttachChunkTimer;
    NProfiling::TEventTimer SealChunkTimer;

    NProfiling::TEventTimer WriteQuorumLag;
    NProfiling::TEventTimer MaxReplicaLag;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    TJournalWriterConfigPtr Config;
    bool EnableMultiplexing = true;
    // TODO(babenko): enable by default
    bool EnableChunkPreallocation = false;

    i64 ReplicaLagLimit = NJournalClient::DefaultReplicaLagLimit;

    TJournalWriterPerformanceCounters Counters;
};

struct TTruncateJournalOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TTableReaderOptions
    : public TTransactionalOptions
{
    bool Unordered = false;
    bool OmitInaccessibleColumns = false;
    bool EnableTableIndex = false;
    bool EnableRowIndex = false;
    bool EnableRangeIndex = false;
    bool EnableTabletIndex = false;
    NTableClient::TTableReaderConfigPtr Config;
};

struct TTableWriterOptions
    : public TTransactionalOptions
{
    bool ValidateAnyIsValidYson = false;

    NTableClient::TTableWriterConfigPtr Config;
};

struct TPullQueueOptions
    : public TSelectRowsOptions
{
    // COMPAT(achulkov2): Remove this once we drop support for legacy PullQueue via SelectRows.
    bool UseNativeTabletNodeApi = true;
};

struct TPullConsumerOptions
    : public TPullQueueOptions
{ };

struct TRegisterQueueConsumerOptions
    : public TTimeoutOptions
{
    std::optional<std::vector<int>> Partitions;
};

struct TUnregisterQueueConsumerOptions
    : public TTimeoutOptions
{ };

struct TListQueueConsumerRegistrationsOptions
    : public TTimeoutOptions
{ };

struct TGetColumnarStatisticsOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;
    NChunkClient::TFetcherConfigPtr FetcherConfig;
    NTableClient::EColumnarStatisticsFetcherMode FetcherMode = NTableClient::EColumnarStatisticsFetcherMode::FromNodes;
    bool EnableEarlyFinish = true;
};

struct TPartitionTablesOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;
    NChunkClient::TFetcherConfigPtr FetcherConfig;
    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcherConfig;
    NTableClient::ETablePartitionMode PartitionMode = NTableClient::ETablePartitionMode::Unordered;
    i64 DataWeightPerPartition;
    std::optional<int> MaxPartitionCount;
    bool AdjustDataWeightPerPartition = true;
    bool EnableKeyGuarantee = false;
};

struct TMultiTablePartition
{
    //! Table ranges are indexed by table index.
    std::vector<NYPath::TRichYPath> TableRanges;

    //! Aggregate statistics of all the table ranges in the partition.
    NTableClient::TChunkStripeStatistics AggregateStatistics;
};

void Serialize(const TMultiTablePartition& partitions, NYson::IYsonConsumer* consumer);

struct TMultiTablePartitions
{
    std::vector<TMultiTablePartition> Partitions;
};

void Serialize(const TMultiTablePartitions& partitions, NYson::IYsonConsumer* consumer);

struct TLocateSkynetShareOptions
    : public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr Config;
};

struct TStartOperationOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TAbortOperationOptions
    : public TTimeoutOptions
{
    std::optional<TString> AbortMessage;
};

struct TSuspendOperationOptions
    : public TTimeoutOptions
{
    bool AbortRunningJobs = false;
};

struct TResumeOperationOptions
    : public TTimeoutOptions
{ };

struct TCompleteOperationOptions
    : public TTimeoutOptions
{ };

struct TUpdateOperationParametersOptions
    : public TTimeoutOptions
{ };

struct TDumpJobContextOptions
    : public TTimeoutOptions
{ };

//! Source to fetch job spec from. Useful in tests.
DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EJobSpecSource, ui16,
    //! Job spec is fetched from exec node.
    ((Node) (1))

    //! Job spec is fetched from job archive.
    ((Archive) (2))

    //! Job spec is fetched from any available source.
    ((Auto) (0xFFFF))
);

struct TGetJobInputOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;
};

struct TGetJobInputPathsOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;
};

struct TGetJobSpecOptions
    : public TTimeoutOptions
{
    //! Where job spec should be retrieved from.
    EJobSpecSource JobSpecSource = EJobSpecSource::Auto;

    bool OmitNodeDirectory = false;
    bool OmitInputTableSpecs = false;
    bool OmitOutputTableSpecs = false;
};


struct TGetJobStderrOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{ };

struct TGetJobFailContextOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{ };

DEFINE_ENUM(EOperationSortDirection,
    ((None)   (0))
    ((Past)   (1))
    ((Future) (2))
);

struct TListOperationsAccessFilter
    : NYTree::TYsonStruct
{
    TString Subject;
    NYTree::EPermissionSet Permissions;

    // This parameter cannot be set from YSON, it must be computed.
    THashSet<TString> SubjectTransitiveClosure;

    REGISTER_YSON_STRUCT(TListOperationsAccessFilter);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("subject", &TThis::Subject);
        registrar.Parameter("permissions", &TThis::Permissions);
    }
};

DECLARE_REFCOUNTED_TYPE(TListOperationsAccessFilter)
DEFINE_REFCOUNTED_TYPE(TListOperationsAccessFilter)

struct TListOperationsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<TInstant> FromTime;
    std::optional<TInstant> ToTime;
    std::optional<TInstant> CursorTime;
    EOperationSortDirection CursorDirection = EOperationSortDirection::Past;
    std::optional<TString> UserFilter;

    TListOperationsAccessFilterPtr AccessFilter;

    std::optional<NScheduler::EOperationState> StateFilter;
    std::optional<NScheduler::EOperationType> TypeFilter;
    std::optional<TString> SubstrFilter;
    std::optional<TString> PoolTree;
    std::optional<TString> Pool;
    std::optional<bool> WithFailedJobs;
    bool IncludeArchive = false;
    bool IncludeCounters = true;
    ui64 Limit = 100;

    std::optional<THashSet<TString>> Attributes;

    // TODO(ignat): Remove this mode when UI migrate to list_operations without enabled UI mode.
    // See st/YTFRONT-1360.
    bool EnableUIMode = false;

    TDuration ArchiveFetchingTimeout = TDuration::Seconds(3);

    TListOperationsOptions()
    {
        ReadFrom = EMasterChannelKind::Cache;
    }
};

struct TPollJobShellResponse
{
    NYson::TYsonString Result;
    // YT-14507: Logging context is required for SOC audit.
    NYson::TYsonString LoggingContext;
};

DEFINE_ENUM(EJobSortField,
    ((None)       (0))
    ((Type)       (1))
    ((State)      (2))
    ((StartTime)  (3))
    ((FinishTime) (4))
    ((Address)    (5))
    ((Duration)   (6))
    ((Progress)   (7))
    ((Id)         (8))
);

DEFINE_ENUM(EJobSortDirection,
    ((Ascending)  (0))
    ((Descending) (1))
);

DEFINE_ENUM(EDataSource,
    ((Archive) (0))
    ((Runtime) (1))
    ((Auto)    (2))
    // Should be used only in tests.
    ((Manual)  (3))
);

struct TListJobsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    NJobTrackerClient::TJobId JobCompetitionId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> State;
    std::optional<TString> Address;
    std::optional<bool> WithStderr;
    std::optional<bool> WithFailContext;
    std::optional<bool> WithSpec;
    std::optional<bool> WithCompetitors;
    std::optional<TString> TaskName;

    TDuration RunningJobsLookbehindPeriod = TDuration::Max();

    EJobSortField SortField = EJobSortField::None;
    EJobSortDirection SortOrder = EJobSortDirection::Ascending;

    i64 Limit = 1000;
    i64 Offset = 0;

    // All options below are deprecated.
    bool IncludeCypress = false;
    bool IncludeControllerAgent = false;
    bool IncludeArchive = false;
    EDataSource DataSource = EDataSource::Auto;
};

struct TAbandonJobOptions
    : public TTimeoutOptions
{ };

struct TPollJobShellOptions
    : public TTimeoutOptions
{ };

struct TAbortJobOptions
    : public TTimeoutOptions
{
    std::optional<TDuration> InterruptTimeout;
};

struct TGetOperationOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<THashSet<TString>> Attributes;
    TDuration ArchiveTimeout = TDuration::Seconds(5);
    TDuration MaximumCypressProgressAge = TDuration::Minutes(2);
    bool IncludeRuntime = false;
};

struct TGetJobOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<THashSet<TString>> Attributes;
};

struct TSelectRowsResult
{
    IUnversionedRowsetPtr Rowset;
    NQueryClient::TQueryStatistics Statistics;
};

struct TGetClusterMetaOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    bool PopulateNodeDirectory = false;
    bool PopulateClusterDirectory = false;
    bool PopulateMediumDirectory = false;
    bool PopulateCellDirectory = false;
    bool PopulateMasterCacheNodeAddresses = false;
    bool PopulateTimestampProviderAddresses = false;
    bool PopulateFeatures = false;
};

struct TClusterMeta
{
    std::shared_ptr<NNodeTrackerClient::NProto::TNodeDirectory> NodeDirectory;
    std::shared_ptr<NHiveClient::NProto::TClusterDirectory> ClusterDirectory;
    std::shared_ptr<NChunkClient::NProto::TMediumDirectory> MediumDirectory;
    std::vector<TString> MasterCacheNodeAddresses;
    std::vector<TString> TimestampProviderAddresses;
    NYTree::IMapNodePtr Features;
};

struct TOperation
{
    std::optional<NScheduler::TOperationId> Id;

    std::optional<NScheduler::EOperationType> Type;
    std::optional<NScheduler::EOperationState> State;

    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;

    std::optional<TString> AuthenticatedUser;

    NYson::TYsonString BriefSpec;
    NYson::TYsonString Spec;
    NYson::TYsonString ProvidedSpec;
    NYson::TYsonString ExperimentAssignments;
    NYson::TYsonString ExperimentAssignmentNames;
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;

    NYson::TYsonString BriefProgress;
    NYson::TYsonString Progress;

    NYson::TYsonString RuntimeParameters;

    std::optional<bool> Suspended;

    NYson::TYsonString Events;
    NYson::TYsonString Result;

    NYson::TYsonString SlotIndexPerPoolTree;
    NYson::TYsonString Alerts;
    NYson::TYsonString AlertEvents;

    NYson::TYsonString TaskNames;

    NYson::TYsonString ControllerFeatures;

    NYTree::IAttributeDictionaryPtr OtherAttributes;
};

void Serialize(
    const TOperation& operation,
    NYson::IYsonConsumer* consumer,
    bool needType = true,
    bool needOperationType = false,
    bool idWithAttributes = false);

void Deserialize(TOperation& operation, NYTree::IAttributeDictionaryPtr attriubutes, bool clone = true);

struct TListOperationsResult
{
    std::vector<TOperation> Operations;
    std::optional<THashMap<TString, i64>> PoolTreeCounts;
    std::optional<THashMap<TString, i64>> PoolCounts;
    std::optional<THashMap<TString, i64>> UserCounts;
    std::optional<TEnumIndexedVector<NScheduler::EOperationState, i64>> StateCounts;
    std::optional<TEnumIndexedVector<NScheduler::EOperationType, i64>> TypeCounts;
    std::optional<i64> FailedJobsCount;
    bool Incomplete = false;
};

struct TJob
{
    NJobTrackerClient::TJobId Id;
    NJobTrackerClient::TJobId OperationId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> ControllerState;
    std::optional<NJobTrackerClient::EJobState> ArchiveState;
    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;
    std::optional<TString> Address;
    std::optional<double> Progress;
    std::optional<ui64> StderrSize;
    std::optional<ui64> FailContextSize;
    std::optional<bool> HasSpec;
    std::optional<bool> HasCompetitors;
    std::optional<bool> HasProbingCompetitors;
    NJobTrackerClient::TJobId JobCompetitionId;
    NJobTrackerClient::TJobId ProbingJobCompetitionId;
    NYson::TYsonString Error;
    NYson::TYsonString BriefStatistics;
    NYson::TYsonString Statistics;
    NYson::TYsonString InputPaths;
    NYson::TYsonString CoreInfos;
    NYson::TYsonString Events;
    NYson::TYsonString ExecAttributes;
    std::optional<TString> TaskName;
    std::optional<TString> PoolTree;
    std::optional<TString> Pool;
    std::optional<TString> MonitoringDescriptor;
    std::optional<ui64> JobCookie;

    std::optional<bool> IsStale;

    std::optional<NJobTrackerClient::EJobState> GetState() const;
};

void Serialize(const TJob& job, NYson::IYsonConsumer* consumer, TStringBuf idKey);

struct TListJobsStatistics
{
    TEnumIndexedVector<NJobTrackerClient::EJobState, i64> StateCounts;
    TEnumIndexedVector<NJobTrackerClient::EJobType, i64> TypeCounts;
};

struct TListJobsResult
{
    std::vector<TJob> Jobs;
    std::optional<int> CypressJobCount;
    std::optional<int> ControllerAgentJobCount;
    std::optional<int> ArchiveJobCount;

    TListJobsStatistics Statistics;

    std::vector<TError> Errors;
};

struct TQuery
{
    NQueryTrackerClient::TQueryId Id;
    std::optional<NQueryTrackerClient::EQueryEngine> Engine;
    std::optional<TString> Query;
    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;
    NYson::TYsonString Settings;
    std::optional<TString> User;
    std::optional<NQueryTrackerClient::EQueryState> State;
    std::optional<i64> ResultCount;
    NYson::TYsonString Progress;
    std::optional<TError> Error;
    NYson::TYsonString Annotations;
    NYTree::IAttributeDictionaryPtr OtherAttributes;
};

void Serialize(const TQuery& query, NYson::IYsonConsumer* consumer);

struct TQueryResult
{
    NQueryTrackerClient::TQueryId Id;
    i64 ResultIndex;
    TError Error;
    NTableClient::TTableSchemaPtr Schema;
    NChunkClient::NProto::TDataStatistics DataStatistics;
};

void Serialize(const TQueryResult& queryResult, NYson::IYsonConsumer* consumer);

struct TListQueueConsumerRegistrationsResult
{
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;
    bool Vital;
    std::optional<std::vector<int>> Partitions;
};

struct TGetFileFromCacheResult
{
    NYPath::TYPath Path;
};

struct TPutFileToCacheResult
{
    NYPath::TYPath Path;
};

struct TCheckClusterLivenessOptions
    : public TTimeoutOptions
{
    //! Checks cypress root availability.
    bool CheckCypressRoot = false;
    //! Checks secondary master cells generic availability.
    bool CheckSecondaryMasterCells = false;
    //! Unless null checks tablet cell bundle health.
    std::optional<TString> CheckTabletCellBundle;

    bool IsCheckTrivial() const;

    // NB: For testing purposes.
    bool operator==(const TCheckClusterLivenessOptions& other) const;
};

struct TBuildSnapshotOptions
    : public TTimeoutOptions
{
    //! Refers either to masters or to tablet cells.
    //! If null then the primary one is assumed.
    NHydra::TCellId CellId;
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
};

struct TBuildMasterSnapshotsOptions
    : public TTimeoutOptions
{
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
    bool Retry = true;
};

struct TSwitchLeaderOptions
    : public TTimeoutOptions
{ };

struct TResetStateHashOptions
    : public TTimeoutOptions
{
    //! If not set, random number is used.
    std::optional<ui64> NewStateHash;
};

struct TGCCollectOptions
    : public TTimeoutOptions
{
    //! Refers to master cell.
    //! If null then the primary one is assumed.
    NHydra::TCellId CellId;
};

struct TKillProcessOptions
    : public TTimeoutOptions
{
    int ExitCode = 42;
};

struct TWriteCoreDumpOptions
    : public TTimeoutOptions
{ };

struct TWriteLogBarrierOptions
    : public TTimeoutOptions
{
    TString Category;
};

struct TWriteOperationControllerCoreDumpOptions
    : public TTimeoutOptions
{ };

struct THealExecNodeOptions
    : public TTimeoutOptions
{
    std::vector<TString> Locations;
    std::vector<TString> AlertTypesToReset;
    bool ForceReset = false;
};

struct TSuspendCoordinatorOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TResumeCoordinatorOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TMigrateReplicationCardsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    NObjectClient::TCellId DestinationCellId;
    std::vector<NChaosClient::TReplicationCardId> ReplicationCardIds;
};

struct TSuspendChaosCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TResumeChaosCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TSuspendTabletCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TResumeTabletCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TDisableChunkLocationsOptions
    : public TTimeoutOptions
{ };

struct TDisableChunkLocationsResult
{
    std::vector<TGuid> LocationUuids;
};

struct TDestroyChunkLocationsOptions
    : public TTimeoutOptions
{ };

struct TDestroyChunkLocationsResult
{
    std::vector<TGuid> LocationUuids;
};

struct TResurrectChunkLocationsOptions
    : public TTimeoutOptions
{ };

struct TResurrectChunkLocationsResult
{
    std::vector<TGuid> LocationUuids;
};

using TCellIdToSnapshotIdMap = THashMap<NHydra::TCellId, int>;

struct TTableBackupManifest
    : public NYTree::TYsonStruct
{
    NYTree::TYPath SourcePath;
    NYTree::TYPath DestinationPath;
    NTabletClient::EOrderedTableBackupMode OrderedMode;

    REGISTER_YSON_STRUCT(TTableBackupManifest);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("source_path", &TThis::SourcePath);
        registrar.Parameter("destination_path", &TThis::DestinationPath);
        registrar.Parameter("ordered_mode", &TThis::OrderedMode)
            .Default(NTabletClient::EOrderedTableBackupMode::Exact);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableBackupManifest)

struct TBackupManifest
    : public NYTree::TYsonStruct
{
    THashMap<TString, std::vector<TTableBackupManifestPtr>> Clusters;

    REGISTER_YSON_STRUCT(TBackupManifest);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("clusters", &TThis::Clusters);
    }
};

DEFINE_REFCOUNTED_TYPE(TBackupManifest)

struct TCreateTableBackupOptions
    : public TTimeoutOptions
{
    TDuration CheckpointTimestampDelay = TDuration::Zero();
    TDuration CheckpointCheckPeriod = TDuration::Zero();
    TDuration CheckpointCheckTimeout = TDuration::Zero();

    bool Force = false;
};

struct TRestoreTableBackupOptions
    : public TTimeoutOptions
{
    bool Force = false;
    bool Mount = false;
    bool EnableReplicas = false;
};

struct TQueryTrackerOptions
{
    TString QueryTrackerStage = "production";
};

struct TStartQueryOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    NYTree::INodePtr Settings;
    bool Draft = false;
    NYTree::IMapNodePtr Annotations;
};

struct TAbortQueryOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    std::optional<TString> AbortMessage;
};

struct TGetQueryResultOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{ };

struct TReadQueryResultOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    std::optional<std::vector<TString>> Columns;
    std::optional<i64> LowerRowIndex;
    std::optional<i64> UpperRowIndex;
};

struct TGetQueryOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    NYTree::TAttributeFilter Attributes;
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
};

struct TListQueriesOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    std::optional<TInstant> FromTime;
    std::optional<TInstant> ToTime;
    std::optional<TInstant> CursorTime;
    EOperationSortDirection CursorDirection = EOperationSortDirection::Past;
    std::optional<TString> UserFilter;

    std::optional<NQueryTrackerClient::EQueryState> StateFilter;
    std::optional<NQueryTrackerClient::EQueryEngine> EngineFilter;
    std::optional<TString> SubstrFilter;
    ui64 Limit = 100;

    NYTree::TAttributeFilter Attributes;
};

struct TListQueriesResult
{
    std::vector<TQuery> Queries;
    bool Incomplete = false;
    NTransactionClient::TTimestamp Timestamp;
};

struct TAlterQueryOptions
    : public TTimeoutOptions
    , public TQueryTrackerOptions
{
    NYTree::IMapNodePtr Annotations;
};

struct TSetUserPasswordOptions
    : public TTimeoutOptions
{ };

struct TIssueTokenOptions
    : public TTimeoutOptions
{ };

struct TIssueTokenResult
{
    TString Token;
};

struct TRevokeTokenOptions
    : public TTimeoutOptions
{ };

struct TListUserTokensOptions
    : public TTimeoutOptions
{ };

struct TListUserTokensResult
{
    // Tokens are SHA256-encoded.
    std::vector<TString> Tokens;
};

struct TAddMaintenanceOptions
    : public TTimeoutOptions
{ };

struct TMaintenanceFilter
{
    struct TByUser
    {
        struct TAll
        { };

        struct TMine
        { };
    };

    // Empty means no filtering by id.
    std::vector<TMaintenanceId> Ids;
    std::optional<EMaintenanceType> Type;
    std::variant<TByUser::TAll, TByUser::TMine, TString> User = {};
};

struct TRemoveMaintenanceOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

//! Provides a basic set of functions that can be invoked
//! both standalone and inside transaction.
/*
 *  This interface contains methods shared by IClient and ITransaction.
 *
 *  Thread affinity: single
 */
struct IClientBase
    : public virtual TRefCounted
{
    virtual IConnectionPtr GetConnection() = 0;

    // Transactions
    virtual TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) = 0;

    // Tables
    virtual TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) = 0;

    virtual TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options = {}) = 0;

    virtual TFuture<std::vector<IUnversionedRowsetPtr>> MultiLookup(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options = {}) = 0;

    virtual TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const TExplainQueryOptions& options = TExplainQueryOptions()) = 0;

    virtual TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const TPullRowsOptions& options = {}) = 0;

    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options = {}) = 0;

    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options = {}) = 0;

    // Cypress
    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = {}) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = {}) = 0;

    virtual TFuture<void> MultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options = {}) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = {}) = 0;

    virtual TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = {}) = 0;

    virtual TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = {}) = 0;

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options = {}) = 0;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = {}) = 0;

    virtual TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options = {}) = 0;

    virtual TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) = 0;


    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = {}) = 0;


    // Files
    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = {}) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options = {}) = 0;

    // Journals
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = {}) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientBase)

////////////////////////////////////////////////////////////////////////////////

//! A central entry point for all interactions with the YT cluster.
/*!
 *  In contrast to IConnection, each IClient represents an authenticated entity.
 *  The needed username is passed to #IConnection::CreateClient via options.
 *  Note that YT API has no built-in authentication mechanisms so it must be wrapped
 *  with appropriate logic.
 *
 *  Most methods accept |TransactionId| as a part of their options.
 *  A similar effect can be achieved by issuing requests via ITransaction.
 */
struct IClient
    : public virtual IClientBase
{
    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    virtual void Terminate() = 0;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() = 0;
    virtual const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() = 0;

    virtual std::optional<TStringBuf> GetClusterName(bool fetchIfNull = true) = 0;

    // Transactions
    virtual ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = {}) = 0;

    // Tables
    virtual TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = {}) = 0;

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = {}) = 0;

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = {}) = 0;

    virtual TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options = {}) = 0;

    virtual TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options = {}) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options = {}) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> ReshardTableAutomatic(
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options = {}) = 0;

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options = {}) = 0;

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options = {}) = 0;

    virtual TFuture<void> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetTablePivotKeys(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options = {}) = 0;

    virtual TFuture<void> CreateTableBackup(
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options = {}) = 0;

    virtual TFuture<void> RestoreTableBackup(
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options = {}) = 0;

    //! Same as above but returns the list of replicas that are in sync w.r.t. all of the table keys.
    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options = {}) = 0;

    virtual TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = {}) = 0;

    virtual TFuture<TGetTabletErrorsResult> GetTabletErrors(
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> BalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options = {}) = 0;

    virtual TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(
        const NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options = {}) = 0;

    virtual TFuture<void> UpdateChaosTableReplicaProgress(
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options = {}) = 0;

    virtual TFuture<void> AlterReplicationCard(
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options = {}) = 0;


    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options = {}) = 0;

    virtual TFuture<TMultiTablePartitions> PartitionTables(
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options) = 0;

    // Queues
    //! Reads a batch of rows from a given partition of a given queue, starting at (at least) the given offset.
    //! Requires the user to have read-access to the specified queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullQueue(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}) = 0;

    //! Same as PullQueue, but requires user to have read-access to the consumer and the consumer being registered for the given queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullConsumerOptions& options = {}) = 0;

    virtual TFuture<void> RegisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options = {}) = 0;

    virtual TFuture<void> UnregisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options = {}) = 0;

    virtual TFuture<std::vector<TListQueueConsumerRegistrationsResult>> ListQueueConsumerRegistrations(
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options = {}) = 0;

    // Journals
    virtual TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options = {}) = 0;

    // Files
    virtual TFuture<TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options = {}) = 0;

    // Security
    virtual TFuture<void> AddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options = {}) = 0;

    virtual TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options = {}) = 0;

    virtual TFuture<void> TransferAccountResources(
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options = {}) = 0;

    virtual TFuture<void> TransferPoolResources(
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options = {}) = 0;

    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = {}) = 0;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options = {}) = 0;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options = {}) = 0;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options = {}) = 0;

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options = {}) = 0;

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options = {}) = 0;

    virtual TFuture<TOperation> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options = {}) = 0;

    virtual TFuture<void> DumpJobContext(
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = {}) = 0;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJobInputPaths(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJobSpec(
        NJobTrackerClient::TJobId jobId,
        const TGetJobSpecOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobStderr(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobFailContext(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options = {}) = 0;

    virtual TFuture<TListOperationsResult> ListOperations(
        const TListOperationsOptions& options = {}) = 0;

    virtual TFuture<TListJobsResult> ListJobs(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJob(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options = {}) = 0;

    virtual TFuture<void> AbandonJob(
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options = {}) = 0;

    virtual TFuture<TPollJobShellResponse> PollJobShell(
        NJobTrackerClient::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options = {}) = 0;

    virtual TFuture<void> AbortJob(
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options = {}) = 0;

    // Metadata
    virtual TFuture<TClusterMeta> GetClusterMeta(
        const TGetClusterMetaOptions& options = {}) = 0;

    virtual TFuture<void> CheckClusterLiveness(
        const TCheckClusterLivenessOptions& options = {}) = 0;

    // Administration
    virtual TFuture<int> BuildSnapshot(
        const TBuildSnapshotOptions& options = {}) = 0;

    virtual TFuture<TCellIdToSnapshotIdMap> BuildMasterSnapshots(
        const TBuildMasterSnapshotsOptions& options = {}) = 0;

    virtual TFuture<void> SwitchLeader(
        NHydra::TCellId cellId,
        const TString& newLeaderAddress,
        const TSwitchLeaderOptions& options = {}) = 0;

    virtual TFuture<void> ResetStateHash(
        NHydra::TCellId cellId,
        const TResetStateHashOptions& options = {}) = 0;

    virtual TFuture<void> GCCollect(
        const TGCCollectOptions& options = {}) = 0;

    virtual TFuture<void> KillProcess(
        const TString& address,
        const TKillProcessOptions& options = {}) = 0;

    virtual TFuture<TString> WriteCoreDump(
        const TString& address,
        const TWriteCoreDumpOptions& options = {}) = 0;

    virtual TFuture<TGuid> WriteLogBarrier(
        const TString& address,
        const TWriteLogBarrierOptions& options) = 0;

    virtual TFuture<TString> WriteOperationControllerCoreDump(
        NJobTrackerClient::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options = {}) = 0;

    virtual TFuture<void> HealExecNode(
        const TString& address,
        const THealExecNodeOptions& options = {}) = 0;

    virtual TFuture<void> SuspendCoordinator(
        NObjectClient::TCellId coordinatorCellId,
        const TSuspendCoordinatorOptions& options = {}) = 0;

    virtual TFuture<void> ResumeCoordinator(
        NObjectClient::TCellId coordinatorCellId,
        const TResumeCoordinatorOptions& options = {}) = 0;

    virtual TFuture<void> MigrateReplicationCards(
        NObjectClient::TCellId chaosCellId,
        const TMigrateReplicationCardsOptions& options = {}) = 0;

    virtual TFuture<void> SuspendChaosCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendChaosCellsOptions& options = {}) = 0;

    virtual TFuture<void> ResumeChaosCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeChaosCellsOptions& options = {}) = 0;

    virtual TFuture<void> SuspendTabletCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendTabletCellsOptions& options = {}) = 0;

    virtual TFuture<void> ResumeTabletCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeTabletCellsOptions& options = {}) = 0;

    virtual TFuture<TMaintenanceId> AddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment,
        const TAddMaintenanceOptions& options = {}) = 0;

    virtual TFuture<TMaintenanceCounts> RemoveMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        const TMaintenanceFilter& filter,
        const TRemoveMaintenanceOptions& options = {}) = 0;

    virtual TFuture<TDisableChunkLocationsResult> DisableChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDisableChunkLocationsOptions& options = {}) = 0;

    virtual TFuture<TDestroyChunkLocationsResult> DestroyChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDestroyChunkLocationsOptions& options = {}) = 0;

    virtual TFuture<TResurrectChunkLocationsResult> ResurrectChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TResurrectChunkLocationsOptions& options = {}) = 0;

    // Query tracker

    virtual TFuture<NQueryTrackerClient::TQueryId> StartQuery(
        NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options = {}) = 0;

    virtual TFuture<void> AbortQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TAbortQueryOptions& options = {}) = 0;

    virtual TFuture<TQueryResult> GetQueryResult(
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex = 0,
        const TGetQueryResultOptions& options = {}) = 0;

    virtual TFuture<IUnversionedRowsetPtr> ReadQueryResult(
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex = 0,
        const TReadQueryResultOptions& options = {}) = 0;

    virtual TFuture<TQuery> GetQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TGetQueryOptions& options = {}) = 0;

    virtual TFuture<TListQueriesResult> ListQueries(const TListQueriesOptions& options = {}) = 0;

    virtual TFuture<void> AlterQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TAlterQueryOptions& options = {}) = 0;

    // Authentication

    // Methods below correspond to simple authentication scheme
    // and are intended to be used on clusters without third-party tokens (e.g. Yandex blackbox).
    virtual TFuture<void> SetUserPassword(
        const TString& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options) = 0;

    virtual TFuture<TIssueTokenResult> IssueToken(
        const TString& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options) = 0;

    virtual TFuture<void> RevokeToken(
        const TString& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options) = 0;

    virtual TFuture<TListUserTokensResult> ListUserTokens(
        const TString& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

class TClusterAwareClientBase
    : public virtual IClient
{
public:
    //! Returns and caches the cluster name corresponding to this client.
    //! If available, the name is taken from the client's connection configuration.
    //! Otherwise, if fetchIfNull is set to true, the first call to this method
    //! will fetch the cluster name from Cypress via master caches.
    //!
    //! NB: Descendants of this class should be able to perform GetNode calls,
    //! so this cannot be used directly in tablet transactions.
    //! Use the transaction's parent client instead.
    std::optional<TStringBuf> GetClusterName(bool fetchIfNull) override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::optional<TString> ClusterName_;

    std::optional<TString> FetchClusterNameFromMasterCache();
};

////////////////////////////////////////////////////////////////////////////////

//! A subset of #TTransactionStartOptions tailored for the case of alien
//! transactions.
struct TAlienTransactionStartOptions
{
    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;

    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;
};

//! A helper for starting an alien transaction at #alienClient.
/*!
 *  Internally, invokes #IClient::StartTransaction and #ITransaction::RegisterAlienTransaction
 *  but also takes care of the following issues:
 *  1) Alien and local transaction ids must be same;
 *  2) If #alienClient and #localTransaction have matching clusters then no
 *     new transaction must be created.
 */
TFuture<ITransactionPtr> StartAlienTransaction(
    const ITransactionPtr& localTransaction,
    const IClientPtr& alienClient,
    const TAlienTransactionStartOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
