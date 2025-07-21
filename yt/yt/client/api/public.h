#pragma once

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/prerequisite_client/public.h>

#include <yt/yt/client/bundle_controller_client/public.h>

#include <yt/yt/library/auth/authentication_options.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>
#include <library/cpp/yt/compact_containers/compact_flat_map.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using TClusterTag = NObjectClient::TCellTag;

// Keep in sync with NRpcProxy::NProto::EMasterReadKind.
// On cache miss request is redirected to next level as follows:
// Client-side cache -> cache -> master-side cache
DEFINE_ENUM(EMasterChannelKind,
    // These options cover the majority of cases.
    ((Leader)                (0))
    ((Follower)              (1))
    ((Cache)                 (2)) // cluster-wide cache

    // These are advanced options. Typically you don't need these.
    ((MasterSideCache)       (3)) // cache located on masters
    ((ClientSideCache)       (4)) // local (per-connection) cache
);

DEFINE_ENUM(EUserWorkloadCategory,
    (Batch)
    (Interactive)
    (Realtime)
);

YT_DEFINE_ERROR_ENUM(
    ((TooManyConcurrentRequests)                         (1900))
    ((JobArchiveUnavailable)                             (1910))
    ((RetriableArchiveError)                             (1911))
    ((NoSuchOperation)                                   (1915))
    ((NoSuchJob)                                         (1916))
    ((UncertainOperationControllerState)                 (1917))
    ((NoSuchAttribute)                                   (1920))
    ((FormatDisabled)                                    (1925))
    ((ClusterLivenessCheckFailed)                        (1926))
    ((UnsupportedArchiveVersion)                         (1927))
);

DEFINE_ENUM(ERowModificationType,
    ((Write)            (0))
    ((Delete)           (1))
    ((VersionedWrite)   (2))
    ((WriteAndLock)     (3))
);

DEFINE_ENUM(EReplicaConsistency,
    ((None)             (0))
    ((Sync)             (1))
);

DEFINE_ENUM(ETransactionCoordinatorCommitMode,
    // Success is reported when phase 2 starts (all participants have prepared but not yet committed).
    ((Eager)  (0))
    // Success is reported when transaction is finished (all participants have committed).
    ((Lazy)   (1))
);

DEFINE_ENUM(ETransactionCoordinatorPrepareMode,
    // Coordinator is prepared just like every other participant.
    ((Early)            (0))
    // Coordinator is prepared after other participants prepared and
    // after commit timestamp generation. In this mode prepare and commit
    // for coordinator are executed in the same mutation, so no preparation
    // locks are required.
    ((Late)             (1))
);

DEFINE_ENUM(EProxyKind,
    ((Http) (1))
    ((Rpc)  (2))
    ((Grpc) (3))
);

DEFINE_ENUM(EOperationSortDirection,
    ((None)   (0))
    ((Past)   (1))
    ((Future) (2))
);

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct IRowset;
template <class TRow>
using IRowsetPtr = TIntrusivePtr<IRowset<TRow>>;

using IUnversionedRowset = IRowset<NTableClient::TUnversionedRow>;
using IVersionedRowset = IRowset<NTableClient::TVersionedRow>;
using ITypeErasedRowset = IRowset<NTableClient::TTypeErasedRow>;

DECLARE_REFCOUNTED_TYPE(IUnversionedRowset)
DECLARE_REFCOUNTED_TYPE(IVersionedRowset)
DECLARE_REFCOUNTED_TYPE(ITypeErasedRowset)
DECLARE_REFCOUNTED_STRUCT(IPersistentQueueRowset)
DECLARE_REFCOUNTED_STRUCT(TSkynetSharePartsLocations)

DECLARE_REFCOUNTED_STRUCT(IJournalWritesObserver)

struct TConnectionOptions;

struct TClientOptions;

struct TTransactionParticipantOptions;

using TTimeoutOptions = NRpc::TTimeoutOptions;
struct TTransactionalOptions;
struct TPrerequisiteOptions;
struct TMasterReadOptions;
struct TMutatingOptions;
struct TReshardTableOptions;
struct TSuppressableAccessTrackingOptions;
struct TTabletRangeOptions;

struct TGetFileFromCacheResult;
struct TPutFileToCacheResult;

struct TGetCurrentUserOptions;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IClientBase)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(IInternalClient)
DECLARE_REFCOUNTED_STRUCT(IDynamicTableTransaction)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_STRUCT(IPrerequisite)
DECLARE_REFCOUNTED_STRUCT(IStickyTransactionPool)

DECLARE_REFCOUNTED_STRUCT(IRowBatchReader)
DECLARE_REFCOUNTED_STRUCT(IRowBatchWriter)

DECLARE_REFCOUNTED_STRUCT(ITableReader)
DECLARE_REFCOUNTED_STRUCT(ITableWriter)

DECLARE_REFCOUNTED_CLASS(ITablePartitionReader)

DECLARE_REFCOUNTED_STRUCT(ITableFragmentWriter);

DECLARE_REFCOUNTED_STRUCT(IFileReader)
DECLARE_REFCOUNTED_STRUCT(IFileWriter)

DECLARE_REFCOUNTED_STRUCT(IJournalReader)
DECLARE_REFCOUNTED_STRUCT(IJournalWriter)

DECLARE_REFCOUNTED_CLASS(TPersistentQueuePoller)

DECLARE_REFCOUNTED_STRUCT(TTableMountCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TConnectionDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TPersistentQueuePollerConfig)

DECLARE_REFCOUNTED_STRUCT(TFileReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TFileWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalReaderConfig)

DECLARE_REFCOUNTED_STRUCT(TJournalChunkWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicJournalWriterConfig)

DECLARE_REFCOUNTED_STRUCT(TJournalChunkWriterOptions)

DECLARE_REFCOUNTED_STRUCT(TSerializableMasterReadOptions)

DECLARE_REFCOUNTED_STRUCT(TPrerequisiteRevisionConfig)

DECLARE_REFCOUNTED_STRUCT(TDetailedProfilingInfo)

DECLARE_REFCOUNTED_STRUCT(TQueryFile)

DECLARE_REFCOUNTED_STRUCT(TQuerySecret)

DECLARE_REFCOUNTED_STRUCT(TSchedulingOptions)

DECLARE_REFCOUNTED_CLASS(TJobInputReader)

DECLARE_REFCOUNTED_CLASS(TClientCache)

DECLARE_REFCOUNTED_STRUCT(TTableBackupManifest)
DECLARE_REFCOUNTED_STRUCT(TBackupManifest)

DECLARE_REFCOUNTED_STRUCT(TListOperationsAccessFilter)
DECLARE_REFCOUNTED_STRUCT(TListOperationsContext)

DECLARE_REFCOUNTED_STRUCT(TShuffleHandle)

DECLARE_REFCOUNTED_STRUCT(TGetCurrentUserResult);

////////////////////////////////////////////////////////////////////////////////

inline const NYPath::TYPath ClusterNamePath("//sys/@cluster_name");
inline const NYPath::TYPath HttpProxiesPath("//sys/http_proxies");
inline const NYPath::TYPath RpcProxiesPath("//sys/rpc_proxies");
inline const NYPath::TYPath GrpcProxiesPath("//sys/grpc_proxies");
inline const std::string AliveNodeName("alive");
inline const std::string BannedAttributeName("banned");
inline const std::string RoleAttributeName("role");
inline const std::string AddressesAttributeName("addresses");
inline const std::string BalancersAttributeName("balancers");
inline const std::string DefaultRpcProxyRole("default");
inline const std::string DefaultHttpProxyRole("data");
inline const std::string JournalPayloadKey("payload");
inline const std::string HunkPayloadKey("payload");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMaintenanceType,
    // 0 is reserved for None.
    ((Ban)                      (1))
    ((Decommission)             (2))
    ((DisableSchedulerJobs)     (3))
    ((DisableWriteSessions)     (4))
    ((DisableTabletCells)       (5))
    ((PendingRestart)           (6))
);

DEFINE_ENUM(EMaintenanceComponent,
    ((ClusterNode) (1))
    ((HttpProxy)   (2))
    ((RpcProxy)    (3))
    ((Host)        (4))
);

using TMaintenanceId = TGuid;
using TMaintenanceCounts = TEnumIndexedArray<EMaintenanceType, int>;

// Almost always there is single maintenance target. The exception is virtual
// "host" target which represents all nodes on a given host.
constexpr int TypicalMaintenanceTargetCount = 1;

using TMaintenanceIdPerTarget = TCompactFlatMap<std::string, TMaintenanceId, TypicalMaintenanceTargetCount>;
using TMaintenanceCountsPerTarget = TCompactFlatMap<std::string, TMaintenanceCounts, TypicalMaintenanceTargetCount>;

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSignedDistributedWriteSessionPtr;
using NTableClient::TSignedWriteFragmentCookiePtr;
using NTableClient::TSignedWriteFragmentResultPtr;
struct TWriteFragmentCookie;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
