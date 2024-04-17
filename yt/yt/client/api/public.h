#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/bundle_controller_client/public.h>

#include <yt/yt/library/auth/authentication_options.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>
#include <library/cpp/yt/small_containers/compact_flat_map.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using TClusterTag = NObjectClient::TCellTag;

// Keep in sync with NRpcProxy::NProto::EMasterReadKind.
// On cache miss request is redirected to next level cache:
// Local cache -> (node) cache -> master cache
DEFINE_ENUM(EMasterChannelKind,
    ((Leader)                (0))
    ((Follower)              (1))
    // Use local (per-connection) cache.
    ((LocalCache)            (4))
    // Use cache located on nodes.
    ((Cache)                 (2))
    // Use cache located on masters (if caching on masters is enabled).
    ((MasterCache)           (3))
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

DEFINE_ENUM(EProxyType,
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

using TClientOptions = NAuth::TAuthenticationOptions;

struct TTransactionParticipantOptions;

struct TTimeoutOptions;
struct TTransactionalOptions;
struct TPrerequisiteOptions;
struct TMasterReadOptions;
struct TMutatingOptions;
struct TReshardTableOptions;
struct TSuppressableAccessTrackingOptions;
struct TTabletRangeOptions;

struct TGetFileFromCacheResult;
struct TPutFileToCacheResult;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IClientBase)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(IInternalClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_STRUCT(IStickyTransactionPool)

DECLARE_REFCOUNTED_STRUCT(ITableReader)
DECLARE_REFCOUNTED_STRUCT(ITableWriter)

DECLARE_REFCOUNTED_STRUCT(IFileReader)
DECLARE_REFCOUNTED_STRUCT(IFileWriter)

DECLARE_REFCOUNTED_STRUCT(IJournalReader)
DECLARE_REFCOUNTED_STRUCT(IJournalWriter)

DECLARE_REFCOUNTED_CLASS(TPersistentQueuePoller)

DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TPersistentQueuePollerConfig)

DECLARE_REFCOUNTED_CLASS(TFileReaderConfig)
DECLARE_REFCOUNTED_CLASS(TFileWriterConfig)
DECLARE_REFCOUNTED_CLASS(TJournalReaderConfig)

DECLARE_REFCOUNTED_CLASS(TJournalChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TJournalWriterConfig)

DECLARE_REFCOUNTED_CLASS(TJournalChunkWriterOptions)

DECLARE_REFCOUNTED_STRUCT(TSerializableMasterReadOptions)

DECLARE_REFCOUNTED_STRUCT(TPrerequisiteRevisionConfig)

DECLARE_REFCOUNTED_STRUCT(TDetailedProfilingInfo)

DECLARE_REFCOUNTED_STRUCT(TQueryFile)

DECLARE_REFCOUNTED_STRUCT(TSchedulingOptions)

DECLARE_REFCOUNTED_CLASS(TJobInputReader)

DECLARE_REFCOUNTED_CLASS(TClientCache)

DECLARE_REFCOUNTED_STRUCT(TTableBackupManifest)
DECLARE_REFCOUNTED_STRUCT(TBackupManifest)

DECLARE_REFCOUNTED_STRUCT(TListOperationsAccessFilter)

////////////////////////////////////////////////////////////////////////////////

inline const TString ClusterNamePath("//sys/@cluster_name");
inline const TString HttpProxiesPath("//sys/http_proxies");
inline const TString RpcProxiesPath("//sys/rpc_proxies");
inline const TString GrpcProxiesPath("//sys/grpc_proxies");
inline const TString AliveNodeName("alive");
inline const TString BannedAttributeName("banned");
inline const TString RoleAttributeName("role");
inline const TString AddressesAttributeName("addresses");
inline const TString BalancersAttributeName("balancers");
inline const TString DefaultRpcProxyRole("default");
inline const TString DefaultHttpProxyRole("data");
inline const TString JournalPayloadKey("payload");
inline const TString HunkPayloadKey("payload");

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

using TMaintenanceIdPerTarget = TCompactFlatMap<TString, TMaintenanceId, TypicalMaintenanceTargetCount>;
using TMaintenanceCountsPerTarget = TCompactFlatMap<TString, TMaintenanceCounts, TypicalMaintenanceTargetCount>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

