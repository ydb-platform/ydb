#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <utility>

#include <util/datetime/base.h>

namespace NYdb::inline Dev::NBridge {

struct TUpdateClusterStateSettings: public TOperationRequestSettings<TUpdateClusterStateSettings> {
    //! Compare-and-set guard: the cluster state generation (TGetClusterStateResult::GetGeneration()) the update is
    //! based on. The update is applied only if the cluster state still has this generation when the change
    //! executes. ABORTED means that the generation differs and the update was not applied; UNAVAILABLE means that
    //! the node has not yet seen this generation and the update was not applied. Other failures do not tell whether
    //! the update was applied: once a greater generation is observed, the update can no longer be applied.
    //! Nodes of older versions ignore it. If unset, the update is applied on top of the node's current state.
    FLUENT_SETTING_OPTIONAL(uint64_t, ExpectedGeneration);
};

struct TGetClusterStateSettings: public TOperationRequestSettings<TGetClusterStateSettings> {
    //! Request a static-node connectivity snapshot, independent of storage health.
    //! An older server may reject the optional query or omit the report.
    FLUENT_SETTING_DEFAULT(bool, IncludePileConnectivity, false);
};

struct TConvertToBridgeSettings: public TOperationRequestSettings<TConvertToBridgeSettings> {
    //! Leave existing storage pools and their groups non-bridged (static storage groups are still converted); they are
    //! not replicated to added piles until converted separately. Requires the bridge config to enable non-uniform
    //! storage.
    FLUENT_SETTING_DEFAULT(bool, SkipStoragePoolConversion, false);
};

struct TAddPileSettings: public TOperationRequestSettings<TAddPileSettings> {};

struct TRemovePileSettings: public TOperationRequestSettings<TRemovePileSettings> {};

struct TSetDatabaseBridgeModeSettings: public TOperationRequestSettings<TSetDatabaseBridgeModeSettings> {
    //! With ordinary mode, retain data copies in this pile. Empty selects copies automatically.
    FLUENT_SETTING_DEFAULT(std::string, RetainPileName, {});
};

enum class EPileState {
    UNSPECIFIED = 0,
    PRIMARY = 1,
    PROMOTED = 2,
    SYNCHRONIZED = 3,
    NOT_SYNCHRONIZED = 4,
    SUSPENDED = 5,
    DISCONNECTED = 6,
    INACTIVE = 7,
};

struct TPileStateUpdate {
    std::string PileName;
    EPileState State = EPileState::DISCONNECTED;
    // Not sent in UpdateClusterState requests; use TUpdateClusterStateSettings::ExpectedGeneration for the guard.
    uint64_t Generation = 0;
};

// Operation changing the set of piles of the cluster that is not finished yet.
struct TPendingPileOperation {
    enum class EKind {
        UNSPECIFIED = 0,
        ADD = 1,    // the pile is being added to the cluster: its storage groups are being created
        REMOVE = 2, // the pile is being removed from the cluster: its storage groups are being deleted
    };

    std::string PileName;
    EKind Kind = EKind::UNSPECIFIED;
    std::string ErrorReason; // why the last attempt has failed; the operation is retried meanwhile
};

enum class ESynchronizationStatus {
    Unspecified = 0,
    Synced = 1,
    Syncing = 2,
    Pending = 3,
    NotRequired = 4,
    Unknown = 5,
};

enum class EStorageMode {
    Unspecified = 0,
    Bridged = 1,
    Ordinary = 2,
    Mixed = 3,
};

struct TStoragePoolId {
    uint64_t BoxId = 0;
    uint64_t PoolId = 0;
};

struct TStoragePool {
    TStoragePoolId Id;
    std::string Name;
};

//! Only groups requiring bridge synchronization are counted.
//! Required = Synced + Syncing + Pending + Unknown.
struct TGroupSyncCounts {
    uint64_t Required = 0;
    uint64_t Synced = 0;
    uint64_t Syncing = 0;
    uint64_t Pending = 0;
    uint64_t Unknown = 0;
};

struct TSynchronizationError {
    std::string Message;
    std::optional<TInstant> OccurredAt;
};

struct TPileSynchronizationSummary {
    std::string PileName;
    TGroupSyncCounts UserGroups;
    TGroupSyncCounts SystemGroups;
};

struct TSynchronizationSummary {
    std::vector<TPileSynchronizationSummary> Piles;
    uint64_t ClusterStateGeneration = 0;
    TInstant ObservedAt;
};

//! Summary of the listed storage pools, attributed to their owner when its name is available.
//! Serverless databases select shared storage. MIXED counts cover bridged pools only.
struct TDatabaseStorageSummary {
    std::optional<std::string> DatabasePath;
    std::vector<TStoragePool> StoragePools;
    EStorageMode StorageMode = EStorageMode::Unspecified;
    std::optional<std::string> PileName;
    TGroupSyncCounts Groups;
    std::optional<TSynchronizationError> LastSyncError;
};

//! Synchronization of a storage group in a pile; not a separately managed object or task.
struct TGroupSynchronization {
    uint32_t GroupId = 0;
    //! Physical group with VDisks, for low-level diagnostics; it may not exist yet.
    std::optional<uint32_t> PhysicalGroupId;
    TStoragePool StoragePool;
    std::optional<std::string> PileName;
    ESynchronizationStatus SyncStatus = ESynchronizationStatus::Unspecified;
    //! Diagnostic protocol phase. Use SyncStatus for programmatic decisions.
    std::string SyncPhase;
    std::optional<TSynchronizationError> LastSyncError;
};

struct TGetSynchronizationSummarySettings: public TOperationRequestSettings<TGetSynchronizationSummarySettings> {};

struct TListDatabaseStorageSettings: public TOperationRequestSettings<TListDatabaseStorageSettings> {
    //! Serverless databases select their shared database's storage.
    FLUENT_SETTING_OPTIONAL(std::string, DatabasePath);
    //! Absence selects all placements; an explicitly empty name is a real pile.
    FLUENT_SETTING_OPTIONAL(std::string, PileName);
    FLUENT_SETTING_DEFAULT(bool, OnlyIncomplete, false);
};

//! Exactly one database or pool is selected. Pool IDs survive renames and disambiguate names.
class TGroupSynchronizationScope {
public:
    enum class EKind { Database,
                       StoragePoolName,
                       StoragePoolId };

    static TGroupSynchronizationScope Database(std::string path) {
        return {EKind::Database, std::move(path), {}};
    }

    static TGroupSynchronizationScope PoolName(std::string name) {
        return {EKind::StoragePoolName, std::move(name), {}};
    }

    static TGroupSynchronizationScope PoolId(TStoragePoolId id) {
        return {EKind::StoragePoolId, {}, id};
    }

    EKind GetKind() const {
        return Kind_;
    }
    //! Database path or pool name, according to GetKind(). Empty for a pool ID selector.
    const std::string& GetName() const {
        return Name_;
    }
    const TStoragePoolId& GetPoolId() const {
        return PoolId_;
    }

private:
    TGroupSynchronizationScope(EKind kind, std::string name, TStoragePoolId poolId)
        : Kind_(kind)
        , Name_(std::move(name))
        , PoolId_(poolId)
    {
    }

    EKind Kind_;
    std::string Name_;
    TStoragePoolId PoolId_;
};

struct TListGroupSynchronizationSettings: public TOperationRequestSettings<TListGroupSynchronizationSettings> {
    FLUENT_SETTING_OPTIONAL(std::string, PileName);
    //! Empty selects all statuses; otherwise their union. Unspecified is invalid.
    FLUENT_SETTING_DEFAULT(std::vector<ESynchronizationStatus>, Statuses, {});
};

class TGetSynchronizationSummaryResult: public TStatus {
public:
    TGetSynchronizationSummaryResult(TStatus&& status, TSynchronizationSummary summary = {})
        : TStatus(std::move(status))
        , Summary_(std::move(summary))
    {
    }

    const TSynchronizationSummary& GetSummary() const {
        return Summary_;
    }

private:
    TSynchronizationSummary Summary_;
};

// Connectivity of one distconf binding component; this is not disk health or a failover vote.
struct TPileConnectivity {
    std::string PileName;
    uint32_t StaticNodes = 0;
    uint32_t ConnectedStaticNodes = 0;
    bool HasNodeQuorum = false;
    bool InConfigQuorum = false;
    bool InDataQuorum = false;
};

struct TPileConnectivityReport {
    uint32_t ReporterNodeId = 0;
    bool ReporterIsRoot = false;
    bool BindingInProgress = false;
    // Independent from TGetClusterStateResult::GetGeneration(); reconcile by generation and pile name.
    uint64_t Generation = 0;
    std::vector<TPileConnectivity> Piles;
};

class TGetClusterStateResult: public TStatus {
public:
    TGetClusterStateResult(TStatus&& status, std::vector<TPileStateUpdate>&& state, uint64_t generation,
                           bool conversionInProgress = false, std::optional<TPendingPileOperation> pendingPileOperation = std::nullopt,
                           std::optional<TPileConnectivityReport> pileConnectivity = std::nullopt)
        : TStatus(std::move(status))
        , State_(std::move(state))
        , Generation_(generation)
        , ConversionInProgress_(conversionInProgress)
        , PendingPileOperation_(std::move(pendingPileOperation))
        , PileConnectivity_(std::move(pileConnectivity))
    {
    }

    const std::vector<TPileStateUpdate>& GetState() const {
        return State_;
    }

    uint64_t GetGeneration() const {
        return Generation_;
    }

    // True after the cluster has been converted from a non-bridged one until all its storage groups are converted
    // too; no piles can be added to the cluster meanwhile.
    bool IsConversionInProgress() const {
        return ConversionInProgress_;
    }

    const std::optional<TPendingPileOperation>& GetPendingPileOperation() const {
        return PendingPileOperation_;
    }

    // Absent when the server did not provide the optional report.
    const std::optional<TPileConnectivityReport>& GetPileConnectivity() const {
        return PileConnectivity_;
    }

private:
    std::vector<TPileStateUpdate> State_;
    uint64_t Generation_ = 0;
    bool ConversionInProgress_ = false;
    std::optional<TPendingPileOperation> PendingPileOperation_;
    std::optional<TPileConnectivityReport> PileConnectivity_;
};

using TAsyncGetClusterStateResult = NThreading::TFuture<TGetClusterStateResult>;

//! The complete selected list. Oversized requests fail; results are never truncated.
class TListDatabaseStorageResult: public TStatus {
public:
    TListDatabaseStorageResult(TStatus&& status, std::vector<TDatabaseStorageSummary> rows = {},
                               uint64_t clusterStateGeneration = 0, TInstant observedAt = {},
                               std::optional<std::string> storageOwnerDatabasePath = std::nullopt)
        : TStatus(std::move(status))
        , Databases_(std::move(rows))
        , ClusterStateGeneration_(clusterStateGeneration)
        , ObservedAt_(observedAt)
        , StorageOwnerDatabasePath_(std::move(storageOwnerDatabasePath))
    {
    }

    const std::vector<TDatabaseStorageSummary>& GetDatabases() const {
        return Databases_;
    }
    uint64_t GetClusterStateGeneration() const {
        return ClusterStateGeneration_;
    }
    TInstant GetObservedAt() const {
        return ObservedAt_;
    }
    const std::optional<std::string>& GetStorageOwnerDatabasePath() const {
        return StorageOwnerDatabasePath_;
    }

private:
    std::vector<TDatabaseStorageSummary> Databases_;
    uint64_t ClusterStateGeneration_ = 0;
    TInstant ObservedAt_;
    std::optional<std::string> StorageOwnerDatabasePath_;
};

//! The complete selected list. Oversized requests fail; results are never truncated.
class TListGroupSynchronizationResult: public TStatus {
public:
    TListGroupSynchronizationResult(TStatus&& status, std::vector<TGroupSynchronization> rows = {},
                                    uint64_t clusterStateGeneration = 0, TInstant observedAt = {},
                                    std::optional<std::string> storageOwnerDatabasePath = std::nullopt)
        : TStatus(std::move(status))
        , Groups_(std::move(rows))
        , ClusterStateGeneration_(clusterStateGeneration)
        , ObservedAt_(observedAt)
        , StorageOwnerDatabasePath_(std::move(storageOwnerDatabasePath))
    {
    }

    const std::vector<TGroupSynchronization>& GetGroups() const {
        return Groups_;
    }
    uint64_t GetClusterStateGeneration() const {
        return ClusterStateGeneration_;
    }
    TInstant GetObservedAt() const {
        return ObservedAt_;
    }
    const std::optional<std::string>& GetStorageOwnerDatabasePath() const {
        return StorageOwnerDatabasePath_;
    }

private:
    std::vector<TGroupSynchronization> Groups_;
    uint64_t ClusterStateGeneration_ = 0;
    TInstant ObservedAt_;
    std::optional<std::string> StorageOwnerDatabasePath_;
};

using TAsyncGetSynchronizationSummaryResult = NThreading::TFuture<TGetSynchronizationSummaryResult>;
using TAsyncListDatabaseStorageResult = NThreading::TFuture<TListDatabaseStorageResult>;
using TAsyncListGroupSynchronizationResult = NThreading::TFuture<TListGroupSynchronizationResult>;

class TBridgeClient {
public:
    explicit TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TBridgeClient();

    TAsyncStatus UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
                                    const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings = {});

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings = {});

    TAsyncGetSynchronizationSummaryResult GetSynchronizationSummary(const TGetSynchronizationSummarySettings& settings = {});

    TAsyncListDatabaseStorageResult ListDatabaseStorage(const TListDatabaseStorageSettings& settings = {});

    TAsyncListGroupSynchronizationResult ListGroupSynchronization(const TGroupSynchronizationScope& scope,
                                                                  const TListGroupSynchronizationSettings& settings = {});

    // Convert a running non-bridged cluster into a bridged one with a single pile. All nodes must already run with
    // a single-pile bridge config.
    TAsyncStatus ConvertToBridge(const TConvertToBridgeSettings& settings = {});

    // Add an INACTIVE pile, listed in the bridge config with its hosts, to the cluster. Completes when every storage
    // group has got its part in the pile; the pile is DISCONNECTED then and may be rejoined.
    TAsyncStatus AddPile(const std::string& pileName, const TAddPileSettings& settings = {});

    // Remove a DISCONNECTED pile from the cluster, deleting its parts of every storage group with all their data.
    // Completes when every storage group has lost its part in the pile; the pile is INACTIVE then and may be dropped
    // from the bridge config along with its hosts.
    TAsyncStatus RemovePile(const std::string& pileName, const TRemovePileSettings& settings = {});

    // Atomically change bridge mode of databasePath. For serverless databases, select their shared database.
    // Requires cluster administrator privileges and non-uniform storage; data synchronization and cleanup continue in the background.
    TAsyncStatus SetDatabaseBridgeMode(const std::string& databasePath, bool bridgeMode, const TSetDatabaseBridgeModeSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev::NBridge
