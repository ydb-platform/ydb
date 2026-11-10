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

namespace NYdb::inline Dev::NBridge {

struct TUpdateClusterStateSettings : public TOperationRequestSettings<TUpdateClusterStateSettings> {
    //! Compare-and-set guard: the cluster state generation (TGetClusterStateResult::GetGeneration()) the update is
    //! based on. The update is applied only if the cluster state still has this generation when the change
    //! executes. ABORTED means that the generation differs and the update was not applied; UNAVAILABLE means that
    //! the node has not yet seen this generation and the update was not applied. Other failures do not tell whether
    //! the update was applied: once a greater generation is observed, the update can no longer be applied.
    //! Nodes of older versions ignore it. If unset, the update is applied on top of the node's current state.
    FLUENT_SETTING_OPTIONAL(uint64_t, ExpectedGeneration);
};

struct TGetClusterStateSettings : public TOperationRequestSettings<TGetClusterStateSettings> {
    //! Request a static-node connectivity snapshot, independent of storage health.
    //! An older server may reject the optional query or omit the report.
    FLUENT_SETTING_DEFAULT(bool, IncludePileConnectivity, false);
};

struct TConvertToBridgeSettings : public TOperationRequestSettings<TConvertToBridgeSettings> {
    //! Leave existing storage pools and their groups non-bridged (static storage groups are still converted); they are
    //! not replicated to added piles until converted separately. Requires the bridge config to enable non-uniform
    //! storage.
    FLUENT_SETTING_DEFAULT(bool, SkipStoragePoolConversion, false);
};

struct TAddPileSettings : public TOperationRequestSettings<TAddPileSettings> {};

struct TRemovePileSettings : public TOperationRequestSettings<TRemovePileSettings> {};

struct TGetRollbackStatusSettings : public TOperationRequestSettings<TGetRollbackStatusSettings> {};

struct TRollbackSettings : public TOperationRequestSettings<TRollbackSettings> {
    //! Don't ask the storage controller whether it can make its groups ordinary ones before static groups are made such;
    //! for the case when it can't start, as it relies on static groups.
    FLUENT_SETTING_DEFAULT(bool, SkipBscValidation, false);
};

struct TSetDatabaseBridgeModeSettings : public TOperationRequestSettings<TSetDatabaseBridgeModeSettings> {
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
        ADD = 1, // the pile is being added to the cluster: its storage groups are being created
        REMOVE = 2, // the pile is being removed from the cluster: its storage groups are being deleted
        ROLLBACK = 3, // the cluster is being rolled back to ordinary one: its storage groups are being made ordinary
    };

    std::string PileName;
    EKind Kind = EKind::UNSPECIFIED;
    std::string ErrorReason; // why the last attempt has failed; the operation is retried meanwhile
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

class TGetClusterStateResult : public TStatus {
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
    {}

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

// How far the rollback of a cluster prepared for bridge mode back to ordinary mode has gone.
class TGetRollbackStatusResult : public TStatus {
public:
    TGetRollbackStatusResult(TStatus&& status, bool bridged = false, std::vector<uint32_t>&& bridgeConfigNodeIds = {},
            std::vector<uint32_t>&& unreportedStaticNodeIds = {}, bool complete = false, bool rollbackInProgress = false,
            std::string errorReason = {})
        : TStatus(std::move(status))
        , Bridged_(bridged)
        , BridgeConfigNodeIds_(std::move(bridgeConfigNodeIds))
        , UnreportedStaticNodeIds_(std::move(unreportedStaticNodeIds))
        , Complete_(complete)
        , RollbackInProgress_(rollbackInProgress)
        , ErrorReason_(std::move(errorReason))
    {}

    // The rollback of the bridged cluster is in progress: storage groups are being made ordinary ones.
    bool IsRollbackInProgress() const {
        return RollbackInProgress_;
    }

    // Why the last attempt to make storage groups ordinary ones has failed; it is retried meanwhile.
    const std::string& GetErrorReason() const {
        return ErrorReason_;
    }

    // The cluster is a bridged one; see Rollback, the cluster state goes away when it finishes.
    bool IsBridged() const {
        return Bridged_;
    }

    // Connected nodes still running with a bridge config; they have to be restarted without one.
    const std::vector<uint32_t>& GetBridgeConfigNodeIds() const {
        return BridgeConfigNodeIds_;
    }

    // Static nodes that have not reported their config, as they are not running or not connected.
    const std::vector<uint32_t>& GetUnreportedStaticNodeIds() const {
        return UnreportedStaticNodeIds_;
    }

    // The cluster is not bridged, and every node has reported running without a bridge config.
    bool IsComplete() const {
        return Complete_;
    }

private:
    bool Bridged_ = false;
    std::vector<uint32_t> BridgeConfigNodeIds_;
    std::vector<uint32_t> UnreportedStaticNodeIds_;
    bool Complete_ = false;
    bool RollbackInProgress_ = false;
    std::string ErrorReason_;
};

using TAsyncGetRollbackStatusResult = NThreading::TFuture<TGetRollbackStatusResult>;

class TBridgeClient {
public:
    explicit TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TBridgeClient();

    TAsyncStatus UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
        const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings = {});

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings = {});

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

    // Report how far the rollback of a cluster prepared for bridge mode has gone: the bridge config is removed from
    // the cluster configuration and every node is restarted without it. Dynamic nodes not connected are not reported.
    TAsyncGetRollbackStatusResult GetRollbackStatus(const TGetRollbackStatusSettings& settings = {});

    // Roll a cluster converted from an ordinary one back to it, while it has the only pile, the original one, and every
    // storage group is inline in that pile. Completes when static storage groups are ordinary ones; the rest is reported
    // by GetRollbackStatus.
    TAsyncStatus Rollback(const TRollbackSettings& settings = {});

    // Atomically change bridge mode of databasePath. For serverless databases, select their shared database.
    // Requires cluster administrator privileges and non-uniform storage; data synchronization and cleanup continue in the background.
    TAsyncStatus SetDatabaseBridgeMode(const std::string& databasePath, bool bridgeMode, const TSetDatabaseBridgeModeSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NBridge
