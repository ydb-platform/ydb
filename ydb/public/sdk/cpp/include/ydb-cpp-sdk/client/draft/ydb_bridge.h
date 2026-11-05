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

    // Atomically change bridge mode of databasePath. For serverless databases, select their shared database.
    // Requires cluster administrator privileges and non-uniform storage; data synchronization and cleanup continue in the background.
    TAsyncStatus SetDatabaseBridgeMode(const std::string& databasePath, bool bridgeMode, const TSetDatabaseBridgeModeSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NBridge
