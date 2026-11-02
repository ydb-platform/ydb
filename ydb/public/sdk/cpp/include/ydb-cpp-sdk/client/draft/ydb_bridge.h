#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <memory>
#include <optional>
#include <string>

namespace NYdb::inline Dev::NBridge {

struct TUpdateClusterStateSettings : public TOperationRequestSettings<TUpdateClusterStateSettings> {};

struct TGetClusterStateSettings : public TOperationRequestSettings<TGetClusterStateSettings> {};

struct TConvertToBridgeSettings : public TOperationRequestSettings<TConvertToBridgeSettings> {
    //! Leave existing storage pools and their groups non-bridged (static storage groups are still converted); they are
    //! not replicated to added piles until converted separately. Requires the bridge config to enable non-uniform
    //! storage.
    FLUENT_SETTING_DEFAULT(bool, SkipStoragePoolConversion, false);
};

struct TAddPileSettings : public TOperationRequestSettings<TAddPileSettings> {};

struct TRemovePileSettings : public TOperationRequestSettings<TRemovePileSettings> {};

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

class TGetClusterStateResult : public TStatus {
public:
    TGetClusterStateResult(TStatus&& status, std::vector<TPileStateUpdate>&& state, uint64_t generation,
            bool conversionInProgress = false, std::optional<TPendingPileOperation> pendingPileOperation = std::nullopt)
        : TStatus(std::move(status))
        , State_(std::move(state))
        , Generation_(generation)
        , ConversionInProgress_(conversionInProgress)
        , PendingPileOperation_(std::move(pendingPileOperation))
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

private:
    std::vector<TPileStateUpdate> State_;
    uint64_t Generation_ = 0;
    bool ConversionInProgress_ = false;
    std::optional<TPendingPileOperation> PendingPileOperation_;
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

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NBridge
