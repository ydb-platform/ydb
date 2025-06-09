#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>

#include <memory>
#include <string>

namespace NYdb::NBridge {

struct TSwitchClusterStateSettings : public TOperationRequestSettings<TSwitchClusterStateSettings> {};

struct TGetClusterStateSettings : public TOperationRequestSettings<TGetClusterStateSettings> {};

struct TClusterState {
    enum EPileState {
        DISCONNECTED = Ydb::Bridge::ClusterState::DISCONNECTED,
        NOT_SYNCHRONIZED = Ydb::Bridge::ClusterState::NOT_SYNCHRONIZED,
        SYNCHRONIZED = Ydb::Bridge::ClusterState::SYNCHRONIZED,
    };

    std::vector<EPileState> PerPileState;
    ui32 PrimaryPile = 0;
    ui32 PromotedPile = 0;
    ui64 Generation = 0;
};

class TGetClusterStateResult : public TStatus {
public:
    TGetClusterStateResult(TStatus&& status, TClusterState&& state)
        : TStatus(std::move(status))
        , State_(std::move(state))
    {}

    const TClusterState& GetState() const {
        return State_;
    }

private:
    TClusterState State_;
};

using TAsyncGetClusterStateResult = NThreading::TFuture<TGetClusterStateResult>;

class TBridgeClient {
public:
    explicit TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TBridgeClient();

    TAsyncStatus SwitchClusterState(const TClusterState& state, const TSwitchClusterStateSettings& settings = {});

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NBridge
