#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NBridge {

struct TUpdateClusterStateSettings : public TOperationRequestSettings<TUpdateClusterStateSettings> {};

struct TGetClusterStateSettings : public TOperationRequestSettings<TGetClusterStateSettings> {};

enum class EPileState {
    UNSPECIFIED = Ydb::Bridge::PileState::UNSPECIFIED,
    DISCONNECTED = Ydb::Bridge::PileState::DISCONNECTED,
    NOT_SYNCHRONIZED = Ydb::Bridge::PileState::NOT_SYNCHRONIZED,
    SYNCHRONIZED = Ydb::Bridge::PileState::SYNCHRONIZED,
    PROMOTE = Ydb::Bridge::PileState::PROMOTE,
    PRIMARY = Ydb::Bridge::PileState::PRIMARY,
    SUSPENDED = Ydb::Bridge::PileState::SUSPENDED,
};

struct TPileStateUpdate {
    std::string PileName;
    EPileState State = EPileState::DISCONNECTED;
};

class TGetClusterStateResult : public TStatus {
public:
    TGetClusterStateResult(TStatus&& status, std::vector<TPileStateUpdate>&& state)
        : TStatus(std::move(status))
        , State_(std::move(state))
    {}

    const std::vector<TPileStateUpdate>& GetState() const {
        return State_;
    }

private:
    std::vector<TPileStateUpdate> State_;
};

using TAsyncGetClusterStateResult = NThreading::TFuture<TGetClusterStateResult>;

class TBridgeClient {
public:
    explicit TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TBridgeClient();

    TAsyncStatus UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
        const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings = {});

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NBridge
