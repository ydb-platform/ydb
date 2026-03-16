#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <memory>
#include <string>
#include <vector>

namespace NYdb::NTestShardSet {

struct TCreateTestShardSetSettings : public TOperationRequestSettings<TCreateTestShardSetSettings> {};

struct TDeleteTestShardSetSettings : public TOperationRequestSettings<TDeleteTestShardSetSettings> {};

class TCreateTestShardSetResult : public TStatus {
public:
    TCreateTestShardSetResult(TStatus&& status, std::vector<uint64_t> tabletIds)
        : TStatus(std::move(status))
        , TabletIds_(std::move(tabletIds))
    {}

    const std::vector<uint64_t>& GetTabletIds() const {
        return TabletIds_;
    }

private:
    std::vector<uint64_t> TabletIds_;
};

using TAsyncCreateTestShardSetResult = NThreading::TFuture<TCreateTestShardSetResult>;

class TTestShardSetClient {
public:
    explicit TTestShardSetClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TTestShardSetClient();

    TAsyncCreateTestShardSetResult CreateTestShardSet(
        const std::string& path,
        const std::vector<std::string>& channels,
        uint32_t count = 1,
        const std::string& config = {},
        const TCreateTestShardSetSettings& settings = {});

    TAsyncStatus DeleteTestShardSet(
        const std::string& path,
        const TDeleteTestShardSetSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NTestShardSet
