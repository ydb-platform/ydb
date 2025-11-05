#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <memory>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NTestShard {

struct TCreateTestShardSettings : public TOperationRequestSettings<TCreateTestShardSettings> {};

struct TDeleteTestShardSettings : public TOperationRequestSettings<TDeleteTestShardSettings> {};

class TCreateTestShardResult : public TStatus {
public:
    TCreateTestShardResult(TStatus&& status, std::vector<uint64_t> tabletIds)
        : TStatus(std::move(status))
        , TabletIds_(std::move(tabletIds))
    {}

    const std::vector<uint64_t>& GetTabletIds() const {
        return TabletIds_;
    }

private:
    std::vector<uint64_t> TabletIds_;
};

using TAsyncCreateTestShardResult = NThreading::TFuture<TCreateTestShardResult>;

class TTestShardClient {
public:
    explicit TTestShardClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TTestShardClient();

    TAsyncCreateTestShardResult CreateTestShard(
        uint64_t ownerIdx,
        const std::vector<std::string>& channels,
        uint32_t count = 1,
        const std::string& config = {},
        const std::string& subdomain = {},
        uint64_t hiveId = 0,
        uint32_t domainUid = 0,
        const TCreateTestShardSettings& settings = {});

    TAsyncStatus DeleteTestShard(
        uint64_t ownerIdx,
        uint32_t count = 1,
        uint64_t hiveId = 0,
        const TDeleteTestShardSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev::NTestShard
