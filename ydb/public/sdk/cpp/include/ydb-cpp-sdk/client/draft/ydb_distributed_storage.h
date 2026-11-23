#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace NYdb::inline Dev::NDistributedStorage {

struct TChangeDataCenterStateSettings : public TOperationRequestSettings<TChangeDataCenterStateSettings> {};

enum class EPoolRedundancy { OnePlusOne, TwoPlusTwo };
struct TSetPoolRedundancySettings : public TOperationRequestSettings<TSetPoolRedundancySettings> {};

class TDistributedStorageClient {
public:
    explicit TDistributedStorageClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    TAsyncStatus SetPoolRedundancy(const std::string& name, EPoolRedundancy target, std::optional<uint32_t> generation,
        const TSetPoolRedundancySettings& settings = {});

    // Success acknowledges the target policy. Storage groups may still be applying it.
    TAsyncStatus TakeDownDataCenter(const std::string& name, bool dryRun = false, const TChangeDataCenterStateSettings& settings = {});
    TAsyncStatus RejoinDataCenter(const std::string& name, const TChangeDataCenterStateSettings& settings = {});

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev::NDistributedStorage
