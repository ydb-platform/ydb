#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NKeyValue {

    struct TDropVolumeSettings: public TOperationRequestSettings<TDropVolumeSettings> {
        using TOperationRequestSettings<TDropVolumeSettings>::TOperationRequestSettings;
    };

    class TKeyValueClient {
        class TImpl;

    public:
        TKeyValueClient(const TDriver& driver, const TCommonClientSettings& settings = {});

        // Drops a Solomon (KeyValue) volume and all its partitions.
        TAsyncStatus DropVolume(const std::string& path, const TDropVolumeSettings& settings = {});

    private:
        std::shared_ptr<TImpl> Impl_;
    };

} // namespace NYdb::inline Dev::NKeyValue
