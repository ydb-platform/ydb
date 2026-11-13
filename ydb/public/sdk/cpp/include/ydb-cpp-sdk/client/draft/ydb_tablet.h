#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace NYdb::inline Dev::NTablet {

    struct TReassignTabletsSettings: public TSimpleRequestSettings<TReassignTabletsSettings> {
        // At least a storage pool or a nonzero group is required.
        FLUENT_SETTING(std::string, StoragePool);
        FLUENT_SETTING_DEFAULT(std::uint32_t, Group, 0);
        // Defaults to 1 on the server; must be positive.
        FLUENT_SETTING_OPTIONAL(std::uint32_t, MaxInFlight);
        // Defaults to 100 on the server; valid values are 0 through 100.
        FLUENT_SETTING_OPTIONAL(std::uint32_t, Percent);
        // Mark channels for reassignment on a subsequent restart.
        FLUENT_SETTING_DEFAULT(bool, Async, false);
    };

    class TReassignTabletsOperation: public TOperation {
    public:
        struct TMetadata {
            std::uint64_t SelectedTablets = 0;
            std::uint64_t CompletedTablets = 0;
        };

        using TOperation::TOperation;
        TReassignTabletsOperation(TStatus&& status, Ydb::Operations::Operation&& operation);
        const TMetadata& Metadata() const;

    private:
        TMetadata Metadata_;
    };

    using TAsyncReassignTabletsOperation = NThreading::TFuture<TReassignTabletsOperation>;

    class TTabletClient {
        class TImpl;

    public:
        TTabletClient(const TDriver& driver, const TCommonClientSettings& settings = {});

        // Requires cluster administrator access. The connection database selects the Hive.
        // Returns when accepted; use TOperationClient::Get/Cancel to monitor/stop scheduling.
        // Dispatched work is not rolled back. State is lost on Hive restart; completed
        // operations are retained for up to 10 minutes, limited to the most recent 1024 jobs.
        TAsyncReassignTabletsOperation ReassignTablets(const TReassignTabletsSettings& settings = {});

    private:
        std::shared_ptr<TImpl> Impl_;
    };

} // namespace NYdb::inline Dev::NTablet
