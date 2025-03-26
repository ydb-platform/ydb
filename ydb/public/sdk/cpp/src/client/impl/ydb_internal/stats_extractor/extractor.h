#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/internal_client/client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extension_common/extension.h>

#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NYdb::inline Dev {

class TStatsExtractor: public NSdkStats::IStatApi {
public:

    TStatsExtractor(std::shared_ptr<IInternalClient> client)
    : Client_(client)
    { }

    virtual void SetMetricRegistry(::NMonitoring::IMetricRegistry* sensorsRegistry) override {
        auto strong = Client_.lock();
        if (strong) {
            strong->StartStatCollecting(sensorsRegistry);
        } else {
            return;
        }
    }

    void Accept(NMonitoring::IMetricConsumer* consumer) const override {

        auto strong = Client_.lock();
        if (strong) {
            auto sensorsRegistry = strong->GetMetricRegistry();
            Y_ABORT_UNLESS(sensorsRegistry, "TMetricRegistry is null in Stats Extractor");
            sensorsRegistry->Accept(TInstant::Zero(), consumer);
        } else {
             throw NSdkStats::DestroyedClientException();
        }
    }
private:
    std::weak_ptr<IInternalClient> Client_;
};

} // namespace NYdb
