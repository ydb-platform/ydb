#pragma once

#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>

#include <library/cpp/monlib/metrics/metric_registry.h>

#include <util/generic/ptr.h>

namespace NSolomonStatExtension {

template <typename TMetricRegistryPtr>
class TMetricRegistryConnector: public NYdb::IExtension {
    static NMonitoring::IMetricRegistry* ToRawPtr(NMonitoring::IMetricRegistry* p) {
        return p;
    }

    static NMonitoring::IMetricRegistry* ToRawPtr(std::shared_ptr<NMonitoring::IMetricRegistry> p) {
        return p.get();
    }

    static NMonitoring::IMetricRegistry* ToRawPtr(TAtomicSharedPtr<NMonitoring::IMetricRegistry> p) {
        return p.Get();
    }

public:
    using IApi = NYdb::NSdkStats::IStatApi;

    class TParams : public TNonCopyable {
        friend class TMetricRegistryConnector;

    public:
        TParams(TMetricRegistryPtr registry)
            : Registry(registry)
        {}

    private:
        TMetricRegistryPtr Registry;
    };

    TMetricRegistryConnector(const TParams& params, IApi* api)
        : MetricRegistry_(params.Registry)
    {
        api->SetMetricRegistry(ToRawPtr(MetricRegistry_));
    }
private:
    TMetricRegistryPtr MetricRegistry_;
};

inline void AddMetricRegistry(NYdb::TDriver& driver, NMonitoring::IMetricRegistry* ptr) {
    if (!ptr)
        return;
    using TConnector = TMetricRegistryConnector<NMonitoring::IMetricRegistry*>;

    driver.AddExtension<TConnector>(TConnector::TParams(ptr));
}

inline void AddMetricRegistry(NYdb::TDriver& driver, std::shared_ptr<NMonitoring::IMetricRegistry> ptr) {
    if (!ptr)
        return;
    using TConnector = TMetricRegistryConnector<std::shared_ptr<NMonitoring::IMetricRegistry>>;

    driver.AddExtension<TConnector>(TConnector::TParams(ptr));
}

inline void AddMetricRegistry(NYdb::TDriver& driver, TAtomicSharedPtr<NMonitoring::IMetricRegistry> ptr) {
    if (!ptr)
        return;
    using TConnector = TMetricRegistryConnector<TAtomicSharedPtr<NMonitoring::IMetricRegistry>>;

    driver.AddExtension<TConnector>(TConnector::TParams(ptr));
}

} // namespace NSolomonStatExtension
