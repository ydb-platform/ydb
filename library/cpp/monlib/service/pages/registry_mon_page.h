#pragma once

#include "pre_mon_page.h"

#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NMonitoring {
    // For now this class can only enumerate all metrics without any grouping or serve JSON/Spack/Prometheus
    class TMetricRegistryPage: public TPreMonPage {
    public:
        TMetricRegistryPage(const TString& path, const TString& title, TAtomicSharedPtr<IMetricSupplier> registry)
            : TPreMonPage(path, title)
            , Registry_(registry)
            , RegistryRawPtr_(Registry_.Get())
        {
        }

        TMetricRegistryPage(const TString& path, const TString& title, IMetricSupplier* registry)
            : TPreMonPage(path, title)
            , RegistryRawPtr_(registry)
        {
        }

        void Output(NMonitoring::IMonHttpRequest& request) override;
        void OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) override;

    private:
        TAtomicSharedPtr<IMetricSupplier> Registry_;
        IMetricSupplier* RegistryRawPtr_;
    };

}
