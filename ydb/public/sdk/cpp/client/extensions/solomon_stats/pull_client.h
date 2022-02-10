#pragma once

#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>

#include <library/cpp/http/server/response.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/monservice.h>

#include <util/generic/vector.h>

namespace NSolomonStatExtension {

class TSolomonStatPullExtension: public NYdb::IExtension {
public:
    using IApi = NYdb::NSdkStats::IStatApi;

    class TParams {
        friend class TSolomonStatPullExtension;

    public:
        TParams(const TString& host
                , ui16 port
                , const TString& project
                , const TString& service
                , const TString& cluster
                , const TVector<std::pair<TString, TString>>& labels = {});

        NMonitoring::TLabels GetLabels() const;

    private:
        const TString Host_;
        ui16 Port_;
        NMonitoring::TLabels Labels_;
    };

    TSolomonStatPullExtension(const TParams& params, IApi* api);
    ~TSolomonStatPullExtension();

private:
    class TSolomonStatPage: public NMonitoring::IMonPage {
        friend class TSolomonStatPullExtension;
    public:
        TSolomonStatPage(const TString& title, const TString& path, IApi* api);

        void Output(NMonitoring::IMonHttpRequest& request) override ;

    private:
        IApi* Api_;
    };

private:
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry_;
    NMonitoring::TMonService2 MonService_;
    TIntrusivePtr<TSolomonStatPage> Page_;
};

} // namespace NSolomonStatExtension
