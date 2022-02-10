#include "pull_client.h"

namespace NSolomonStatExtension {

TSolomonStatPullExtension::TParams::TParams(const TString& host
    , ui16 port
    , const TString& project
    , const TString& service
    , const TString& cluster
    , const TVector<std::pair<TString, TString>>& labels)
    : Host_(host), Port_(port), Labels_()
{
    Labels_.Add("project", project);
    Labels_.Add("service", service);
    Labels_.Add("cluster", cluster);
    for (const auto& label: labels) {
         Labels_.Add(label.first, label.second);
    }
}

NMonitoring::TLabels TSolomonStatPullExtension::TParams::GetLabels() const {
    return Labels_;
}


TSolomonStatPullExtension::TSolomonStatPage::TSolomonStatPage(const TString& title, const TString& path, IApi* api)
    : NMonitoring::IMonPage(title, path), Api_(api)
    { }

void TSolomonStatPullExtension::TSolomonStatPage::Output(NMonitoring::IMonHttpRequest& request) {
    request.Output() << NMonitoring::HTTPOKJSON;
    auto json = NMonitoring::EncoderJson(&request.Output());
    Api_->Accept(json.Get());
}

TSolomonStatPullExtension::TSolomonStatPullExtension(const TSolomonStatPullExtension::TParams& params, IApi* api)
    : MetricRegistry_(new NMonitoring::TMetricRegistry(params.GetLabels()))
    , MonService_(params.Port_, params.Host_, 0), Page_( new TSolomonStatPage("stats", "Statistics", api) ) {
        api->SetMetricRegistry(MetricRegistry_.get());
        MonService_.Register(Page_);
        MonService_.StartOrThrow();
    }

TSolomonStatPullExtension::~TSolomonStatPullExtension()
    { }

} // NSolomonStatExtension
