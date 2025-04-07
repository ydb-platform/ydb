#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extensions/solomon_stats/pull_client.h>

namespace NSolomonStatExtension::inline Dev {

TSolomonStatPullExtension::TParams::TParams(const std::string& host
    , ui16 port
    , const std::string& project
    , const std::string& service
    , const std::string& cluster
    , const std::vector<std::pair<std::string, std::string>>& labels)
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


TSolomonStatPullExtension::TSolomonStatPage::TSolomonStatPage(const std::string& title, const std::string& path, IApi* api)
    : NMonitoring::IMonPage(TString(title), TString(path)), Api_(api)
    { }

void TSolomonStatPullExtension::TSolomonStatPage::Output(NMonitoring::IMonHttpRequest& request) {
    request.Output() << NMonitoring::HTTPOKJSON;
    auto json = NMonitoring::EncoderJson(&request.Output());
    Api_->Accept(json.Get());
}

TSolomonStatPullExtension::TSolomonStatPullExtension(const TSolomonStatPullExtension::TParams& params, IApi* api)
    : MetricRegistry_(new NMonitoring::TMetricRegistry(params.GetLabels()))
    , MonService_(params.Port_, TString(params.Host_), 0), Page_( new TSolomonStatPage("stats", "Statistics", api) ) {
        api->SetMetricRegistry(MetricRegistry_.get());
        MonService_.Register(Page_);
        MonService_.StartOrThrow();
    }

TSolomonStatPullExtension::~TSolomonStatPullExtension()
    { }

} // NSolomonStatExtension
