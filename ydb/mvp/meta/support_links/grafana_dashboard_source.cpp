#include "grafana_dashboard_source.h"

#include "grafana_dashboard_common.h"
#include "source_common.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {
namespace {

class TGrafanaDashboardSource : public ILinkSource {
public:
    TGrafanaDashboardSource(TString sourceName, TString title, TString url, TString grafanaEndpoint, TResolvedParamBindings paramBindings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TString resolvedUrl = BuildGrafanaDashboardUrl(GrafanaEndpoint, Url, input, ParamBindings);
        TResolveOutput result{
            .Name = SourceName,
        };
        if (!resolvedUrl.empty()) {
            result.Links.emplace_back(TResolvedLink{
                .Title = Title,
                .Url = std::move(resolvedUrl),
            });
        }
        return result;
    }

private:
    TString SourceName;
    TString Title;
    TString Url;
    TString GrafanaEndpoint;
    TResolvedParamBindings ParamBindings;
};

} // namespace

void ValidateGrafanaDashboardSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << config.GetSource();
    }
    if (!IsAbsoluteUrl(config.GetUrl()) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultGrafanaDashboardParamBindings()), config);
}

std::shared_ptr<ILinkSource> MakeGrafanaDashboardSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaDashboardSourceConfig(config, metaSettings);
    auto paramBindings = ResolveParamBindings(config, BuildDefaultGrafanaDashboardParamBindings());
    return std::make_shared<TGrafanaDashboardSource>(
        config.GetSource(), config.GetTitle(), config.GetUrl(), metaSettings.SupportLinks.GrafanaEndpoint, std::move(paramBindings));
}

} // namespace NMVP::NSupportLinks
