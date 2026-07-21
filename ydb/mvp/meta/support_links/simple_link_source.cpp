#include "simple_link_source.h"

#include "param_bindings.h"
#include "source_common.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {
namespace {

TResolvedParamBindings BuildDefaultSimpleLinkParamBindings() {
    return TResolvedParamBindings{
        .RequestMappings = {
            {"cluster", "cluster"},
            {"database", "database"},
            {"node", "node"},
            {"host", "host"},
        },
        .ClusterInfoMappings = {},
        .StaticMappings = {},
    };
}

std::pair<TString, TCgiParameters> BuildSimpleLinkUrlParts(TStringBuf url) {
    TString path = TString(TStringBuf(url).Before('?'));
    const TStringBuf queryString = TStringBuf(url).After('?');

    TCgiParameters queryParameters;
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }

    return {std::move(path), std::move(queryParameters)};
}

void InsertOrReplaceQueryParam(TCgiParameters& queryParameters, TStringBuf parameter, TStringBuf value) {
    queryParameters.EraseAll(parameter);
    queryParameters.InsertUnescaped(parameter, value);
}

void ApplyResolvedParams(TCgiParameters& queryParameters, const TVector<std::pair<TString, TString>>& paramValues) {
    for (const auto& [parameter, value] : paramValues) {
        InsertOrReplaceQueryParam(queryParameters, parameter, value);
    }
}

TString BuildSimpleLinkUrl(const TString& url, const ILinkSource::TLinkResolveInput& input, const TResolvedParamBindings& paramBindings) {
    const TCgiParameters requestParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    auto [path, queryParameters] = BuildSimpleLinkUrlParts(url);

    ApplyResolvedParams(queryParameters, BuildNonIdentityRequestParamValues(requestParameters));
    ApplyResolvedParams(queryParameters, BuildClusterInfoParamValues(input.ClusterInfo, paramBindings.ClusterInfoMappings));
    ApplyResolvedParams(queryParameters, BuildStaticParamValues(paramBindings.StaticMappings));
    ApplyResolvedParams(queryParameters, BuildRequestParamValues(requestParameters, paramBindings.RequestMappings));

    return queryParameters.empty()
        ? path
        : TStringBuilder() << path << '?' << queryParameters.Print();
}

class TSimpleLinkSource : public ILinkSource {
public:
    TSimpleLinkSource(TString title, TString url, TResolvedParamBindings paramBindings)
        : Title(std::move(title))
        , Url(std::move(url))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TResolveOutput result{
            .Name = TString(SOURCE_META),
        };
        result.Links.emplace_back(TResolvedLink{
            .Title = Title,
            .Url = BuildSimpleLinkUrl(Url, input, ParamBindings),
        });
        return result;
    }

private:
    TString Title;
    TString Url;
    TResolvedParamBindings ParamBindings;
};

} // namespace

void ValidateSimpleLinkSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings&) {
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required when source is omitted";
    }
    if (config.TagSize() != 0) {
        ythrow yexception() << "tag is not supported when source is omitted";
    }
    if (config.FolderSize() != 0) {
        ythrow yexception() << "folder is not supported when source is omitted";
    }
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultSimpleLinkParamBindings()), config);
}

std::shared_ptr<ILinkSource> MakeSimpleLinkSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateSimpleLinkSourceConfig(config, metaSettings);
    auto paramBindings = ResolveParamBindings(config, BuildDefaultSimpleLinkParamBindings());
    return std::make_shared<TSimpleLinkSource>(
        config.GetTitle(),
        config.GetUrl(),
        std::move(paramBindings));
}

} // namespace NMVP::NSupportLinks
