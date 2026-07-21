#include "url_source.h"

#include "param_bindings.h"
#include "source_common.h"
#include "url_template.h"

#include <fmt/args.h>
#include <fmt/format.h>

#include <library/cpp/uri/encode.h>
#include <library/cpp/string_utils/url/url.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

namespace NMVP::NSupportLinks {
namespace {

using TResolvedParamPool = THashMap<TString, TString>;
using TTemplateParamSet = THashSet<TString>;

struct TUrlSourceUrlParts {
    TString Path;
    TCgiParameters QueryParameters;
    TString Fragment;
};

TResolvedParamBindings BuildDefaultUrlSourceParamBindings() {
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

TUrlSourceUrlParts BuildUrlSourceUrlParts(TStringBuf url) {
    TStringBuf sanitizedUrl;
    TStringBuf queryString;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(url, sanitizedUrl, queryString, fragment);
    TCgiParameters queryParameters;
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }

    return {
        .Path = TString(sanitizedUrl),
        .QueryParameters = std::move(queryParameters),
        .Fragment = TString(fragment),
    };
}

void InsertOrReplaceQueryParam(TCgiParameters& queryParameters, TStringBuf parameter, TStringBuf value) {
    queryParameters.EraseAll(parameter);
    queryParameters.InsertUnescaped(parameter, value);
}

void ApplyResolvedParams(
    TCgiParameters& queryParameters,
    const TVector<std::pair<TString, TString>>& paramValues,
    const TTemplateParamSet& templatedParameters)
{
    for (const auto& [parameter, value] : paramValues) {
        if (templatedParameters.contains(parameter)) {
            continue;
        }
        InsertOrReplaceQueryParam(queryParameters, parameter, value);
    }
}

void PutResolvedParam(TResolvedParamPool& resolvedParams, TStringBuf name, TStringBuf value) {
    if (!name.empty() && !value.empty()) {
        resolvedParams[TString(name)] = TString(value);
    }
}

void PopulateResolvedParamsFromRequest(const TCgiParameters& requestParameters, TResolvedParamPool& resolvedParams) {
    for (const auto& [name, value] : requestParameters) {
        PutResolvedParam(resolvedParams, name, value);
    }
}

void PopulateResolvedParamsFromClusterInfo(const THashMap<TString, TString>& clusterInfo, TResolvedParamPool& resolvedParams) {
    for (const auto& [name, value] : clusterInfo) {
        PutResolvedParam(resolvedParams, name, value);
    }
}

void RemoveMappedParameters(const TResolvedParamBindings& paramBindings, TResolvedParamPool& resolvedParams) {
    for (const auto& [_, parameter] : paramBindings.RequestMappings) {
        resolvedParams.erase(parameter);
    }
    for (const auto& [_, parameter] : paramBindings.ClusterInfoMappings) {
        resolvedParams.erase(parameter);
    }
    for (const auto& [_, parameter] : paramBindings.StaticMappings) {
        resolvedParams.erase(parameter);
    }
}

void PopulateResolvedParamsFromMappings(
    const TCgiParameters& requestParameters,
    const THashMap<TString, TString>& clusterInfo,
    const TResolvedParamBindings& paramBindings,
    TResolvedParamPool& resolvedParams)
{
    for (const auto& [requestName, parameter] : paramBindings.RequestMappings) {
        const TString value = requestParameters.Get(requestName);
        PutResolvedParam(resolvedParams, parameter, value);
    }
    for (const auto& [clusterInfoField, parameter] : paramBindings.ClusterInfoMappings) {
        if (const auto it = clusterInfo.find(clusterInfoField); it != clusterInfo.end()) {
            PutResolvedParam(resolvedParams, parameter, it->second);
        }
    }
    for (const auto& [staticValue, parameter] : paramBindings.StaticMappings) {
        resolvedParams[parameter] = staticValue;
    }
}

TResolvedParamPool BuildResolvedTemplateParams(
    const TCgiParameters& requestParameters,
    const THashMap<TString, TString>& clusterInfo,
    const TResolvedParamBindings& paramBindings)
{
    TResolvedParamPool resolvedParams;
    PopulateResolvedParamsFromClusterInfo(clusterInfo, resolvedParams);
    PopulateResolvedParamsFromRequest(requestParameters, resolvedParams);
    RemoveMappedParameters(paramBindings, resolvedParams);
    PopulateResolvedParamsFromMappings(requestParameters, clusterInfo, paramBindings, resolvedParams);
    return resolvedParams;
}

TString RenderTemplatedUrl(TStringBuf urlTemplate, const TResolvedParamPool& resolvedParams) {
    TResolvedParamPool escapedParams;
    escapedParams.reserve(resolvedParams.size());
    for (const auto& [name, value] : resolvedParams) {
        TString escapedValue;
        TStringOutput out(escapedValue);
        NUri::NEncode::TEncoder::Encode(out, value);
        escapedParams.emplace(name, std::move(escapedValue));
    }

    fmt::dynamic_format_arg_store<fmt::format_context> store;
    for (const auto& [name, value] : escapedParams) {
        store.push_back(fmt::arg(name.c_str(), value));
    }

    try {
        return TString(fmt::vformat(std::string_view(urlTemplate.data(), urlTemplate.size()), store));
    } catch (const fmt::format_error& e) {
        ythrow yexception() << "failed to render URL template '" << urlTemplate << "': " << e.what();
    }
}

TString BuildUrlSourceUrl(const TString& url, const ILinkSource::TLinkResolveInput& input, const TResolvedParamBindings& paramBindings) {
    const TCgiParameters requestParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    const TVector<TString> templateParameters = ExtractUrlTemplateParameters(url);
    TTemplateParamSet templatedParameters;
    for (const auto& parameter : templateParameters) {
        templatedParameters.insert(parameter);
    }
    const TResolvedParamPool resolvedTemplateParams = BuildResolvedTemplateParams(requestParameters, input.ClusterInfo, paramBindings);
    const TString renderedUrl = RenderTemplatedUrl(url, resolvedTemplateParams);
    auto [path, queryParameters, fragment] = BuildUrlSourceUrlParts(renderedUrl);

    ApplyResolvedParams(queryParameters, BuildNonIdentityRequestParamValues(requestParameters), templatedParameters);
    ApplyResolvedParams(queryParameters, BuildClusterInfoParamValues(input.ClusterInfo, paramBindings.ClusterInfoMappings), templatedParameters);
    ApplyResolvedParams(queryParameters, BuildStaticParamValues(paramBindings.StaticMappings), templatedParameters);
    ApplyResolvedParams(queryParameters, BuildRequestParamValues(requestParameters, paramBindings.RequestMappings), templatedParameters);

    TStringBuilder builder;
    builder << path;
    if (!queryParameters.empty()) {
        builder << '?' << queryParameters.Print();
    }
    if (!fragment.empty()) {
        builder << '#' << fragment;
    }
    return builder;
}

class TUrlSource : public ILinkSource {
public:
    TUrlSource(TString sourceName, TString title, TString url, TResolvedParamBindings paramBindings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TResolveOutput result{
            .Name = SourceName,
        };
        try {
            result.Links.emplace_back(TResolvedLink{
                .Title = Title,
                .Url = BuildUrlSourceUrl(Url, input, ParamBindings),
            });
        } catch (const std::exception& e) {
            result.Errors.emplace_back(TSupportError{
                .Source = SourceName,
                .Message = e.what(),
            });
        }
        return result;
    }

private:
    TString SourceName;
    TString Title;
    TString Url;
    TResolvedParamBindings ParamBindings;
};

} // namespace

void ValidateUrlSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings&) {
    if (config.GetUrl().empty()) {
        if (!config.HasSource()) {
            ythrow yexception() << "url is required when source is omitted";
        }
        ythrow yexception() << "url is required for source=" << config.GetSource();
    }
    if (config.TagSize() != 0) {
        if (!config.HasSource()) {
            ythrow yexception() << "tag is not supported when source is omitted";
        }
        ythrow yexception() << "tag is not supported for source=" << config.GetSource();
    }
    if (config.FolderSize() != 0) {
        if (!config.HasSource()) {
            ythrow yexception() << "folder is not supported when source is omitted";
        }
        ythrow yexception() << "folder is not supported for source=" << config.GetSource();
    }
    ValidateUrlTemplateSyntax(config.GetUrl());
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultUrlSourceParamBindings()), config);
}

std::shared_ptr<ILinkSource> MakeUrlSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateUrlSourceConfig(config, metaSettings);
    auto paramBindings = ResolveParamBindings(config, BuildDefaultUrlSourceParamBindings());
    return std::make_shared<TUrlSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        std::move(paramBindings));
}

} // namespace NMVP::NSupportLinks
