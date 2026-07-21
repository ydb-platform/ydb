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
using TQueryParamSet = THashSet<TString>;

TResolvedParamBindings BuildDefaultUrlSourceQueryParamBindings(TStringBuf url) {
    if (HasUrlTemplateExpressions(url)) {
        return TResolvedParamBindings{
            .RequestMappings = {},
            .ClusterInfoMappings = {},
            .StaticMappings = {},
        };
    }

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

void ApplyQueryParams(
    TCgiParameters& queryParameters,
    const TVector<std::pair<TString, TString>>& paramValues,
    TStringBuf urlTemplate,
    const TQueryParamSet& renderedQueryParameters,
    bool replaceExistingQueryParameters)
{
    for (const auto& [parameter, value] : paramValues) {
        if (HasUrlTemplateParameter(urlTemplate, parameter)
            || renderedQueryParameters.contains(parameter)
            || (!replaceExistingQueryParameters && queryParameters.Has(parameter)))
        {
            continue;
        }
        if (replaceExistingQueryParameters) {
            queryParameters.ReplaceUnescaped(parameter, value);
        } else {
            queryParameters.InsertUnescaped(parameter, value);
        }
    }
}

void ValidateTemplateMappingsAreUsedInUrl(
    const TResolvedParamBindings& paramBindings,
    TStringBuf url,
    const TSupportLinkEntryConfig& config)
{
    const auto validateMappings = [&](const TVector<std::pair<TString, TString>>& paramMappings) {
        for (const auto& mapping : paramMappings) {
            const TString& parameter = mapping.second;
            if (!HasUrlTemplateParameter(url, parameter)) {
                ythrow yexception()
                    << "template_parameter_mappings.parameter=" << parameter
                    << " is not used in url for source=" << config.GetSource();
            }
        }
    };

    validateMappings(paramBindings.RequestMappings);
    validateMappings(paramBindings.ClusterInfoMappings);
    validateMappings(paramBindings.StaticMappings);
}

void PutResolvedParam(TResolvedParamPool& resolvedParams, TStringBuf name, TStringBuf value) {
    if (!name.empty() && !value.empty()) {
        resolvedParams[TString(name)] = TString(value);
    }
}

void RemoveMappedParameters(const TResolvedParamBindings& paramBindings, TResolvedParamPool& resolvedParams) {
    for (const auto& mapping : paramBindings.RequestMappings) {
        resolvedParams.erase(mapping.second);
    }
    for (const auto& mapping : paramBindings.ClusterInfoMappings) {
        resolvedParams.erase(mapping.second);
    }
    for (const auto& mapping : paramBindings.StaticMappings) {
        resolvedParams.erase(mapping.second);
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
    for (const auto& [name, value] : clusterInfo) {
        PutResolvedParam(resolvedParams, name, value);
    }
    for (const auto& [name, value] : requestParameters) {
        PutResolvedParam(resolvedParams, name, value);
    }
    RemoveMappedParameters(paramBindings, resolvedParams);
    PopulateResolvedParamsFromMappings(requestParameters, clusterInfo, paramBindings, resolvedParams);
    return resolvedParams;
}

TString RenderTemplatedUrl(TStringBuf urlTemplate, const TResolvedParamPool& resolvedParams) {
    TVector<TString> escapedValues;
    escapedValues.reserve(resolvedParams.size());
    fmt::dynamic_format_arg_store<fmt::format_context> store;

    for (const auto& [name, value] : resolvedParams) {
        escapedValues.emplace_back();
        TString& escapedValue = escapedValues.back();
        TStringOutput out(escapedValue);
        NUri::NEncode::TEncoder::Encode(out, value);
        store.push_back(fmt::arg(name.c_str(), escapedValue));
    }

    try {
        return TString(fmt::vformat(std::string_view(urlTemplate.data(), urlTemplate.size()), store));
    } catch (const fmt::format_error& e) {
        ythrow yexception() << "failed to render URL template '" << urlTemplate << "': " << e.what();
    }
}

TString BuildUrlSourceUrl(
    const TString& url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& templateParamBindings,
    const TResolvedParamBindings& queryParamBindings)
{
    const TCgiParameters requestParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    const TResolvedParamPool resolvedTemplateParams = BuildResolvedTemplateParams(requestParameters, input.ClusterInfo, templateParamBindings);
    const TString renderedUrl = RenderTemplatedUrl(url, resolvedTemplateParams);
    TStringBuf path;
    TStringBuf queryString;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(renderedUrl, path, queryString, fragment);
    TCgiParameters queryParameters(queryString);
    TQueryParamSet renderedQueryParameters;
    for (const auto& [name, value] : queryParameters) {
        Y_UNUSED(value);
        renderedQueryParameters.insert(name);
    }

    ApplyQueryParams(queryParameters, BuildNonIdentityRequestParamValues(requestParameters), url, renderedQueryParameters, false);
    ApplyQueryParams(queryParameters, BuildClusterInfoParamValues(input.ClusterInfo, queryParamBindings.ClusterInfoMappings), url, renderedQueryParameters, true);
    ApplyQueryParams(queryParameters, BuildStaticParamValues(queryParamBindings.StaticMappings), url, renderedQueryParameters, true);
    ApplyQueryParams(queryParameters, BuildRequestParamValues(requestParameters, queryParamBindings.RequestMappings), url, renderedQueryParameters, true);

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
    TUrlSource(
        TString sourceName,
        TString title,
        TString url,
        TResolvedParamBindings templateParamBindings,
        TResolvedParamBindings queryParamBindings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , TemplateParamBindings(std::move(templateParamBindings))
        , QueryParamBindings(std::move(queryParamBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TResolveOutput result{
            .Name = SourceName,
        };
        try {
            result.Links.emplace_back(TResolvedLink{
                .Title = Title,
                .Url = BuildUrlSourceUrl(Url, input, TemplateParamBindings, QueryParamBindings),
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
    TResolvedParamBindings TemplateParamBindings;
    TResolvedParamBindings QueryParamBindings;
};

} // namespace

void ValidateUrlSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings&) {
    const TString sourceDescription = config.HasSource()
        ? TStringBuilder() << "for source=" << config.GetSource()
        : TString("when source is omitted");
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required " << sourceDescription;
    }
    if (config.TagSize() != 0) {
        ythrow yexception() << "tag is not supported " << sourceDescription;
    }
    if (config.FolderSize() != 0) {
        ythrow yexception() << "folder is not supported " << sourceDescription;
    }
    ValidateUrlTemplateSyntax(config.GetUrl());
    const TResolvedParamBindings templateParamBindings = ResolveTemplateParamBindings(config);
    ValidateTemplateParamsAreUnique(templateParamBindings, config);
    ValidateTemplateMappingsAreUsedInUrl(templateParamBindings, config.GetUrl(), config);
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultUrlSourceQueryParamBindings(config.GetUrl())), config);
}

std::shared_ptr<ILinkSource> MakeUrlSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateUrlSourceConfig(config, metaSettings);
    auto templateParamBindings = ResolveTemplateParamBindings(config);
    auto queryParamBindings = ResolveParamBindings(config, BuildDefaultUrlSourceQueryParamBindings(config.GetUrl()));
    return std::make_shared<TUrlSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        std::move(templateParamBindings),
        std::move(queryParamBindings));
}

} // namespace NMVP::NSupportLinks
