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

TVector<std::pair<TString, TString>> RemoveUrlTemplateParams(const TVector<std::pair<TString, TString>>& parametersToAdd, TStringBuf url) {
    TVector<std::pair<TString, TString>> filteredParams;
    filteredParams.reserve(parametersToAdd.size());

    for (const auto& [parameter, value] : parametersToAdd) {
        if (!HasUrlTemplateParameter(url, parameter)) {
            filteredParams.emplace_back(parameter, value);
        }
    }

    return filteredParams;
}

void ApplyQueryParams(TCgiParameters& queryParameters, const TVector<std::pair<TString, TString>>& parametersToAdd) {
    TQueryParamSet renderedQueryParameters;
    for (const auto& [name, value] : queryParameters) {
        Y_UNUSED(value);
        renderedQueryParameters.insert(name);
    }

    for (const auto& [parameter, value] : parametersToAdd) {
        if (renderedQueryParameters.contains(parameter)) {
            continue;
        }
        queryParameters.ReplaceUnescaped(parameter, value);
    }
}

void PutResolvedParam(TResolvedParamPool& resolvedParams, TStringBuf name, TStringBuf value) {
    if (!name.empty() && !value.empty()) {
        resolvedParams[TString(name)] = TString(value);
    }
}

TResolvedParamPool BuildResolvedTemplateParams(const TCgiParameters& requestParameters, const THashMap<TString, TString>& clusterInfo) {
    TResolvedParamPool resolvedParams;
    for (const auto& [name, value] : clusterInfo) {
        PutResolvedParam(resolvedParams, name, value);
    }
    for (const auto& [name, value] : requestParameters) {
        PutResolvedParam(resolvedParams, name, value);
    }
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

TString BuildUrlSourceUrl(const TString& url, const ILinkSource::TLinkResolveInput& input, const TResolvedParamBindings& queryParamBindings) {
    const TCgiParameters requestParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    const TResolvedParamPool resolvedTemplateParams = BuildResolvedTemplateParams(requestParameters, input.ClusterInfo);
    const TString renderedUrl = RenderTemplatedUrl(url, resolvedTemplateParams);
    TStringBuf path;
    TStringBuf queryString;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(renderedUrl, path, queryString, fragment);
    TCgiParameters queryParameters(queryString);
    const auto parametersToAdd = RemoveUrlTemplateParams(
        BuildParametersToAdd(requestParameters, input.ClusterInfo, queryParamBindings),
        url);

    ApplyQueryParams(queryParameters, parametersToAdd);

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
    TUrlSource(TString sourceName, TString title, TString url, TResolvedParamBindings queryParamBindings) : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , QueryParamBindings(std::move(queryParamBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TResolveOutput result{
            .Name = SourceName,
        };
        try {
            result.Links.emplace_back(TResolvedLink{
                .Title = Title,
                .Url = BuildUrlSourceUrl(Url, input, QueryParamBindings),
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
    TResolvedParamBindings QueryParamBindings;
};

} // namespace

void ValidateUrlSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings&) {
    const TString sourceDescription = TStringBuilder() << "for source=" << config.GetSource();
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
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultUrlSourceQueryParamBindings(config.GetUrl())), config);
}

std::shared_ptr<ILinkSource> MakeUrlSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateUrlSourceConfig(config, metaSettings);
    auto queryParamBindings = ResolveParamBindings(config, BuildDefaultUrlSourceQueryParamBindings(config.GetUrl()));
    return std::make_shared<TUrlSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        std::move(queryParamBindings));
}

} // namespace NMVP::NSupportLinks
