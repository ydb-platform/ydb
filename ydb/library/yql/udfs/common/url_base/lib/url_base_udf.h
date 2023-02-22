#pragma once

#include "url_parse.h"
#include "url_query.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

#include <library/cpp/tld/tld.h>
#include <library/cpp/charset/wide.h>
#include <library/cpp/unicode/punycode/punycode.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/string_utils/url/url.h>

#include <util/string/split.h>
#include <util/string/subst.h>

using namespace NKikimr;
using namespace NUdf;
using namespace NTld;
using namespace NUrlUdf;

inline bool PrepareUrl(const std::string_view& keyStr, TUri& parser) {
    const NUri::TParseFlags& parseFlags(TUri::FeaturesRecommended);
    return parser.ParseAbs(keyStr, parseFlags) == TUri::ParsedOK;
}

#define ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(udfName, functionName) \
    BEGIN_SIMPLE_ARROW_UDF(udfName, TOptional<char*>(TOptional<char*>)) { \
        EMPTY_RESULT_ON_EMPTY_ARG(0); \
        const std::string_view url(args[0].AsStringRef()); \
        const std::string_view res(functionName(url)); \
        return res.empty() ? TUnboxedValue() : \
            valueBuilder->SubString(args[0], std::distance(url.begin(), res.begin()), res.size()); \
    } \
    struct udfName##KernelExec : public TUnaryKernelExec<udfName##KernelExec> { \
        template <typename TSink> \
        static void Process(TBlockItem arg, const TSink& sink) { \
            if (!arg) { \
                return sink(TBlockItem()); \
            } \
            const std::string_view url(arg.AsStringRef()); \
            const std::string_view res(functionName(url)); \
            if (res.empty()) { \
                return sink(TBlockItem()); \
            } \
            sink(TBlockItem(TStringRef(res))); \
        } \
    }; \
    END_SIMPLE_ARROW_UDF(udfName, udfName##KernelExec::Do);

SIMPLE_UDF(TNormalize, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    TUri url;
    const bool success = PrepareUrl(args[0].AsStringRef(), url);
    return success
                ? valueBuilder->NewString(url.PrintS(TUri::FlagNoFrag))
                : TUnboxedValue();
}

SIMPLE_UDF(TGetScheme, char*(TAutoMap<char*>)) {
    const std::string_view url(args[0].AsStringRef());
    const std::string_view prefix(GetSchemePrefix(url));
    return valueBuilder->SubString(args[0], std::distance(url.begin(), prefix.begin()), prefix.size());
}

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TGetHost, GetOnlyHost)

SIMPLE_UDF(TGetHostPort, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const std::string_view host(GetHostAndPort(CutSchemePrefix(url)));
    return host.empty() ? TUnboxedValue() :
        valueBuilder->SubString(args[0], std::distance(url.begin(), host.begin()), host.size());
}

SIMPLE_UDF(TGetSchemeHost, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const std::string_view host(GetSchemeHost(url, /* trimHttp */ false));
    return host.empty() ? TUnboxedValue() :
        valueBuilder->SubString(args[0], 0U, std::distance(url.begin(), host.end()));
}

SIMPLE_UDF(TGetSchemeHostPort, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const std::string_view host(GetSchemeHostAndPort(url, /* trimHttp */ false, /* trimDefaultPort */ false));
    return host.empty() ? TUnboxedValue() :
        valueBuilder->SubString(args[0], 0U, std::distance(url.begin(), host.end()));
}

SIMPLE_UDF(TGetPort, TOptional<ui64>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    Y_UNUSED(valueBuilder);
    ui16 port = 0;
    TStringBuf scheme, host;
    TString lowerUri(args[0].AsStringRef());
    std::transform(lowerUri.cbegin(), lowerUri.cbegin() + GetSchemePrefixSize(lowerUri),
                    lowerUri.begin(), [](unsigned char c){ return std::tolower(c); });
    return TryGetSchemeHostAndPort(lowerUri, scheme, host, port) && port
        ? TUnboxedValuePod(port)
        : TUnboxedValuePod();
}

SIMPLE_UDF(TGetTail, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const TStringBuf url(args[0].AsStringRef());
    TStringBuf host, tail;
    SplitUrlToHostAndPath(url, host, tail);
    return tail.StartsWith('/')
            ? valueBuilder->NewString(tail)
            : valueBuilder->NewString(TString('/').append(tail));
}

SIMPLE_UDF(TGetPath, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    std::string_view cut(CutSchemePrefix(url));
    const auto s = cut.find('/');
    if (s == std::string_view::npos) {
        return valueBuilder->NewString("/");
    }

    cut.remove_prefix(s);
    const auto end = cut.find_first_of("?#");
    if (std::string_view::npos != end) {
        cut.remove_suffix(cut.size() - end);
    }

    return valueBuilder->SubString(args[0], std::distance(url.begin(), cut.begin()), cut.length());
}

SIMPLE_UDF(TGetFragment, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const auto pos = url.find('#');
    return pos == std::string_view::npos ? TUnboxedValue() :
        valueBuilder->SubString(args[0], pos + 1U, url.length() - pos - 1U);
}

SIMPLE_UDF(TGetDomain, TOptional<char*>(TOptional<char*>, ui8)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const std::string_view host(GetOnlyHost(url));
    const ui8 level = args[1].Get<ui8>();
    std::vector<std::string_view> parts;
    StringSplitter(host).Split('.').AddTo(&parts);
    if (level && parts.size() >= level) {
        const auto& result = host.substr(std::distance(host.begin(), parts[parts.size() - level].begin()));
        return result.empty() ? TUnboxedValue() :
            valueBuilder->SubString(args[0], std::distance(url.begin(), result.begin()), result.size());
    }

    return TUnboxedValue();
}

SIMPLE_UDF(TGetTLD, char*(TAutoMap<char*>)) {
    const TStringBuf url(args[0].AsStringRef());
    return valueBuilder->NewString(GetZone(GetOnlyHost(url)));
}

SIMPLE_UDF(TGetDomainLevel, ui64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    std::vector<std::string_view> parts;
    StringSplitter(GetOnlyHost(args[0].AsStringRef())).Split('.').AddTo(&parts);
    return TUnboxedValuePod(ui64(parts.size()));
}

SIMPLE_UDF_OPTIONS(TGetSignificantDomain, char*(TAutoMap<char*>, TOptional<TListType<char*>>),
                    builder.OptionalArgs(1)) {
    const std::string_view url(args[0].AsStringRef());
    const std::string_view host(GetOnlyHost(url));
    std::vector<std::string_view> parts;
    StringSplitter(host).Split('.').AddTo(&parts);
    if (parts.size() > 2) {
        const auto& secondLevel = parts.at(parts.size() - 2);
        bool secondLevelIsZone = false;

        if (args[1]) {
            const auto& zonesIterator = args[1].GetListIterator();
            for (TUnboxedValue item; zonesIterator.Next(item);) {
                if (secondLevel == item.AsStringRef()) {
                    secondLevelIsZone = true;
                    break;
                }
            }
        } else {
            static const std::set<std::string_view> zones{"com", "net", "org", "co", "gov", "edu"};
            secondLevelIsZone = zones.count(secondLevel);
        }

        const auto from = parts[parts.size() - (secondLevelIsZone ? 3U : 2U)].begin();
        return valueBuilder->SubString(args[0], std::distance(url.begin(), from), std::distance(from, parts.back().end()));
    }
    return valueBuilder->SubString(args[0], std::distance(url.begin(), host.begin()), host.length());
}

SIMPLE_UDF(TGetCGIParam, TOptional<char*>(TOptional<char*>, char*)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const std::string_view key(args[1].AsStringRef());
    const auto queryStart = url.find('?');
    if (queryStart != std::string_view::npos) {
        const auto from = queryStart + 1U;
        const auto anc = url.find('#', from);
        const auto end = anc == std::string_view::npos ? url.length() : anc;
        for (auto pos = from; pos && pos < end; ++pos) {
            const auto equal = url.find('=', pos);
            const auto amper = url.find('&', pos);
            if (equal < amper) {
                const auto& param = url.substr(pos, equal - pos);
                if (param == key) {
                    return valueBuilder->SubString(args[0], equal + 1U, std::min(amper, end) - equal - 1U);
                }
            }

            pos = amper;
        }
    }

    return TUnboxedValue();
}

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutScheme, CutSchemePrefix)

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutWWW, CutWWWPrefix)

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutWWW2, CutWWWNumberedPrefix)

SIMPLE_UDF(TCutQueryStringAndFragment, char*(TAutoMap<char*>)) {
    const std::string_view input(args[0].AsStringRef());
    const auto cut = input.find_first_of("?#");
    return std::string_view::npos == cut ? NUdf::TUnboxedValue(args[0]) : valueBuilder->SubString(args[0], 0U, cut);
}

SIMPLE_UDF(TEncode, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view input(args[0].AsStringRef());
    if (input.empty()) {
        return NUdf::TUnboxedValuePod();
    }
    TString url(input);
    UrlEscape(url);
    return input == url ? NUdf::TUnboxedValue(args[0]) : valueBuilder->NewString(url);
}

SIMPLE_UDF(TDecode, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view input(args[0].AsStringRef());
    if (input.empty()) {
        return NUdf::TUnboxedValuePod();
    }
    TString url(input);
    SubstGlobal(url, '+', ' ');
    UrlUnescape(url);
    return input == url ? NUdf::TUnboxedValue(args[0]) : valueBuilder->NewString(url);
}

SIMPLE_UDF(TIsKnownTLD, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(IsTld(args[0].AsStringRef()));
}

SIMPLE_UDF(TIsWellKnownTLD, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(IsVeryGoodTld(args[0].AsStringRef()));
}

SIMPLE_UDF(THostNameToPunycode, TOptional<char*>(TAutoMap<char*>)) try {
    const TUtf16String& input = UTF8ToWide(args[0].AsStringRef());
    return valueBuilder->NewString(HostNameToPunycode(input));
} catch (TPunycodeError&) {
    return TUnboxedValue();
}

SIMPLE_UDF(TForceHostNameToPunycode, char*(TAutoMap<char*>)) {
    const TUtf16String& input = UTF8ToWide(args[0].AsStringRef());
    return valueBuilder->NewString(ForceHostNameToPunycode(input));
}

SIMPLE_UDF(TPunycodeToHostName, TOptional<char*>(TAutoMap<char*>)) try {
    const TStringRef& input = args[0].AsStringRef();
    const auto& result = WideToUTF8(PunycodeToHostName(input));
    return valueBuilder->NewString(result);
} catch (TPunycodeError&) {
    return TUnboxedValue();
}

SIMPLE_UDF(TForcePunycodeToHostName, char*(TAutoMap<char*>)) {
    const TStringRef& input = args[0].AsStringRef();
    const auto& result = WideToUTF8(ForcePunycodeToHostName(input));
    return valueBuilder->NewString(result);
}

SIMPLE_UDF(TCanBePunycodeHostName, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(CanBePunycodeHostName(args[0].AsStringRef()));
}

#define EXPORTED_URL_BASE_UDF \
    TNormalize, \
    TParse, \
    TGetScheme, \
    TGetHost, \
    TGetHostPort, \
    TGetSchemeHost, \
    TGetSchemeHostPort, \
    TGetPort, \
    TGetTail, \
    TGetPath, \
    TGetFragment, \
    TGetDomain, \
    TGetTLD, \
    TGetDomainLevel, \
    TGetSignificantDomain, \
    TGetCGIParam, \
    TCutScheme, \
    TCutWWW, \
    TCutWWW2, \
    TCutQueryStringAndFragment, \
    TEncode, \
    TDecode, \
    TIsKnownTLD, \
    TIsWellKnownTLD, \
    THostNameToPunycode, \
    TForceHostNameToPunycode, \
    TPunycodeToHostName, \
    TForcePunycodeToHostName, \
    TCanBePunycodeHostName, \
    TQueryStringToList, \
    TQueryStringToDict, \
    TBuildQueryString
