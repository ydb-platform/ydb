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
        static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) { \
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

BEGIN_SIMPLE_ARROW_UDF(TNormalize, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    TUri url;
    const bool success = PrepareUrl(args[0].AsStringRef(), url);
    return success
                ? valueBuilder->NewString(url.PrintS(TUri::FlagNoFrag))
                : TUnboxedValue();
}
struct TNormalizeKernelExec : public TUnaryKernelExec<TNormalizeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        TUri url;
        const bool success = PrepareUrl(arg.AsStringRef(), url);
        if (success) {
            return sink(TBlockItem(TStringRef(url.PrintS(TUri::FlagNoFrag))));
        }
        sink(TBlockItem());
    }
};
END_SIMPLE_ARROW_UDF(TNormalize, TNormalizeKernelExec::Do);

BEGIN_SIMPLE_STRICT_ARROW_UDF(TGetScheme, char*(TAutoMap<char*>)) {
    const std::string_view url(args[0].AsStringRef());
    const std::string_view prefix(GetSchemePrefix(url));
    return valueBuilder->SubString(args[0], std::distance(url.begin(), prefix.begin()), prefix.size());
}
struct TGetSchemeKernelExec : public TUnaryKernelExec<TGetSchemeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        const std::string_view url(arg.AsStringRef());
        const std::string_view prefix(GetSchemePrefix(url));
        const std::string_view scheme = url.substr(std::distance(url.begin(), prefix.begin()), prefix.size());
        sink(TBlockItem(scheme));
    }
};
END_SIMPLE_ARROW_UDF(TGetScheme, TGetSchemeKernelExec::Do);

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TGetHost, GetOnlyHost)

std::string_view GetHostAndPortAfterCut(const std::string_view url) {
    return GetHostAndPort(CutSchemePrefix(url));
}

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TGetHostPort, GetHostAndPortAfterCut)

std::string_view GetSchemeHostParameterized(const std::string_view url) {
    return GetSchemeHost(url, /* trimHttp */ false);
}

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TGetSchemeHost, GetSchemeHostParameterized);

std::string_view GetSchemeHostPortParameterized(const std::string_view url) {
    return GetSchemeHostAndPort(url, /* trimHttp */ false, /* trimDefaultPort */ false);
}

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TGetSchemeHostPort, GetSchemeHostPortParameterized);

BEGIN_SIMPLE_ARROW_UDF(TGetPort, TOptional<ui64>(TOptional<char*>)) {
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
struct TGetPortKernelExec : public TUnaryKernelExec<TGetPortKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        ui16 port = 0;
        TStringBuf scheme, host;
        TString lowerUri(arg.AsStringRef());
        std::transform(lowerUri.cbegin(), lowerUri.cbegin() + GetSchemePrefixSize(lowerUri),
                        lowerUri.begin(), [](unsigned char c){ return std::tolower(c); });
        if (TryGetSchemeHostAndPort(lowerUri, scheme, host, port) && port) {
            return sink(TBlockItem(port));
        }
        sink(TBlockItem());
    }
};
END_SIMPLE_ARROW_UDF(TGetPort, TGetPortKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TGetTail, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const TStringBuf url(args[0].AsStringRef());
    TStringBuf host, tail;
    SplitUrlToHostAndPath(url, host, tail);
    return tail.StartsWith('/')
            ? valueBuilder->NewString(tail)
            : valueBuilder->NewString(TString('/').append(tail));
}
struct TGetTailKernelExec : public TUnaryKernelExec<TGetTailKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        const TStringBuf url(arg.AsStringRef());
        TStringBuf host, tail;
        SplitUrlToHostAndPath(url, host, tail);
        if (tail.StartsWith('/')) {
            return sink(TBlockItem(TStringRef(tail)));
        }
        sink(TBlockItem(TStringRef(TString('/').append(tail))));
    }
};
END_SIMPLE_ARROW_UDF(TGetTail, TGetTailKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TGetPath, TOptional<char*>(TOptional<char*>)) {
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
struct TGetPathKernelExec : public TUnaryKernelExec<TGetPathKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        const std::string_view url(arg.AsStringRef());
        std::string_view cut(CutSchemePrefix(url));
        const auto s = cut.find('/');
        if (s == std::string_view::npos) {
            return sink(TBlockItem(TStringRef("/")));
        }

        cut.remove_prefix(s);
        const auto end = cut.find_first_of("?#");
        if (std::string_view::npos != end) {
            cut.remove_suffix(cut.size() - end);
        }
        sink(TBlockItem(TStringRef(cut)));
    }
};
END_SIMPLE_ARROW_UDF(TGetPath, TGetPathKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TGetFragment, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url(args[0].AsStringRef());
    const auto pos = url.find('#');
    return pos == std::string_view::npos ? TUnboxedValue() :
        valueBuilder->SubString(args[0], pos + 1U, url.length() - pos - 1U);
}
struct TGetFragmentKernelExec : public TUnaryKernelExec<TGetFragmentKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        const std::string_view url(arg.AsStringRef());
        const auto pos = url.find('#');
        if (pos == std::string_view::npos) {
            return sink(TBlockItem());
        }
        return sink(TBlockItem(arg.AsStringRef().Substring(pos + 1U, url.length() - pos - 1U)));
    }
};
END_SIMPLE_ARROW_UDF(TGetFragment, TGetFragmentKernelExec::Do);

std::optional<std::pair<ui32, ui32>> GetDomain(const std::string_view url, const ui8 level) {
    const std::string_view host(GetOnlyHost(url));
    std::vector<std::string_view> parts;
    StringSplitter(host).Split('.').AddTo(&parts);
    if (level && parts.size() >= level) {
        const auto& result = host.substr(std::distance(host.begin(), parts[parts.size() - level].begin()));
        if (result.empty()) {
            return std::nullopt;
        }
        return std::make_pair(std::distance(url.begin(), result.begin()), result.size());
    }
    return std::nullopt;
}

BEGIN_SIMPLE_ARROW_UDF(TGetDomain, TOptional<char*>(TOptional<char*>, ui8)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url = args[0].AsStringRef();
    const std::optional<std::pair<ui32, ui32>> resultOpt = GetDomain(url, args[1].Get<ui8>());
    if (!resultOpt) {
        return TUnboxedValue();
    }
    const std::pair<ui32, ui32> result = *resultOpt;
    return valueBuilder->SubString(args[0], result.first, result.second);
}
struct TGetDomainKernelExec : public TBinaryKernelExec<TGetDomainKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
        if (!arg1) {
            return sink(TBlockItem());
        }
        const auto resultOpt = GetDomain(arg1.AsStringRef(), arg2.As<ui8>());
        if (!resultOpt) {
            return sink(TBlockItem());
        }
        const auto result = *resultOpt;
        sink(TBlockItem(arg1.AsStringRef().Substring(result.first, result.second)));
    }
};
END_SIMPLE_ARROW_UDF(TGetDomain, TGetDomainKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TGetTLD, char*(TAutoMap<char*>)) {
    const TStringBuf url(args[0].AsStringRef());
    return valueBuilder->NewString(GetZone(GetOnlyHost(url)));
}
struct TGetTLDKernelExec : public TUnaryKernelExec<TGetTLDKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        const TStringBuf url(arg.AsStringRef());
        return sink(TBlockItem(GetZone(GetOnlyHost(url))));
    }
};
END_SIMPLE_ARROW_UDF(TGetTLD, TGetTLDKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TGetDomainLevel, ui64(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    std::vector<std::string_view> parts;
    StringSplitter(GetOnlyHost(args[0].AsStringRef())).Split('.').AddTo(&parts);
    return TUnboxedValuePod(ui64(parts.size()));
}
struct TGetDomainLevelKernelExec : public TUnaryKernelExec<TGetDomainLevelKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        std::vector<std::string_view> parts;
        StringSplitter(GetOnlyHost(arg.AsStringRef())).Split('.').AddTo(&parts);
        return sink(TBlockItem(ui64(parts.size())));
    }
};
END_SIMPLE_ARROW_UDF(TGetDomainLevel, TGetDomainLevelKernelExec::Do);

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TGetSignificantDomain, char*(TAutoMap<char*>, TOptional<TListType<char*>>), 1) {
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

std::optional<std::pair<ui32, ui32>> GetCGIParam(const std::string_view url, const std::string_view key) {
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
                    return std::make_pair(equal + 1U, std::min(amper, end) - equal - 1U);
                }
            }
            pos = amper;
        }
    }
    return std::nullopt;
}

BEGIN_SIMPLE_ARROW_UDF(TGetCGIParam, TOptional<char*>(TOptional<char*>, char*)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view url = args[0].AsStringRef();
    const std::optional<std::pair<ui32, ui32>> resultOpt = GetCGIParam(url, args[1].AsStringRef());
    if (!resultOpt) {
        return TUnboxedValue();
    }
    const std::pair<ui32, ui32> result = *resultOpt;
    return valueBuilder->SubString(args[0], result.first, result.second);
}
struct TGetCGIParamKernelExec : public TBinaryKernelExec<TGetCGIParamKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
        if (!arg1) {
            return sink(TBlockItem());
        }
        const auto resultOpt = GetCGIParam(arg1.AsStringRef(), arg2.AsStringRef());
        if (!resultOpt) {
            return sink(TBlockItem());
        }
        const auto result = *resultOpt;
        sink(TBlockItem(arg1.AsStringRef().Substring(result.first, result.second)));
    }
};
END_SIMPLE_ARROW_UDF(TGetCGIParam, TGetCGIParamKernelExec::Do);

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutScheme, CutSchemePrefix)

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutWWW, CutWWWPrefix)

ARROW_UDF_SINGLE_STRING_FUNCTION_FOR_URL(TCutWWW2, CutWWWNumberedPrefix)

BEGIN_SIMPLE_ARROW_UDF(TCutQueryStringAndFragment, char*(TAutoMap<char*>)) {
    const std::string_view input(args[0].AsStringRef());
    const auto cut = input.find_first_of("?#");
    return std::string_view::npos == cut ? NUdf::TUnboxedValue(args[0]) : valueBuilder->SubString(args[0], 0U, cut);
}
struct TCutQueryStringAndFragmentKernelExec : public TUnaryKernelExec<TCutQueryStringAndFragmentKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        const std::string_view input(arg.AsStringRef());
        const auto cut = input.find_first_of("?#");
        sink(TBlockItem(arg.AsStringRef().Substring(0U, cut)));
    }
};
END_SIMPLE_ARROW_UDF(TCutQueryStringAndFragment, TCutQueryStringAndFragmentKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TEncode, TOptional<char*>(TOptional<char*>)) {
    EMPTY_RESULT_ON_EMPTY_ARG(0);
    const std::string_view input(args[0].AsStringRef());
    if (input.empty()) {
        return NUdf::TUnboxedValuePod();
    }
    TString url(input);
    UrlEscape(url);
    return input == url ? NUdf::TUnboxedValue(args[0]) : valueBuilder->NewString(url);
}
struct TEncodeKernelExec : public TUnaryKernelExec<TEncodeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        const std::string_view input(arg.AsStringRef());
        if (input.empty()) {
            return sink(TBlockItem());
        }
        TString url(input);
        UrlEscape(url);
        sink(TBlockItem(TStringRef(url)));
    }
};
END_SIMPLE_ARROW_UDF(TEncode, TEncodeKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TDecode, TOptional<char*>(TOptional<char*>)) {
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
struct TDecodeKernelExec : public TUnaryKernelExec<TDecodeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        if (!arg) {
            return sink(TBlockItem());
        }
        const std::string_view input(arg.AsStringRef());
        if (input.empty()) {
            return sink(TBlockItem());
        }
        TString url(input);
        SubstGlobal(url, '+', ' ');
        UrlUnescape(url);
        sink(TBlockItem(TStringRef(url)));
    }
};
END_SIMPLE_ARROW_UDF(TDecode, TDecodeKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TIsKnownTLD, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(IsTld(args[0].AsStringRef()));
}
struct TIsKnownTLDKernelExec : public TUnaryKernelExec<TIsKnownTLDKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        sink(TBlockItem(static_cast<ui8>(IsTld(arg.AsStringRef()))));
    }
};
END_SIMPLE_ARROW_UDF(TIsKnownTLD, TIsKnownTLDKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TIsWellKnownTLD, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(IsVeryGoodTld(args[0].AsStringRef()));
}
struct TIsWellKnownTLDKernelExec : public TUnaryKernelExec<TIsWellKnownTLDKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        sink(TBlockItem(static_cast<ui8>(IsVeryGoodTld(arg.AsStringRef()))));
    }
};
END_SIMPLE_ARROW_UDF(TIsWellKnownTLD, TIsWellKnownTLDKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(THostNameToPunycode, TOptional<char*>(TAutoMap<char*>)) try {
    const TUtf16String& input = UTF8ToWide(args[0].AsStringRef());
    return valueBuilder->NewString(HostNameToPunycode(input));
} catch (TPunycodeError&) {
    return TUnboxedValue();
}
struct THostNameToPunycodeKernelExec : public TUnaryKernelExec<THostNameToPunycodeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) try {
        const TUtf16String& input = UTF8ToWide(arg.AsStringRef());
        return sink(TBlockItem(TStringRef(HostNameToPunycode(input))));
    } catch (TPunycodeError&) {
        return sink(TBlockItem());
    }
};
END_SIMPLE_ARROW_UDF(THostNameToPunycode, THostNameToPunycodeKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TForceHostNameToPunycode, char*(TAutoMap<char*>)) {
    const TUtf16String& input = UTF8ToWide(args[0].AsStringRef());
    return valueBuilder->NewString(ForceHostNameToPunycode(input));
}
struct TForceHostNameToPunycodeKernelExec : public TUnaryKernelExec<TForceHostNameToPunycodeKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        const TUtf16String& input = UTF8ToWide(arg.AsStringRef());
        sink(TBlockItem(TStringRef(ForceHostNameToPunycode(input))));
    }
};
END_SIMPLE_ARROW_UDF(TForceHostNameToPunycode, TForceHostNameToPunycodeKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TPunycodeToHostName, TOptional<char*>(TAutoMap<char*>)) try {
    const TStringRef& input = args[0].AsStringRef();
    const auto& result = WideToUTF8(PunycodeToHostName(input));
    return valueBuilder->NewString(result);
} catch (TPunycodeError&) {
    return TUnboxedValue();
}
struct TPunycodeToHostNameKernelExec : public TUnaryKernelExec<TPunycodeToHostNameKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) try {
        const TStringRef& input = arg.AsStringRef();
        const auto& result = WideToUTF8(PunycodeToHostName(input));
        return sink(TBlockItem(TStringRef(result)));
    } catch (TPunycodeError&) {
        return sink(TBlockItem());
    }
};
END_SIMPLE_ARROW_UDF(TPunycodeToHostName, TPunycodeToHostNameKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TForcePunycodeToHostName, char*(TAutoMap<char*>)) {
    const TStringRef& input = args[0].AsStringRef();
    const auto& result = WideToUTF8(ForcePunycodeToHostName(input));
    return valueBuilder->NewString(result);
}
struct TForcePunycodeToHostNameKernelExec : public TUnaryKernelExec<TForcePunycodeToHostNameKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        const TStringRef& input = arg.AsStringRef();
        const auto& result = WideToUTF8(ForcePunycodeToHostName(input));
        sink(TBlockItem(TStringRef(result)));
    }
};
END_SIMPLE_ARROW_UDF(TForcePunycodeToHostName, TForcePunycodeToHostNameKernelExec::Do);

BEGIN_SIMPLE_ARROW_UDF(TCanBePunycodeHostName, bool(TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(CanBePunycodeHostName(args[0].AsStringRef()));
}
struct TCanBePunycodeHostNameKernelExec : public TUnaryKernelExec<TCanBePunycodeHostNameKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg, const TSink& sink) {
        sink(TBlockItem(static_cast<ui8>(CanBePunycodeHostName(arg.AsStringRef()))));
    }
};
END_SIMPLE_ARROW_UDF(TCanBePunycodeHostName, TCanBePunycodeHostNameKernelExec::Do);

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
