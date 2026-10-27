#include "yql_dispatch.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

#include <util/string/split.h>
#include <util/random/random.h>
#include <util/datetime/base.h>

namespace NYql::NCommon {

namespace NPrivate {

constexpr char ContainerSettingDelimiters[] = ",;| ";

template <>
TParser<TString> GetDefaultParser<TString>() {
    return [](const TString& str) {
        return str;
    };
}

template <>
TParser<bool> GetDefaultParser<bool>() {
    // Special handle of empty value to properly set setting for empty pragmas like yt.UseFeautre;
    return [](const TString& str) {
        return str ? FromString<bool>(str) : true;
    };
}

template <>
TParser<TGUID> GetDefaultParser<TGUID>() {
    return [](const TString& str) {
        TGUID guid;
        if (!GetGuid(str, guid)) {
            throw yexception() << "Bad GUID format";
        }
        return guid;
    };
}

template <>
TParser<NSize::TSize> GetDefaultParser<NSize::TSize>() {
    return [](const TString& str) -> NSize::TSize {
        return NSize::ParseSize(str); // Support suffixes k, m, g, ...
    };
}

template <>
TParser<TInstant> GetDefaultParser<TInstant>() {
    return [](const TString& str) {
        TInstant val;
        if (!TInstant::TryParseIso8601(str, val) && !TInstant::TryParseRfc822(str, val) && !TInstant::TryParseHttp(str, val) && !TInstant::TryParseX509(str, val)) {
            throw yexception() << "Bad date/time format";
        }
        return val;
    };
}

#define YQL_DEFINE_PRIMITIVE_SETTING_PARSER(type)                    \
    template <>                                                      \
    TParser<type> GetDefaultParser<type>() {                         \
        return [](const TString& s) { return FromString<type>(s); }; \
    }

#define YQL_DEFINE_CONTAINER_SETTING_PARSER(type)                                   \
    template <>                                                                     \
    TParser<type> GetDefaultParser<type>() {                                        \
        return [](const TString& str) {                                             \
            type res;                                                               \
            StringSplitter(str).SplitBySet(ContainerSettingDelimiters).AddTo(&res); \
            for (auto& s : res) {                                                   \
                if (s.empty()) {                                                    \
                    throw yexception() << "Empty value item";                       \
                }                                                                   \
            }                                                                       \
            return res;                                                             \
        };                                                                          \
    }

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DEFINE_PRIMITIVE_SETTING_PARSER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DEFINE_CONTAINER_SETTING_PARSER)

template <>
TSerializer<TString> GetDefaultSerializer<TString>() {
    return [](const TString& v) {
        return v;
    };
}

template <>
TSerializer<bool> GetDefaultSerializer<bool>() {
    return [](const bool& v) -> TString {
        return v ? "true" : "false";
    };
}

template <>
TSerializer<TGUID> GetDefaultSerializer<TGUID>() {
    return [](const TGUID& v) {
        return GetGuidAsString(v);
    };
}

template <>
TSerializer<NSize::TSize> GetDefaultSerializer<NSize::TSize>() {
    return [](const NSize::TSize& v) {
        return ToString(ui64(v));
    };
}

template <>
TSerializer<TInstant> GetDefaultSerializer<TInstant>() {
    return [](const TInstant& v) {
        return v.ToString();
    };
}

#define YQL_DEFINE_PRIMITIVE_SETTING_SERIALIZER(type)     \
    template <>                                           \
    TSerializer<type> GetDefaultSerializer<type>() {      \
        return [](const type& v) { return ToString(v); }; \
    }

#define YQL_DEFINE_CONTAINER_SETTING_SERIALIZER(type)                                                                                                  \
    template <>                                                                                                                                        \
    TSerializer<type> GetDefaultSerializer<type>() {                                                                                                   \
        return [](const type& v) {                                                                                                                     \
            for (const auto& item : v) {                                                                                                               \
                if (item.find_first_of(ContainerSettingDelimiters) == TString::npos) {                                                                 \
                    continue;                                                                                                                          \
                }                                                                                                                                      \
                throw yexception() << "Setting " << TString(item).Quote() << " contains forbidden char from \"" << ContainerSettingDelimiters << "\""; \
            }                                                                                                                                          \
            return JoinSeq(",", v);                                                                                                                    \
        };                                                                                                                                             \
    }

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DEFINE_PRIMITIVE_SETTING_SERIALIZER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DEFINE_CONTAINER_SETTING_SERIALIZER)

} // namespace NPrivate

TMaybe<TString> TSettingHandlerBase::ValidateDispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                                                      const THashSet<TString>& validClusters) const {
    if (cluster != ALL_CLUSTERS) {
        if (!IsPerCluster()) {
            return TStringBuilder() << "Static setting " << name.Quote() << " cannot be set for specific cluster";
        }
        if (!validClusters.contains(cluster)) {
            TStringBuilder nearClusterMsg;
            for (const auto& item : validClusters) {
                if (NLevenshtein::Distance(cluster, item) < DefaultMistypeDistance) {
                    nearClusterMsg << ", did you mean " << item.Quote() << '?';
                    break;
                }
            }
            return TStringBuilder() << "Unknown cluster name " << cluster.Quote() << " for setting " << name.Quote() << nearClusterMsg;
        }
    }
    if (!value && !IsRuntime()) {
        return TStringBuilder() << "Static setting " << name.Quote() << " cannot be reset to default";
    }
    return Nothing();
}

bool TSettingHandlerBase::Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                                   ISettingDispatcher::EStage stage, const ISettingDispatcher::TErrorCallback& errorCallback,
                                   const TQContext& qContext, const THashSet<TString>& validClusters, const std::function<bool(bool validateOnly)>& handleSetting) const {
    if (auto error = ValidateDispatch(cluster, name, value, validClusters)) {
        return errorCallback(*error, true);
    }
    if (qContext.CanRead() && qContext.CaptureMode() == EQPlayerCaptureMode::Full &&
        stage != ISettingDispatcher::EStage::CONFIG && IgnoreInFullReplay()) {
        YQL_CLOG(INFO, ProviderCommon) << "Ignoring setting " << name.Quote() << " in full replay mode";
        return true;
    }

    bool validateOnly = true;
    switch (stage) {
        case ISettingDispatcher::EStage::RUNTIME:
            validateOnly = !IsRuntime();
            break;
        case ISettingDispatcher::EStage::STATIC:
            validateOnly = IsRuntime();
            break;
        case ISettingDispatcher::EStage::CONFIG:
            validateOnly = false;
            break;
    }
    return handleSetting(validateOnly);
}

ISettingDispatcher::TErrorCallback GetDefaultErrorCallback() {
    return [](const TString& msg, bool isError) -> bool {
        if (isError) {
            YQL_LOG(ERROR) << msg;
            throw yexception() << msg;
        }
        YQL_LOG(WARN) << msg;
        return true;
    };
}

ISettingDispatcher::TErrorCallback GetErrorCallback(TPositionHandle pos, TExprContext& ctx) {
    return [pos, &ctx](const TString& msg, bool isError) -> bool {
        if (isError) {
            ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::DEFAULT_ERROR, msg));
            return false;
        } else {
            return ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), TIssuesIds::YQL_PRAGMA_WARNING_MSG, msg));
        }
    };
}

} // namespace NYql::NCommon
