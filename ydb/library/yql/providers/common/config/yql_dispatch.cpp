#include "yql_dispatch.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

#include <util/string/split.h>
#include <util/random/random.h>
#include <util/datetime/base.h>

namespace NYql {

namespace NPrivate {

template <>
TParser<TString> GetDefaultParser<TString>() {
    return [] (const TString& str) {
        return str;
    };
}

template<>
TParser<bool> GetDefaultParser<bool>() {
    // Special handle of empty value to properly set setting for empty pragmas like yt.UseFeautre;
    return [] (const TString& str) {
        return str ? FromString<bool>(str) : true;
    };
}

template <>
TParser<TGUID> GetDefaultParser<TGUID>() {
    return [] (const TString& str) {
        TGUID guid;
        if (!GetGuid(str, guid)) {
            throw yexception() << "Bad GUID format";
        }
        return guid;
    };
}

template<>
TParser<NSize::TSize> GetDefaultParser<NSize::TSize>() {
    return [] (const TString& str) -> NSize::TSize {
        return NSize::ParseSize(str); // Support suffixes k, m, g, ...
    };
}

template<>
TParser<TInstant> GetDefaultParser<TInstant>() {
    return [] (const TString& str) {
        TInstant val;
        if (!TInstant::TryParseIso8601(str, val)
            && !TInstant::TryParseRfc822(str, val)
            && !TInstant::TryParseHttp(str, val)
            && !TInstant::TryParseX509(str, val)
        ) {
            throw yexception() << "Bad date/time format";
        }
        return val;
    };
}

#define YQL_DEFINE_PRIMITIVE_SETTING_PARSER(type)                       \
    template<>                                                          \
    TParser<type> GetDefaultParser<type>() {                            \
        return [] (const TString& s) { return FromString<type>(s); };   \
    }

#define YQL_DEFINE_CONTAINER_SETTING_PARSER(type)                       \
    template <>                                                         \
    TParser<type> GetDefaultParser<type>() {                            \
        return [] (const TString& str) {                                \
            type res;                                                   \
            StringSplitter(str).SplitBySet(",;| ").AddTo(&res);         \
            for (auto& s: res) {                                        \
                if (s.empty()) {                                        \
                    throw yexception() << "Empty value item";           \
                }                                                       \
            }                                                           \
            return res;                                                 \
        };                                                              \
    }

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DEFINE_PRIMITIVE_SETTING_PARSER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DEFINE_CONTAINER_SETTING_PARSER)

} // NPrivate

namespace NCommon {

bool TSettingDispatcher::IsRuntime(const TString& name) {
    auto normalizedName = NormalizeName(name);
    if (auto handler = Handlers.Value(normalizedName, TSettingHandler::TPtr())) {
        return handler->IsRuntime();
    }
    return false;
}

bool TSettingDispatcher::Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value, EStage stage, const TErrorCallback& errorCallback) {
    auto normalizedName = NormalizeName(name);
    if (auto handler = Handlers.Value(normalizedName, TSettingHandler::TPtr())) {
        if (cluster != ALL_CLUSTERS) {
            if (!handler->IsRuntime()) {
                return errorCallback(TStringBuilder() << "Static setting " << name.Quote() << " cannot be set for specific cluster", true);
            }
            if (!ValidClusters.contains(cluster)) {
                TStringBuilder nearClusterMsg;
                for (auto& item: ValidClusters) {
                    if (NLevenshtein::Distance(cluster, item) < DefaultMistypeDistance) {
                        nearClusterMsg << ", did you mean " << item.Quote() << '?';
                        break;
                    }
                }
                return errorCallback(TStringBuilder() << "Unknown cluster name " << cluster.Quote()
                    << " for setting " << name.Quote() << nearClusterMsg, true);
            }
        }
        if (!value && !handler->IsRuntime()) {
            return errorCallback(TStringBuilder() << "Static setting " << name.Quote() << " cannot be reset to default", true);
        }

        bool validateOnly = true;
        switch (stage) {
        case EStage::RUNTIME:
            validateOnly = !handler->IsRuntime();
            break;
        case EStage::STATIC:
            validateOnly = handler->IsRuntime();
            break;
        case EStage::CONFIG:
            validateOnly = false;
            break;
        }

        return handler->Handle(cluster, value, validateOnly, errorCallback);
    } else {
        // ignore unknown names in config
        if (stage == EStage::CONFIG) {
            return true;
        }

        TStringBuilder nearHandlerMsg;
        for (auto& item: Handlers) {
            if (NLevenshtein::Distance(normalizedName, item.first) < DefaultMistypeDistance) {
                nearHandlerMsg << ", did you mean " << item.second->GetDisplayName().Quote() << '?';
                break;
            }
        }
        return errorCallback(TStringBuilder() << "Unknown setting name " << name.Quote() << nearHandlerMsg, true);
    }
}

void TSettingDispatcher::FreezeDefaults() {
    for (auto& item: Handlers) {
        item.second->FreezeDefault();
    }
}

void TSettingDispatcher::Restore() {
    for (auto& item: Handlers) {
        item.second->Restore(ALL_CLUSTERS);
    }
}

TSettingDispatcher::TErrorCallback TSettingDispatcher::GetDefaultErrorCallback() {
    return [] (const TString& msg, bool isError) -> bool {
        if (isError) {
            YQL_LOG(ERROR) << msg;
            throw yexception() << msg;
        }
        YQL_LOG(WARN) << msg;
        return true;
    };
}

TSettingDispatcher::TErrorCallback TSettingDispatcher::GetErrorCallback(TPositionHandle pos, TExprContext& ctx) {
    return [pos, &ctx](const TString& msg, bool isError) -> bool {
        if (isError) {
            ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::DEFAULT_ERROR, msg));
            return false;
        } else {
            return ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), TIssuesIds::YQL_PRAGMA_WARNING_MSG, msg));
        }
    };
}


} // NCommon
} // NYql
