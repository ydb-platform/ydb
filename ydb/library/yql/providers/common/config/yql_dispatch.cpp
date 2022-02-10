#include "yql_dispatch.h" 
 
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
 
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
 
bool TSettingDispatcher::Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value, EStage stage) { 
    auto normalizedName = NormalizeName(name); 
    if (auto handler = Handlers.Value(normalizedName, TSettingHandler::TPtr())) { 
        if (cluster != ALL_CLUSTERS) { 
            if (!handler->IsRuntime()) { 
                ythrow yexception() << "Static setting " << name.Quote() << " cannot be set for specific cluster"; 
            } 
            if (!ValidClusters.contains(cluster)) {
                TStringBuilder nearClusterMsg; 
                for (auto& item: ValidClusters) { 
                    if (NLevenshtein::Distance(cluster, item) < DefaultMistypeDistance) {
                        nearClusterMsg << ", did you mean " << item.Quote() << '?'; 
                        break; 
                    } 
                } 
                ythrow yexception() << "Unknown cluster name " << cluster.Quote() 
                    << " for setting " << name.Quote() << nearClusterMsg; 
            } 
        } 
        if (!value && !handler->IsRuntime()) { 
            ythrow yexception() << "Static setting " << name.Quote() << " cannot be reset to default"; 
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
 
        handler->Handle(cluster, value, validateOnly); 
        return !validateOnly; 
    } else { 
        // ignore unknown names in config
        if (stage == EStage::CONFIG) {
            return false;
        }

        TStringBuilder nearHandlerMsg; 
        for (auto& item: Handlers) { 
            if (NLevenshtein::Distance(normalizedName, item.first) < DefaultMistypeDistance) {
                nearHandlerMsg << ", did you mean " << item.second->GetDisplayName().Quote() << '?'; 
                break; 
            } 
        } 
        ythrow yexception() << "Unknown setting name " << name.Quote() << nearHandlerMsg; 
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

template <class TActivation> 
bool TSettingDispatcher::Allow(const TActivation& activation, const TString& userName) { 
    if (AnyOf(activation.GetIncludeUsers(), [&](const auto& user) { return user == userName; })) { 
        return true; 
    } 
    if (AnyOf(activation.GetExcludeUsers(), [&](const auto& user) { return user == userName; })) { 
        return false; 
    } 
    if (userName.StartsWith("robot") && activation.GetExcludeRobots()) { 
        return false; 
    } 
 
    ui32 percent = activation.GetPercentage(); 
    if (activation.ByHourSize()) { 
        auto now = TInstant::Now(); 
        struct tm local = {}; 
        now.LocalTime(&local); 
        const auto hour = ui32(local.tm_hour); 
 
        for (auto& byHour: activation.GetByHour()) { 
            if (byHour.GetHour() == hour) { 
                percent = byHour.GetPercentage(); 
                break; 
            } 
        } 
    } 
    const auto random = RandomNumber<ui8>(100); 
    return random < percent; 
} 
 
template bool TSettingDispatcher::Allow<NYql::TActivationPercentage>(const NYql::TActivationPercentage& activation, const TString& userName); 
 
} // NCommon 
} // NYql 
