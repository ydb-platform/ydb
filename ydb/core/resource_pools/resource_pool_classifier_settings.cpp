#include "resource_pool_classifier_settings.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <ydb/library/aclib/aclib.h>


namespace NKikimr::NResourcePool {

//// TClassifierSettings::TParser

void TClassifierSettings::TParser::operator()(i64* setting) const {
    *setting = FromString<i64>(Value);
    if (*setting < -1) {
        throw yexception() << "Invalid integer value " << *setting << ", it is should be greater or equal -1";
    }
}

void TClassifierSettings::TParser::operator()(TString* setting) const {
    *setting = Value;
}

void TClassifierSettings::TParser::operator()(std::optional<TString>* setting) const {
    *setting = Value;
}

void TClassifierSettings::TParser::operator()(std::optional<TRegexPredicate>* setting) const {
    if (Value.empty()) {
        setting->reset();
    } else {
        *setting = TRegexPredicate::Compile(Value);
    }
}

void TClassifierSettings::TParser::operator()(std::optional<EClassifierAction>* setting) const {
    if (Value.empty()) {
        setting->reset();
        return;
    }
    const auto lowered = to_lower(Value);
    if (lowered == "reject") {
        *setting = EClassifierAction::Reject;
        return;
    }
    throw yexception() << "Invalid action '" << Value << "', supported values: 'reject'";
}

//// TClassifierSettings::TExtractor

TString TClassifierSettings::TExtractor::operator()(i64* setting) const {
    return ToString(*setting);
}

TString TClassifierSettings::TExtractor::operator()(TString* setting) const {
    return *setting;
}

TString TClassifierSettings::TExtractor::operator()(std::optional<TString>* setting) const {
    return setting->value_or(TString{});
}

TString TClassifierSettings::TExtractor::operator()(std::optional<TRegexPredicate>* setting) const {
    if (*setting) {
        return (*setting)->Pattern;
    }
    return TString{};
}

TString TClassifierSettings::TExtractor::operator()(std::optional<EClassifierAction>* setting) const {
    if (!*setting) {
        return TString{};
    }
    switch (**setting) {
        case EClassifierAction::Reject:
            return "reject";
    }
    return TString{};
}

//// TClassifierSettings

std::unordered_map<TString, TClassifierSettings::TProperty> TClassifierSettings::GetPropertiesMap() {
    std::unordered_map<TString, TProperty> properties = {
        {"rank", &Rank},
        {"resource_pool", &ResourcePool},
        {"member_name", &MemberName},
        {"has_app_name", &HasAppName},
        {"action", &Action}
    };
    return properties;
}

std::optional<TString> TClassifierSettings::Validate() const {
    if (!MemberName) {
        return std::nullopt;
    }
    NACLib::TUserToken token(*MemberName, TVector<NACLib::TSID>{});
    if (token.IsSystemUser()) {
        return TStringBuilder() << "Invalid resource pool classifier configuration, cannot create classifier for system user " << *MemberName;
    }
    return std::nullopt;
}

}  // namespace NKikimr::NResourcePool
