#include "resource_pool_classifier_settings.h"


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

//// TClassifierSettings::TExtractor

TString TClassifierSettings::TExtractor::operator()(i64* setting) const {
    return ToString(*setting);
}

TString TClassifierSettings::TExtractor::operator()(TString* setting) const {
    return *setting;
}

//// TPoolSettings

std::unordered_map<TString, TClassifierSettings::TProperty> TClassifierSettings::GetPropertiesMap() {
    std::unordered_map<TString, TProperty> properties = {
        {"rank", &Rank},
        {"resource_pool", &ResourcePool},
        {"membername", &Membername}
    };
    return properties;
}

}  // namespace NKikimr::NResourcePool
