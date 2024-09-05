#include "helpers.h"


namespace NKikimr::NKqp::NWorkload {

NYql::TIssues GroupIssues(const NYql::TIssues& issues, const TString& message) {
    NYql::TIssue rootIssue(message);
    for (const NYql::TIssue& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    return {rootIssue};
}

void ParsePoolSettings(const NKikimrSchemeOp::TResourcePoolDescription& description, NResourcePool::TPoolSettings& poolConfig) {
    const auto& properties = description.GetProperties().GetProperties();
    for (auto& [property, value] : poolConfig.GetPropertiesMap()) {
        if (auto propertyIt = properties.find(property); propertyIt != properties.end()) {
            std::visit(NResourcePool::TPoolSettings::TParser{propertyIt->second}, value);
        }
    }
}

ui64 SaturationSub(ui64 x, ui64 y) {
    return (x > y) ? x - y : 0;
}

}  // NKikimr::NKqp::NWorkload
