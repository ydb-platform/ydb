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
    for (auto& [property, value] : NResourcePool::GetPropertiesMap(poolConfig)) {
        if (auto propertyIt = properties.find(property); propertyIt != properties.end()) {
            std::visit(NResourcePool::TSettingsParser{propertyIt->second}, value);
        }
    }
}

}  // NKikimr::NKqp::NWorkload
