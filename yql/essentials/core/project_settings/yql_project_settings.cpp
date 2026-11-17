#include "yql_project_settings.h"

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/yexception.h>

#include <utility>

namespace NYql {

TProjectSettings TProjectSettings::FromJson(TStringBuf json) {
    return FromYson(NYT::NodeFromJsonString(json));
}

TProjectSettings TProjectSettings::FromYson(const NYT::TNode& settings) {
    Y_ENSURE(settings.IsMap(), "Project settings must be a map");
    TProjectSettings projectSettings;
    if (settings.HasKey("Pragmas")) {
        for (const auto& pragma : settings["Pragmas"].AsList()) {
            TVector<TString> values;
            for (const auto& value : pragma["Value"].AsList()) {
                values.push_back(value.AsString());
            }
            projectSettings.AddPragma(pragma["Name"].AsString(), std::move(values));
        }
    }
    return projectSettings;
}

NYT::TNode TProjectSettings::ToYson() const {
    auto pragmas = NYT::TNode::CreateList();
    for (const auto& pragma : Pragmas) {
        auto values = NYT::TNode::CreateList();
        for (const auto& value : pragma.Value) {
            values.Add(value);
        }
        pragmas.Add(NYT::TNode()("Name", pragma.Name)("Value", values));
    }
    return NYT::TNode()("Pragmas", pragmas);
}

void TProjectSettings::AddPragma(TString name, TVector<TString> values) {
    Pragmas.push_back(TPragma{.Name = std::move(name), .Value = std::move(values)});
}

void TProjectSettings::AddPragma(TString name, TString value) {
    AddPragma(std::move(name), TVector<TString>{std::move(value)});
}

const TProjectSettings::TPragma* TProjectSettings::FindPragma(TStringBuf name) const {
    for (const auto& pragma : Pragmas) {
        if (pragma.Name == name) {
            return &pragma;
        }
    }
    return nullptr;
}

} // namespace NYql
