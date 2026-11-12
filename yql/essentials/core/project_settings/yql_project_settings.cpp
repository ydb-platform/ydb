#include "yql_project_settings.h"

#include <utility>

namespace NYql {

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
