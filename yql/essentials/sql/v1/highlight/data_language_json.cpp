#include "data_language_json.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

namespace NSQLHighlight {

TVector<TString> LoadTypes() {
    TString resource = NResource::Find("types.json");
    NJson::TJsonValue json = NJson::ReadJsonFastTree(resource);

    TVector<TString> types;
    for (const NJson::TJsonValue& value : json.GetArraySafe()) {
        types.emplace_back(value["name"].GetStringSafe());
    }
    return types;
}

TVector<TString> LoadHints() {
    TString resource = NResource::Find("statements_opensource.json");
    NJson::TJsonValue json = NJson::ReadJsonFastTree(resource);

    TVector<TString> hints;
    for (const auto& [statement, services] : json.GetMapSafe()) {
        for (const auto& [service, kinds] : services.GetMapSafe()) {
            for (const auto& hint : kinds["hints"].GetArraySafe()) {
                hints.emplace_back(hint["name"].GetStringSafe());
            }
        }
    }
    return hints;
}

} // namespace NSQLHighlight
