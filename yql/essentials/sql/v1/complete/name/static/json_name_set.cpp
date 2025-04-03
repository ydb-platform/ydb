#include "name_service.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

namespace NSQLComplete {

    NJson::TJsonValue LoadJsonResource(const TStringBuf filename) {
        TString text;
        Y_ENSURE(NResource::FindExact(filename, &text));
        return NJson::ReadJsonFastTree(text);
    }

    template <class T, class U>
    T Merge(T lhs, U rhs) {
        std::copy(std::begin(rhs), std::end(rhs), std::back_inserter(lhs));
        return lhs;
    }

    TVector<TString> ParseNames(NJson::TJsonValue::TArray& json) {
        TVector<TString> keys;
        keys.reserve(json.size());
        for (auto& item : json) {
            keys.emplace_back(item.GetMapSafe().at("name").GetStringSafe());
        }
        return keys;
    }

    TVector<TString> ParseTypes(NJson::TJsonValue json) {
        return ParseNames(json.GetArraySafe());
    }

    TVector<TString> ParseFunctions(NJson::TJsonValue json) {
        return ParseNames(json.GetArraySafe());
    }

    TVector<TString> ParseUfs(NJson::TJsonValue json) {
        TVector<TString> names;
        for (auto& [module, v] : json.GetMapSafe()) {
            auto functions = ParseNames(v.GetArraySafe());
            for (auto& function : functions) {
                function.prepend("::").prepend(module);
            }
            std::copy(std::begin(functions), std::end(functions), std::back_inserter(names));
        }
        return names;
    }

    NameSet MakeDefaultNameSet() {
        return {
            .Types = ParseTypes(LoadJsonResource("types.json")),
            .Functions = Merge(
                ParseFunctions(LoadJsonResource("sql_functions.json")),
                ParseUfs(LoadJsonResource("udfs_basic.json"))),
        };
    }

} // namespace NSQLComplete
