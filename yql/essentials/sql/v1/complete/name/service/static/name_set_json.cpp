#include "name_set.h"
#include "name_set_json.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    NJson::TJsonValue LoadJsonResource(TStringBuf filename) {
        TString text;
        Y_ENSURE(NResource::FindExact(filename, &text), filename);
        return NJson::ReadJsonFastTree(text);
    }

    TVector<TString> ParseNames(NJson::TJsonValue::TArray& json) {
        TVector<TString> keys;
        keys.reserve(json.size());
        for (auto& item : json) {
            keys.emplace_back(item.GetMapSafe().at("name").GetStringSafe());
        }
        return keys;
    }

    TVector<TString> ParsePragmas(NJson::TJsonValue json) {
        return ParseNames(json.GetArraySafe());
    }

    TVector<TString> ParseTypes(NJson::TJsonValue json) {
        return ParseNames(json.GetArraySafe());
    }

    TVector<TString> ParseFunctions(NJson::TJsonValue json) {
        return ParseNames(json.GetArraySafe());
    }

    TVector<TString> ParseUdfs(NJson::TJsonValue json) {
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

    // TODO(YQL-19747): support multiple systems, name service/set hierarchy - common & special
    THashMap<EStatementKind, TVector<TString>> ParseHints(NJson::TJsonValue json) {
        THashMap<EStatementKind, TVector<TString>> hints;

        THashMap<EStatementKind, TString> StatementNames = {
            {EStatementKind::Select, "read"},
            {EStatementKind::Insert, "insert"},
        };

        for (const auto& [k, kname] : StatementNames) {
            for (auto& [_, values] : json.GetMapSafe().at(kname).GetMapSafe()) {
                for (auto& name : ParseNames(values.GetMapSafe().at("hints").GetArraySafe())) {
                    hints[k].emplace_back(std::move(name));
                }
            }
        }

        for (auto& [_, hints] : hints) {
            for (auto& hint : hints) {
                hint = ToUpperUTF8(hint);
            }
        }

        return hints;
    }

    TNameSet LoadDefaultNameSet() {
        return {
            .Pragmas = ParsePragmas(LoadJsonResource("pragmas_opensource.json")),
            .Types = ParseTypes(LoadJsonResource("types.json")),
            .Functions = Merge(
                ParseFunctions(LoadJsonResource("sql_functions.json")),
                ParseUdfs(LoadJsonResource("udfs_basic.json"))),
            .Hints = ParseHints(LoadJsonResource("statements_opensource.json")),
        };
    }

    TNameSet MakeDefaultNameSet() {
        return Pruned(LoadDefaultNameSet(), LoadFrequencyData());
    }

} // namespace NSQLComplete
