#include "name_service.h"

#include "frequency.h"
#include "name_index.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/charset/utf8.h>

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

    TVector<TString> Pruned(TVector<TString> names, const THashMap<TString, size_t>& frequency) {
        THashMap<TString, TVector<std::tuple<TString, size_t>>> groups;

        for (auto& [normalized, original] : BuildNameIndex(std::move(names), NormalizeName)) {
            size_t freq = 0;
            if (const size_t* it = frequency.FindPtr(original)) {
                freq = *it;
            }
            groups[normalized].emplace_back(std::move(original), freq);
        }

        for (auto& [_, group] : groups) {
            Sort(group, [](const auto& lhs, const auto& rhs) {
                return std::get<1>(lhs) < std::get<1>(rhs);
            });
        }

        names = TVector<TString>();
        names.reserve(groups.size());
        for (auto& [_, group] : groups) {
            Y_ASSERT(!group.empty());
            names.emplace_back(std::move(std::get<0>(group.back())));
        }
        return names;
    }

    NameSet Pruned(NameSet names) {
        auto frequency = LoadFrequencyDataForPrunning();
        names.Pragmas = Pruned(std::move(names.Pragmas), frequency.Pragmas);
        names.Types = Pruned(std::move(names.Types), frequency.Types);
        names.Functions = Pruned(std::move(names.Functions), frequency.Functions);
        for (auto& [k, h] : names.Hints) {
            h = Pruned(h, frequency.Hints);
        }
        return names;
    }

    NameSet MakeDefaultNameSet() {
        return Pruned({
            .Pragmas = ParsePragmas(LoadJsonResource("pragmas_opensource.json")),
            .Types = ParseTypes(LoadJsonResource("types.json")),
            .Functions = Merge(
                ParseFunctions(LoadJsonResource("sql_functions.json")),
                ParseUdfs(LoadJsonResource("udfs_basic.json"))),
            .Hints = ParseHints(LoadJsonResource("statements_opensource.json")),
        });
    }

} // namespace NSQLComplete
