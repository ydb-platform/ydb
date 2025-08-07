#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    constexpr struct {
        struct {
            const char* Parent = "parent";
            const char* Rule = "rule";
            const char* Sum = "sum";
        } Key;
        struct {
            const char* Pragma = "PRAGMA";
            const char* Type = "TYPE";
            const char* Func = "FUNC";
            const char* Keyword = "KEYWORD";
            const char* Module = "MODULE";
            const char* ModuleFunc = "MODULE_FUNC";
            const char* ReadHint = "READ_HINT";
            const char* InsertHint = "INSERT_HINT";
        } Parent;
    } Json;

    struct TFrequencyItem {
        TString Parent;
        TString Rule;
        size_t Sum;

        static TFrequencyItem ParseJsonMap(NJson::TJsonValue::TMapType&& json) {
            return {
                .Parent = json.at(Json.Key.Parent).GetStringSafe(),
                .Rule = json.at(Json.Key.Rule).GetStringSafe(),
                .Sum = json.at(Json.Key.Sum).GetUIntegerSafe(),
            };
        }

        static TVector<TFrequencyItem> ParseListFromJsonArray(NJson::TJsonValue::TArray& json) {
            TVector<TFrequencyItem> items;
            items.reserve(json.size());
            for (auto& element : json) {
                auto item = TFrequencyItem::ParseJsonMap(std::move(element.GetMapSafe()));
                items.emplace_back(std::move(item));
            }
            return items;
        }

        static TVector<TFrequencyItem> ParseListFromJsonText(TStringBuf text) {
            NJson::TJsonValue json = NJson::ReadJsonFastTree(text);
            return ParseListFromJsonArray(json.GetArraySafe());
        }
    };

    TFrequencyData Collect(const TVector<TFrequencyItem>& items) {
        TFrequencyData data;
        for (auto& item : items) {
            if (item.Parent == Json.Parent.Pragma) {
                data.Pragmas[item.Rule] += item.Sum;
            } else if (item.Parent == Json.Parent.Type) {
                data.Types[item.Rule] += item.Sum;
            } else if (item.Parent == Json.Parent.Keyword) {
                data.Keywords[item.Rule] += item.Sum;
            } else if (item.Parent == Json.Parent.Module) {
                // Ignore, unsupported: Modules
            } else if (item.Parent == Json.Parent.Func ||
                       item.Parent == Json.Parent.ModuleFunc) {
                data.Functions[item.Rule] += item.Sum;
            } else if (item.Parent == Json.Parent.ReadHint ||
                       item.Parent == Json.Parent.InsertHint) {
                data.Hints[item.Rule] += item.Sum;
            } else {
                // Ignore, unsupported: Parser Call Stacks
            }
        }
        return data;
    }

    THashMap<TString, size_t> PrunedBy(const THashMap<TString, size_t>& data, auto normalize) {
        THashMap<TString, size_t> pruned;
        for (const auto& [name, count] : data) {
            pruned[normalize(name)] += count;
        }
        return pruned;
    }

    TFrequencyData PrunedBy(const TFrequencyData& data, auto normalize) {
        return {
            .Keywords = PrunedBy(data.Keywords, normalize),
            .Pragmas = PrunedBy(data.Pragmas, normalize),
            .Types = PrunedBy(data.Types, normalize),
            .Functions = PrunedBy(data.Functions, normalize),
            .Hints = PrunedBy(data.Hints, normalize),
        };
    }

    TFrequencyData Pruned(const TFrequencyData& data) {
        return PrunedBy(data, [](TStringBuf s) {
            return NormalizeName(s);
        });
    }

    TFrequencyData ParseJsonFrequencyData(TStringBuf text) {
        return Collect(TFrequencyItem::ParseListFromJsonText(text));
    }

    TFrequencyData LoadFrequencyData() {
        TString text;
        Y_ENSURE(NResource::FindExact("rules_corr_basic.json", &text));
        return ParseJsonFrequencyData(text);
    }

} // namespace NSQLComplete
