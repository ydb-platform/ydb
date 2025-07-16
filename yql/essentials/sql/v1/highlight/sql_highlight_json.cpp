#include "sql_highlight_json.h"

#include <util/string/cast.h>

namespace NSQLHighlight {

    struct {
        const char* Units = "units";
        struct {
            const char* Kind = "kind";
            const char* Patterns = "patterns";
            const char* PatternsANSI = "patterns-ansi";
        } Unit;
        struct {
            const char* Body = "body";
            const char* After = "after";
            const char* IsCaseInsensitive = "is-case-insensitive";
        } Pattern;
    } JsonKey;

    NJson::TJsonValue ToJson(const NSQLTranslationV1::TRegexPattern& pattern) {
        NJson::TJsonMap map;
        map[JsonKey.Pattern.Body] = pattern.Body;
        if (!pattern.After.empty()) {
            map[JsonKey.Pattern.After] = pattern.After;
        }
        if (pattern.IsCaseInsensitive) {
            map[JsonKey.Pattern.IsCaseInsensitive] = pattern.IsCaseInsensitive;
        }
        return map;
    }

    NJson::TJsonValue ToJson(const TVector<NSQLTranslationV1::TRegexPattern>& patterns) {
        NJson::TJsonArray array;
        for (const auto& pattern : patterns) {
            array.AppendValue(ToJson(pattern));
        }
        return array;
    }

    NJson::TJsonValue ToJson(const TUnit& unit) {
        NJson::TJsonMap map;
        map[JsonKey.Unit.Kind] = ToString(unit.Kind);
        if (!unit.Patterns.empty()) {
            map[JsonKey.Unit.Patterns] = ToJson(unit.Patterns);
        }
        if (!unit.PatternsANSI.Empty()) {
            map[JsonKey.Unit.PatternsANSI] = ToJson(*unit.PatternsANSI);
        }
        return map;
    }

    NJson::TJsonValue ToJson(const TVector<TUnit>& units) {
        NJson::TJsonArray array;
        for (const auto& unit : units) {
            array.AppendValue(ToJson(unit));
        }
        return array;
    }

    NJson::TJsonValue ToJson(const THighlighting& highlighting) {
        NJson::TJsonMap map;
        map[JsonKey.Units] = ToJson(highlighting.Units);
        return map;
    }

} // namespace NSQLHighlight
