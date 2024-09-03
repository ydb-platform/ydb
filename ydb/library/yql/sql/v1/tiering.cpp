#include "tiering.h"

#include <util/stream/str.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NSQLTranslationV1 {

TString SerializeTieringRules(const std::vector<TTieringRule>& input) {
    NJson::TJsonValue tieringRules;
    for (const TTieringRule& inputRule : input) {
        NJson::TJsonValue tieringRule;
        tieringRule.InsertValue("tierName", inputRule.TierName);
        tieringRule.InsertValue("durationForEvict", inputRule.SerializedDurationForEvict);
        tieringRules.AppendValue(tieringRule);
    }
    
    NJson::TJsonValue result;
    result.InsertValue("rules", tieringRules);

    return WriteJson(result, false);
}

}
