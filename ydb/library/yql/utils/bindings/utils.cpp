#include "utils.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/yson/writer.h>

namespace NYql {

namespace {

TString SerializeJsonValueAsYsonText(const NJson::TJsonValue& inputValue) {
    TString result;
    TStringOutput resultStream(result);
    NYson::TYsonWriter ysonWriter(&resultStream, NYson::EYsonFormat::Pretty, ::NYson::EYsonType::Node, false);
    NJson2Yson::SerializeJsonValueAsYson(inputValue, &ysonWriter);
    return result;
}

} /* namespace { */

void LoadBindings(THashMap<TString, NSQLTranslation::TTableBindingSettings>& dst, TStringBuf jsonText)
{
    NJson::TJsonValue out;
    YQL_ENSURE(NJson::ReadJsonTree(jsonText, &out));
    YQL_ENSURE(out.IsMap());
    for (const auto& [bindingName, bindingJson] : out.GetMap()) {
        NSQLTranslation::TTableBindingSettings& binding = dst[bindingName];
        YQL_ENSURE(bindingJson.IsMap());
        
        for (const auto& [k, v] : bindingJson.GetMap()) {            
            if (k == "ClusterType") {
                YQL_ENSURE(v.IsString());
                binding.ClusterType = v.GetString();
            } else if (k == "schema") {
                binding.Settings["schema"] = SerializeJsonValueAsYsonText(v);
            } else if (k == "constraints") {
                binding.Settings["constraints"] = SerializeJsonValueAsYsonText(v);
            } else {
                YQL_ENSURE(v.IsString());
                binding.Settings.emplace(k, v.GetString());
            }
        }
    }
}

} /* namespace NYql */
