#include "utils.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/yson/json2yson.h>

namespace NFq {

void EnumeratePlans(NYson::TYsonWriter& writer, NJson::TJsonValue& value) {
    if (auto* subNode = value.GetValueByPath("Plans")) {
        ui32 index = 0;
        TString nodeType = "Unknown";
        if (auto* subNode = value.GetValueByPath("PlanNodeType")) {
            nodeType = subNode->GetStringRobust();
        }
        for (auto plan : subNode->GetArray()) {
            writer.OnKeyedItem(nodeType + "." + ToString(index++));
            writer.OnBeginMap();
            EnumeratePlans(writer, plan);
            if (auto* subNode = plan.GetValueByPath("Stats")) {
                for (auto& [key, value] : subNode->GetMapSafe()) {
                    auto v = value.GetIntegerRobust();
                    writer.OnKeyedItem(key);
                    writer.OnBeginMap();
                        writer.OnKeyedItem("sum");
                        writer.OnInt64Scalar(v);
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(v);
                        writer.OnKeyedItem("avg");
                        writer.OnInt64Scalar(v);
                        writer.OnKeyedItem("max");
                        writer.OnInt64Scalar(v);
                        writer.OnKeyedItem("min");
                        writer.OnInt64Scalar(v);
                    writer.OnEndMap();
                }
            }
            writer.OnEndMap();
        }
    }
}

TString GetV1StatFromV2Plan(const TString& plan) {
    Y_UNUSED(plan);
    TStringStream out;
    NYson::TYsonWriter writer(&out);
    writer.OnBeginMap();
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    if (NJson::ReadJsonTree(plan, &jsonConfig, &stat)) {
        if (auto* subNode = stat.GetValueByPath("Plan")) {
            EnumeratePlans(writer, *subNode);
        }
    }
    writer.OnEndMap();
    auto s = NJson2Yson::ConvertYson2Json(out.Str());
    return s;
}

} // namespace NFq
