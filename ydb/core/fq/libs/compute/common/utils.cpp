#include "utils.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/yson/json2yson.h>

namespace NFq {

void WriteNamedNode(NYson::TYsonWriter& writer, NJson::TJsonValue& node, const TString& name) {
    switch (node.GetType()) {
        case NJson::JSON_INTEGER:
        case NJson::JSON_DOUBLE:
        case NJson::JSON_UINTEGER: 
            if (name) {
                auto v = node.GetIntegerRobust();
                writer.OnKeyedItem(name);
                writer.OnBeginMap();
                    writer.OnKeyedItem("sum");
                    writer.OnInt64Scalar(v);
                    writer.OnKeyedItem("count");
                    writer.OnInt64Scalar(1);
                    writer.OnKeyedItem("avg");
                    writer.OnInt64Scalar(v);
                    writer.OnKeyedItem("max");
                    writer.OnInt64Scalar(v);
                    writer.OnKeyedItem("min");
                    writer.OnInt64Scalar(v);
                writer.OnEndMap();
            }
            break;
        case NJson::JSON_ARRAY:
            for (auto item : node.GetArray()) {
                if (auto* subNode = item.GetValueByPath("Name")) {
                    WriteNamedNode(writer, item, name + "=" + subNode->GetStringRobust());
                }
            }
            break;
        case NJson::JSON_MAP:
            if (auto* subNode = node.GetValueByPath("Sum")) {
                auto sum = subNode->GetIntegerRobust();
                auto count = 1;
                if (auto* subNode = node.GetValueByPath("Count")) {
                    count = subNode->GetIntegerRobust();
                    if (count <= 1) {
                        count = 1;
                    }
                }
                auto min = sum;
                if (auto* subNode = node.GetValueByPath("Min")) {
                    min = subNode->GetIntegerRobust();
                }
                auto max = sum;
                if (auto* subNode = node.GetValueByPath("Max")) {
                    max = subNode->GetIntegerRobust();
                }
                writer.OnKeyedItem(name);
                writer.OnBeginMap();
                    writer.OnKeyedItem("sum");
                    writer.OnInt64Scalar(sum);
                    writer.OnKeyedItem("count");
                    writer.OnInt64Scalar(count);
                    writer.OnKeyedItem("avg");
                    writer.OnInt64Scalar(sum / count);
                    writer.OnKeyedItem("max");
                    writer.OnInt64Scalar(max);
                    writer.OnKeyedItem("min");
                    writer.OnInt64Scalar(min);
                writer.OnEndMap();
            } else {
                if (name) {
                    writer.OnKeyedItem(name);
                    writer.OnBeginMap();
                }
                for (auto& [key, value] : node.GetMapSafe()) {
                    WriteNamedNode(writer, value, key);
                }
                if (name) {
                    writer.OnEndMap();
                }
            }
            break;
        default:
            break;
    }
}

void EnumeratePlans(NYson::TYsonWriter& writer, NJson::TJsonValue& value) {
    if (auto* subNode = value.GetValueByPath("Plans")) {
        for (auto plan : subNode->GetArray()) {
            EnumeratePlans(writer, plan);
        }
    }
    if (auto* statNode = value.GetValueByPath("Stats")) {
        TString nodeType = "";
        if (auto* typeNode = value.GetValueByPath("Node Type")) {
            nodeType = TString("_") + typeNode->GetStringRobust();
        }
        ui64 nodeId = 0;
        if (auto* idNode = value.GetValueByPath("PlanNodeId")) {
            nodeId = idNode->GetIntegerRobust();
        }
        writer.OnKeyedItem("Stage_" + ToString(nodeId) + nodeType);
        writer.OnBeginMap();
            WriteNamedNode(writer, *statNode, "");
        writer.OnEndMap();
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
        if (auto* topNode = stat.GetValueByPath("Plan")) {
            if (auto* subNode = topNode->GetValueByPath("Plans")) {
                for (auto plan : subNode->GetArray()) {
                    if (auto* typeNode = plan.GetValueByPath("Node Type")) {
                        auto nodeType = typeNode->GetStringRobust();
                        writer.OnKeyedItem(nodeType);
                        writer.OnBeginMap();
                        EnumeratePlans(writer, plan);
                        writer.OnEndMap();
                    }
                }
            }
        }
    }
    writer.OnEndMap();
    return NJson2Yson::ConvertYson2Json(out.Str());
}

} // namespace NFq
