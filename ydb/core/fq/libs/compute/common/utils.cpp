#include "utils.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/yson/json2yson.h>

namespace NFq {

using TAggregates = std::map<TString, std::optional<ui64>>;

struct TTotalStatistics {
    ui64 MaxMemoryUsage = 0;
    ui64 CpuTimeUs = 0;
    ui64 SourceCpuTimeUs = 0;
    ui64 TotalInputRows = 0;
    ui64 TotalInputBytes = 0;
    ui64 TotalOutputRows = 0;
    ui64 TotalOutputBytes = 0;
    ui64 TotalIngressBytes = 0;
    ui64 TotalEgressBytes = 0;
    TAggregates Aggregates;
};

TString FormatDurationMs(ui64 durationMs) {
    TStringBuilder builder;

    if (durationMs && durationMs < 100) {
        builder << durationMs << "ms";
    } else {
        auto seconds = durationMs / 1'000;
        if (seconds >= 60) {
            auto minutes = seconds / 60;
            if (minutes >= 60) {
                auto hours = minutes / 60;
                builder << hours << 'h';
                if (hours < 24) {
                    auto minutes60 = minutes % 60;
                    builder << ' ';
                    if (minutes60 < 10) {
                        builder << '0';
                    }
                    builder << minutes60 << 'm';
                }
            } else {
                auto seconds60 = seconds % 60;
                builder << minutes << "m ";
                if (seconds60 < 10) {
                    builder << '0';
                }
                builder << seconds60 << 's';
            }
        } else {
            auto hundredths = (durationMs % 1'000) / 10;
            builder << seconds << '.';
            if (hundredths < 10) {
                builder << '0';
            }
            builder << hundredths << 's';
        }
    }

    return builder;
}

TString FormatDurationUs(ui64 durationUs) {
    if (durationUs && durationUs < 1000) {
        return TStringBuilder() << durationUs << "us";
    }

    return FormatDurationMs(durationUs / 1000);
}

TString FormatInstant(TInstant instant) {
    TStringBuilder builder;
    builder << instant.FormatLocalTime("%H:%M:%S.");
    auto msd = (instant.MilliSeconds() % 1000) / 10;
    if (msd < 10) {
        builder << '0';
    }
    builder << msd << 's';
    return builder;
}

void WriteNamedNode(NYson::TYsonWriter& writer, NJson::TJsonValue& node, const TString& name, TTotalStatistics& totals) {
    switch (node.GetType()) {
        case NJson::JSON_INTEGER:
        case NJson::JSON_DOUBLE:
        case NJson::JSON_UINTEGER: 
            if (name) {
                auto sum = node.GetIntegerSafe();
                if (name == "TotalInputRows") {
                    totals.TotalInputRows += sum;
                } else if (name == "TotalInputBytes") {
                    totals.TotalInputBytes += sum;
                } else if (name == "TotalOutputRows") {
                    totals.TotalOutputRows += sum;
                } else if (name == "TotalOutputBytes") {
                    totals.TotalOutputBytes += sum;
                }
                writer.OnKeyedItem(name);
                writer.OnBeginMap();
                    writer.OnKeyedItem("sum");
                    if (name.EndsWith("Us")) {
                        writer.OnStringScalar(FormatDurationUs(sum));
                    } else if (name.EndsWith("Ms")) {
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(sum)));
                    } else {
                        writer.OnInt64Scalar(sum);
                    }
                    writer.OnKeyedItem("count");
                    writer.OnInt64Scalar(1);
                writer.OnEndMap();
            }
            break;
        case NJson::JSON_ARRAY:
            for (auto item : node.GetArray()) {
                if (auto* subNode = item.GetValueByPath("Name")) {
                    WriteNamedNode(writer, item, name + "=" + subNode->GetStringSafe(), totals);
                }
                if (name == "Ingress") {
                    if (auto* ingressNode = item.GetValueByPath("Ingress.Bytes.Sum")) {
                        totals.TotalIngressBytes += ingressNode->GetIntegerSafe();
                    }
                } else if (name == "Egress") {
                    if (auto* egressNode = item.GetValueByPath("Egress.Bytes.Sum")) {
                        totals.TotalEgressBytes += egressNode->GetIntegerSafe();
                    }
                }
            }
            break;
        case NJson::JSON_MAP: {
            std::optional<ui64> count;
            std::optional<ui64> sum;
            std::optional<ui64> min;
            std::optional<ui64> max;

            if (auto* subNode = node.GetValueByPath("Count")) {
                count = subNode->GetIntegerSafe();
                if (*count <= 1) {
                    *count = 1;
                }
            }
            if (auto* subNode = node.GetValueByPath("Sum")) {
                sum = subNode->GetIntegerSafe();
                if (name == "MaxMemoryUsage") {
                    totals.MaxMemoryUsage += *sum;
                } else if (name == "CpuTimeUs") {
                    totals.CpuTimeUs += *sum;
                } else if (name == "SourceCpuTimeUs") {
                    totals.SourceCpuTimeUs += *sum;
                }
            }
            if (auto* subNode = node.GetValueByPath("Min")) {
                min = subNode->GetIntegerSafe();
            }
            if (auto* subNode = node.GetValueByPath("Max")) {
                max = subNode->GetIntegerSafe();
            }

            if (count || sum || min || max) {
                writer.OnKeyedItem(name);
                writer.OnBeginMap();
                if (name.EndsWith("Us")) { // TDuration
                    if (sum) {
                        writer.OnKeyedItem("sum");
                        writer.OnStringScalar(FormatDurationUs(*sum));
                    }
                    if (count) {
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(*count);
                    }
                    if (sum && count && *count) {
                        writer.OnKeyedItem("avg");
                        writer.OnStringScalar(FormatDurationUs(*sum / *count));
                    }
                    if (max) {
                        writer.OnKeyedItem("max");
                        writer.OnStringScalar(FormatDurationUs(*max));
                    }
                    if (min) {
                        writer.OnKeyedItem("min");
                        writer.OnStringScalar(FormatDurationUs(*min));
                    }
                } else if (name.EndsWith("Ms")) { // TInstant
                    if (sum) { // sum of timestamps has no meaning
                        writer.OnKeyedItem("avg");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds((count && *count) ? (*sum / *count) : *sum)));
                    } 
                    if (min && max) { // render duration here
                        writer.OnKeyedItem("sum");
                        writer.OnStringScalar(FormatDurationMs(*max - *min));
                    }
                    if (count) {
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(*count);
                    }
                    if (max) {
                        writer.OnKeyedItem("max");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(*max)));
                    }
                    if (min) {
                        writer.OnKeyedItem("min");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(*min)));
                    }
                } else {
                    if (sum) {
                        writer.OnKeyedItem("sum");
                        writer.OnInt64Scalar(*sum);
                    }
                    if (count) {
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(*count);
                    }
                    if (sum && count && *count) {
                        writer.OnKeyedItem("avg");
                        writer.OnInt64Scalar(*sum / *count);
                    }
                    if (max) {
                        writer.OnKeyedItem("max");
                        writer.OnInt64Scalar(*max);
                    }
                    if (min) {
                        writer.OnKeyedItem("min");
                        writer.OnInt64Scalar(*min);
                    }
                }
                writer.OnEndMap();
            } else {
                if (name) {
                    writer.OnKeyedItem(name);
                    writer.OnBeginMap();
                }
                for (auto& [key, value] : node.GetMapSafe()) {
                    WriteNamedNode(writer, value, key, totals);
                }
                if (name) {
                    writer.OnEndMap();
                }
            }
            break;
        }
        default:
            break;
    }
}

void EnumeratePlans(NYson::TYsonWriter& writer, NJson::TJsonValue& value, ui32& stageViewIndex, TTotalStatistics& totals) {
    if (auto* subNode = value.GetValueByPath("Plans")) {
        for (auto plan : subNode->GetArray()) {
            EnumeratePlans(writer, plan, stageViewIndex, totals);
        }
    }
    if (auto* statNode = value.GetValueByPath("Stats")) {

        TStringBuilder builder;
        stageViewIndex++;
        if (stageViewIndex < 10) {
            builder << '0';
        }
        builder << stageViewIndex;
        if (auto* idNode = value.GetValueByPath("PlanNodeId")) {
            builder << '_' << idNode->GetIntegerSafe();
        }
        if (auto* typeNode = value.GetValueByPath("Node Type")) {
            builder << '_' << typeNode->GetStringSafe();
        }

        writer.OnKeyedItem(builder);
        writer.OnBeginMap();
            WriteNamedNode(writer, *statNode, "", totals);
        writer.OnEndMap();
    }
}

TString GetV1StatFromV2Plan(const TString& plan) {
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
                        auto nodeType = typeNode->GetStringSafe();
                        TTotalStatistics totals;
                        ui32 stageViewIndex = 0;
                        writer.OnKeyedItem(nodeType);
                        writer.OnBeginMap();
                        EnumeratePlans(writer, plan, stageViewIndex, totals);
                        if (totals.MaxMemoryUsage) {
                            writer.OnKeyedItem("MaxMemoryUsage");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.MaxMemoryUsage);
                            writer.OnEndMap();
                        }
                        if (totals.CpuTimeUs) {
                            writer.OnKeyedItem("CpuTimeUs");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnStringScalar(FormatDurationUs(totals.CpuTimeUs));
                            writer.OnEndMap();
                        }
                        if (totals.SourceCpuTimeUs) {
                            writer.OnKeyedItem("SourceCpuTimeUs");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnStringScalar(FormatDurationUs(totals.SourceCpuTimeUs));
                            writer.OnEndMap();
                        }
                        if (totals.TotalInputRows) {
                            writer.OnKeyedItem("TotalInputRows");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalInputRows);
                            writer.OnEndMap();
                        }
                        if (totals.TotalInputBytes) {
                            writer.OnKeyedItem("TotalInputBytes");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalInputBytes);
                            writer.OnEndMap();
                        }
                        if (totals.TotalOutputRows) {
                            writer.OnKeyedItem("TotalOutputRows");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalOutputRows);
                            writer.OnEndMap();
                        }
                        if (totals.TotalOutputBytes) {
                            writer.OnKeyedItem("TotalOutputBytes");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalOutputBytes);
                            writer.OnEndMap();
                        }
                        if (totals.TotalIngressBytes) {
                            writer.OnKeyedItem("TotalIngressBytes");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalIngressBytes);
                            writer.OnEndMap();
                        }
                        if (totals.TotalEgressBytes) {
                            writer.OnKeyedItem("TotalEgressBytes");
                            writer.OnBeginMap();
                                writer.OnKeyedItem("sum");
                                writer.OnInt64Scalar(totals.TotalEgressBytes);
                            writer.OnEndMap();
                        }
                        writer.OnEndMap();
                    }
                }
            }
        }
    }
    writer.OnEndMap();
    return NJson2Yson::ConvertYson2Json(out.Str());
}

std::optional<ui64> WriteMetric(NYson::TYsonWriter& writer, NJson::TJsonValue& node, const TString& column, const TString& name, const TString& tag) {
    std::optional<ui64> value;
    if (auto* subNode = node.GetValueByPath(name)) {
        auto t = tag;
        if (t == "") {
            if (column == "first" || column == "pause") t = "Min";
            else if (column == "resume" || column == "last") t = "Max";
            else t = "Sum";
        }

        if (t == "Avg") {
            if (auto* metricNode = subNode->GetValueByPath("Sum")) {
                value = metricNode->GetIntegerSafe();
                if (auto* metricNode = subNode->GetValueByPath("Count")) {
                    auto count = metricNode->GetIntegerSafe();
                    if (count) {
                        *value /= count;
                    }
                }
            }
        } else if (auto* metricNode = subNode->GetValueByPath(t)) {
            value = metricNode->GetIntegerSafe();
        }

        if (value) {
            writer.OnKeyedItem(column);
            if (t == "Count") {
                writer.OnInt64Scalar(*value);
            } else {
                if (name.EndsWith("Us")) {
                    writer.OnStringScalar(FormatDurationUs(*value));
                } else if (name.EndsWith("Ms")) {
                    writer.OnStringScalar(t == "Sum" ? "-" : FormatInstant(TInstant::MilliSeconds(*value)));
                } else {
                    writer.OnInt64Scalar(*value);
                }
            }
        }
    }
    return value;
}

std::vector<std::pair<TString, TString>> columns = {
    std::make_pair<TString, TString>("id", ""), 
    std::make_pair<TString, TString>("cpu", ""), 
    std::make_pair<TString, TString>("scpu", ""), 
    std::make_pair<TString, TString>("mem", ""), 
    std::make_pair<TString, TString>("first", "FirstMessageMs"),
    std::make_pair<TString, TString>("pause", "PauseMessageMs"),
    std::make_pair<TString, TString>("resume", "PauseMessageMs"),
    std::make_pair<TString, TString>("last", "LastMessageMs"),
    std::make_pair<TString, TString>("active", "ActiveTimeUs"),
    std::make_pair<TString, TString>("wait", "WaitTimeUs"),
    std::make_pair<TString, TString>("bytes", "Bytes"),
    std::make_pair<TString, TString>("rows", "Rows")
};

void WriteAggregates(NYson::TYsonWriter& writer, TAggregates& aggregates) {
    for (auto& p : aggregates) {
        if (*p.second) {
            writer.OnKeyedItem(p.first);
            if (p.first == "first" || p.first == "pause" || p.first == "resume" || p.first == "last") {
                writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(*p.second)));
            } else if (p.first == "active" || p.first == "wait") {
                writer.OnStringScalar(FormatDurationUs(*p.second));
            } else {
                writer.OnInt64Scalar(*p.second);
            }
        }
    }
}

void MergeAggregates(TAggregates& parentAggregates, TAggregates& aggregates) {
    for (auto& p : aggregates) {
        if (*p.second) {
            auto& aggr = parentAggregates[p.first];
            if (!aggr) {
                aggr = *p.second;
            } else if (p.first == "first" || p.first == "pause") {
                aggr = std::min(*aggr, *p.second);
            } else if (p.first == "resume" || p.first == "last") {
                aggr = std::max(*aggr, *p.second);
            } else {
                *aggr += *p.second;
            }
        }
    }
}

void WriteAsyncStatNode(NYson::TYsonWriter& writer, NJson::TJsonValue& node, const TString& name, TAggregates& aggregates) {
        writer.OnKeyedItem(name);
        writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    auto value = WriteMetric(writer, node, p.first, p.second, "");
                    if (value) {
                        auto& aggr = aggregates[p.first];
                        if (!aggr) {
                            aggr = *value;
                        } else if (p.first == "first" || p.first == "pause") {
                            aggr = std::min(*aggr, *value);
                        } else if (p.first == "resume" || p.first == "last") {
                            aggr = std::max(*aggr, *value);
                        } else {
                            *aggr += *value;
                        }
                    }
                }
            }
            writer.OnKeyedItem("1_Min");
            writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    WriteMetric(writer, node, p.first, p.second, "Min");
                }
            }
            writer.OnEndMap();
            writer.OnKeyedItem("2_Avg");
            writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    WriteMetric(writer, node, p.first, p.second, "Avg");
                }
            }
            writer.OnEndMap();
            writer.OnKeyedItem("3_Max");
            writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    WriteMetric(writer, node, p.first, p.second, "Max");
                }
            }
            writer.OnEndMap();
            writer.OnKeyedItem("4_Sum");
            writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    WriteMetric(writer, node, p.first, p.second, "Sum");
                }
            }
            writer.OnEndMap();
            writer.OnKeyedItem("5_Count");
            writer.OnBeginMap();
            for (auto& p : columns) {
                if (p.second) {
                    WriteMetric(writer, node, p.first, p.second, "Count");
                }
            }
            writer.OnEndMap();
        writer.OnEndMap();
}

void WriteAsyncIoNode(NYson::TYsonWriter& writer, NJson::TJsonValue& node, const TString& prefix, TAggregates& parentAggregates) {
    if (node.GetType() == NJson::JSON_ARRAY) {
        for (auto item : node.GetArray()) {
            if (auto* subNode = item.GetValueByPath("Name")) {
                writer.OnKeyedItem(prefix + "_" + subNode->GetStringSafe());
                writer.OnBeginMap();
                TAggregates aggregates;
                if (auto* subNode = item.GetValueByPath("Ingress")) {
                    WriteAsyncStatNode(writer, *subNode, "1_Ingress", aggregates);
                }
                if (auto* subNode = item.GetValueByPath("Push")) {
                    WriteAsyncStatNode(writer, *subNode, "2_Push", aggregates);
                }
                if (auto* subNode = item.GetValueByPath("Pop")) {
                    WriteAsyncStatNode(writer, *subNode, "3_Pop", aggregates);
                }
                if (auto* subNode = item.GetValueByPath("Egress")) {
                    WriteAsyncStatNode(writer, *subNode, "4_Egress", aggregates);
                }
                WriteAggregates(writer, aggregates);
                MergeAggregates(parentAggregates, aggregates);
                writer.OnEndMap();
            }
        }
    }
}

void EnumeratePlansV2(NYson::TYsonWriter& writer, NJson::TJsonValue& value, ui32& stageViewIndex, TTotalStatistics& totals) {
    if (auto* subNode = value.GetValueByPath("Plans")) {
        for (auto plan : subNode->GetArray()) {
            EnumeratePlansV2(writer, plan, stageViewIndex, totals);
        }
    }
    if (auto* statNode = value.GetValueByPath("Stats")) {

        TStringBuilder builder;
        stageViewIndex++;
        if (stageViewIndex < 10) {
            builder << '0';
        }
        builder << stageViewIndex;
        if (auto* idNode = value.GetValueByPath("PlanNodeId")) {
            builder << '_' << idNode->GetIntegerSafe();
        }
        if (auto* typeNode = value.GetValueByPath("Node Type")) {
            builder << '_' << typeNode->GetStringSafe();
        }

        writer.OnKeyedItem(builder);
        writer.OnBeginMap();
            TAggregates aggregates;
            if (auto* subNode = statNode->GetValueByPath("Ingress")) {
                WriteAsyncIoNode(writer, *subNode, "1_Ingress", aggregates);
            }
            if (auto* subNode = statNode->GetValueByPath("Push")) {
                WriteAsyncIoNode(writer, *subNode, "2_Push", aggregates);
            }
            if (auto* subNode = statNode->GetValueByPath("Pop")) {
                WriteAsyncIoNode(writer, *subNode, "3_Pop", aggregates);
            }
            if (auto* subNode = statNode->GetValueByPath("Egress")) {
                WriteAsyncIoNode(writer, *subNode, "4_Egress", aggregates);
            }
            WriteAggregates(writer, aggregates);
            MergeAggregates(totals.Aggregates, aggregates);
            if (auto* subNode = statNode->GetValueByPath("MaxMemoryUsage.Sum")) {
                auto sum = subNode->GetIntegerSafe();
                totals.MaxMemoryUsage += sum;
                writer.OnKeyedItem("mem");
                writer.OnInt64Scalar(sum);
            }
            if (auto* subNode = statNode->GetValueByPath("CpuTimeUs.Sum")) {
                auto sum = subNode->GetIntegerSafe();
                totals.CpuTimeUs += sum;
                writer.OnKeyedItem("cpu");
                writer.OnStringScalar(FormatDurationUs(sum));
            }
            if (auto* subNode = statNode->GetValueByPath("SourceCpuTimeUs.Sum")) {
                auto sum = subNode->GetIntegerSafe();
                totals.SourceCpuTimeUs += sum;
                writer.OnKeyedItem("scpu");
                writer.OnStringScalar(FormatDurationUs(sum));
            }
        writer.OnEndMap();
    }
}

TString GetV1StatFromV2PlanV2(const TString& plan) {
    TStringStream out;
    NYson::TYsonWriter writer(&out);
    writer.OnBeginMap();
 
    writer.OnKeyedItem("Columns");
    writer.OnBeginList();
    for (auto& p : columns) {
        writer.OnListItem();
        writer.OnStringScalar(p.first);
    }
    writer.OnEndList();
 
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    if (NJson::ReadJsonTree(plan, &jsonConfig, &stat)) {
        if (auto* topNode = stat.GetValueByPath("Plan")) {
            if (auto* subNode = topNode->GetValueByPath("Plans")) {
                for (auto plan : subNode->GetArray()) {
                    if (auto* typeNode = plan.GetValueByPath("Node Type")) {
                        auto nodeType = typeNode->GetStringSafe();
                        TTotalStatistics totals;
                        ui32 stageViewIndex = 0;
                        writer.OnKeyedItem(nodeType);
                        writer.OnBeginMap();
                        EnumeratePlansV2(writer, plan, stageViewIndex, totals);
                        WriteAggregates(writer, totals.Aggregates);
                        if (totals.MaxMemoryUsage) {
                            writer.OnKeyedItem("mem");
                            writer.OnInt64Scalar(totals.MaxMemoryUsage);
                        }
                        if (totals.CpuTimeUs) {
                            writer.OnKeyedItem("cpu");
                            writer.OnStringScalar(FormatDurationUs(totals.CpuTimeUs));
                        }
                        if (totals.SourceCpuTimeUs) {
                            writer.OnKeyedItem("scpu");
                            writer.OnStringScalar(FormatDurationUs(totals.SourceCpuTimeUs));
                        }
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
