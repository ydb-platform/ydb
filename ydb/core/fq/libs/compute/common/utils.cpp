#include "utils.h"
#include "plan2svg.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NFq {

using TAggregates = std::map<TString, std::optional<ui64>>;

void WriteValue(NYson::TYsonWriter& writer, const TString& name, ui64 value) {
    if (name.EndsWith("Us")) {
        writer.OnStringScalar(FormatDurationUs(value));
    } else if (name.EndsWith("Ms")) {
        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(value)));
    } else {
        writer.OnInt64Scalar(value);
    }
}

// TODO Dedup Code with KQP and YQL
struct TAggregate {
    ui64 Min = 0;
    ui64 Max = 0;
    ui64 Sum = 0;
    ui64 Count = 0;
    void Add(ui64 value) {
        if (Count) {
            if (Min > value) {
                Min = value;
            }
            if (Max < value) {
                Max = value;
            }
            Sum += value;
        } else {
            Min = Max = Sum = value;
        }
        Count++;
    }
    
    void Write(NYson::TYsonWriter& writer, const TString& keyName) {
        if (Count) {
            writer.OnKeyedItem(keyName);
            writer.OnBeginMap();
                writer.OnKeyedItem("min");
                WriteValue(writer, keyName, Min);
                writer.OnKeyedItem("max");
                WriteValue(writer, keyName, Max);
                writer.OnKeyedItem("avg");
                WriteValue(writer, keyName, Sum / Count);
                writer.OnKeyedItem("sum");
                WriteValue(writer, keyName, Sum);
                writer.OnKeyedItem("count");
                writer.OnInt64Scalar(Count);
            writer.OnEndMap();
        }
    }
};

struct TTotalStatistics {
    TAggregate MaxMemoryUsage;
    TAggregate CpuTimeUs;
    TAggregate SourceCpuTimeUs;
    TAggregate InputBytes;
    TAggregate InputRows;
    TAggregate OutputBytes;
    TAggregate OutputRows;
    TAggregate ResultBytes;
    TAggregate ResultRows;
    TAggregate IngressBytes;
    TAggregate IngressDecompressedBytes;
    TAggregate IngressRows;
    TAggregate EgressBytes;
    TAggregate EgressRows;
    TAggregate Tasks;
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

namespace detail {

struct TDurationParser {
    constexpr static bool IsDigit(char c) noexcept {
        return '0' <= c && c <= '9';
    }

    constexpr std::string_view ConsumeLastFraction() noexcept {
        ConsumeWhitespace();
        if (Src.empty()) {
            return Src;
        }

        auto it = Src.end() - 1;
        while (true) {
            // we rely on non-empty number before fraction
            if (IsDigit(*it) || it == Src.begin()) {
                ++it;
                break;
            }
            --it;
        }
        auto start = it - Src.begin();
        auto res = Src.substr(start);
        Src = Src.substr(0, start);
        return res;
    }

    constexpr ui32 ConsumeNumberPortion() noexcept {
        ui32 dec = 1;
        ui32 res = 0;
        while (!Src.empty() && IsDigit(Src.back())) {
            res += (Src.back() - '0') * dec;
            dec *= 10;
            Src.remove_suffix(1);
        }
        return res;
    }

    constexpr void ConsumeWhitespace() noexcept {
        while (!Src.empty() && Src.back() == ' ') {
            Src.remove_suffix(1);
        }
    }

    constexpr std::chrono::microseconds ParseDuration() {
        auto fraction = ConsumeLastFraction();
        if (fraction == "us") {
            return std::chrono::microseconds{ConsumeNumberPortion()};
        } else if (fraction == "ms") {
            return std::chrono::milliseconds{ConsumeNumberPortion()};
        }

        std::chrono::microseconds result{};
        if (fraction == "s") {
            auto part = ConsumeNumberPortion();
            if (!Src.empty() && Src.back() == '.') {
                // parsed milliseconds (cantiseconds actually)
                part *= 10;
                result += std::chrono::milliseconds(part);

                Src.remove_suffix(1);
                result += std::chrono::seconds(ConsumeNumberPortion());
            } else {
                result += std::chrono::seconds{part};
            }
            fraction = ConsumeLastFraction();
        }
        if (fraction == "m") {
            result += std::chrono::minutes{ConsumeNumberPortion()};
            fraction = ConsumeLastFraction();
        }

        if (fraction == "h") {
            result += std::chrono::hours{ConsumeNumberPortion()};
        }
        return result;
    }

    std::string_view Src;
};
}

TDuration ParseDuration(TStringBuf str) {
    return detail::TDurationParser{.Src = str}.ParseDuration();
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
                    totals.InputRows.Add(sum);
                } else if (name == "TotalInputBytes") {
                    totals.InputBytes.Add(sum);
                } else if (name == "TotalOutputRows") {
                    totals.OutputRows.Add(sum);
                } else if (name == "TotalOutputBytes") {
                    totals.OutputBytes.Add(sum);
                } else if (name == "Tasks") {
                    totals.Tasks.Add(sum);
                }
                writer.OnKeyedItem(name);
                writer.OnBeginMap();
                    writer.OnKeyedItem("sum");
                    WriteValue(writer, name, sum);
                    writer.OnKeyedItem("count");
                    writer.OnInt64Scalar(1);
                writer.OnEndMap();
            }
            break;
        case NJson::JSON_ARRAY: {
            for (auto item : node.GetArray()) {
                if (auto* subNode = item.GetValueByPath("Name")) {
                    WriteNamedNode(writer, item, name + "=" + subNode->GetStringSafe(), totals);
                }
            }
            break;
        }
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
                if (sum && *sum) {
                           if (name == "InputBytes") {
                        totals.InputBytes.Add(*sum);
                    } else if (name == "InputRows") {
                        totals.InputRows.Add(*sum);
                    } else if (name == "OutputBytes") {
                        totals.OutputBytes.Add(*sum);
                    } else if (name == "OutputRows") {
                        totals.OutputRows.Add(*sum);
                    } else if (name == "ResultBytes") {
                        totals.ResultBytes.Add(*sum);
                    } else if (name == "ResultRows") {
                        totals.ResultRows.Add(*sum);
                    } else if (name == "IngressBytes") {
                        totals.IngressBytes.Add(*sum);
                    } else if (name == "IngressDecompressedBytes") {
                        totals.IngressDecompressedBytes.Add(*sum);
                    } else if (name == "IngressRows") {
                        totals.IngressRows.Add(*sum);
                    } else if (name == "EgressBytes") {
                        totals.EgressBytes.Add(*sum);
                    } else if (name == "EgressRows") {
                        totals.EgressRows.Add(*sum);
                    } else if (name == "MaxMemoryUsage") {
                        totals.MaxMemoryUsage.Add(*sum);
                    } else if (name == "CpuTimeUs") {
                        totals.CpuTimeUs.Add(*sum);
                    } else if (name == "SourceCpuTimeUs") {
                        totals.SourceCpuTimeUs.Add(*sum);
                    } else if (name == "Tasks") {
                        totals.Tasks.Add(*sum);
                    }
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

TString GetV1StatFromV2Plan(const TString& plan, double* cpuUsage, TString* timeline) {
    TStringStream out;
    NYson::TYsonWriter writer(&out);
    writer.OnBeginMap();
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    TPlanVisualizer planViz;
    if (NJson::ReadJsonTree(plan, &jsonConfig, &stat)) {
        if (auto* topNode = stat.GetValueByPath("Plan")) {
            if (auto* subNode = topNode->GetValueByPath("Plans")) {
                for (auto plan : subNode->GetArray()) {
                    if (auto* typeNode = plan.GetValueByPath("Node Type")) {
                        auto nodeType = typeNode->GetStringSafe();
                        if (timeline) {
                            planViz.LoadPlan(nodeType, plan);
                        }
                        TTotalStatistics totals;
                        ui32 stageViewIndex = 0;
                        writer.OnKeyedItem(nodeType);
                        writer.OnBeginMap();
                        EnumeratePlans(writer, plan, stageViewIndex, totals);
                        totals.MaxMemoryUsage.Write(writer, "MaxMemoryUsage");
                        totals.CpuTimeUs.Write(writer, "CpuTimeUs");
                        totals.SourceCpuTimeUs.Write(writer, "SourceCpuTimeUs");
                        if (cpuUsage) {
                            *cpuUsage = (totals.CpuTimeUs.Sum + totals.SourceCpuTimeUs.Sum) / 1000000.0;
                        }
                        totals.InputBytes.Write(writer, "InputBytes");
                        totals.InputRows.Write(writer, "InputRows");
                        totals.OutputBytes.Write(writer, "OutputBytes");
                        totals.OutputRows.Write(writer, "OutputRows");
                        totals.ResultBytes.Write(writer, "ResultBytes");
                        totals.ResultRows.Write(writer, "ResultRows");
                        totals.IngressBytes.Write(writer, "IngressBytes");
                        totals.IngressDecompressedBytes.Write(writer, "IngressDecompressedBytes");
                        totals.IngressRows.Write(writer, "IngressRows");
                        totals.EgressBytes.Write(writer, "EgressBytes");
                        totals.EgressRows.Write(writer, "EgressRows");
                        totals.Tasks.Write(writer, "Tasks");
                        writer.OnEndMap();
                    }
                }
            }
        }
    }
    if (timeline) {
        planViz.PostProcessPlans();
        *timeline = planViz.PrintSvgSafe();
        // remove json "timeline" field after migration
        writer.OnKeyedItem("timeline");
        writer.OnStringScalar(*timeline);
    }
    writer.OnEndMap();
    return NJson2Yson::ConvertYson2Json(out.Str());
}

namespace {

void SerializeStats(google::protobuf::RepeatedPtrField<Ydb::ValuePair>& dest, const THashMap<TString, i64>& stats) {
    for (const auto& [name, stat] : stats) {
        auto& elem = *dest.Add();
        elem.mutable_key()->set_text_value(name);
        elem.mutable_payload()->set_int64_value(stat);
    }
}

struct TStatsAggregator {
    bool TryExtractAggregates(const NJson::TJsonValue& node, const TString& name) {
        auto dstAggr = Aggregates.find(name);
        if (dstAggr != Aggregates.end()) {
            if (auto sum = node.GetValueByPath("Sum")) {
                dstAggr->second += sum->GetIntegerSafe();
                return true;
            }
        }
        return false;
    }

    bool TryExtractSourceStats(const NJson::TJsonValue& node, const TString& name) {
        constexpr TStringBuf prefix = "Ingress=";
        if (!name.StartsWith(prefix)) {
            return false;
        }
        bool success = false;
        if (auto ingress = node.GetValueByPath("Ingress.Bytes.Sum")) {
            auto source = name.substr(prefix.size());
            Aggregates[source + ".Bytes"] += ingress->GetIntegerSafe();
            success = true;
        }
        if (auto ingress = node.GetValueByPath("Ingress.DecompressedBytes.Sum")) {
            auto source = name.substr(prefix.size());
            Aggregates[source + ".DecompressedBytes"] += ingress->GetIntegerSafe();
            success = true;
        }
        if (auto ingress = node.GetValueByPath("Ingress.Rows.Sum")) {
            auto source = name.substr(prefix.size());
            Aggregates[source + ".Rows"] += ingress->GetIntegerSafe();
            success = true;
        }
        if (auto ingress = node.GetValueByPath("Ingress.Splits.Sum")) {
            auto source = name.substr(prefix.size());
            Aggregates[source + ".Splits"] += ingress->GetIntegerSafe();
            success = true;
        }
        return success;
    }

    THashMap<TString, i64> Aggregates{std::pair<TString, i64>
        {"IngressBytes", 0},
        {"IngressDecompressedBytes", 0},
        {"EgressBytes", 0},
        {"IngressRows", 0},
        {"EgressRows", 0},
        {"InputBytes", 0},
        {"OutputBytes", 0},
        {"CpuTimeUs", 0}
    };
};

void TraverseOperators(const NJson::TJsonValue& node, TStatsAggregator& aggregator) {
    auto type = node.GetType();
    if (type == NJson::JSON_MAP) {
        if (auto source = node.GetValueByPath("ExternalDataSource")) {
            if (auto sourceType = node.GetValueByPath("SourceType")) {
                aggregator.Aggregates["Operator." + sourceType->GetStringSafe()]++;
            }
                        
            if (auto format = node.GetValueByPath("Format")) {
                aggregator.Aggregates["Format." + format->GetStringSafe()]++;
            }

            if (auto format = node.GetValueByPath("Compression")) {
                aggregator.Aggregates["Compression." + format->GetStringSafe()]++;
            }
        } else if (auto name = node.GetValueByPath("Name")) {
            aggregator.Aggregates["Operator." + name->GetStringSafe()]++;
        }
    } else if (type == NJson::JSON_ARRAY) {
        for (const auto& subNode : node.GetArray()) {
            TraverseOperators(subNode, aggregator);
        }
    }
}

void TraverseStats(const NJson::TJsonValue& node, const TString& name, TStatsAggregator& aggregator) {
    auto type = node.GetType();
    if (type == NJson::JSON_MAP) {
        if (!aggregator.TryExtractAggregates(node, name) && !aggregator.TryExtractSourceStats(node, name)) {
            for (const auto& [key, value] : node.GetMapSafe()) {
                TraverseStats(value, key, aggregator);
            }
        }
    } else if (type == NJson::JSON_ARRAY) {
        for (const auto& subNode : node.GetArray()) {
            if (auto nameNode = subNode.GetValueByPath("Name")) {
                TraverseStats(subNode, name + "=" + nameNode->GetStringSafe(), aggregator);
            }
        }
    }
}

void TraversePlans(const NJson::TJsonValue& node, TStatsAggregator& aggregator) {
    if (auto* plans = node.GetValueByPath("Plans")) {
        for (const auto& plan : plans->GetArray()) {
            TraversePlans(plan, aggregator);
        }
    }

    if (auto stats = node.GetValueByPath("Stats")) {
        TraverseStats(*stats, "", aggregator);
    }

    if (auto operators = node.GetValueByPath("Operators")) {
        TraverseOperators(*operators, aggregator);
    }
}
}

THashMap<TString, i64> AggregateStats(TStringBuf plan) {
    TStatsAggregator aggregator;

    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(plan, &jsonConfig, &root)) {
        return aggregator.Aggregates;
    }
    NJson::TJsonValue* plans = nullptr;
    if (plans = root.GetValueByPath("Plan.Plans"); !plans) {
        return aggregator.Aggregates;
    }

    for (const auto& subPlan : plans->GetArray()) {
        if (!subPlan.GetValueByPath("Node Type")) {
            continue;
        }
        TraversePlans(subPlan, aggregator);
    }
    return aggregator.Aggregates;
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
                totals.MaxMemoryUsage.Add(sum);
                writer.OnKeyedItem("mem");
                writer.OnInt64Scalar(sum);
            }
            if (auto* subNode = statNode->GetValueByPath("CpuTimeUs.Sum")) {
                auto sum = subNode->GetIntegerSafe();
                totals.CpuTimeUs.Add(sum);
                writer.OnKeyedItem("cpu");
                writer.OnStringScalar(FormatDurationUs(sum));
            }
            if (auto* subNode = statNode->GetValueByPath("SourceCpuTimeUs.Sum")) {
                auto sum = subNode->GetIntegerSafe();
                totals.SourceCpuTimeUs.Add(sum);
                writer.OnKeyedItem("scpu");
                writer.OnStringScalar(FormatDurationUs(sum));
            }
        writer.OnEndMap();
    }
}

TString GetV1StatFromV2PlanV2(const TString& plan, double* cpuUsage) {
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
                        if (totals.MaxMemoryUsage.Sum) {
                            writer.OnKeyedItem("mem");
                            writer.OnInt64Scalar(totals.MaxMemoryUsage.Sum);
                        }
                        if (totals.CpuTimeUs.Sum) {
                            writer.OnKeyedItem("cpu");
                            writer.OnStringScalar(FormatDurationUs(totals.CpuTimeUs.Sum));
                            if (cpuUsage) {
                                *cpuUsage = totals.CpuTimeUs.Sum / 1000000.0;
                            }
                        }
                        if (totals.SourceCpuTimeUs.Sum) {
                            writer.OnKeyedItem("scpu");
                            writer.OnStringScalar(FormatDurationUs(totals.SourceCpuTimeUs.Sum));
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

namespace {
void RemapValue(NYson::TYsonWriter& writer, const NJson::TJsonValue& node, const TString& key) {
    writer.OnKeyedItem(key);
    if (auto* keyNode = node.GetValueByPath(key)) {
        switch (keyNode->GetType()) {
        case NJson::JSON_BOOLEAN:
            writer.OnBooleanScalar(keyNode->GetBoolean());
            break;
        case NJson::JSON_INTEGER:
            writer.OnInt64Scalar(keyNode->GetInteger());
            break;
        case NJson::JSON_DOUBLE:
            writer.OnDoubleScalar(keyNode->GetDouble());
            break;
        case NJson::JSON_STRING:
        default:
            writer.OnStringScalar(keyNode->GetStringSafe());
            break;
        }
    } else {
        writer.OnStringScalar("-");
    }
}

void RemapNode(NYson::TYsonWriter& writer, const NJson::TJsonValue& node, const TString& path, const TString& key) {
    if (auto* subNode = node.GetValueByPath(path)) {
        writer.OnKeyedItem(key);
        writer.OnBeginMap();
            RemapValue(writer, *subNode, "sum");
            RemapValue(writer, *subNode, "count");
            RemapValue(writer, *subNode, "avg");
            RemapValue(writer, *subNode, "max");
            RemapValue(writer, *subNode, "min");
        writer.OnEndMap();
    }
}
}

TString GetPrettyStatistics(const TString& statistics) {
    TStringStream out;
    NYson::TYsonWriter writer(&out);
    writer.OnBeginMap();
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    if (NJson::ReadJsonTree(statistics, &jsonConfig, &stat)) {

        //  EXP 
        if (stat.GetValueByPath("Columns")) {
            return statistics;
        }

        for (const auto& p : stat.GetMap()) {
            // YQv1
            if (p.first.StartsWith("Graph=") || p.first.StartsWith("Precompute=")) {
                writer.OnKeyedItem(p.first);
                writer.OnBeginMap();
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.Tasks", "Tasks");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.CpuTimeUs", "CpuTimeUs");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.IngressBytes", "IngressBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.DecompressedBytes", "DecompressedBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.IngressRows", "IngressRows");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.InputBytes", "InputBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.InputRows", "InputRows");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.OutputBytes", "OutputBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.OutputRows", "OutputRows");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.ResultBytes", "ResultBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.ResultRows", "ResultRows");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.EgressBytes", "EgressBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.EgressRows", "EgressRows");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.MkqlMaxMemoryUsage", "MaxMemoryUsage");
                writer.OnEndMap();
            }
            // YQv2
            // if (p.first.StartsWith("Query")) 
            else {
                writer.OnKeyedItem(p.first);
                writer.OnBeginMap();
                    RemapNode(writer, p.second, "Tasks", "Tasks");
                    RemapNode(writer, p.second, "CpuTimeUs", "CpuTimeUs");
                    RemapNode(writer, p.second, "IngressBytes", "IngressBytes");
                    RemapNode(writer, p.second, "IngressDecompressedBytes", "IngressDecompressedBytes");
                    RemapNode(writer, p.second, "IngressRows", "IngressRows");
                    RemapNode(writer, p.second, "InputBytes", "InputBytes");
                    RemapNode(writer, p.second, "InputRows", "InputRows");
                    RemapNode(writer, p.second, "OutputBytes", "OutputBytes");
                    RemapNode(writer, p.second, "OutputRows", "OutputRows");
                    RemapNode(writer, p.second, "ResultBytes", "ResultBytes");
                    RemapNode(writer, p.second, "ResultRows", "ResultRows");
                    RemapNode(writer, p.second, "EgressBytes", "EgressBytes");
                    RemapNode(writer, p.second, "EgressRows", "EgressRows");
                    RemapNode(writer, p.second, "MaxMemoryUsage", "MaxMemoryUsage");
                writer.OnEndMap();
            }
        }
    }
    writer.OnEndMap();
    return NJson2Yson::ConvertYson2Json(out.Str());
}

std::optional<int> GetValue(const NJson::TJsonValue& node, const TString& name) {
    if (auto* keyNode = node.GetValueByPath(name)) {
        auto result = keyNode->GetInteger();
        if (result) {
            return result;
        }
    }
    return {};
}

void AggregateNode(const NJson::TJsonValue& node, const TString& name, ui64& sum) {
    if (node.GetType() == NJson::JSON_MAP) {
        if (auto* subNode = node.GetValueByPath(name)) {
            if (auto* keyNode = subNode->GetValueByPath("count")) {
                auto nodeCount = keyNode->GetInteger();
                if (nodeCount) {
                    if (auto* keyNode = subNode->GetValueByPath("sum")) {
                        sum += keyNode->GetInteger();
                    }
                }
            }
        }
    }
}

std::optional<int> GetNodeValue(const NJson::TJsonValue& node, const TString& name, bool aggregate = false) {
    if (aggregate) {
        ui64 sum = 0;
        if (node.GetType() == NJson::JSON_MAP) {
            for (const auto& p : node.GetMap()) {
                AggregateNode(p.second, name, sum);
            }
        }
        if (sum) {
            return sum;
        }
        return {};
    }
    if (auto* subNode = node.GetValueByPath(name)) {
        return GetValue(*subNode, "sum");
    }
    return {};
}

std::optional<int> Sum(const std::optional<int>& a, const std::optional<int>& b) {
    if (!a) {
        return b;
    }

    if (!b) {
        return a;
    }

    return *a + *b;
}

TPublicStat GetPublicStat(const TString& statistics) {
    TPublicStat counters;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    if (NJson::ReadJsonTree(statistics, &jsonConfig, &stat)) {

        //  EXP 
        if (stat.GetValueByPath("Columns")) {
            return counters;
        }

        for (const auto& p : stat.GetMap()) {
            counters.MemoryUsageBytes = Sum(counters.MemoryUsageBytes, GetNodeValue(p.second, "MaxMemoryUsage"));
            counters.CpuUsageUs = Sum(counters.CpuUsageUs, GetNodeValue(p.second, "CpuTimeUs"));
            counters.InputBytes = Sum(counters.InputBytes, GetNodeValue(p.second, "IngressBytes"));
            counters.OutputBytes = Sum(counters.OutputBytes, GetNodeValue(p.second, "EgressBytes"));
            counters.SourceInputRecords = Sum(counters.SourceInputRecords, GetNodeValue(p.second, "IngressRows"));
            counters.SinkOutputRecords = Sum(counters.SinkOutputRecords, GetNodeValue(p.second, "EgressRows"));
            counters.RunningTasks = Sum(counters.RunningTasks, GetNodeValue(p.second, "Tasks", true));
        }
    }
    return counters;
}

void CleanupPlans(NJson::TJsonValue& node) {
    node.EraseValue("Stats");
    if (auto* plans = node.GetValueByPath("Plans")) {
        if (plans->IsArray()) {
            for (auto& plan : plans->GetArraySafe()) {
                CleanupPlans(plan);
            }
        }
    }
}

TString CleanupPlanStats(const TString& plan) {
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue planRoot;

    if (NJson::ReadJsonTree(plan, &jsonConfig, &planRoot)) {
        planRoot.EraseValue("SimplifiedPlan");
        if (auto* plan = planRoot.GetValueByPath("Plan")) {
            CleanupPlans(*plan);
            return NJson::WriteJson(&planRoot, false);
        }
    }

    return plan;
}

TString SimplifiedPlan(const TString& plan) {
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue planRoot;

    if (NJson::ReadJsonTree(plan, &jsonConfig, &planRoot)) {
        if (auto* simplified = planRoot.GetValueByPath("SimplifiedPlan")) {
            if (auto* plan = planRoot.GetValueByPath("Plan")) {
                plan->Swap(*simplified);
                planRoot.EraseValue("SimplifiedPlan");
                return NJson::WriteJson(&planRoot, false);
            }
        }
    }

    return plan;
}

struct TNoneStatProcessor : IPlanStatProcessor {
    NYdb::NQuery::EStatsMode GetStatsMode() override {
        return NYdb::NQuery::EStatsMode::None;
    }

    TString ConvertPlan(const TString& plan) override {
        return plan;
    }

    TString GetPlanVisualization(const TString& plan) override {
        return plan;
    }

    TString GetQueryStat(const TString&, double& cpuUsage, TString*) override {
        cpuUsage = 0.0;
        return "";
    }

    TPublicStat GetPublicStat(const TString&) override {
        return TPublicStat{};
    }

    THashMap<TString, i64> GetFlatStat(TStringBuf) override {
        return {};
    }
};

struct TBasicStatProcessor : TNoneStatProcessor {
    NYdb::NQuery::EStatsMode GetStatsMode() override {
        return NYdb::NQuery::EStatsMode::Basic;
    }
};

struct TPlanStatProcessor : IPlanStatProcessor {
    NYdb::NQuery::EStatsMode GetStatsMode() override {
        return NYdb::NQuery::EStatsMode::Full;
    }

    TString ConvertPlan(const TString& plan) override {
        return plan;
    }

    TString GetPlanVisualization(const TString& plan) override {
        return plan;
    }

    TString GetQueryStat(const TString& plan, double& cpuUsage, TString* timeline) override {
        return GetV1StatFromV2Plan(plan, &cpuUsage, timeline);
    }

    TPublicStat GetPublicStat(const TString& stat) override {
        return ::NFq::GetPublicStat(stat);
    }

    THashMap<TString, i64> GetFlatStat(TStringBuf plan) override {
        return AggregateStats(plan);
    }
};

struct TFullStatProcessor : TPlanStatProcessor {
    TString GetPlanVisualization(const TString& plan) override {
        return CleanupPlanStats(plan);
    }
};

struct TCostStatProcessor : TPlanStatProcessor {
    TString GetPlanVisualization(const TString& plan) override {
        return SimplifiedPlan(plan);
    }
};

struct TProfileStatProcessor : TPlanStatProcessor {
    NYdb::NQuery::EStatsMode GetStatsMode() override {
        return NYdb::NQuery::EStatsMode::Profile;
    }
};

struct TProdStatProcessor : TFullStatProcessor {
    TString GetQueryStat(const TString& plan, double& cpuUsage, TString* timeline) override {
        return GetPrettyStatistics(GetV1StatFromV2Plan(plan, &cpuUsage, timeline));
    }
};

std::unique_ptr<IPlanStatProcessor> CreateStatProcessor(const TString& statViewName) {
    // disallow none and basic stat since they do not support metering
    // if (statViewName == "stat_none") return std::make_unique<TNoneStatProcessor>();
    // if (statViewName == "stat_basc") return std::make_unique<TBasicStatProcessor>();
    if (statViewName == "stat_plan") return std::make_unique<TPlanStatProcessor>();
    if (statViewName == "stat_full") return std::make_unique<TFullStatProcessor>();
    if (statViewName == "stat_cost") return std::make_unique<TCostStatProcessor>();
    if (statViewName == "stat_prof") return std::make_unique<TProfileStatProcessor>();
    if (statViewName == "stat_prod") return std::make_unique<TProdStatProcessor>();
    return std::make_unique<TFullStatProcessor>();
}

PingTaskRequestBuilder::PingTaskRequestBuilder(const NConfig::TCommonConfig& commonConfig, std::unique_ptr<IPlanStatProcessor>&& processor) 
    : Compressor(commonConfig.GetQueryArtifactsCompressionMethod(), commonConfig.GetQueryArtifactsCompressionMinSize())
    , Processor(std::move(processor)), ShowQueryTimeline(commonConfig.GetShowQueryTimeline())
{}

Fq::Private::PingTaskRequest PingTaskRequestBuilder::Build(
    const NYdb::NQuery::TExecStats& queryStats,
    const NYql::TIssues& issues, 
    std::optional<FederatedQuery::QueryMeta::ComputeStatus> computeStatus,
    std::optional<NYql::NDqProto::StatusIds::StatusCode> pendingStatusCode
) {
    Fq::Private::PingTaskRequest pingTaskRequest = Build(queryStats);

    if (issues) {
        NYql::IssuesToMessage(issues, pingTaskRequest.mutable_issues());
    }

    if (computeStatus) {
        pingTaskRequest.set_status(*computeStatus);
    }

    if (pendingStatusCode) {
        pingTaskRequest.set_pending_status_code(*pendingStatusCode);
    }

    return pingTaskRequest;
}


Fq::Private::PingTaskRequest PingTaskRequestBuilder::Build(const NYdb::NQuery::TExecStats& queryStats) {
    const auto& statsProto = NYdb::TProtoAccessor().GetProto(queryStats); 
    return Build(statsProto.query_plan(), statsProto.query_ast(), statsProto.compilation().duration_us(), statsProto.total_duration_us());
}

Fq::Private::PingTaskRequest PingTaskRequestBuilder::Build(const TString& queryPlan, const TString& queryAst, int64_t compilationTimeUs, int64_t computeTimeUs) {
    Fq::Private::PingTaskRequest pingTaskRequest;

    Issues.Clear();

    auto plan = queryPlan;
    try {
        plan = Processor->ConvertPlan(plan);
    } catch(const NJson::TJsonException& ex) {
        Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Error plan conversion: " << ex.what()));
    }

    auto planView = plan;
    try {
        planView = Processor->GetPlanVisualization(planView);
    } catch(const NJson::TJsonException& ex) {
        Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Error plan visualization: " << ex.what()));
    }

    if (Compressor.IsEnabled()) {
        auto [astCompressionMethod, astCompressed] = Compressor.Compress(queryAst);
        pingTaskRequest.mutable_ast_compressed()->set_method(astCompressionMethod);
        pingTaskRequest.mutable_ast_compressed()->set_data(astCompressed);

        auto [planCompressionMethod, planCompressed] = Compressor.Compress(planView);
        pingTaskRequest.mutable_plan_compressed()->set_method(planCompressionMethod);
        pingTaskRequest.mutable_plan_compressed()->set_data(planCompressed);
    } else {
        pingTaskRequest.set_ast(queryAst);
        pingTaskRequest.set_plan(planView);
    }

    CpuUsage = 0.0;
    try {
        TString timeline;
        auto stat = Processor->GetQueryStat(plan, CpuUsage, ShowQueryTimeline ? &timeline : nullptr);
        pingTaskRequest.set_statistics(stat);
        pingTaskRequest.set_dump_raw_statistics(true);
        if (timeline) {
            pingTaskRequest.set_timeline(timeline);
        }
        auto flatStat = Processor->GetFlatStat(plan);
        flatStat["CompilationTimeUs"] = compilationTimeUs;
        flatStat["ComputeTimeUs"] = computeTimeUs;
        SerializeStats(*pingTaskRequest.mutable_flat_stats(), flatStat);
        PublicStat = Processor->GetPublicStat(stat);
    } catch(const NJson::TJsonException& ex) {
        Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Error stat conversion: " << ex.what()));
    }

    return pingTaskRequest;
}

TString GetStatViewName(const TRunActorParams& params) {
    static TStringBuf hint("--fq_dev_hint_");
    auto p = params.Sql.find(hint);
    if (p != params.Sql.npos) {
        p += hint.size();
        auto p1 = params.Sql.find("\n", p);
        TString mode = params.Sql.substr(p, p1 == params.Sql.npos ? params.Sql.npos : p1 - p);
        if (mode) {
            return mode;
        }
    }

    if (!params.Config.GetControlPlaneStorage().GetDumpRawStatistics()) {
        return "stat_prod";
    }

    switch (params.Config.GetControlPlaneStorage().GetStatsMode()) {
        case Ydb::Query::StatsMode::STATS_MODE_UNSPECIFIED:
            return "stat_full";
        case Ydb::Query::StatsMode::STATS_MODE_NONE:
            return "stat_none";
        case Ydb::Query::StatsMode::STATS_MODE_BASIC:
            return "stat_basc";
        case Ydb::Query::StatsMode::STATS_MODE_FULL:
            return "stat_full";
        case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
            return "stat_prof";
        default:
            return "stat_full";
    }
}

} // namespace NFq
