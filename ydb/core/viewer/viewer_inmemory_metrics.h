#pragma once

#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/strip.h>
#include <util/string/builder.h>
#include <util/string/split.h>

#include <cmath>
#include <optional>
#include <regex>

namespace NKikimr::NViewer {

using namespace NActors;

namespace {

    inline TString EscapeInMemoryMetricLabelValue(const TString& value) {
        TStringBuilder escaped;
        for (const char ch : value) {
            if (ch == '\\' || ch == '"') {
                escaped << '\\';
            }
            escaped << ch;
        }
        return escaped;
    }

    inline TString BuildInMemoryMetricTarget(const TLineSnapshot& line) {
        TStringBuilder target;
        target << line.Name;
        if (!line.Labels.empty()) {
            target << '{';
            bool first = true;
            for (const auto& label : line.Labels) {
                if (!first) {
                    target << ',';
                }
                target << label.Name << "=\"" << EscapeInMemoryMetricLabelValue(label.Value) << '"';
                first = false;
            }
            target << '}';
        }
        return target;
    }

    inline TVector<TString> CollectInMemoryMetricTargets(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        TVector<TString> targets;
        const auto snapshot = inMemoryMetrics.Snapshot();
        targets.reserve(snapshot.Lines().size());
        for (const auto& line : snapshot.Lines()) {
            targets.push_back(BuildInMemoryMetricTarget(line));
        }
        Sort(targets);
        return targets;
    }

    inline TStringBuf GetInMemoryMetricName(TStringBuf target) {
        const size_t pos = target.find('{');
        return pos == TStringBuf::npos ? target : target.SubString(0, pos);
    }

    inline bool MatchGraphiteQuery(TStringBuf query, TStringBuf value) {
        const TStringBuf candidate = query.find('{') == TStringBuf::npos ? GetInMemoryMetricName(value) : value;
        if (query.empty()) {
            return candidate.empty();
        }

        size_t queryPos = 0;
        size_t valuePos = 0;
        size_t lastStar = TStringBuf::npos;
        size_t lastMatch = 0;

        while (valuePos < candidate.size()) {
            if (queryPos < query.size() && (query[queryPos] == candidate[valuePos] || query[queryPos] == '?')) {
                ++queryPos;
                ++valuePos;
            } else if (queryPos < query.size() && query[queryPos] == '*') {
                lastStar = queryPos++;
                lastMatch = valuePos;
            } else if (lastStar != TStringBuf::npos) {
                queryPos = lastStar + 1;
                valuePos = ++lastMatch;
            } else {
                return false;
            }
        }

        while (queryPos < query.size() && query[queryPos] == '*') {
            ++queryPos;
        }

        return queryPos == query.size();
    }

    inline void SplitGraphiteExpressions(TStringBuf raw, TVector<TString>& items) {
        size_t start = 0;
        int bracesDepth = 0;
        int parensDepth = 0;
        int bracketsDepth = 0;
        bool inQuotes = false;
        bool escaped = false;

        auto flush = [&](size_t end) {
            const TStringBuf piece = StripString(raw.SubString(start, end - start));
            if (!piece.empty()) {
                items.emplace_back(piece);
            }
        };

        for (size_t pos = 0; pos < raw.size(); ++pos) {
            const char ch = raw[pos];
            if (escaped) {
                escaped = false;
                continue;
            }
            if (inQuotes && ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (ch == '{') {
                    ++bracesDepth;
                    continue;
                }
                if (ch == '}') {
                    if (bracesDepth > 0) {
                        --bracesDepth;
                    }
                    continue;
                }
                if (ch == '(') {
                    ++parensDepth;
                    continue;
                }
                if (ch == ')') {
                    if (parensDepth > 0) {
                        --parensDepth;
                    }
                    continue;
                }
                if (ch == '[') {
                    ++bracketsDepth;
                    continue;
                }
                if (ch == ']') {
                    if (bracketsDepth > 0) {
                        --bracketsDepth;
                    }
                    continue;
                }
                if (ch == ',' && bracesDepth == 0 && parensDepth == 0 && bracketsDepth == 0) {
                    flush(pos);
                    start = pos + 1;
                }
            }
        }

        flush(raw.size());
    }

    inline bool TryParseGraphiteFunction(TStringBuf expression, TString& name, TVector<TString>& args) {
        const TStringBuf stripped = StripString(expression);
        const size_t openPos = stripped.find('(');
        if (openPos == TStringBuf::npos || !stripped.EndsWith(")")) {
            return false;
        }

        int parensDepth = 0;
        bool inQuotes = false;
        bool escaped = false;
        size_t closePos = TStringBuf::npos;
        for (size_t pos = openPos; pos < stripped.size(); ++pos) {
            const char ch = stripped[pos];
            if (escaped) {
                escaped = false;
                continue;
            }
            if (inQuotes && ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inQuotes = !inQuotes;
                continue;
            }
            if (inQuotes) {
                continue;
            }
            if (ch == '(') {
                ++parensDepth;
            } else if (ch == ')') {
                --parensDepth;
                if (parensDepth == 0) {
                    closePos = pos;
                    break;
                }
            }
        }

        if (closePos == TStringBuf::npos || closePos + 1 != stripped.size()) {
            return false;
        }

        name = StripString(stripped.SubString(0, openPos));
        args.clear();
        SplitGraphiteExpressions(stripped.SubString(openPos + 1, closePos - openPos - 1), args);
        return !name.empty();
    }

    inline TString UnquoteGraphiteString(TStringBuf value) {
        const TStringBuf stripped = StripString(value);
        if (stripped.size() < 2 || stripped.front() != '"' || stripped.back() != '"') {
            return TString(stripped);
        }

        TString result;
        result.reserve(stripped.size() - 2);
        bool escaped = false;
        for (size_t pos = 1; pos + 1 < stripped.size(); ++pos) {
            const char ch = stripped[pos];
            if (escaped) {
                if (ch == '\\' || ch == '"') {
                    result.push_back(ch);
                } else {
                    result.push_back('\\');
                    result.push_back(ch);
                }
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else {
                result.push_back(ch);
            }
        }
        if (escaped) {
            result.push_back('\\');
        }
        return result;
    }

    inline TString ConvertGraphiteReplacementToStdRegex(TStringBuf replacement) {
        TString result;
        result.reserve(replacement.size());
        bool escaped = false;
        for (const char ch : replacement) {
            if (escaped) {
                if (ch >= '0' && ch <= '9') {
                    result.push_back('$');
                    result.push_back(ch);
                } else {
                    result.push_back(ch);
                }
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else {
                result.push_back(ch);
            }
        }
        if (escaped) {
            result.push_back('\\');
        }
        return result;
    }

} // namespace

class TJsonInMemoryMetrics : public TViewerPipeClient {
    using TThis = TJsonInMemoryMetrics;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    struct TPoint {
        ui64 Timestamp = 0;
        double Value = 0;
    };

    struct TSeries {
        TString Target;
        TString Name;
        TVector<TLabel> Labels;
        TVector<TPoint> Points;
    };

    TVector<TString> Targets;
    mutable std::optional<TString> Error;

public:
    TJsonInMemoryMetrics(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        NeedRedirect = false;
        CheckDatabase = false;
    }

    void Bootstrap() override {
        ParseTargets();
        if (Targets.empty()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "no 'target' parameter specified"));
        }

        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            NJson::TJsonValue json;
            json["status"] = "error";
            json["error"] = "In-memory metrics subsystem is not registered";
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }

        ReplyAndPassAway(BuildResponseJson(*inMemoryMetrics));
    }

    void ReplyAndPassAway() override {}

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "In-memory metrics data",
            .Description = "Returns in-memory metrics data in graphite or prometheus format",
        });
        yaml.AddParameter({
            .Name = "target",
            .Description = "exact line target, repeated or comma delimited; labels use name{label=\"value\"}",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "from",
            .Description = "time in seconds",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "until",
            .Description = "time in seconds",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "maxDataPoints",
            .Description = "maximum number of data points per target",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "format",
            .Description = "response format, could be prometheus or graphite",
            .Type = "string",
        });
        return yaml;
    }

private:
    void ParseTargets() {
        if (!Params.Has("target")) {
            return;
        }

        for (size_t index = 0;; ++index) {
            const TString raw = Params.Get("target", index);
            if (raw.empty()) {
                break;
            }

            SplitGraphiteExpressions(raw, Targets);
        }
    }

    std::optional<ui64> GetFrom() const {
        if (!Params.Has("from")) {
            return {};
        }
        return FromStringWithDefault<ui64>(Params.Get("from"), 0);
    }

    std::optional<ui64> GetUntil() const {
        if (!Params.Has("until")) {
            return {};
        }
        return FromStringWithDefault<ui64>(Params.Get("until"), 0);
    }

    ui32 GetMaxDataPoints() const {
        if (!Params.Has("maxDataPoints")) {
            return 0;
        }
        return FromStringWithDefault<ui32>(Params.Get("maxDataPoints"), 0);
    }

    static void Downsample(TVector<TPoint>& points, ui32 maxDataPoints) {
        if (!maxDataPoints || points.size() <= maxDataPoints) {
            return;
        }

        TVector<TPoint> downsampled;
        downsampled.reserve(maxDataPoints);

        const size_t total = points.size();
        for (ui32 bucket = 0; bucket < maxDataPoints; ++bucket) {
            const size_t begin = static_cast<size_t>(bucket) * total / maxDataPoints;
            const size_t end = static_cast<size_t>(bucket + 1) * total / maxDataPoints;
            if (begin >= end) {
                continue;
            }

            double acc = 0;
            for (size_t i = begin; i < end; ++i) {
                acc += points[i].Value;
            }

            downsampled.push_back(TPoint{
                .Timestamp = points[end - 1].Timestamp,
                .Value = acc / (end - begin),
            });
        }

        points = std::move(downsampled);
    }

    TSeries BuildSeries(const TLineSnapshot& line, const std::optional<ui64>& from, const std::optional<ui64>& until, ui32 maxDataPoints) const {
        TSeries series;
        series.Target = BuildInMemoryMetricTarget(line);
        series.Name = line.Name;
        series.Labels = line.Labels;

        line.ForEachRecord([&](const TRecordView& record) {
            const ui64 timestamp = record.Timestamp.Seconds();
            if (from && timestamp < *from) {
                return;
            }
            if (until && timestamp > *until) {
                return;
            }

            series.Points.push_back(TPoint{
                .Timestamp = timestamp,
                .Value = static_cast<double>(record.Value),
            });
        });

        Downsample(series.Points, maxDataPoints);
        return series;
    }

    TVector<TSeries> EvaluateTarget(const TString& expression,
            const THashMap<TString, const TLineSnapshot*>& linesByTarget,
            const THashMap<TString, TVector<const TLineSnapshot*>>& linesByName,
            const std::optional<ui64>& from,
            const std::optional<ui64>& until,
            ui32 maxDataPoints) const
    {
        TString functionName;
        TVector<TString> args;
        if (TryParseGraphiteFunction(expression, functionName, args)) {
            if (functionName == "aliasSub" && args.size() == 3) {
                auto series = EvaluateTarget(args[0], linesByTarget, linesByName, from, until, maxDataPoints);
                try {
                    const std::string patternString(UnquoteGraphiteString(args[1]).c_str());
                    const std::regex pattern(patternString);
                    const std::string replacement(ConvertGraphiteReplacementToStdRegex(UnquoteGraphiteString(args[2])).c_str());
                    for (auto& line : series) {
                        const std::string source(line.Target.c_str());
                        line.Target = std::regex_replace(source, pattern, replacement).c_str();
                    }
                } catch (const std::regex_error& error) {
                    Error = TString(TStringBuilder() << "invalid aliasSub regex: " << error.what());
                    return {};
                }
                return series;
            }
            if (functionName == "alias" && args.size() == 2) {
                auto series = EvaluateTarget(args[0], linesByTarget, linesByName, from, until, maxDataPoints);
                const TString alias = UnquoteGraphiteString(args[1]);
                for (auto& line : series) {
                    line.Target = alias;
                }
                return series;
            }
        }

        if (const auto it = linesByTarget.find(expression); it != linesByTarget.end()) {
            return {BuildSeries(*it->second, from, until, maxDataPoints)};
        }
        if (expression.find('{') == TString::npos) {
            if (const auto it = linesByName.find(expression); it != linesByName.end()) {
                TVector<TSeries> series;
                series.reserve(it->second.size());
                for (const auto* line : it->second) {
                    series.push_back(BuildSeries(*line, from, until, maxDataPoints));
                }
                return series;
            }
        }

        return {TSeries{
            .Target = expression,
            .Name = expression,
        }};
    }

    TString BuildResponseJson(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        const auto snapshot = inMemoryMetrics.Snapshot();
        THashMap<TString, const TLineSnapshot*> linesByTarget;
        THashMap<TString, TVector<const TLineSnapshot*>> linesByName;
        const auto lines = snapshot.Lines();
        linesByTarget.reserve(lines.size());
        linesByName.reserve(lines.size());
        for (const auto& line : lines) {
            linesByTarget.emplace(BuildInMemoryMetricTarget(line), &line);
            linesByName[line.Name].push_back(&line);
        }

        const std::optional<ui64> from = GetFrom();
        const std::optional<ui64> until = GetUntil();
        const ui32 maxDataPoints = GetMaxDataPoints();

        TVector<TSeries> series;
        series.reserve(Targets.size());
        for (const auto& target : Targets) {
            auto resolved = EvaluateTarget(target, linesByTarget, linesByName, from, until, maxDataPoints);
            series.insert(series.end(),
                std::make_move_iterator(resolved.begin()),
                std::make_move_iterator(resolved.end()));
        }
        if (Error) {
            return GetHTTPBADREQUEST("text/plain", *Error);
        }

        NJson::TJsonValue json;
        if (!Params.Has("format") || Params.Get("format") == "graphite" || Params.Get("format") == "json") {
            json = BuildGraphiteJson(series);
        } else {
            json = BuildPrometheusJson(series);
        }

        return GetHTTPOKJSON(json);
    }

    static NJson::TJsonValue BuildGraphiteJson(const TVector<TSeries>& series) {
        NJson::TJsonValue json(NJson::JSON_ARRAY);
        for (const auto& line : series) {
            NJson::TJsonValue& jsonMetric = json.AppendValue({});
            jsonMetric["target"] = line.Target;
            jsonMetric["title"] = line.Target;
            jsonMetric["tags"]["name"] = line.Name;
            for (const auto& label : line.Labels) {
                jsonMetric["tags"][label.Name] = label.Value;
            }

            NJson::TJsonValue& datapoints = jsonMetric["datapoints"];
            datapoints.SetType(NJson::JSON_ARRAY);
            for (const auto& point : line.Points) {
                NJson::TJsonValue& datapoint = datapoints.AppendValue(NJson::TJsonValue(NJson::JSON_ARRAY));
                datapoint.AppendValue(point.Value);
                datapoint.AppendValue(point.Timestamp);
            }
        }
        return json;
    }

    static NJson::TJsonValue BuildPrometheusJson(const TVector<TSeries>& series) {
        NJson::TJsonValue json;
        json["status"] = "success";
        NJson::TJsonValue& data = json["data"];
        data["resultType"] = "matrix";

        NJson::TJsonValue& results = data["result"];
        results.SetType(NJson::JSON_ARRAY);
        for (const auto& line : series) {
            NJson::TJsonValue& result = results.AppendValue({});
            result["metric"]["__name__"] = line.Name;
            for (const auto& label : line.Labels) {
                result["metric"][label.Name] = label.Value;
            }

            NJson::TJsonValue& values = result["values"];
            values.SetType(NJson::JSON_ARRAY);
            for (const auto& point : line.Points) {
                NJson::TJsonValue& item = values.AppendValue(NJson::TJsonValue(NJson::JSON_ARRAY));
                item.AppendValue(point.Timestamp);
                item.AppendValue(point.Value);
            }
        }
        return json;
    }
};

class TJsonInMemoryMetricsTargets : public TViewerPipeClient {
    using TThis = TJsonInMemoryMetricsTargets;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

public:
    TJsonInMemoryMetricsTargets(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        NeedRedirect = false;
        CheckDatabase = false;
    }

    void Bootstrap() override {
        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            NJson::TJsonValue json;
            json["status"] = "error";
            json["error"] = "In-memory metrics subsystem is not registered";
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }

        ReplyAndPassAway(BuildResponseJson(*inMemoryMetrics));
    }

    void ReplyAndPassAway() override {}

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "In-memory metric targets",
            .Description = "Returns available in-memory metric targets",
        });
        yaml.AddParameter({
            .Name = "prefix",
            .Description = "optional target prefix filter",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "limit",
            .Description = "maximum number of returned targets",
            .Type = "integer",
        });
        return yaml;
    }

private:
    TString GetPrefix() const {
        return Params.Get("prefix");
    }

    ui32 GetLimit() const {
        return FromStringWithDefault<ui32>(Params.Get("limit"), 0);
    }

    TString BuildResponseJson(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        TVector<TString> targets;
        const TString prefix = GetPrefix();
        const ui32 limit = GetLimit();

        for (TString target : CollectInMemoryMetricTargets(inMemoryMetrics)) {
            if (!prefix.empty() && !target.StartsWith(prefix)) {
                continue;
            }
            targets.push_back(std::move(target));
        }

        if (limit && targets.size() > limit) {
            targets.resize(limit);
        }

        NJson::TJsonValue json(NJson::JSON_ARRAY);
        for (const auto& target : targets) {
            json.AppendValue(target);
        }

        return GetHTTPOKJSON(json);
    }
};

class TJsonInMemoryMetricsGraphiteFind : public TViewerPipeClient {
    using TThis = TJsonInMemoryMetricsGraphiteFind;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

public:
    TJsonInMemoryMetricsGraphiteFind(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        NeedRedirect = false;
        CheckDatabase = false;
    }

    void Bootstrap() override {
        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            NJson::TJsonValue json;
            json["status"] = "error";
            json["error"] = "In-memory metrics subsystem is not registered";
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }

        ReplyAndPassAway(BuildResponseJson(*inMemoryMetrics));
    }

    void ReplyAndPassAway() override {}

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Graphite metric discovery for in-memory metrics",
            .Description = "Returns Graphite-compatible metric discovery payload for in-memory metrics",
        });
        yaml.AddParameter({
            .Name = "query",
            .Description = "Graphite wildcard query",
            .Type = "string",
            .Required = true,
        });
        return yaml;
    }

private:
    TString GetQuery() const {
        return Params.Get("query");
    }

    TString BuildResponseJson(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        const TString query = Params.Has("query") ? GetQuery() : "*";
        NJson::TJsonValue json(NJson::JSON_ARRAY);

        for (const TString& target : CollectInMemoryMetricTargets(inMemoryMetrics)) {
            if (!MatchGraphiteQuery(query, target)) {
                continue;
            }

            NJson::TJsonValue& item = json.AppendValue({});
            item["text"] = target;
            item["id"] = target;
            item["path"] = target;
            item["name"] = target;
            item["leaf"] = 1;
            item["expandable"] = 0;
            item["allowChildren"] = 0;
        }

        return GetHTTPOKJSON(json);
    }
};

} // namespace NKikimr::NViewer
