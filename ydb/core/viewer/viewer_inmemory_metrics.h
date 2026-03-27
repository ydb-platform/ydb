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
#include <algorithm>

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

    enum class EPrometheusLabelMatchOp {
        Equal,
        NotEqual,
        Regex,
        NotRegex,
    };

    struct TPrometheusLabelMatcher {
        TString Name;
        EPrometheusLabelMatchOp Op = EPrometheusLabelMatchOp::Equal;
        TString Value;
    };

    struct TPrometheusVectorSelector {
        TString MetricName;
        TVector<TPrometheusLabelMatcher> Matchers;
    };

    inline void SplitPrometheusMatchers(TStringBuf raw, TVector<TString>& items) {
        size_t start = 0;
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
            if (!inQuotes && ch == ',') {
                flush(pos);
                start = pos + 1;
            }
        }

        flush(raw.size());
    }

    inline bool TryParsePrometheusVectorSelector(TStringBuf expression, TPrometheusVectorSelector& selector, TString& error) {
        selector = {};
        error.clear();

        const TStringBuf stripped = StripString(expression);
        if (stripped.empty()) {
            error = "empty query";
            return false;
        }

        const size_t bracePos = stripped.find('{');
        if (bracePos == TStringBuf::npos) {
            selector.MetricName = TString(stripped);
            return true;
        }
        if (!stripped.EndsWith("}")) {
            error = "only vector selectors are supported";
            return false;
        }

        selector.MetricName = TString(StripString(stripped.SubString(0, bracePos)));
        const TStringBuf rawMatchers = stripped.SubString(bracePos + 1, stripped.size() - bracePos - 2);
        TVector<TString> items;
        SplitPrometheusMatchers(rawMatchers, items);

        for (const TString& item : items) {
            static constexpr std::array<TStringBuf, 4> Operators = {"=~", "!~", "!=", "="};

            size_t opPos = TString::npos;
            TStringBuf op;
            for (const TStringBuf candidate : Operators) {
                opPos = item.find(candidate);
                if (opPos != TString::npos) {
                    op = candidate;
                    break;
                }
            }
            if (opPos == TString::npos) {
                error = TStringBuilder() << "invalid label matcher: " << item;
                return false;
            }

            const TStringBuf name = StripString(TStringBuf(item).SubString(0, opPos));
            const TStringBuf value = StripString(TStringBuf(item).SubString(opPos + op.size(), item.size() - opPos - op.size()));
            if (name.empty() || value.empty() || value.front() != '"' || value.back() != '"') {
                error = TStringBuilder() << "invalid label matcher: " << item;
                return false;
            }

            TPrometheusLabelMatcher matcher;
            matcher.Name = TString(name);
            matcher.Value = UnquoteGraphiteString(value);
            if (op == "=") {
                matcher.Op = EPrometheusLabelMatchOp::Equal;
            } else if (op == "!=") {
                matcher.Op = EPrometheusLabelMatchOp::NotEqual;
            } else if (op == "=~") {
                matcher.Op = EPrometheusLabelMatchOp::Regex;
            } else {
                matcher.Op = EPrometheusLabelMatchOp::NotRegex;
            }
            selector.Matchers.push_back(std::move(matcher));
        }

        if (selector.MetricName.empty() && selector.Matchers.empty()) {
            error = "empty selector";
            return false;
        }
        return true;
    }

    inline TString FindLineLabelValue(const TLineSnapshot& line, TStringBuf labelName) {
        if (labelName == "__name__") {
            return line.Name;
        }
        for (const auto& label : line.Labels) {
            if (label.Name == labelName) {
                return label.Value;
            }
        }
        return {};
    }

    inline bool MatchPrometheusLabelMatcher(const TLineSnapshot& line, const TPrometheusLabelMatcher& matcher, TString& error) {
        const TString actual = FindLineLabelValue(line, matcher.Name);
        switch (matcher.Op) {
            case EPrometheusLabelMatchOp::Equal:
                return actual == matcher.Value;
            case EPrometheusLabelMatchOp::NotEqual:
                return actual != matcher.Value;
            case EPrometheusLabelMatchOp::Regex:
            case EPrometheusLabelMatchOp::NotRegex:
                try {
                    const bool matches = std::regex_match(actual.c_str(), std::regex(matcher.Value.c_str()));
                    return matcher.Op == EPrometheusLabelMatchOp::Regex ? matches : !matches;
                } catch (const std::regex_error& e) {
                    error = TStringBuilder() << "invalid regex for label '" << matcher.Name << "': " << e.what();
                    return false;
                }
        }
        return false;
    }

    inline bool MatchPrometheusSelector(const TLineSnapshot& line, const TPrometheusVectorSelector& selector, TString& error) {
        if (!selector.MetricName.empty() && line.Name != selector.MetricName) {
            return false;
        }
        for (const auto& matcher : selector.Matchers) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusLabelMatcher(line, matcher, error);
            if (!error.empty() && error.size() != prevErrorSize) {
                return false;
            }
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    inline void SortAndUniqueStrings(TVector<TString>& values) {
        Sort(values);
        values.erase(std::unique(values.begin(), values.end()), values.end());
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

class TJsonInMemoryMetricsPrometheus : public TViewerPipeClient {
    using TThis = TJsonInMemoryMetricsPrometheus;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    struct TSelectedSeries {
        const TLineSnapshot* Line = nullptr;
    };

public:
    TJsonInMemoryMetricsPrometheus(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        NeedRedirect = false;
        CheckDatabase = false;
    }

    void Bootstrap() override {
        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            return ReplyAndPassAway(BuildErrorResponse("execution", "In-memory metrics subsystem is not registered"));
        }

        ReplyAndPassAway(HandleRequest(*inMemoryMetrics));
    }

    void ReplyAndPassAway() override {}

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Prometheus-compatible in-memory metrics API",
            .Description = "Returns selector-only Prometheus-compatible responses for in-memory metrics",
        });
        yaml.AddParameter({
            .Name = "query",
            .Description = "Prometheus vector selector",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "match[]",
            .Description = "Prometheus series selector",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "start",
            .Description = "range start timestamp in seconds",
            .Type = "number",
        });
        yaml.AddParameter({
            .Name = "end",
            .Description = "range end timestamp in seconds",
            .Type = "number",
        });
        yaml.AddParameter({
            .Name = "time",
            .Description = "evaluation timestamp in seconds",
            .Type = "number",
        });
        yaml.AddParameter({
            .Name = "step",
            .Description = "query range step",
            .Type = "string",
        });
        return yaml;
    }

private:
    TString GetRequestPath() const {
        if (Event) {
            return TString("/") + Event->Get()->Request.GetPage()->Path + Event->Get()->Request.GetPathInfo();
        }
        if (HttpEvent) {
            return TString(HttpEvent->Get()->Request->GetURI());
        }
        return {};
    }

    TString GetLabelNameFromPath() const {
        static constexpr TStringBuf Prefix = "/viewer/inmemory_metrics/prometheus/api/v1/label/";
        static constexpr TStringBuf Suffix = "/values";
        const TString path = GetRequestPath();
        if (!path.StartsWith(Prefix) || !path.EndsWith(Suffix)) {
            return {};
        }
        return path.substr(Prefix.size(), path.size() - Prefix.size() - Suffix.size());
    }

    TString BuildErrorResponse(TStringBuf errorType, TStringBuf message) {
        NJson::TJsonValue json;
        json["status"] = "error";
        json["errorType"] = TString(errorType);
        json["error"] = TString(message);

        TStringStream content;
        NJson::WriteJson(&content, &json, {
            .FormatOutput = false,
            .SortKeys = false,
            .ValidateUtf8 = false,
        });
        return GetHTTPBADREQUEST("application/json", content.Str());
    }

    TString HandleRequest(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        const TString path = GetRequestPath();
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/labels") {
            return HandleLabels(inMemoryMetrics);
        }
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/series") {
            return HandleSeries(inMemoryMetrics);
        }
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
            return HandleQuery(inMemoryMetrics);
        }
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range") {
            return HandleQueryRange(inMemoryMetrics);
        }
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/label/__name__/values") {
            return HandleLabelValues(inMemoryMetrics, "__name__");
        }
        if (path.StartsWith("/viewer/inmemory_metrics/prometheus/api/v1/label/") && path.EndsWith("/values")) {
            const TString labelName = GetLabelNameFromPath();
            if (labelName.empty()) {
                return BuildErrorResponse("bad_data", "invalid label path");
            }
            return HandleLabelValues(inMemoryMetrics, labelName);
        }

        return BuildErrorResponse("bad_data", "unsupported endpoint");
    }

    std::optional<double> GetFloatParam(TStringBuf name) const {
        if (!Params.Has(name)) {
            return {};
        }
        return FromStringWithDefault<double>(Params.Get(name), 0);
    }

    bool ParseSelectorParam(TStringBuf query, TPrometheusVectorSelector& selector, TString& error) const {
        if (!TryParsePrometheusVectorSelector(query, selector, error)) {
            return false;
        }
        if (query.find_first_of("()[]+-/*") != TStringBuf::npos && query.find('{') == TStringBuf::npos) {
            // Keep the contract explicit: bare metric name is fine, anything else is not.
            const TStringBuf trimmed = StripString(query);
            if (trimmed.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:") != TStringBuf::npos) {
                error = "only selector-only Prometheus queries are supported";
                return false;
            }
        }
        return true;
    }

    TVector<TPrometheusVectorSelector> ParseMatchers(TStringBuf paramName, TString& error) const {
        TVector<TPrometheusVectorSelector> selectors;
        for (size_t index = 0;; ++index) {
            const TString raw = Params.Get(paramName, index);
            if (raw.empty()) {
                break;
            }
            TPrometheusVectorSelector selector;
            if (!ParseSelectorParam(raw, selector, error)) {
                selectors.clear();
                return selectors;
            }
            selectors.push_back(std::move(selector));
        }
        return selectors;
    }

    TVector<const TLineSnapshot*> SelectSeries(const TVector<TLineSnapshot>& lines,
            const TVector<TPrometheusVectorSelector>& selectors,
            TString& error) const
    {
        TVector<const TLineSnapshot*> selected;
        if (selectors.empty()) {
            selected.reserve(lines.size());
            for (const auto& line : lines) {
                selected.push_back(&line);
            }
            return selected;
        }

        selected.reserve(lines.size());
        for (const auto& line : lines) {
            for (const auto& selector : selectors) {
                const size_t prevErrorSize = error.size();
                const bool matched = MatchPrometheusSelector(line, selector, error);
                if (!error.empty() && error.size() != prevErrorSize) {
                    selected.clear();
                    return selected;
                }
                if (matched) {
                    selected.push_back(&line);
                    break;
                }
            }
        }
        return selected;
    }

    static NJson::TJsonValue BuildMetricObject(const TLineSnapshot& line) {
        NJson::TJsonValue metric;
        metric["__name__"] = line.Name;
        for (const auto& label : line.Labels) {
            metric[label.Name] = label.Value;
        }
        return metric;
    }

    static TString FormatSampleValue(double value) {
        return TStringBuilder() << value;
    }

    TString HandleLabels(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(lines, selectors, error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }

        TVector<TString> labels;
        labels.reserve(8);
        labels.push_back("__name__");
        for (const auto* line : selected) {
            for (const auto& label : line->Labels) {
                labels.push_back(label.Name);
            }
        }
        SortAndUniqueStrings(labels);

        NJson::TJsonValue data(NJson::JSON_ARRAY);
        for (const auto& label : labels) {
            data.AppendValue(label);
        }
        NJson::TJsonValue json;
        json["status"] = "success";
        json["data"] = std::move(data);
        return GetHTTPOKJSON(json);
    }

    TString HandleLabelValues(const TInMemoryMetricsRegistry& inMemoryMetrics, TStringBuf labelName) {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(lines, selectors, error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }

        TVector<TString> values;
        for (const auto* line : selected) {
            const TString value = FindLineLabelValue(*line, labelName);
            if (!value.empty()) {
                values.push_back(value);
            }
        }
        SortAndUniqueStrings(values);

        NJson::TJsonValue data(NJson::JSON_ARRAY);
        for (const auto& value : values) {
            data.AppendValue(value);
        }
        NJson::TJsonValue json;
        json["status"] = "success";
        json["data"] = std::move(data);
        return GetHTTPOKJSON(json);
    }

    TString HandleSeries(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(lines, selectors, error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }

        NJson::TJsonValue data(NJson::JSON_ARRAY);
        for (const auto* line : selected) {
            data.AppendValue(BuildMetricObject(*line));
        }
        NJson::TJsonValue json;
        json["status"] = "success";
        json["data"] = std::move(data);
        return GetHTTPOKJSON(json);
    }

    TString HandleQuery(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        const TString query = Params.Get("query");
        if (query.empty()) {
            return BuildErrorResponse("bad_data", "query is required");
        }

        TPrometheusVectorSelector selector;
        TString error;
        if (!ParseSelectorParam(query, selector, error)) {
            return BuildErrorResponse("bad_data", error);
        }

        const double evalTime = GetFloatParam("time").value_or(TInstant::Now().Seconds());
        const ui64 evalTimestamp = static_cast<ui64>(evalTime);

        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        NJson::TJsonValue result(NJson::JSON_ARRAY);

        for (const auto& line : lines) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusSelector(line, selector, error);
            if (!error.empty() && error.size() != prevErrorSize) {
                return BuildErrorResponse("bad_data", error);
            }
            if (!matched) {
                continue;
            }

            struct TLastPoint {
                ui64 Timestamp = 0;
                double Value = 0;
            };
            std::optional<TLastPoint> lastPoint;
            line.ForEachRecord([&](const TRecordView& record) {
                const ui64 ts = record.Timestamp.Seconds();
                if (ts <= evalTimestamp) {
                    lastPoint = TLastPoint{
                        .Timestamp = ts,
                        .Value = static_cast<double>(record.Value),
                    };
                }
            });
            if (!lastPoint) {
                continue;
            }

            NJson::TJsonValue& item = result.AppendValue({});
            item["metric"] = BuildMetricObject(line);
            NJson::TJsonValue& value = item["value"];
            value.SetType(NJson::JSON_ARRAY);
            value.AppendValue(lastPoint->Timestamp);
            value.AppendValue(FormatSampleValue(lastPoint->Value));
        }

        NJson::TJsonValue json;
        json["status"] = "success";
        json["data"]["resultType"] = "vector";
        json["data"]["result"] = std::move(result);
        return GetHTTPOKJSON(json);
    }

    TString HandleQueryRange(const TInMemoryMetricsRegistry& inMemoryMetrics) {
        const TString query = Params.Get("query");
        if (query.empty()) {
            return BuildErrorResponse("bad_data", "query is required");
        }
        const auto start = GetFloatParam("start");
        const auto end = GetFloatParam("end");
        if (!start || !end) {
            return BuildErrorResponse("bad_data", "start and end are required");
        }
        if (*end < *start) {
            return BuildErrorResponse("bad_data", "end must be greater than or equal to start");
        }

        TPrometheusVectorSelector selector;
        TString error;
        if (!ParseSelectorParam(query, selector, error)) {
            return BuildErrorResponse("bad_data", error);
        }

        const ui64 startTs = static_cast<ui64>(*start);
        const ui64 endTs = static_cast<ui64>(*end);

        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        NJson::TJsonValue result(NJson::JSON_ARRAY);

        for (const auto& line : lines) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusSelector(line, selector, error);
            if (!error.empty() && error.size() != prevErrorSize) {
                return BuildErrorResponse("bad_data", error);
            }
            if (!matched) {
                continue;
            }

            NJson::TJsonValue values(NJson::JSON_ARRAY);
            bool hasValues = false;
            line.ForEachRecord([&](const TRecordView& record) {
                const ui64 ts = record.Timestamp.Seconds();
                if (ts < startTs || ts > endTs) {
                    return;
                }

                NJson::TJsonValue& item = values.AppendValue(NJson::TJsonValue(NJson::JSON_ARRAY));
                item.AppendValue(ts);
                item.AppendValue(FormatSampleValue(static_cast<double>(record.Value)));
                hasValues = true;
            });
            if (!hasValues) {
                continue;
            }

            NJson::TJsonValue& item = result.AppendValue({});
            item["metric"] = BuildMetricObject(line);
            item["values"] = std::move(values);
        }

        NJson::TJsonValue json;
        json["status"] = "success";
        json["data"]["resultType"] = "matrix";
        json["data"]["result"] = std::move(result);
        return GetHTTPOKJSON(json);
    }
};

} // namespace NKikimr::NViewer
