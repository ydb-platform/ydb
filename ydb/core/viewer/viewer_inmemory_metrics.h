#pragma once

#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/strip.h>
#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/datetime/base.h>

#include <cmath>
#include <cctype>
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

    inline TVector<TLabel> BuildEffectiveMetricLabels(const TVector<TLabel>& commonLabels, const TLineSnapshot& line) {
        TVector<TLabel> labels = commonLabels;
        labels.reserve(commonLabels.size() + line.Labels.size());
        for (const auto& label : line.Labels) {
            auto it = FindIf(labels, [&](const TLabel& current) {
                return current.Name == label.Name;
            });
            if (it != labels.end()) {
                it->Value = label.Value;
            } else {
                labels.push_back(label);
            }
        }
        return labels;
    }

    inline TString BuildInMemoryMetricTarget(const TVector<TLabel>& commonLabels, const TLineSnapshot& line) {
        TStringBuilder target;
        target << line.Name;
        const auto labels = BuildEffectiveMetricLabels(commonLabels, line);
        if (!labels.empty()) {
            target << '{';
            bool first = true;
            for (const auto& label : labels) {
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
            targets.push_back(BuildInMemoryMetricTarget(snapshot.CommonLabels, line));
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

    enum class EPrometheusAggregationOp {
        Sum,
        Avg,
        Min,
        Max,
        Count,
    };

    struct TPrometheusAggregation {
        EPrometheusAggregationOp Op = EPrometheusAggregationOp::Sum;
        TVector<TString> ByLabels;
    };

    struct TPrometheusQueryExpression {
        TPrometheusVectorSelector Selector;
        std::optional<TPrometheusAggregation> Aggregation;
        std::optional<TDuration> LastOverTimeWindow;
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

        bool metricNameFromMatchers = false;
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
                const TStringBuf itemBuf(item);
                const TStringBuf strippedItem = StripString(itemBuf);
                if (selector.MetricName.empty() && !metricNameFromMatchers
                        && !strippedItem.empty()
                        && strippedItem.front() == '"'
                        && strippedItem.back() == '"')
                {
                    selector.MetricName = UnquoteGraphiteString(strippedItem);
                    metricNameFromMatchers = true;
                    continue;
                }
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

    inline bool TryConsumePrometheusKeyword(TStringBuf input, size_t& pos, TStringBuf keyword) {
        if (input.SubString(pos, input.size() - pos).StartsWith(keyword)) {
            pos += keyword.size();
            return true;
        }
        return false;
    }

    inline void SkipPrometheusWhitespace(TStringBuf input, size_t& pos) {
        while (pos < input.size() && IsAsciiSpace(input[pos])) {
            ++pos;
        }
    }

    inline bool FindMatchingParen(TStringBuf input, size_t openPos, size_t& closePos) {
        if (openPos >= input.size() || input[openPos] != '(') {
            return false;
        }

        int depth = 0;
        bool inQuotes = false;
        bool escaped = false;
        for (size_t pos = openPos; pos < input.size(); ++pos) {
            const char ch = input[pos];
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
                ++depth;
            } else if (ch == ')') {
                --depth;
                if (depth == 0) {
                    closePos = pos;
                    return true;
                }
            }
        }

        return false;
    }

    inline bool ParsePrometheusLabelList(TStringBuf raw, TVector<TString>& labels, TString& error) {
        labels.clear();
        TVector<TString> items;
        SplitPrometheusMatchers(raw, items);
        for (const TString& item : items) {
            const TStringBuf label = StripString(TStringBuf(item));
            if (label.empty()) {
                continue;
            }
            if (label.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != TStringBuf::npos) {
                error = TStringBuilder() << "invalid label name in aggregation: " << item;
                labels.clear();
                return false;
            }
            labels.push_back(TString(label));
        }
        Sort(labels);
        labels.erase(std::unique(labels.begin(), labels.end()), labels.end());
        return true;
    }

    inline std::optional<EPrometheusAggregationOp> TryParsePrometheusAggregationOp(TStringBuf name) {
        if (name == "sum") {
            return EPrometheusAggregationOp::Sum;
        }
        if (name == "avg") {
            return EPrometheusAggregationOp::Avg;
        }
        if (name == "min") {
            return EPrometheusAggregationOp::Min;
        }
        if (name == "max") {
            return EPrometheusAggregationOp::Max;
        }
        if (name == "count") {
            return EPrometheusAggregationOp::Count;
        }
        return {};
    }

    inline bool TryParsePrometheusRangeSelector(TStringBuf expression, TPrometheusVectorSelector& selector, TDuration& window, TString& error) {
        selector = {};
        window = TDuration::Zero();
        error.clear();

        const TStringBuf stripped = StripString(expression);
        if (stripped.empty() || !stripped.EndsWith("]")) {
            error = "range selector must end with ']'";
            return false;
        }

        const size_t bracketPos = stripped.rfind('[');
        if (bracketPos == TStringBuf::npos || bracketPos == 0) {
            error = "invalid range selector";
            return false;
        }

        if (!TryParsePrometheusVectorSelector(stripped.SubString(0, bracketPos), selector, error)) {
            return false;
        }

        const TStringBuf rawWindow = StripString(stripped.SubString(bracketPos + 1, stripped.size() - bracketPos - 2));
        if (rawWindow.empty()) {
            error = "range selector window is required";
            return false;
        }
        if (!TDuration::TryParse(rawWindow, window) || window <= TDuration::Zero()) {
            error = TStringBuilder() << "invalid range selector window: " << rawWindow;
            return false;
        }
        return true;
    }

    inline bool TryParsePrometheusQueryExpression(TStringBuf expression, TPrometheusQueryExpression& query, TString& error) {
        query = {};
        error.clear();

        const TStringBuf stripped = StripString(expression);
        if (stripped.empty()) {
            error = "empty query";
            return false;
        }

        if (stripped.EndsWith(")")) {
            const size_t openPos = stripped.find('(');
            if (openPos != TStringBuf::npos) {
                size_t closePos = TStringBuf::npos;
                if (FindMatchingParen(stripped, openPos, closePos) && closePos + 1 == stripped.size()) {
                    const TStringBuf functionName = StripString(stripped.SubString(0, openPos));
                    if (functionName == "last_over_time") {
                        TDuration window;
                        if (!TryParsePrometheusRangeSelector(stripped.SubString(openPos + 1, closePos - openPos - 1), query.Selector, window, error)) {
                            return false;
                        }
                        query.LastOverTimeWindow = window;
                        return true;
                    }
                }
            }
        }

        size_t pos = 0;
        while (pos < stripped.size() && std::isalpha(static_cast<unsigned char>(stripped[pos]))) {
            ++pos;
        }

        const TStringBuf candidateOp = stripped.SubString(0, pos);
        const auto aggregationOp = TryParsePrometheusAggregationOp(candidateOp);
        if (!aggregationOp) {
            return TryParsePrometheusVectorSelector(stripped, query.Selector, error);
        }
        if (pos < stripped.size() && !IsAsciiSpace(stripped[pos])) {
            return TryParsePrometheusVectorSelector(stripped, query.Selector, error);
        }

        SkipPrometheusWhitespace(stripped, pos);
        if (!TryConsumePrometheusKeyword(stripped, pos, "by")) {
            error = "only selector-only queries and '<aggregation> by (...) (...)' are supported";
            return false;
        }

        SkipPrometheusWhitespace(stripped, pos);
        if (pos >= stripped.size() || stripped[pos] != '(') {
            error = "aggregation labels must be enclosed in parentheses";
            return false;
        }

        size_t labelsClosePos = TStringBuf::npos;
        if (!FindMatchingParen(stripped, pos, labelsClosePos)) {
            error = "unterminated aggregation label list";
            return false;
        }

        TPrometheusAggregation aggregation;
        aggregation.Op = *aggregationOp;
        if (!ParsePrometheusLabelList(stripped.SubString(pos + 1, labelsClosePos - pos - 1), aggregation.ByLabels, error)) {
            return false;
        }

        pos = labelsClosePos + 1;
        SkipPrometheusWhitespace(stripped, pos);
        if (pos >= stripped.size() || stripped[pos] != '(') {
            error = "aggregation argument must be enclosed in parentheses";
            return false;
        }

        size_t selectorClosePos = TStringBuf::npos;
        if (!FindMatchingParen(stripped, pos, selectorClosePos)) {
            error = "unterminated aggregation argument";
            return false;
        }

        size_t tailPos = selectorClosePos + 1;
        SkipPrometheusWhitespace(stripped, tailPos);
        if (tailPos != stripped.size()) {
            error = "unexpected trailing data after aggregation";
            return false;
        }

        if (!TryParsePrometheusVectorSelector(stripped.SubString(pos + 1, selectorClosePos - pos - 1), query.Selector, error)) {
            return false;
        }

        query.Aggregation = std::move(aggregation);
        return true;
    }

    inline TString FindLineLabelValue(const TVector<TLabel>& commonLabels, const TLineSnapshot& line, TStringBuf labelName) {
        if (labelName == "__name__") {
            return line.Name;
        }
        for (const auto& label : line.Labels) {
            if (label.Name == labelName) {
                return label.Value;
            }
        }
        for (const auto& label : commonLabels) {
            if (label.Name == labelName) {
                return label.Value;
            }
        }
        return {};
    }

    inline bool MatchPrometheusLabelValue(TStringBuf actual, const TPrometheusLabelMatcher& matcher, TString& error) {
        switch (matcher.Op) {
            case EPrometheusLabelMatchOp::Equal:
                return actual == matcher.Value;
            case EPrometheusLabelMatchOp::NotEqual:
                return actual != matcher.Value;
            case EPrometheusLabelMatchOp::Regex:
            case EPrometheusLabelMatchOp::NotRegex:
                try {
                    const std::string actualString(actual.data(), actual.size());
                    const bool matches = std::regex_match(actualString, std::regex(matcher.Value.c_str()));
                    return matcher.Op == EPrometheusLabelMatchOp::Regex ? matches : !matches;
                } catch (const std::regex_error& e) {
                    error = TStringBuilder() << "invalid regex for label '" << matcher.Name << "': " << e.what();
                    return false;
                }
        }
        return false;
    }

    inline bool MatchPrometheusLabelMatcher(const TVector<TLabel>& commonLabels, const TLineSnapshot& line, const TPrometheusLabelMatcher& matcher, TString& error) {
        const TString actual = FindLineLabelValue(commonLabels, line, matcher.Name);
        return MatchPrometheusLabelValue(actual, matcher, error);
    }

    inline TString SerializeJsonValue(const NJson::TJsonValue& json, bool sortKeys = false) {
        TStringStream content;
        NJson::WriteJson(&content, &json, {
            .FormatOutput = false,
            .SortKeys = sortKeys,
            .ValidateUtf8 = false,
        });
        return content.Str();
    }

    inline bool ParseJsonValue(TStringBuf raw, NJson::TJsonValue& json) {
        return NJson::ReadJsonTree(raw, &json, false);
    }

    inline bool MatchPrometheusSelector(const TVector<TLabel>& commonLabels, const TLineSnapshot& line, const TPrometheusVectorSelector& selector, TString& error) {
        if (!selector.MetricName.empty() && line.Name != selector.MetricName) {
            return false;
        }
        for (const auto& matcher : selector.Matchers) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusLabelMatcher(commonLabels, line, matcher, error);
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

    inline TString BuildCanonicalMetricKey(const NJson::TJsonValue& metric) {
        return SerializeJsonValue(metric, true);
    }

    inline std::optional<ui32> TryGetExactNodeIdMatcher(const TPrometheusVectorSelector& selector) {
        std::optional<ui32> exactNodeId;
        for (const auto& matcher : selector.Matchers) {
            if (matcher.Name != "node_id") {
                continue;
            }
            if (matcher.Op != EPrometheusLabelMatchOp::Equal) {
                return {};
            }
            const ui32 nodeId = FromStringWithDefault<ui32>(matcher.Value, 0);
            if (!nodeId) {
                return {};
            }
            if (exactNodeId && *exactNodeId != nodeId) {
                return {};
            }
            exactNodeId = nodeId;
        }
        return exactNodeId;
    }

    inline TString FindMetricLabelValue(const TVector<TLabel>& labels, TStringBuf labelName) {
        for (const auto& label : labels) {
            if (label.Name == labelName) {
                return label.Value;
            }
        }
        return {};
    }

    inline TString SerializePrometheusVectorSelector(const TPrometheusVectorSelector& selector) {
        TStringBuilder builder;
        if (!selector.MetricName.empty()) {
            builder << selector.MetricName;
        }
        if (!selector.Matchers.empty()) {
            builder << '{';
            bool first = true;
            for (const auto& matcher : selector.Matchers) {
                if (!first) {
                    builder << ',';
                }
                builder << matcher.Name;
                switch (matcher.Op) {
                    case EPrometheusLabelMatchOp::Equal:
                        builder << '=';
                        break;
                    case EPrometheusLabelMatchOp::NotEqual:
                        builder << "!=";
                        break;
                    case EPrometheusLabelMatchOp::Regex:
                        builder << "=~";
                        break;
                    case EPrometheusLabelMatchOp::NotRegex:
                        builder << "!~";
                        break;
                }
                builder << '"' << EscapeInMemoryMetricLabelValue(matcher.Value) << '"';
                first = false;
            }
            builder << '}';
        }
        return builder;
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

    TSeries BuildSeries(const TVector<TLabel>& commonLabels, const TLineSnapshot& line, const std::optional<ui64>& from, const std::optional<ui64>& until, ui32 maxDataPoints) const {
        TSeries series;
        series.Target = BuildInMemoryMetricTarget(commonLabels, line);
        series.Name = line.Name;
        series.Labels = BuildEffectiveMetricLabels(commonLabels, line);

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
            const TVector<TLabel>& commonLabels,
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
                auto series = EvaluateTarget(args[0], commonLabels, linesByTarget, linesByName, from, until, maxDataPoints);
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
                auto series = EvaluateTarget(args[0], commonLabels, linesByTarget, linesByName, from, until, maxDataPoints);
                const TString alias = UnquoteGraphiteString(args[1]);
                for (auto& line : series) {
                    line.Target = alias;
                }
                return series;
            }
        }

        if (const auto it = linesByTarget.find(expression); it != linesByTarget.end()) {
            return {BuildSeries(commonLabels, *it->second, from, until, maxDataPoints)};
        }
        if (expression.find('{') == TString::npos) {
            if (const auto it = linesByName.find(expression); it != linesByName.end()) {
                TVector<TSeries> series;
                series.reserve(it->second.size());
                for (const auto* line : it->second) {
                    series.push_back(BuildSeries(commonLabels, *line, from, until, maxDataPoints));
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
            linesByTarget.emplace(BuildInMemoryMetricTarget(snapshot.CommonLabels, line), &line);
            linesByName[line.Name].push_back(&line);
        }

        const std::optional<ui64> from = GetFrom();
        const std::optional<ui64> until = GetUntil();
        const ui32 maxDataPoints = GetMaxDataPoints();

        TVector<TSeries> series;
        series.reserve(Targets.size());
        for (const auto& target : Targets) {
            auto resolved = EvaluateTarget(target, snapshot.CommonLabels, linesByTarget, linesByName, from, until, maxDataPoints);
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

class TInMemoryMetricsPrometheusProcessor {
public:
    struct TResult {
        bool Success = true;
        NJson::TJsonValue Json;
    };

    TInMemoryMetricsPrometheusProcessor(TString path, const TCgiParameters& params)
        : Path(std::move(path))
        , Params(params)
    {}

    const TString& GetPath() const {
        return Path;
    }

    bool SupportsFanout() const {
        return Path == "/viewer/inmemory_metrics/prometheus/api/v1/query"
            || Path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range"
            || Path == "/viewer/inmemory_metrics/prometheus/api/v1/series"
            || Path == "/viewer/inmemory_metrics/prometheus/api/v1/labels"
            || Path == "/viewer/inmemory_metrics/prometheus/api/v1/label/__name__/values"
            || (Path.StartsWith("/viewer/inmemory_metrics/prometheus/api/v1/label/") && Path.EndsWith("/values"));
    }

    TVector<TPrometheusVectorSelector> GetRoutingSelectors(TString& error) const {
        error.clear();

        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/query"
                || Path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range")
        {
            const TString query = Params.Get("query");
            if (query.empty()) {
                return {};
            }

            TPrometheusQueryExpression expression;
            if (!ParseQueryParam(query, expression, error)) {
                return {};
            }
            return {std::move(expression.Selector)};
        }

        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/series"
                || Path == "/viewer/inmemory_metrics/prometheus/api/v1/labels"
                || Path == "/viewer/inmemory_metrics/prometheus/api/v1/label/__name__/values"
                || (Path.StartsWith("/viewer/inmemory_metrics/prometheus/api/v1/label/") && Path.EndsWith("/values")))
        {
            return ParseMatchers("match[]", error);
        }

        return {};
    }

    bool UsesAggregation(TString& error) const {
        error.clear();
        if (Path != "/viewer/inmemory_metrics/prometheus/api/v1/query"
                && Path != "/viewer/inmemory_metrics/prometheus/api/v1/query_range")
        {
            return false;
        }

        const TString query = Params.Get("query");
        if (query.empty()) {
            return false;
        }

        TPrometheusQueryExpression expression;
        if (!ParseQueryParam(query, expression, error)) {
            return false;
        }
        return expression.Aggregation.has_value();
    }

    bool UsesLastOverTime(TString& error) const {
        error.clear();
        if (Path != "/viewer/inmemory_metrics/prometheus/api/v1/query"
                && Path != "/viewer/inmemory_metrics/prometheus/api/v1/query_range")
        {
            return false;
        }

        const TString query = Params.Get("query");
        if (query.empty()) {
            return false;
        }

        TPrometheusQueryExpression expression;
        if (!ParseQueryParam(query, expression, error)) {
            return false;
        }
        return expression.LastOverTimeWindow.has_value();
    }

    bool ParseQueryExpression(TPrometheusQueryExpression& expression, TString& error) const {
        error.clear();
        if (Path != "/viewer/inmemory_metrics/prometheus/api/v1/query"
                && Path != "/viewer/inmemory_metrics/prometheus/api/v1/query_range")
        {
            error = "query expressions are only supported for query endpoints";
            return false;
        }

        const TString query = Params.Get("query");
        if (query.empty()) {
            error = "query is required";
            return false;
        }

        return ParseQueryParam(query, expression, error);
    }

    TString BuildSelectorOnlyQueryString(TString& error) const {
        error.clear();

        TPrometheusQueryExpression expression;
        if (!ParseQueryExpression(expression, error)) {
            return {};
        }

        if (!expression.Aggregation && !expression.LastOverTimeWindow) {
            return Params.Print();
        }

        TCgiParameters rewritten = Params;
        rewritten.ReplaceUnescaped("query", SerializePrometheusVectorSelector(expression.Selector));
        return rewritten.Print();
    }

    TResult Execute(const TInMemoryMetricsRegistry& inMemoryMetrics) const {
        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/labels") {
            return HandleLabels(inMemoryMetrics);
        }
        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/series") {
            return HandleSeries(inMemoryMetrics);
        }
        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
            return HandleQuery(inMemoryMetrics);
        }
        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range") {
            return HandleQueryRange(inMemoryMetrics);
        }
        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/label/__name__/values") {
            return HandleLabelValues(inMemoryMetrics, "__name__");
        }
        if (Path.StartsWith("/viewer/inmemory_metrics/prometheus/api/v1/label/") && Path.EndsWith("/values")) {
            const TString labelName = GetLabelNameFromPath();
            if (labelName.empty()) {
                return MakeError("bad_data", "invalid label path");
            }
            return HandleLabelValues(inMemoryMetrics, labelName);
        }

        return MakeError("bad_data", "unsupported endpoint");
    }

    bool ApplyAggregationToResult(NJson::TJsonValue& json, TString& error) const {
        error.clear();

        TPrometheusQueryExpression expression;
        if (!ParseQueryExpression(expression, error)) {
            return false;
        }
        if (!expression.Aggregation && !expression.LastOverTimeWindow) {
            return true;
        }

        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
            if (expression.LastOverTimeWindow) {
                error = "last_over_time fanout merge is only supported for query_range";
                return false;
            }
            TVector<TInstantSample> input;
            for (const auto& item : json["data"]["result"].GetArray()) {
                const auto& metric = item["metric"];
                input.push_back(TInstantSample{
                    .MetricName = metric["__name__"].GetStringSafe(),
                    .Labels = ExtractMetricLabels(metric),
                    .Timestamp = item["value"][0].GetUIntegerRobust(),
                    .Value = FromStringWithDefault<double>(item["value"][1].GetStringRobust(), 0),
                });
            }

            const auto aggregated = AggregateInstantSamples(input, *expression.Aggregation);
            NJson::TJsonValue result(NJson::JSON_ARRAY);
            for (const auto& sample : aggregated) {
                AppendInstantResult(result, sample);
            }
            json["data"]["result"] = std::move(result);
            return true;
        }

        if (Path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range") {
            TVector<TRangeSample> input;
            for (const auto& item : json["data"]["result"].GetArray()) {
                TRangeSample sample;
                const auto& metric = item["metric"];
                sample.MetricName = metric["__name__"].GetStringSafe();
                sample.Labels = ExtractMetricLabels(metric);
                for (const auto& sourceValue : item["values"].GetArray()) {
                    sample.Values.emplace_back(
                        sourceValue[0].GetUIntegerRobust(),
                        FromStringWithDefault<double>(sourceValue[1].GetStringRobust(), 0));
                }
                input.push_back(std::move(sample));
            }

            if (expression.LastOverTimeWindow) {
                const auto start = GetFloatParam("start");
                const auto end = GetFloatParam("end");
                if (!start || !end) {
                    error = "start and end are required";
                    return false;
                }
                const auto step = GetStepParam(error);
                if (!error.empty()) {
                    return false;
                }
                if (!step) {
                    error = "step is required";
                    return false;
                }

                const auto evaluated = EvaluateLastOverTimeRange(
                    input,
                    static_cast<ui64>(*start),
                    static_cast<ui64>(*end),
                    *step,
                    *expression.LastOverTimeWindow);
                NJson::TJsonValue result(NJson::JSON_ARRAY);
                for (const auto& sample : evaluated) {
                    AppendRangeResult(result, sample);
                }
                json["data"]["result"] = std::move(result);
                return true;
            }

            const auto aggregated = AggregateRangeSamples(input, *expression.Aggregation);
            NJson::TJsonValue result(NJson::JSON_ARRAY);
            for (const auto& sample : aggregated) {
                AppendRangeResult(result, sample);
            }
            json["data"]["result"] = std::move(result);
            return true;
        }

        return true;
    }

private:
    TString Path;
    const TCgiParameters& Params;

    static TResult MakeError(TStringBuf errorType, TStringBuf message) {
        TResult result;
        result.Success = false;
        result.Json["status"] = "error";
        result.Json["errorType"] = TString(errorType);
        result.Json["error"] = TString(message);
        return result;
    }

    TString GetLabelNameFromPath() const {
        static constexpr TStringBuf Prefix = "/viewer/inmemory_metrics/prometheus/api/v1/label/";
        static constexpr TStringBuf Suffix = "/values";
        if (!Path.StartsWith(Prefix) || !Path.EndsWith(Suffix)) {
            return {};
        }
        return Path.substr(Prefix.size(), Path.size() - Prefix.size() - Suffix.size());
    }

    std::optional<double> GetFloatParam(TStringBuf name) const {
        if (!Params.Has(name)) {
            return {};
        }
        return FromStringWithDefault<double>(Params.Get(name), 0);
    }

    std::optional<TDuration> GetStepParam(TString& error) const {
        error.clear();
        if (!Params.Has("step")) {
            return {};
        }

        const TString raw = Params.Get("step");
        TDuration step;
        if (TDuration::TryParse(raw, step)) {
            if (step <= TDuration::Zero()) {
                error = "step must be greater than zero";
                return {};
            }
            return step;
        }

        const double seconds = FromStringWithDefault<double>(raw, 0);
        if (seconds <= 0) {
            error = TStringBuilder() << "invalid step: " << raw;
            return {};
        }
        return TDuration::MilliSeconds(static_cast<ui64>(seconds * 1000.0));
    }

    struct TInstantSample {
        TString MetricName;
        TVector<TLabel> Labels;
        ui64 Timestamp = 0;
        double Value = 0;
    };

    struct TRangeSample {
        TString MetricName;
        TVector<TLabel> Labels;
        TLineMeta Meta;
        std::optional<std::pair<ui64, double>> PreviousValue;
        TVector<std::pair<ui64, double>> Values;
    };

    struct TAggregationState {
        ui64 Timestamp = 0;
        double Sum = 0;
        double Min = 0;
        double Max = 0;
        ui64 Count = 0;
        bool HasValue = false;

        void Add(ui64 timestamp, double value) {
            if (!HasValue) {
                Timestamp = timestamp;
                Sum = value;
                Min = value;
                Max = value;
                Count = 1;
                HasValue = true;
                return;
            }

            Timestamp = std::max(Timestamp, timestamp);
            Sum += value;
            Min = std::min(Min, value);
            Max = std::max(Max, value);
            ++Count;
        }

        double GetValue(EPrometheusAggregationOp op) const {
            switch (op) {
                case EPrometheusAggregationOp::Sum:
                    return Sum;
                case EPrometheusAggregationOp::Avg:
                    return Count ? Sum / static_cast<double>(Count) : 0;
                case EPrometheusAggregationOp::Min:
                    return Min;
                case EPrometheusAggregationOp::Max:
                    return Max;
                case EPrometheusAggregationOp::Count:
                    return static_cast<double>(Count);
            }
        }
    };

    bool ParseSelectorParam(TStringBuf query, TPrometheusVectorSelector& selector, TString& error) const {
        if (!TryParsePrometheusVectorSelector(query, selector, error)) {
            return false;
        }
        if (query.find_first_of("()[]+-/*") != TStringBuf::npos && query.find('{') == TStringBuf::npos) {
            const TStringBuf trimmed = StripString(query);
            if (trimmed.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:") != TStringBuf::npos) {
                error = "only selector-only Prometheus matchers are supported";
                return false;
            }
        }
        return true;
    }

    bool ParseQueryParam(TStringBuf query, TPrometheusQueryExpression& expression, TString& error) const {
        if (!TryParsePrometheusQueryExpression(query, expression, error)) {
            return false;
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

    TVector<const TLineSnapshot*> SelectSeries(const TVector<TLabel>& commonLabels,
            const TVector<TLineSnapshot>& lines,
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
                const bool matched = MatchPrometheusSelector(commonLabels, line, selector, error);
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

    static NJson::TJsonValue BuildMetricObject(const TVector<TLabel>& commonLabels, const TLineSnapshot& line) {
        NJson::TJsonValue metric;
        metric["__name__"] = line.Name;
        for (const auto& label : BuildEffectiveMetricLabels(commonLabels, line)) {
            metric[label.Name] = label.Value;
        }
        return metric;
    }

    static TString FormatSampleValue(double value) {
        return TStringBuilder() << value;
    }

    static NJson::TJsonValue BuildMetricObject(const TString& metricName, const TVector<TLabel>& labels) {
        NJson::TJsonValue metric;
        if (!metricName.empty()) {
            metric["__name__"] = metricName;
        }
        for (const auto& label : labels) {
            metric[label.Name] = label.Value;
        }
        return metric;
    }

    static TString BuildAggregationGroupKey(const TVector<TLabel>& labels, const TVector<TString>& byLabels) {
        TVector<TLabel> selected;
        selected.reserve(byLabels.size());
        for (const auto& byLabel : byLabels) {
            const TString value = FindMetricLabelValue(labels, byLabel);
            if (!value.empty()) {
                selected.push_back(TLabel{
                    .Name = byLabel,
                    .Value = value,
                });
            }
        }
        return SerializeJsonValue(BuildMetricObject("", selected), true);
    }

    static TVector<TLabel> BuildAggregationMetricLabels(const TVector<TLabel>& labels, const TVector<TString>& byLabels) {
        TVector<TLabel> selected;
        selected.reserve(byLabels.size());
        for (const auto& byLabel : byLabels) {
            const TString value = FindMetricLabelValue(labels, byLabel);
            if (!value.empty()) {
                selected.push_back(TLabel{
                    .Name = byLabel,
                    .Value = value,
                });
            }
        }
        return selected;
    }

    TVector<TInstantSample> CollectInstantSamples(const TVector<TLabel>& commonLabels,
            const TVector<TLineSnapshot>& lines,
            const TPrometheusVectorSelector& selector,
            ui64 evalTimestamp,
            TString& error) const
    {
        TVector<TInstantSample> samples;
        for (const auto& line : lines) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusSelector(commonLabels, line, selector, error);
            if (!error.empty() && error.size() != prevErrorSize) {
                samples.clear();
                return samples;
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

            samples.push_back(TInstantSample{
                .MetricName = line.Name,
                .Labels = BuildEffectiveMetricLabels(commonLabels, line),
                .Timestamp = lastPoint->Timestamp,
                .Value = lastPoint->Value,
            });
        }
        return samples;
    }

    TVector<TRangeSample> CollectRangeSamples(const TVector<TLabel>& commonLabels,
            const TVector<TLineSnapshot>& lines,
            const TPrometheusVectorSelector& selector,
            ui64 startTs,
            ui64 endTs,
            TString& error) const
    {
        TVector<TRangeSample> samples;
        for (const auto& line : lines) {
            const size_t prevErrorSize = error.size();
            const bool matched = MatchPrometheusSelector(commonLabels, line, selector, error);
            if (!error.empty() && error.size() != prevErrorSize) {
                samples.clear();
                return samples;
            }
            if (!matched) {
                continue;
            }

            TRangeSample sample;
            sample.MetricName = line.Name;
            sample.Labels = BuildEffectiveMetricLabels(commonLabels, line);
            sample.Meta = line.Meta;
            line.ForEachRecord([&](const TRecordView& record) {
                const ui64 ts = record.Timestamp.Seconds();
                if (ts < startTs) {
                    sample.PreviousValue = std::pair<ui64, double>{ts, static_cast<double>(record.Value)};
                    return;
                }
                if (ts > endTs) {
                    return;
                }
                sample.Values.emplace_back(ts, static_cast<double>(record.Value));
            });
            if (!sample.Values.empty()
                || (sample.Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat && sample.PreviousValue)) {
                samples.push_back(std::move(sample));
            }
        }
        return samples;
    }

    TVector<TRangeSample> FillOnChangeWithHeartbeatRangeGaps(const TVector<TRangeSample>& input,
            ui64 startTs,
            ui64 endTs,
            TDuration step) const
    {
        const ui64 stepSeconds = Max<ui64>(1, step.Seconds());

        TVector<TRangeSample> output;
        output.reserve(input.size());
        for (const auto& source : input) {
            if (source.Meta.PublishPolicy != EPublishPolicy::OnChangeWithHeartbeat
                || source.Meta.Heartbeat <= TDuration::Zero()) {
                output.push_back(source);
                continue;
            }

            const ui64 heartbeatSeconds = Max<ui64>(1, source.Meta.Heartbeat.Seconds());

            TRangeSample sample;
            sample.MetricName = source.MetricName;
            sample.Labels = source.Labels;
            sample.Meta = source.Meta;

            std::optional<std::pair<ui64, double>> lastValue = source.PreviousValue;
            size_t nextCandidate = 0;

            for (ui64 evalTs = startTs; evalTs <= endTs; evalTs += stepSeconds) {
                while (nextCandidate < source.Values.size() && source.Values[nextCandidate].first <= evalTs) {
                    lastValue = source.Values[nextCandidate];
                    ++nextCandidate;
                }
                if (lastValue
                    && evalTs >= lastValue->first
                    && evalTs - lastValue->first <= heartbeatSeconds) {
                    sample.Values.emplace_back(evalTs, lastValue->second);
                }
                if (endTs - evalTs < stepSeconds) {
                    break;
                }
            }

            if (!sample.Values.empty()) {
                output.push_back(std::move(sample));
            }
        }

        return output;
    }

    TVector<TInstantSample> EvaluateLastOverTimeInstant(const TVector<TRangeSample>& input, ui64 evalTimestamp, TDuration window) const {
        const i64 windowSeconds = Max<i64>(1, window.Seconds());
        TVector<TInstantSample> output;
        for (const auto& sample : input) {
            std::optional<std::pair<ui64, double>> lastValue;
            const i64 windowStart = static_cast<i64>(evalTimestamp) - windowSeconds;
            for (const auto& [timestamp, value] : sample.Values) {
                if (timestamp > evalTimestamp) {
                    break;
                }
                if (static_cast<i64>(timestamp) < windowStart) {
                    continue;
                }
                lastValue = std::pair<ui64, double>{timestamp, value};
            }
            if (!lastValue) {
                continue;
            }
            output.push_back(TInstantSample{
                .MetricName = sample.MetricName,
                .Labels = sample.Labels,
                .Timestamp = evalTimestamp,
                .Value = lastValue->second,
            });
        }
        return output;
    }

    TVector<TRangeSample> EvaluateLastOverTimeRange(const TVector<TRangeSample>& input,
            ui64 startTs,
            ui64 endTs,
            TDuration step,
            TDuration window) const
    {
        const ui64 stepSeconds = Max<ui64>(1, step.Seconds());
        const i64 windowSeconds = Max<i64>(1, window.Seconds());

        TVector<TRangeSample> output;
        for (const auto& source : input) {
            TRangeSample sample;
            sample.MetricName = source.MetricName;
            sample.Labels = source.Labels;

            size_t firstInWindow = 0;
            size_t nextCandidate = 0;
            std::optional<std::pair<ui64, double>> lastValue;

            for (ui64 evalTs = startTs; evalTs <= endTs; evalTs += stepSeconds) {
                while (nextCandidate < source.Values.size() && source.Values[nextCandidate].first <= evalTs) {
                    lastValue = source.Values[nextCandidate];
                    ++nextCandidate;
                }
                const i64 windowStart = static_cast<i64>(evalTs) - windowSeconds;
                while (firstInWindow < nextCandidate && static_cast<i64>(source.Values[firstInWindow].first) < windowStart) {
                    ++firstInWindow;
                }
                if (lastValue && firstInWindow < nextCandidate) {
                    sample.Values.emplace_back(evalTs, lastValue->second);
                }
                if (endTs - evalTs < stepSeconds) {
                    break;
                }
            }

            if (!sample.Values.empty()) {
                output.push_back(std::move(sample));
            }
        }

        return output;
    }

    void AppendInstantResult(NJson::TJsonValue& output, const TInstantSample& sample) const {
        NJson::TJsonValue& item = output.AppendValue({});
        item["metric"] = BuildMetricObject(sample.MetricName, sample.Labels);
        NJson::TJsonValue& value = item["value"];
        value.SetType(NJson::JSON_ARRAY);
        value.AppendValue(sample.Timestamp);
        value.AppendValue(FormatSampleValue(sample.Value));
    }

    void AppendRangeResult(NJson::TJsonValue& output, const TRangeSample& sample) const {
        NJson::TJsonValue values(NJson::JSON_ARRAY);
        for (const auto& [timestamp, value] : sample.Values) {
            NJson::TJsonValue& item = values.AppendValue(NJson::TJsonValue(NJson::JSON_ARRAY));
            item.AppendValue(timestamp);
            item.AppendValue(FormatSampleValue(value));
        }

        NJson::TJsonValue& item = output.AppendValue({});
        item["metric"] = BuildMetricObject(sample.MetricName, sample.Labels);
        item["values"] = std::move(values);
    }

    TVector<TInstantSample> AggregateInstantSamples(const TVector<TInstantSample>& input, const TPrometheusAggregation& aggregation) const {
        struct TGroupedResult {
            TVector<TLabel> Labels;
            TAggregationState State;
        };

        THashMap<TString, TGroupedResult> grouped;
        for (const auto& sample : input) {
            const TString key = BuildAggregationGroupKey(sample.Labels, aggregation.ByLabels);
            auto& result = grouped[key];
            if (result.Labels.empty()) {
                result.Labels = BuildAggregationMetricLabels(sample.Labels, aggregation.ByLabels);
            }
            result.State.Add(sample.Timestamp, sample.Value);
        }

        TVector<TString> keys;
        keys.reserve(grouped.size());
        for (const auto& [key, _] : grouped) {
            keys.push_back(key);
        }
        Sort(keys);

        TVector<TInstantSample> output;
        output.reserve(keys.size());
        for (const auto& key : keys) {
            const auto& result = grouped.at(key);
            output.push_back(TInstantSample{
                .MetricName = "",
                .Labels = result.Labels,
                .Timestamp = result.State.Timestamp,
                .Value = result.State.GetValue(aggregation.Op),
            });
        }
        return output;
    }

    TVector<TRangeSample> AggregateRangeSamples(const TVector<TRangeSample>& input, const TPrometheusAggregation& aggregation) const {
        struct TGroupedRange {
            TVector<TLabel> Labels;
            THashMap<ui64, TAggregationState> Points;
        };

        THashMap<TString, TGroupedRange> grouped;
        for (const auto& sample : input) {
            const TString key = BuildAggregationGroupKey(sample.Labels, aggregation.ByLabels);
            auto& result = grouped[key];
            if (result.Labels.empty()) {
                result.Labels = BuildAggregationMetricLabels(sample.Labels, aggregation.ByLabels);
            }
            for (const auto& [timestamp, value] : sample.Values) {
                result.Points[timestamp].Add(timestamp, value);
            }
        }

        TVector<TString> keys;
        keys.reserve(grouped.size());
        for (const auto& [key, _] : grouped) {
            keys.push_back(key);
        }
        Sort(keys);

        TVector<TRangeSample> output;
        output.reserve(keys.size());
        for (const auto& key : keys) {
            auto& result = grouped[key];
            TVector<ui64> timestamps;
            timestamps.reserve(result.Points.size());
            for (const auto& [timestamp, _] : result.Points) {
                timestamps.push_back(timestamp);
            }
            Sort(timestamps);

            TRangeSample sample;
            sample.MetricName = "";
            sample.Labels = result.Labels;
            sample.Values.reserve(timestamps.size());
            for (ui64 timestamp : timestamps) {
                sample.Values.emplace_back(timestamp, result.Points.at(timestamp).GetValue(aggregation.Op));
            }
            output.push_back(std::move(sample));
        }
        return output;
    }

    static TVector<TLabel> ExtractMetricLabels(const NJson::TJsonValue& metric) {
        TVector<TLabel> labels;
        for (const auto& [name, value] : metric.GetMapSafe()) {
            if (name == "__name__") {
                continue;
            }
            labels.push_back(TLabel{
                .Name = name,
                .Value = value.GetStringRobust(),
            });
        }
        Sort(labels, [](const TLabel& lhs, const TLabel& rhs) {
            return lhs.Name < rhs.Name;
        });
        return labels;
    }


    TResult HandleLabels(const TInMemoryMetricsRegistry& inMemoryMetrics) const {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(snapshot.CommonLabels, lines, selectors, error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        TVector<TString> labels;
        labels.reserve(8);
        labels.push_back("__name__");
        for (const auto& label : snapshot.CommonLabels) {
            labels.push_back(label.Name);
        }
        for (const auto* line : selected) {
            for (const auto& label : line->Labels) {
                labels.push_back(label.Name);
            }
        }
        SortAndUniqueStrings(labels);

        TResult result;
        result.Json["status"] = "success";
        NJson::TJsonValue& data = result.Json["data"];
        data.SetType(NJson::JSON_ARRAY);
        for (const auto& label : labels) {
            data.AppendValue(label);
        }
        return result;
    }

    TResult HandleLabelValues(const TInMemoryMetricsRegistry& inMemoryMetrics, TStringBuf labelName) const {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(snapshot.CommonLabels, lines, selectors, error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        TVector<TString> values;
        for (const auto* line : selected) {
            const TString value = FindLineLabelValue(snapshot.CommonLabels, *line, labelName);
            if (!value.empty()) {
                values.push_back(value);
            }
        }
        SortAndUniqueStrings(values);

        TResult result;
        result.Json["status"] = "success";
        NJson::TJsonValue& data = result.Json["data"];
        data.SetType(NJson::JSON_ARRAY);
        for (const auto& value : values) {
            data.AppendValue(value);
        }
        return result;
    }

    TResult HandleSeries(const TInMemoryMetricsRegistry& inMemoryMetrics) const {
        TString error;
        const auto selectors = ParseMatchers("match[]", error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }
        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        auto selected = SelectSeries(snapshot.CommonLabels, lines, selectors, error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        TResult result;
        result.Json["status"] = "success";
        NJson::TJsonValue& data = result.Json["data"];
        data.SetType(NJson::JSON_ARRAY);
        for (const auto* line : selected) {
            data.AppendValue(BuildMetricObject(snapshot.CommonLabels, *line));
        }
        return result;
    }

    TResult HandleQuery(const TInMemoryMetricsRegistry& inMemoryMetrics) const {
        const TString query = Params.Get("query");
        if (query.empty()) {
            return MakeError("bad_data", "query is required");
        }

        TPrometheusQueryExpression expression;
        TString error;
        if (!ParseQueryParam(query, expression, error)) {
            return MakeError("bad_data", error);
        }

        const double evalTime = GetFloatParam("time").value_or(TInstant::Now().Seconds());
        const ui64 evalTimestamp = static_cast<ui64>(evalTime);

        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        if (expression.LastOverTimeWindow) {
            const i64 windowSeconds = Max<i64>(1, expression.LastOverTimeWindow->Seconds());
            const ui64 windowStart = evalTimestamp > static_cast<ui64>(windowSeconds)
                ? evalTimestamp - static_cast<ui64>(windowSeconds)
                : 0;
            const auto rangeSamples = CollectRangeSamples(snapshot.CommonLabels, lines, expression.Selector, windowStart, evalTimestamp, error);
            if (!error.empty()) {
                return MakeError("bad_data", error);
            }

            TResult result;
            result.Json["status"] = "success";
            result.Json["data"]["resultType"] = "vector";
            NJson::TJsonValue& output = result.Json["data"]["result"];
            output.SetType(NJson::JSON_ARRAY);

            for (const auto& sample : EvaluateLastOverTimeInstant(rangeSamples, evalTimestamp, *expression.LastOverTimeWindow)) {
                AppendInstantResult(output, sample);
            }
            return result;
        }

        const auto samples = CollectInstantSamples(snapshot.CommonLabels, lines, expression.Selector, evalTimestamp, error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        TResult result;
        result.Json["status"] = "success";
        result.Json["data"]["resultType"] = "vector";
        NJson::TJsonValue& output = result.Json["data"]["result"];
        output.SetType(NJson::JSON_ARRAY);

        const auto rendered = expression.Aggregation
            ? AggregateInstantSamples(samples, *expression.Aggregation)
            : samples;
        for (const auto& sample : rendered) {
            AppendInstantResult(output, sample);
        }

        return result;
    }

    TResult HandleQueryRange(const TInMemoryMetricsRegistry& inMemoryMetrics) const {
        const TString query = Params.Get("query");
        if (query.empty()) {
            return MakeError("bad_data", "query is required");
        }
        const auto start = GetFloatParam("start");
        const auto end = GetFloatParam("end");
        if (!start || !end) {
            return MakeError("bad_data", "start and end are required");
        }
        if (*end < *start) {
            return MakeError("bad_data", "end must be greater than or equal to start");
        }

        TPrometheusQueryExpression expression;
        TString error;
        if (!ParseQueryParam(query, expression, error)) {
            return MakeError("bad_data", error);
        }

        const ui64 startTs = static_cast<ui64>(*start);
        const ui64 endTs = static_cast<ui64>(*end);
        const auto step = GetStepParam(error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        const auto snapshot = inMemoryMetrics.Snapshot();
        const auto lines = snapshot.Lines();
        const auto samples = CollectRangeSamples(snapshot.CommonLabels, lines, expression.Selector, startTs, endTs, error);
        if (!error.empty()) {
            return MakeError("bad_data", error);
        }

        TResult result;
        result.Json["status"] = "success";
        result.Json["data"]["resultType"] = "matrix";
        NJson::TJsonValue& output = result.Json["data"]["result"];
        output.SetType(NJson::JSON_ARRAY);

        if (expression.LastOverTimeWindow) {
            if (!step) {
                return MakeError("bad_data", "step is required");
            }
            for (const auto& sample : EvaluateLastOverTimeRange(samples, startTs, endTs, *step, *expression.LastOverTimeWindow)) {
                AppendRangeResult(output, sample);
            }
            return result;
        }

        const auto filled = step
            ? FillOnChangeWithHeartbeatRangeGaps(samples, startTs, endTs, *step)
            : samples;
        const auto rendered = expression.Aggregation
            ? AggregateRangeSamples(filled, *expression.Aggregation)
            : filled;
        for (const auto& sample : rendered) {
            AppendRangeResult(output, sample);
        }

        return result;
    }
};

class TJsonInMemoryMetricsPrometheus : public TViewerPipeClient {
    using TThis = TJsonInMemoryMetricsPrometheus;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    TRequestResponse<TEvInterconnect::TEvNodesInfo> NodesInfo;
    THashMap<ui32, TRequestResponse<TEvViewer::TEvViewerResponse>> RemoteResponses;
    TVector<TPrometheusVectorSelector> RoutingSelectors;
    TString FanoutQueryString;

public:
    TJsonInMemoryMetricsPrometheus(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {
        NeedRedirect = false;
        CheckDatabase = false;
    }

    void Bootstrap() override {
        TString error;
        const TInMemoryMetricsPrometheusProcessor processor(GetRequestPath(), Params);
        RoutingSelectors = processor.GetRoutingSelectors(error);
        if (!error.empty()) {
            return ReplyAndPassAway(BuildErrorResponse("bad_data", error));
        }

        const bool usesAggregation = processor.UsesAggregation(error);
        if (!error.empty()) {
            return ReplyAndPassAway(BuildErrorResponse("bad_data", error));
        }
        const bool usesLastOverTime = processor.UsesLastOverTime(error);
        if (!error.empty()) {
            return ReplyAndPassAway(BuildErrorResponse("bad_data", error));
        }
        FanoutQueryString = (usesAggregation || usesLastOverTime) ? processor.BuildSelectorOnlyQueryString(error) : Params.Print();
        if (!error.empty()) {
            return ReplyAndPassAway(BuildErrorResponse("bad_data", error));
        }

        if (const auto exactNodeId = GetRequestedExactNodeId()) {
            if (*exactNodeId != TActivationContext::ActorSystem()->NodeId) {
                return ReplyAndPassAway(MakeForward({*exactNodeId}));
            }
        } else if (usesLastOverTime && GetRequestPath() == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
            return ReplyAndPassAway(BuildErrorResponse("not_implemented", "last_over_time fanout is only supported for query_range"));
        } else if (processor.SupportsFanout()) {
            NodesInfo = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
            return Become(&TThis::StateFanout, Timeout, new TEvents::TEvWakeup());
        }

        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            return ReplyAndPassAway(BuildErrorResponse("execution", "In-memory metrics subsystem is not registered"));
        }

        ReplyAndPassAway(BuildHttpResponse(processor.Execute(*inMemoryMetrics)));
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
    STATEFN(StateFanout) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleFanoutTimeout);
        }
    }

    TString GetRequestPath() const {
        if (Event) {
            return TString("/") + Event->Get()->Request.GetPage()->Path + Event->Get()->Request.GetPathInfo();
        }
        if (HttpEvent) {
            return TString(HttpEvent->Get()->Request->GetURI());
        }
        return {};
    }

    TString BuildErrorResponse(TStringBuf errorType, TStringBuf message) {
        NJson::TJsonValue json;
        json["status"] = "error";
        json["errorType"] = TString(errorType);
        json["error"] = TString(message);
        return GetHTTPBADREQUEST("application/json", SerializeJsonValue(json));
    }

    TString BuildHttpResponse(const TInMemoryMetricsPrometheusProcessor::TResult& result) {
        const TString json = SerializeJsonValue(result.Json);
        if (!result.Success) {
            return GetHTTPBADREQUEST("application/json", json);
        }
        return GetHTTPOKJSON(json);
    }

    std::optional<ui32> GetRequestedExactNodeId() const {
        if (RoutingSelectors.empty()) {
            return {};
        }

        std::optional<ui32> nodeId;
        for (const auto& selector : RoutingSelectors) {
            const auto selectorNodeId = TryGetExactNodeIdMatcher(selector);
            if (!selectorNodeId) {
                return {};
            }
            if (nodeId && *nodeId != *selectorNodeId) {
                return {};
            }
            nodeId = selectorNodeId;
        }
        return nodeId;
    }

    bool SelectorMatchesNodeId(const TPrometheusVectorSelector& selector, ui32 nodeId, TString& error) const {
        const TString actual = ToString(nodeId);
        bool hasNodeMatcher = false;
        for (const auto& matcher : selector.Matchers) {
            if (matcher.Name != "node_id") {
                continue;
            }
            hasNodeMatcher = true;
            if (!MatchPrometheusLabelValue(actual, matcher, error)) {
                return false;
            }
            if (!error.empty()) {
                return false;
            }
        }
        return !hasNodeMatcher || error.empty();
    }

    bool ShouldQueryNode(ui32 nodeId, TString& error) const {
        error.clear();
        if (RoutingSelectors.empty()) {
            return true;
        }
        for (const auto& selector : RoutingSelectors) {
            const bool matched = SelectorMatchesNodeId(selector, nodeId, error);
            if (!error.empty()) {
                return false;
            }
            if (matched) {
                return true;
            }
        }
        return false;
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (!NodesInfo.Set(std::move(ev))) {
            return;
        }

        TString error;
        THashSet<ui32> uniqueNodes;
        for (const auto& node : NodesInfo->Nodes) {
            if (ShouldQueryNode(node.NodeId, error)) {
                uniqueNodes.insert(node.NodeId);
            }
        }
        if (!error.empty()) {
            return ReplyAndPassAway(BuildErrorResponse("bad_data", error));
        }

        for (ui32 nodeId : uniqueNodes) {
            if (nodeId == TActivationContext::ActorSystem()->NodeId) {
                continue;
            }

            auto request = MakeHolder<TEvViewer::TEvViewerRequest>();
            request->Record.MutableLocation()->AddNodeId(nodeId);
            auto* inMemoryRequest = request->Record.MutableInMemoryMetricsRequest();
            inMemoryRequest->SetPath(GetRequestPath());
            inMemoryRequest->SetQuery(FanoutQueryString);

            RemoteResponses.emplace(nodeId,
                MakeRequestViewer(nodeId,
                    request.Release(),
                    IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
        }

        if (RemoteResponses.empty()) {
            ReplyAndPassAway(BuildFanoutResponse(false));
        }
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        const ui32 nodeId = ev.Get()->Cookie;
        auto it = RemoteResponses.find(nodeId);
        if (it == RemoteResponses.end()) {
            return;
        }
        if (it->second.Set(std::move(ev)) && AllRemoteResponsesDone()) {
            ReplyAndPassAway(BuildFanoutResponse(false));
        }
    }

    void HandleFanoutTimeout() {
        ReplyAndPassAway(BuildFanoutResponse(true));
    }

    bool AllRemoteResponsesDone() const {
        for (const auto& [_, response] : RemoteResponses) {
            if (!response.IsDone()) {
                return false;
            }
        }
        return true;
    }

    NJson::TJsonValue BuildEmptyMergedResponse() const {
        const TString path = GetRequestPath();
        NJson::TJsonValue json;
        json["status"] = "success";

        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
            json["data"]["resultType"] = "vector";
            json["data"]["result"].SetType(NJson::JSON_ARRAY);
            return json;
        }
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/query_range") {
            json["data"]["resultType"] = "matrix";
            json["data"]["result"].SetType(NJson::JSON_ARRAY);
            return json;
        }

        json["data"].SetType(NJson::JSON_ARRAY);
        return json;
    }

    void MergeResponseData(NJson::TJsonValue& merged, const NJson::TJsonValue& response) const {
        const TString path = GetRequestPath();
        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/labels"
                || path == "/viewer/inmemory_metrics/prometheus/api/v1/label/__name__/values"
                || (path.StartsWith("/viewer/inmemory_metrics/prometheus/api/v1/label/") && path.EndsWith("/values")))
        {
            THashSet<TString> seen;
            TVector<TString> values;
            auto collect = [&](const NJson::TJsonValue& value) {
                for (const auto& item : value["data"].GetArray()) {
                    const TString raw = item.GetStringRobust();
                    if (seen.emplace(raw).second) {
                        values.push_back(raw);
                    }
                }
            };
            collect(merged);
            collect(response);
            SortAndUniqueStrings(values);

            NJson::TJsonValue data(NJson::JSON_ARRAY);
            for (const auto& value : values) {
                data.AppendValue(value);
            }
            merged["data"] = std::move(data);
            return;
        }

        if (path == "/viewer/inmemory_metrics/prometheus/api/v1/series") {
            THashMap<TString, NJson::TJsonValue> byKey;
            auto collect = [&](const NJson::TJsonValue& value) {
                for (const auto& item : value["data"].GetArray()) {
                    byKey[BuildCanonicalMetricKey(item)] = item;
                }
            };
            collect(merged);
            collect(response);

            TVector<TString> keys;
            keys.reserve(byKey.size());
            for (const auto& [key, _] : byKey) {
                keys.push_back(key);
            }
            Sort(keys);

            NJson::TJsonValue data(NJson::JSON_ARRAY);
            for (const auto& key : keys) {
                data.AppendValue(byKey.at(key));
            }
            merged["data"] = std::move(data);
            return;
        }

        THashMap<TString, NJson::TJsonValue> byKey;
        auto collect = [&](const NJson::TJsonValue& value) {
            for (const auto& item : value["data"]["result"].GetArray()) {
                const TString key = BuildCanonicalMetricKey(item["metric"]);
                if (path == "/viewer/inmemory_metrics/prometheus/api/v1/query") {
                    byKey[key] = item;
                    continue;
                }

                auto& target = byKey[key];
                if (target.GetType() == NJson::JSON_UNDEFINED) {
                    target = item;
                    continue;
                }

                THashSet<ui64> timestamps;
                NJson::TJsonValue values(NJson::JSON_ARRAY);
                auto append = [&](const NJson::TJsonValue& source) {
                    for (const auto& sourceValue : source["values"].GetArray()) {
                        const ui64 ts = sourceValue[0].GetUIntegerRobust();
                        if (timestamps.emplace(ts).second) {
                            values.AppendValue(sourceValue);
                        }
                    }
                };
                append(target);
                append(item);
                target["values"] = std::move(values);
            }
        };
        collect(merged);
        collect(response);

        TVector<TString> keys;
        keys.reserve(byKey.size());
        for (const auto& [key, _] : byKey) {
            keys.push_back(key);
        }
        Sort(keys);

        NJson::TJsonValue result(NJson::JSON_ARRAY);
        for (const auto& key : keys) {
            result.AppendValue(byKey.at(key));
        }
        merged["data"]["result"] = std::move(result);
    }

    TString BuildFanoutResponse(bool timedOut) {
        const auto* inMemoryMetrics = TActivationContext::ActorSystem()->GetSubSystem<TInMemoryMetricsRegistry>();
        if (!inMemoryMetrics) {
            return BuildErrorResponse("execution", "In-memory metrics subsystem is not registered");
        }

        TInMemoryMetricsPrometheusProcessor processor(GetRequestPath(), Params);
        TString error;
        const bool usesAggregation = processor.UsesAggregation(error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        const bool usesLastOverTime = processor.UsesLastOverTime(error);
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        const TString queryString = (usesAggregation || usesLastOverTime) ? processor.BuildSelectorOnlyQueryString(error) : Params.Print();
        if (!error.empty()) {
            return BuildErrorResponse("bad_data", error);
        }
        TCgiParameters executionParams(queryString);
        TInMemoryMetricsPrometheusProcessor executionProcessor(GetRequestPath(), executionParams);
        NJson::TJsonValue merged = BuildEmptyMergedResponse();
        TVector<TString> warnings;

        TString selfError;
        const bool includeSelf = ShouldQueryNode(TActivationContext::ActorSystem()->NodeId, selfError);
        if (!selfError.empty()) {
            return BuildErrorResponse("bad_data", selfError);
        }
        if (includeSelf) {
            const auto localResult = executionProcessor.Execute(*inMemoryMetrics);
            if (!localResult.Success) {
                return BuildHttpResponse(localResult);
            }
            MergeResponseData(merged, localResult.Json);
        }

        if (!NodesInfo.IsDone()) {
            warnings.push_back("node discovery timed out; returning local response only");
        } else if (!NodesInfo.IsOk()) {
            warnings.push_back("failed to discover cluster nodes; returning local response only");
        }

        for (const auto& [nodeId, response] : RemoteResponses) {
            if (!response.IsDone()) {
                if (timedOut) {
                    warnings.push_back(TStringBuilder() << "node " << nodeId << " did not respond before timeout");
                }
                continue;
            }
            if (!response.IsOk() || response->Record.Response_case() != NKikimrViewer::TEvViewerResponse::kInMemoryMetricsResponse) {
                warnings.push_back(TStringBuilder() << "node " << nodeId << " returned invalid in-memory metrics response");
                continue;
            }

            NJson::TJsonValue json;
            if (!ParseJsonValue(response->Record.GetInMemoryMetricsResponse().GetJson(), json)) {
                warnings.push_back(TStringBuilder() << "node " << nodeId << " returned malformed json");
                continue;
            }
            if (json["status"].GetStringSafe() != "success") {
                warnings.push_back(TStringBuilder() << "node " << nodeId << " returned error: " << json["error"].GetStringSafe());
                continue;
            }
            MergeResponseData(merged, json);
        }

        if (usesAggregation || usesLastOverTime) {
            if (!processor.ApplyAggregationToResult(merged, error)) {
                return BuildErrorResponse("bad_data", error);
            }
        }

        if (!warnings.empty()) {
            SortAndUniqueStrings(warnings);
            NJson::TJsonValue& jsonWarnings = merged["warnings"];
            jsonWarnings.SetType(NJson::JSON_ARRAY);
            for (const auto& warning : warnings) {
                jsonWarnings.AppendValue(warning);
            }
        }

        return GetHTTPOKJSON(SerializeJsonValue(merged));
    }
};

} // namespace NKikimr::NViewer
