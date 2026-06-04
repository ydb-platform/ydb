#pragma once

#include "grafana_dashboard_common.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <limits>

namespace NMVP::NSupportLinks {

struct TGrafanaDashboardCandidate {
    TString Title;
    TString ResolvedUrl;
    TString DashboardApiUrl;
    TCgiParameters QueryParameters;
};

struct TGrafanaProbeGroup {
    TString DatasourceUid;
    TString Query;
    TVector<TString> Buckets;
};

namespace NGrafanaDashboardProbeDetails {

enum class EExpressionOrigin {
    VariableQuery,
    PanelExpr,
};

struct TExpressionCandidate {
    TString Expression;
    TString DatasourceRef;
    EExpressionOrigin Origin = EExpressionOrigin::VariableQuery;
};

struct TLabelMatcher {
    TString Name;
    TString Op;
    TString Value;
};

struct TPromSelector {
    TString Metric;
    TVector<TLabelMatcher> Matchers;
};

struct TBucketProbe {
    TString Selector;
    int Score = std::numeric_limits<int>::min();
};

inline bool IsIdentifierChar(char ch) {
    return (ch >= 'a' && ch <= 'z')
        || (ch >= 'A' && ch <= 'Z')
        || (ch >= '0' && ch <= '9')
        || ch == '_'
        || ch == ':';
}

inline bool IsSpace(char ch) {
    return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t';
}

inline void SkipSpaces(TStringBuf text, size_t& pos) {
    while (pos < text.size() && IsSpace(text[pos])) {
        ++pos;
    }
}

inline TString GetJsonStringField(const NJson::TJsonValue& value, TStringBuf key) {
    if (!value.Has(TString(key))) {
        return {};
    }
    const auto& field = value[TString(key)];
    return field.GetType() == NJson::JSON_STRING
        ? field.GetString()
        : TString();
}

inline TString GetDatasourceReference(const NJson::TJsonValue& value) {
    if (value.GetType() == NJson::JSON_STRING) {
        return value.GetString();
    }
    if (value.GetType() != NJson::JSON_MAP) {
        return {};
    }
    TString uid = GetJsonStringField(value, "uid");
    if (!uid.empty()) {
        return uid;
    }
    return GetJsonStringField(value, "name");
}

inline TString GetVariableQueryText(const NJson::TJsonValue& value) {
    if (value.GetType() == NJson::JSON_STRING) {
        return value.GetString();
    }
    if (value.GetType() == NJson::JSON_MAP) {
        TString query = GetJsonStringField(value, "query");
        if (!query.empty()) {
            return query;
        }
    }
    return {};
}

inline TString EscapePromLabelValue(TStringBuf value) {
    TStringBuilder builder;
    for (char ch : value) {
        if (ch == '\\' || ch == '"') {
            builder << '\\';
        }
        builder << ch;
    }
    return builder;
}

inline TString BuildSelectorText(const TPromSelector& selector) {
    TStringBuilder builder;
    builder << selector.Metric << '{';
    bool first = true;
    for (const auto& matcher : selector.Matchers) {
        if (!first) {
            builder << ',';
        }
        first = false;
        builder
            << matcher.Name
            << matcher.Op
            << '"'
            << EscapePromLabelValue(matcher.Value)
            << '"';
    }
    builder << '}';
    return builder;
}

inline TString BuildSignalProbeQuery(TStringBuf selector) {
    return TStringBuilder()
        << "count(count_over_time(" << selector << "[1m])) or on() vector(0)";
}

inline bool TryGetGrafanaVariableValue(
    const TCgiParameters& queryParameters,
    TStringBuf variableName,
    TString& value)
{
    const TString key = TStringBuilder() << "var-" << variableName;
    if (!queryParameters.Has(key)) {
        return false;
    }
    value = queryParameters.Get(key);
    return true;
}

inline bool ResolveTemplateString(
    TStringBuf rawValue,
    const TCgiParameters& queryParameters,
    TString& resolvedValue,
    bool& hasAllValue)
{
    resolvedValue.clear();
    hasAllValue = false;

    for (size_t pos = 0; pos < rawValue.size();) {
        if (rawValue[pos] != '$') {
            resolvedValue.push_back(rawValue[pos]);
            ++pos;
            continue;
        }

        TString variableName;
        size_t nextPos = pos;
        if (pos + 1 < rawValue.size() && rawValue[pos + 1] == '{') {
            const size_t endPos = rawValue.find('}', pos + 2);
            if (endPos == TStringBuf::npos) {
                return false;
            }
            TStringBuf variableSpec = rawValue.SubStr(pos + 2, endPos - pos - 2);
            variableName = TString(variableSpec.Before(':'));
            nextPos = endPos + 1;
        } else {
            size_t endPos = pos + 1;
            while (endPos < rawValue.size() && (IsIdentifierChar(rawValue[endPos]) || rawValue[endPos] == '-')) {
                ++endPos;
            }
            if (endPos == pos + 1) {
                resolvedValue.push_back(rawValue[pos]);
                ++pos;
                continue;
            }
            variableName = TString(rawValue.SubStr(pos + 1, endPos - pos - 1));
            nextPos = endPos;
        }

        TString variableValue;
        if (!TryGetGrafanaVariableValue(queryParameters, variableName, variableValue)) {
            return false;
        }
        if (variableValue == "$__all" || variableValue == "__all") {
            hasAllValue = true;
        }
        resolvedValue += variableValue;
        pos = nextPos;
    }

    return true;
}

inline bool ParseQuotedValue(TStringBuf text, size_t& pos, TString& value) {
    if (pos >= text.size() || text[pos] != '"') {
        return false;
    }

    ++pos;
    value.clear();
    bool escaped = false;
    while (pos < text.size()) {
        const char ch = text[pos++];
        if (escaped) {
            value.push_back(ch);
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            return true;
        }
        value.push_back(ch);
    }

    return false;
}

inline bool TryParseSelector(TStringBuf selectorText, TPromSelector& selector) {
    const size_t openBracePos = selectorText.find('{');
    const size_t closeBracePos = selectorText.rfind('}');
    if (openBracePos == TStringBuf::npos || closeBracePos == TStringBuf::npos || closeBracePos < openBracePos) {
        return false;
    }

    selector.Metric = StripString(TString(selectorText.SubStr(0, openBracePos)));
    selector.Matchers.clear();

    const TStringBuf matchersText = selectorText.SubStr(openBracePos + 1, closeBracePos - openBracePos - 1);
    size_t pos = 0;
    while (pos < matchersText.size()) {
        SkipSpaces(matchersText, pos);
        if (pos == matchersText.size()) {
            break;
        }
        if (matchersText[pos] == ',') {
            ++pos;
            continue;
        }

        const size_t nameStart = pos;
        while (pos < matchersText.size() && IsIdentifierChar(matchersText[pos])) {
            ++pos;
        }
        if (pos == nameStart) {
            return false;
        }

        TLabelMatcher matcher;
        matcher.Name = TString(matchersText.SubStr(nameStart, pos - nameStart));
        SkipSpaces(matchersText, pos);
        if (pos >= matchersText.size()) {
            return false;
        }

        if (matchersText[pos] == '=' && pos + 1 < matchersText.size() && matchersText[pos + 1] == '~') {
            matcher.Op = "=~";
            pos += 2;
        } else if (matchersText[pos] == '!' && pos + 1 < matchersText.size() && matchersText[pos + 1] == '=') {
            matcher.Op = "!=";
            pos += 2;
        } else if (matchersText[pos] == '!' && pos + 1 < matchersText.size() && matchersText[pos + 1] == '~') {
            matcher.Op = "!~";
            pos += 2;
        } else if (matchersText[pos] == '=') {
            matcher.Op = "=";
            ++pos;
        } else {
            return false;
        }

        SkipSpaces(matchersText, pos);
        if (!ParseQuotedValue(matchersText, pos, matcher.Value)) {
            return false;
        }

        selector.Matchers.push_back(std::move(matcher));
        SkipSpaces(matchersText, pos);
        if (pos < matchersText.size() && matchersText[pos] == ',') {
            ++pos;
        }
    }

    return true;
}

inline size_t FindSelectorEnd(TStringBuf expression, size_t openBracePos) {
    bool insideString = false;
    bool escaped = false;

    for (size_t pos = openBracePos + 1; pos < expression.size(); ++pos) {
        const char ch = expression[pos];
        if (insideString) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                insideString = false;
            }
            continue;
        }
        if (ch == '"') {
            insideString = true;
            continue;
        }
        if (ch == '}') {
            return pos;
        }
    }

    return TStringBuf::npos;
}

inline TVector<TString> ExtractSelectors(TStringBuf expression) {
    TVector<TString> selectors;
    bool insideString = false;
    bool escaped = false;

    for (size_t pos = 0; pos < expression.size(); ++pos) {
        const char ch = expression[pos];
        if (insideString) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                insideString = false;
            }
            continue;
        }
        if (ch == '"') {
            insideString = true;
            continue;
        }
        if (ch != '{') {
            continue;
        }

        const size_t selectorEnd = FindSelectorEnd(expression, pos);
        if (selectorEnd == TStringBuf::npos) {
            break;
        }

        size_t selectorStart = pos;
        while (selectorStart > 0 && IsIdentifierChar(expression[selectorStart - 1])) {
            --selectorStart;
        }

        TString selector(expression.SubStr(selectorStart, selectorEnd - selectorStart + 1));
        if (selector.Contains("__bucket__")) {
            selectors.push_back(std::move(selector));
        }
        pos = selectorEnd;
    }

    return selectors;
}

inline bool HasNonTenantMatcher(const TPromSelector& selector) {
    for (const auto& matcher : selector.Matchers) {
        if (matcher.Name != "__workspace__" && matcher.Name != "__bucket__") {
            return true;
        }
    }
    return false;
}

inline bool ContainsRegexMeta(TStringBuf value) {
    for (char ch : value) {
        if (ch == '\\'
            || ch == '.'
            || ch == '*'
            || ch == '+'
            || ch == '?'
            || ch == '['
            || ch == ']'
            || ch == '('
            || ch == ')'
            || ch == '{'
            || ch == '}')
        {
            return true;
        }
    }
    return false;
}

inline bool TrySplitAlternatives(TStringBuf value, TVector<TString>& values) {
    values.clear();
    if (value.empty() || ContainsRegexMeta(value)) {
        return false;
    }

    for (TStringBuf token = value.NextTok('|'); !token.empty(); token = value.NextTok('|')) {
        values.push_back(TString(token));
    }

    return !values.empty();
}

inline bool TryResolveTenantValues(
    TStringBuf op,
    TStringBuf value,
    TVector<TString>& values)
{
    values.clear();
    if (value.empty() || value == ".*") {
        return false;
    }

    if (op == "=") {
        values.push_back(TString(value));
        return true;
    }

    if (op == "=~") {
        return TrySplitAlternatives(value, values);
    }

    return false;
}

inline int ScoreSelector(const TPromSelector& selector, EExpressionOrigin origin) {
    int score = origin == EExpressionOrigin::PanelExpr ? 20 : 10;
    if (!selector.Metric.empty()) {
        score += 10;
    }
    for (const auto& matcher : selector.Matchers) {
        if (matcher.Name == "__workspace__" || matcher.Name == "__bucket__") {
            continue;
        }
        if (matcher.Op == "=") {
            score += 10;
        } else if (matcher.Op == "=~") {
            score += 2;
        }
        if ((matcher.Name == "database"
                || matcher.Name == "host"
                || matcher.Name == "node"
                || matcher.Name == "cluster"
                || matcher.Name == "storagePool")
            && matcher.Op == "=")
        {
            score += 10;
        }
    }
    return score;
}

inline TString ResolveDatasourceUid(TStringBuf datasourceRef, const TCgiParameters& queryParameters) {
    TString datasourceUid;
    if (!datasourceRef.empty()) {
        bool hasAllValue = false;
        if (ResolveTemplateString(datasourceRef, queryParameters, datasourceUid, hasAllValue) && !datasourceUid.empty()) {
            if (datasourceUid != "$__all" && datasourceUid != "__all") {
                return datasourceUid;
            }
        } else if (!datasourceRef.Contains('$')) {
            return TString(datasourceRef);
        }
    }

    if (queryParameters.Has("var-ds")) {
        return queryParameters.Get("var-ds");
    }

    return {};
}

inline bool BuildConcreteSelectors(
    const TPromSelector& selector,
    const TCgiParameters& queryParameters,
    TVector<TPromSelector>& concreteSelectors)
{
    concreteSelectors.clear();

    if (selector.Metric.empty() && !HasNonTenantMatcher(selector)) {
        return false;
    }

    TVector<TString> bucketValues;
    TVector<TString> workspaceValues;
    bool hasWorkspaceMatcher = false;

    TPromSelector resolvedSelector;
    resolvedSelector.Metric = selector.Metric;

    for (const auto& rawMatcher : selector.Matchers) {
        bool hasAllValue = false;
        TString resolvedValue;
        if (!ResolveTemplateString(rawMatcher.Value, queryParameters, resolvedValue, hasAllValue)) {
            return false;
        }

        TLabelMatcher resolvedMatcher = rawMatcher;
        if (hasAllValue) {
            if (resolvedMatcher.Op == "=~" || resolvedMatcher.Op == "!~") {
                resolvedValue = ".*";
            } else {
                return false;
            }
        }
        if (resolvedValue.empty()) {
            return false;
        }

        if (resolvedMatcher.Name == "__bucket__") {
            if (!TryResolveTenantValues(resolvedMatcher.Op, resolvedValue, bucketValues)) {
                return false;
            }
            resolvedMatcher.Op = "=";
            resolvedMatcher.Value = resolvedValue;
        } else if (resolvedMatcher.Name == "__workspace__") {
            hasWorkspaceMatcher = true;
            if (!TryResolveTenantValues(resolvedMatcher.Op, resolvedValue, workspaceValues)) {
                return false;
            }
            resolvedMatcher.Op = "=";
            resolvedMatcher.Value = resolvedValue;
        } else {
            resolvedMatcher.Value = resolvedValue;
        }

        resolvedSelector.Matchers.push_back(std::move(resolvedMatcher));
    }

    if (bucketValues.empty()) {
        return false;
    }
    if (!hasWorkspaceMatcher) {
        workspaceValues.push_back({});
    }

    for (const auto& bucketValue : bucketValues) {
        for (const auto& workspaceValue : workspaceValues) {
            TPromSelector concreteSelector = resolvedSelector;
            for (auto& matcher : concreteSelector.Matchers) {
                if (matcher.Name == "__bucket__") {
                    matcher.Op = "=";
                    matcher.Value = bucketValue;
                } else if (matcher.Name == "__workspace__" && hasWorkspaceMatcher) {
                    matcher.Op = "=";
                    matcher.Value = workspaceValue;
                }
            }
            concreteSelectors.push_back(std::move(concreteSelector));
        }
    }

    return !concreteSelectors.empty();
}

inline TString FindBucketValue(const TPromSelector& selector) {
    for (const auto& matcher : selector.Matchers) {
        if (matcher.Name == "__bucket__") {
            return matcher.Value;
        }
    }
    return {};
}

inline void CollectTemplatingExpressions(
    const NJson::TJsonValue& dashboard,
    TVector<TExpressionCandidate>& expressions)
{
    if (!dashboard.Has("templating") || dashboard["templating"].GetType() != NJson::JSON_MAP) {
        return;
    }
    const auto& templating = dashboard["templating"];
    if (!templating.Has("list") || templating["list"].GetType() != NJson::JSON_ARRAY) {
        return;
    }

    for (const auto& item : templating["list"].GetArray()) {
        if (item.GetType() != NJson::JSON_MAP) {
            continue;
        }

        TString queryText;
        if (item.Has("query")) {
            queryText = GetVariableQueryText(item["query"]);
        }
        if (queryText.empty()) {
            queryText = GetJsonStringField(item, "definition");
        }
        if (queryText.empty() || !queryText.Contains("__bucket__")) {
            continue;
        }

        expressions.push_back(TExpressionCandidate{
            .Expression = std::move(queryText),
            .DatasourceRef = item.Has("datasource") ? GetDatasourceReference(item["datasource"]) : TString(),
            .Origin = EExpressionOrigin::VariableQuery,
        });
    }
}

inline void CollectPanelExpressions(
    const NJson::TJsonValue& panel,
    TStringBuf inheritedDatasource,
    TVector<TExpressionCandidate>& expressions)
{
    if (panel.GetType() != NJson::JSON_MAP) {
        return;
    }

    TString panelDatasource(inheritedDatasource);
    if (panel.Has("datasource")) {
        const TString datasourceRef = GetDatasourceReference(panel["datasource"]);
        if (!datasourceRef.empty()) {
            panelDatasource = datasourceRef;
        }
    }

    if (panel.Has("targets") && panel["targets"].GetType() == NJson::JSON_ARRAY) {
        for (const auto& target : panel["targets"].GetArray()) {
            if (target.GetType() != NJson::JSON_MAP || !target.Has("expr") || target["expr"].GetType() != NJson::JSON_STRING) {
                continue;
            }
            const TString expression = target["expr"].GetString();
            if (!expression.Contains("__bucket__")) {
                continue;
            }

            TString datasourceRef = panelDatasource;
            if (target.Has("datasource")) {
                const TString targetDatasource = GetDatasourceReference(target["datasource"]);
                if (!targetDatasource.empty()) {
                    datasourceRef = targetDatasource;
                }
            }

            expressions.push_back(TExpressionCandidate{
                .Expression = expression,
                .DatasourceRef = std::move(datasourceRef),
                .Origin = EExpressionOrigin::PanelExpr,
            });
        }
    }

    if (panel.Has("panels") && panel["panels"].GetType() == NJson::JSON_ARRAY) {
        for (const auto& nestedPanel : panel["panels"].GetArray()) {
            CollectPanelExpressions(nestedPanel, panelDatasource, expressions);
        }
    }
}

inline void CollectDashboardExpressions(
    const NJson::TJsonValue& dashboard,
    TVector<TExpressionCandidate>& expressions)
{
    expressions.clear();
    CollectTemplatingExpressions(dashboard, expressions);

    if (!dashboard.Has("panels") || dashboard["panels"].GetType() != NJson::JSON_ARRAY) {
        return;
    }

    for (const auto& panel : dashboard["panels"].GetArray()) {
        CollectPanelExpressions(panel, TStringBuf(), expressions);
    }
}

inline TString BuildDashboardApiUrl(TStringBuf grafanaEndpoint, const NJson::TJsonValue& item) {
    TString uid = GetJsonStringField(item, "uid");
    if (!uid.empty()) {
        return JoinUrl(grafanaEndpoint, TStringBuilder() << "/api/dashboards/uid/" << uid);
    }

    TString dashboardUrl = GetJsonStringField(item, "url");
    if (dashboardUrl.empty()) {
        dashboardUrl = GetJsonStringField(item, "uri");
    }
    if (dashboardUrl.empty()) {
        return {};
    }

    const auto pathAndQuery = BuildGrafanaDashboardUrlParts(grafanaEndpoint, dashboardUrl);
    TStringBuf path = pathAndQuery.first;
    if (IsAbsoluteUrl(path)) {
        const size_t schemePos = path.find("://");
        if (schemePos != TStringBuf::npos) {
            const size_t pathPos = path.find('/', schemePos + 3);
            path = pathPos == TStringBuf::npos
                ? TStringBuf("/")
                : path.SubStr(pathPos);
        }
    }

    TVector<TString> pathParts;
    TStringBuf pathRemainder(path);
    while (!pathRemainder.empty() && pathRemainder[0] == '/') {
        pathRemainder = pathRemainder.SubStr(1);
    }
    for (TStringBuf token = pathRemainder.NextTok('/'); !token.empty(); token = pathRemainder.NextTok('/')) {
        pathParts.push_back(TString(token));
    }

    if (pathParts.size() >= 2 && pathParts[0] == "d") {
        return JoinUrl(grafanaEndpoint, TStringBuilder() << "/api/dashboards/uid/" << pathParts[1]);
    }
    if (pathParts.size() >= 2 && pathParts[0] == "db") {
        return JoinUrl(grafanaEndpoint, TStringBuilder() << "/api/dashboards/db/" << pathParts[1]);
    }

    return {};
}

inline bool TryBuildCandidate(
    const NJson::TJsonValue& item,
    TStringBuf grafanaEndpoint,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    TGrafanaDashboardCandidate& candidate)
{
    TString dashboardUrl = GetJsonStringField(item, "url");
    if (dashboardUrl.empty()) {
        dashboardUrl = GetJsonStringField(item, "uri");
    }
    if (dashboardUrl.empty()) {
        return false;
    }

    candidate = {};
    candidate.Title = GetJsonStringField(item, "title");
    candidate.ResolvedUrl = BuildGrafanaDashboardUrl(grafanaEndpoint, dashboardUrl, clusterInfo, requestQueryParameters);
    candidate.DashboardApiUrl = BuildDashboardApiUrl(grafanaEndpoint, item);
    const auto resolvedUrlParts = BuildGrafanaDashboardUrlParts(grafanaEndpoint, candidate.ResolvedUrl);
    candidate.QueryParameters = resolvedUrlParts.second;

    return !candidate.ResolvedUrl.empty() && !candidate.DashboardApiUrl.empty();
}

inline TVector<TGrafanaProbeGroup> BuildProbeGroups(
    const NJson::TJsonValue& dashboard,
    const TCgiParameters& queryParameters)
{
    TVector<TExpressionCandidate> expressions;
    CollectDashboardExpressions(dashboard, expressions);

    TMap<TString, TMap<TString, TBucketProbe>> bestProbes;
    for (const auto& expression : expressions) {
        const TString datasourceUid = ResolveDatasourceUid(expression.DatasourceRef, queryParameters);
        if (datasourceUid.empty()) {
            continue;
        }

        const auto selectorTexts = ExtractSelectors(expression.Expression);
        for (const auto& selectorText : selectorTexts) {
            TPromSelector rawSelector;
            if (!TryParseSelector(selectorText, rawSelector)) {
                continue;
            }

            TVector<TPromSelector> concreteSelectors;
            if (!BuildConcreteSelectors(rawSelector, queryParameters, concreteSelectors)) {
                continue;
            }

            for (const auto& concreteSelector : concreteSelectors) {
                const TString bucketValue = FindBucketValue(concreteSelector);
                if (bucketValue.empty()) {
                    continue;
                }

                const int score = ScoreSelector(concreteSelector, expression.Origin);
                auto& probe = bestProbes[datasourceUid][bucketValue];
                if (probe.Selector.empty() || score > probe.Score) {
                    probe.Selector = BuildSelectorText(concreteSelector);
                    probe.Score = score;
                }
            }
        }
    }

    TVector<TGrafanaProbeGroup> probeGroups;
    for (const auto& [datasourceUid, probesByBucket] : bestProbes) {
        TGrafanaProbeGroup probeGroup;
        probeGroup.DatasourceUid = datasourceUid;
        for (const auto& [bucket, probe] : probesByBucket) {
            if (probe.Selector.empty()) {
                continue;
            }
            if (!probeGroup.Query.empty()) {
                probeGroup.Query += " + ";
            }
            probeGroup.Query += TStringBuilder() << '(' << BuildSignalProbeQuery(probe.Selector) << ')';
            probeGroup.Buckets.push_back(bucket);
        }
        if (!probeGroup.Query.empty()) {
            probeGroups.push_back(std::move(probeGroup));
        }
    }

    return probeGroups;
}

inline bool TryExtractNumericValue(const NJson::TJsonValue& value, double& number) {
    if (value.GetType() == NJson::JSON_DOUBLE) {
        number = value.GetDouble();
        return true;
    }
    if (value.GetType() == NJson::JSON_INTEGER) {
        number = value.GetInteger();
        return true;
    }
    if (value.GetType() == NJson::JSON_STRING) {
        return TryFromString(value.GetString(), number);
    }
    return false;
}

inline bool HasSignalInProbeResponse(TStringBuf body) {
    NJson::TJsonValue responseJson;
    NJson::TJsonReaderConfig jsonReaderConfig;
    if (!NJson::ReadJsonTree(body, &jsonReaderConfig, &responseJson) || responseJson.GetType() != NJson::JSON_MAP) {
        return false;
    }
    if (!responseJson.Has("data") || responseJson["data"].GetType() != NJson::JSON_MAP) {
        return false;
    }

    const auto& data = responseJson["data"];
    if (data.Has("result") && data["result"].GetType() == NJson::JSON_ARRAY) {
        const auto& result = data["result"].GetArray();
        if (!result.empty() && result[0].GetType() == NJson::JSON_MAP) {
            for (const auto& item : result) {
                if (!item.Has("value") || item["value"].GetType() != NJson::JSON_ARRAY) {
                    continue;
                }
                const auto& value = item["value"].GetArray();
                if (value.size() < 2) {
                    continue;
                }
                double numericValue = 0;
                if (TryExtractNumericValue(value[1], numericValue) && numericValue > 0) {
                    return true;
                }
            }
        } else if (result.size() >= 2) {
            double numericValue = 0;
            return TryExtractNumericValue(result[1], numericValue) && numericValue > 0;
        }
    }

    return false;
}

} // namespace NGrafanaDashboardProbeDetails

inline TVector<TGrafanaProbeGroup> BuildGrafanaDashboardProbeGroups(
    const NJson::TJsonValue& dashboard,
    const TCgiParameters& queryParameters)
{
    return NGrafanaDashboardProbeDetails::BuildProbeGroups(dashboard, queryParameters);
}

inline bool TryBuildGrafanaDashboardCandidate(
    const NJson::TJsonValue& item,
    TStringBuf grafanaEndpoint,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    TGrafanaDashboardCandidate& candidate)
{
    return NGrafanaDashboardProbeDetails::TryBuildCandidate(
        item,
        grafanaEndpoint,
        clusterInfo,
        requestQueryParameters,
        candidate);
}

inline bool HasGrafanaDashboardProbeSignal(TStringBuf body) {
    return NGrafanaDashboardProbeDetails::HasSignalInProbeResponse(body);
}

} // namespace NMVP::NSupportLinks
