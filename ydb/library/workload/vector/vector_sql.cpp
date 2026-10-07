#include "vector_sql.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/subst.h>

#include <format>
#include <string>

#include <algorithm>

namespace NYdbWorkload {

TVector<TString> MakeIndexReadReplicasQueries(const TVectorWorkloadParams& params, const TString& settings) {
    if (settings.empty()) {
        return {};
    }
    const auto colon = settings.find(':');
    const TString mode = to_upper(settings.substr(0, colon));
    const TString countText = colon == TString::npos ? TString() : settings.substr(colon + 1);
    ui64 count = 0;
    Y_ENSURE((mode == "PER_AZ" || mode == "ANY_AZ") && !countText.empty()
        && std::all_of(countText.begin(), countText.end(), [](char c) { return c >= '0' && c <= '9'; })
        && TryFromString<ui64>(countText, count),
        "Invalid --read-replicas-settings: expected PER_AZ:N or ANY_AZ:N with an unsigned integer count");

    TVector<TString> queries;
    for (const TStringBuf implTable : {"indexImplLevelTable", "indexImplPostingTable"}) {
        const TString path = TStringBuilder() << params.DbPath << "/" << params.TableOpts.Name
            << "/" << params.IndexName << "/" << implTable;
        TString escapedPath = EscapeC(path);
        SubstGlobal(escapedPath, "`", "\\`");
        queries.push_back(TStringBuilder() << "ALTER TABLE `" << escapedPath << "` SET (\n"
            << "    READ_REPLICAS_SETTINGS = '" << mode << ":" << count << "'\n);");
    }
    return queries;
}


// Utility function to get metric info for SQL query
// Returns a tuple of (function_name, is_ascending)
std::tuple<std::string, bool> GetMetricInfo(NYdb::NTable::TVectorIndexSettings::EMetric metric) {
    switch (metric) {
        case NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct:
            return {"InnerProductSimilarity", false}; // Similarity, higher is better (DESC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity:
            return {"CosineSimilarity", false}; // Similarity, higher is better (DESC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance:
            return {"CosineDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Manhattan:
            return {"ManhattanDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean:
            return {"EuclideanDistance", true}; // Distance, lower is better (ASC)

        case NYdb::NTable::TVectorIndexSettings::EMetric::Unspecified:
        default:
            Y_ABORT("Unspecified metric");
    }
}


std::string MakeKeyExpression(const TVectorWorkloadParams& params, const std::string& tableAlias) {
    TStringBuilder ret;
    if (params.KeyColumns.size() == 1) {
        ret << "UNWRAP(CAST(" << tableAlias << params.KeyColumns[0] << " AS string))";
        return ret;
    }
    ret << "UNWRAP(\"\\\"\" || ";
    for (size_t i = 0; i < params.KeyColumns.size(); i++) {
        if (i > 0) {
            ret << " || \"\\\",\\\"\" || ";
        }
        ret << "String::EscapeC(CAST(" << tableAlias << params.KeyColumns[i] << " AS string))";
    }
    ret << " || \"\\\"\")";
    return ret;
}


// Utility function to create select query
std::string MakeSelect(const TVectorWorkloadParams& params, const TString& indexName) {
    auto [functionName, isAscending] = GetMetricInfo(params.Metric);

    TStringBuilder ret;
    ret << "--!syntax_v1" << "\n";
    if (params.Hnsw) {
        ret << "PRAGMA ydb.HNSWEfSearch=\"" << params.HnswEfSearch << "\";\n";
    }
    ret << "DECLARE $Embedding as String;" << "\n";
    if (params.PrefixColumn)
        ret << "DECLARE $PrefixValue as " << params.PrefixType << ";" << "\n";
    ret << "pragma ydb.KMeansTreeSearchTopSize=\"" << params.KmeansTreeSearchClusters << "\";" << "\n";
    ret << "SELECT " << MakeKeyExpression(params, "") << " AS id FROM `" << params.TableOpts.Name << "`\n";
    if (!indexName.empty()) {
        ret << "VIEW " << indexName << "\n";
    }
    if (params.PrefixColumn)
        ret << "WHERE " << params.PrefixColumn << " = $PrefixValue" << "\n";
    ret << "ORDER BY Knn::" << functionName << "(" << params.EmbeddingColumn << ", $Embedding) " << (isAscending ? "ASC" : "DESC") << "\n";
    ret << "LIMIT $Limit" << "\n";
    return ret;
}


// Utility function to create parameters for select query
NYdb::TParams MakeSelectParams(const std::string& embeddingBytes, const std::optional<NYdb::TValue>& prefixValue, ui64 limit) {
    NYdb::TParamsBuilder paramsBuilder;

    paramsBuilder.AddParam("$Embedding").String(embeddingBytes).Build();
    paramsBuilder.AddParam("$Limit").Uint64(limit).Build();

    if (prefixValue.has_value()) {
        paramsBuilder.AddParam("$PrefixValue", *prefixValue);
    }

    return paramsBuilder.Build();
}

} // namespace NYdbWorkload
