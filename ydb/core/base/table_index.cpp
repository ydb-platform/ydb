#include "table_index.h"

#include <util/string/cast.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <util/string/escape.h>

namespace NKikimr::NTableIndex {
namespace {

const TString* IsUnique(const TVector<TString>& names, THashSet<TString>& tmp) {
    tmp.clear();
    for (const auto& name : names) {
        if (!tmp.emplace(name).second) {
            return &name;
        }
    }
    return nullptr;
}

const TString* IsContains(const TVector<TString>& names, const THashSet<TString>& columns, bool contains = false) {
    for (const auto& name : names) {
        if (columns.contains(name) == contains) {
            return &name;
        }
    }
    return nullptr;
}

bool Contains(const auto& names, std::string_view str) {
    return std::find(std::begin(names), std::end(names), str) != std::end(names);
}

bool ContainsSystemColumn(const auto& columns) {
    for (const auto& column : columns) {
        if (column.StartsWith(SYSTEM_COLUMN_PREFIX)) {
            return true;
        }
    }
    return false;
}

const TString ImplTables[] = {
    ImplTable,
    NKMeans::LevelTable,
    NKMeans::PostingTable,
    NKMeans::PrefixTable,
    TString{NKMeans::PostingTable} + NKMeans::BuildSuffix0,
    TString{NKMeans::PostingTable} + NKMeans::BuildSuffix1,
    NFulltext::DocsTable,
    NFulltext::DictTable,
    NFulltext::StatsTable,
};

constexpr std::string_view GlobalSecondaryImplTables[] = {
    ImplTable,
};
static_assert(std::is_sorted(std::begin(GlobalSecondaryImplTables), std::end(GlobalSecondaryImplTables)));

constexpr std::string_view GlobalKMeansTreeImplTables[] = {
    NKMeans::LevelTable, NKMeans::PostingTable,
};
static_assert(std::is_sorted(std::begin(GlobalKMeansTreeImplTables), std::end(GlobalKMeansTreeImplTables)));

constexpr std::string_view PrefixedGlobalKMeansTreeImplTables[] = {
    NKMeans::LevelTable, NKMeans::PostingTable, NKMeans::PrefixTable,
};
static_assert(std::is_sorted(std::begin(PrefixedGlobalKMeansTreeImplTables), std::end(PrefixedGlobalKMeansTreeImplTables)));

constexpr std::string_view GlobalFulltextPlainImplTables[] = {
    ImplTable,
};
static_assert(std::is_sorted(std::begin(GlobalFulltextPlainImplTables), std::end(GlobalFulltextPlainImplTables)));

constexpr std::string_view GlobalFulltextWithRelevanceImplTables[] = {
    NFulltext::DictTable,
    NFulltext::DocsTable,
    NFulltext::StatsTable,
    ImplTable,
};
static_assert(std::is_sorted(std::begin(GlobalFulltextWithRelevanceImplTables), std::end(GlobalFulltextWithRelevanceImplTables)));

bool IsSecondaryIndex(NKikimrSchemeOp::EIndexType indexType) {
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            return true;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            return false;
        default:
            Y_ENSURE(false, InvalidIndexType(indexType));
    }
}

}

TTableColumns CalcTableImplDescription(NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index) {
    TTableColumns result;

    const bool isSecondaryIndex = IsSecondaryIndex(indexType);

    auto takeKeyColumns = index.KeyColumns.size();
    if (!isSecondaryIndex) { // vector and fulltext indexes have special embedding and text key columns
        Y_ASSERT(indexType == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree
            || indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain
            || indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
        takeKeyColumns--;
    }

    std::for_each(index.KeyColumns.begin(), index.KeyColumns.begin() + takeKeyColumns, [&] (const auto& ik) {
        result.Keys.push_back(ik);
        result.Columns.emplace(ik);
    });

    for (const auto& tk : table.Keys) {
        if (result.Columns.emplace(tk).second) {
            result.Keys.push_back(tk);
        }
    }

    for (const auto& dk : index.DataColumns) {
        result.Columns.emplace(dk);
    }

    return result;
}

NKikimrSchemeOp::EIndexType GetIndexType(const NKikimrSchemeOp::TIndexCreationConfig& indexCreation) {
    // TODO: always provide EIndexTypeGlobal value instead of null
    // TODO: do not cast unknown index types to EIndexTypeGlobal (proto2 specific)
    return indexCreation.HasType()
        ? indexCreation.GetType()
        : NKikimrSchemeOp::EIndexTypeGlobal;
}

TString InvalidIndexType(NKikimrSchemeOp::EIndexType indexType) {
    return TStringBuilder() << "Invalid index type " << static_cast<int>(indexType);
}

std::optional<NKikimrSchemeOp::EIndexType> TryConvertIndexType(Ydb::Table::TableIndex::TypeCase type) {
    switch (type) {
        case Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET:
        case Ydb::Table::TableIndex::TypeCase::kGlobalIndex:
            return NKikimrSchemeOp::EIndexTypeGlobal;
        case Ydb::Table::TableIndex::TypeCase::kGlobalAsyncIndex:
            return NKikimrSchemeOp::EIndexTypeGlobalAsync;
        case Ydb::Table::TableIndex::TypeCase::kGlobalUniqueIndex:
            return NKikimrSchemeOp::EIndexTypeGlobalUnique;
        case Ydb::Table::TableIndex::TypeCase::kGlobalVectorKmeansTreeIndex:
            return NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree;
        case Ydb::Table::TableIndex::TypeCase::kGlobalFulltextPlainIndex:
            return NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        case Ydb::Table::TableIndex::TypeCase::kGlobalFulltextRelevanceIndex:
            return NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        default:
            return std::nullopt;
    }
}

NKikimrSchemeOp::EIndexType ConvertIndexType(Ydb::Table::TableIndex::TypeCase type) {
    const auto result = TryConvertIndexType(type);
    Y_ENSURE(result);
    return *result;
}

bool IsCompatibleIndex(NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index, TString& explain) {
    if (const auto* broken = IsContains(table.Keys, table.Columns)) {
        explain = TStringBuilder()
                  << "all table key columns should be in table columns, table key column "
                  << *broken << " is missed";
        return false;
    }

    if (const auto* broken = IsContains(index.KeyColumns, table.Columns)) {
        explain = TStringBuilder()
                  << "all index key columns should be in table columns, index key column "
                  << *broken << " is missed";
        return false;
    }

    if (const auto* broken = IsContains(index.DataColumns, table.Columns)) {
        explain = TStringBuilder()
                  << "all index data columns should be in table columns, index data column "
                  << *broken << " is missed";
        return false;
    }

    THashSet<TString> tmp;

    if (const auto* broken = IsUnique(table.Keys, tmp)) {
        explain = TStringBuilder()
                  << "all table key columns should be unique, for example " << *broken;
        return false;
    }

    if (const auto* broken = IsUnique(index.KeyColumns, tmp)) {
        explain = TStringBuilder()
                  << "all index key columns should be unique, for example " << *broken;
        return false;
    }

    if (const auto* broken = IsUnique(index.DataColumns, tmp)) {
        explain = TStringBuilder()
                  << "all index data columns should be unique, for example " << *broken;
        return false;
    }

    const bool isSecondaryIndex = IsSecondaryIndex(indexType);

    if (index.KeyColumns.size() < 1) {
        explain = "should be at least single index key column";
        return false;
    }
    if (isSecondaryIndex) {
        if (index.KeyColumns == table.Keys) {
            explain = "index keys shouldn't be table keys";
            return false;
        }
    } else {
        if (ContainsSystemColumn(table.Keys)) {
            explain = TStringBuilder() << "table key column shouldn't have a reserved name";
            return false;
        }
        if (ContainsSystemColumn(index.KeyColumns)) {
            explain = TStringBuilder() << "index key column shouldn't have a reserved name";
            return false;
        }
        if (ContainsSystemColumn(index.DataColumns)) {
            explain = TStringBuilder() << "index data column shouldn't have a reserved name";
            return false;
        }
    }
    tmp.clear();
    tmp.insert(table.Keys.begin(), table.Keys.end());
    if (isSecondaryIndex) {
        tmp.insert(index.KeyColumns.begin(), index.KeyColumns.end());
    } else {
        // Vector and fulltext indexes allow to add all columns both to index & data
        Y_ASSERT(indexType == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree
            || indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain
            || indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
    }
    if (const auto* broken = IsContains(index.DataColumns, tmp, true)) {
        explain = TStringBuilder()
                  << "the same column can't be used as key and data column for one index, for example " << *broken;
        return false;
    }
    return true;
}

bool DoesIndexSupportTTL(NKikimrSchemeOp::EIndexType indexType) {
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            return true;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            return false;
        default:
            Y_DEBUG_ABORT_S(InvalidIndexType(indexType));
            return false;
    }
}

std::span<const std::string_view> GetImplTables(
        NKikimrSchemeOp::EIndexType indexType,
        std::span<const TString> indexKeys)
{
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            return GlobalSecondaryImplTables;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            if (indexKeys.size() == 1) {
                return GlobalKMeansTreeImplTables;
            } else {
                return PrefixedGlobalKMeansTreeImplTables;
            }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            return GetFulltextImplTables(Ydb::Table::FulltextIndexSettings::FLAT);
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            return GetFulltextImplTables(Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE);
        default:
            Y_ENSURE(false, InvalidIndexType(indexType));
    }
}

std::span<const std::string_view> GetFulltextImplTables(Ydb::Table::FulltextIndexSettings::Layout layout) {
    if (layout == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
        return GlobalFulltextWithRelevanceImplTables;
    }
    return GlobalFulltextPlainImplTables;
}

bool IsImplTable(std::string_view tableName) {
    return Contains(ImplTables, tableName);
}

bool IsBuildImplTable(std::string_view tableName) {
    // all impl tables that ends with "build" should be used only for index creation and dropped when index build is finished
    return tableName.ends_with(NKMeans::BuildSuffix0)
        || tableName.ends_with(NKMeans::BuildSuffix1);
}

namespace NFulltext {

namespace {

constexpr double EPSILON = 1e-6;

ui32 NormalizeMinimumShouldMatch(i32 wordsCount, i32 minimumShouldMatch) {
    // NOTE
    // as per lucene documentation, minimum should match should be at least 1
    // and at most the number of words in the query
    if (minimumShouldMatch <= 0) {
        return 1;
    }
    if (minimumShouldMatch > wordsCount) {
        return wordsCount;
    }

    return minimumShouldMatch;
}

}

EDefaultOperator DefaultOperatorFromString(const TString& mode, TString& explain) {
    if (mode.empty()) {
        return EDefaultOperator::And;
    } else if (to_lower(mode) == "and") {
        return EDefaultOperator::And;
    } else if (to_lower(mode) == "or") {
        return EDefaultOperator::Or;
    } else {
        explain = TStringBuilder() << "Unsupported default operator: `" << EscapeC(mode) << "`. Should be `and` or `or`";
        return EDefaultOperator::Invalid;
    }
}

ui32 MinimumShouldMatchFromString(i32 wordsCount, EDefaultOperator defaultOperator, const TString& minimumShouldMatch, TString& explain) {
    if (minimumShouldMatch.empty()) {
        if (defaultOperator == EDefaultOperator::And) {
            return wordsCount;
        } else {
            // at least one word should be matched
            return 1;
        }
    } else {
        if (defaultOperator != EDefaultOperator::Or) {
            explain = TStringBuilder() << "MinimumShouldMatch is not supported for AND default operator";
            return 0;
        }

        if (minimumShouldMatch.EndsWith("%")) {
            i32 intValue;
            if (!TryFromString<i32>(minimumShouldMatch.substr(0, minimumShouldMatch.size() - 1), intValue)) {
                explain = TStringBuilder() << "MinimumShouldMatch is incorrect. Invalid percentage: `" << EscapeC(minimumShouldMatch) << "`. Should be a number";
                return 0;
            }
            if (intValue <= 0) {
                explain = TStringBuilder() << "MinimumShouldMatch is incorrect. Invalid percentage: `" << EscapeC(minimumShouldMatch) << "`. Should be positive";
                return 0;
            }
            if (intValue > 100) {
                explain = TStringBuilder() << "MinimumShouldMatch is incorrect. Invalid percentage: `" << EscapeC(minimumShouldMatch) << "`. Should be less than or equal to 100";
                return 0;
            }

            return NormalizeMinimumShouldMatch(wordsCount, (i32)(std::floor(static_cast<double>(wordsCount) * intValue / 100.0 + EPSILON) + EPSILON));
        }

        i32 intValue;
        if (!TryFromString<i32>(minimumShouldMatch, intValue)) {
            explain = TStringBuilder() << "MinimumShouldMatch is incorrect. Invalid value: `" << EscapeC(minimumShouldMatch) << "`. Should be a number";
            return 0;
        }

        if (intValue <= -wordsCount) {
            return 1;
        } if (intValue > wordsCount) {
            return wordsCount;
        }

        if (intValue < 0) {
            return NormalizeMinimumShouldMatch(wordsCount, wordsCount + intValue);
        }

        return NormalizeMinimumShouldMatch(wordsCount, intValue);
    }
}

}

namespace NKMeans {

bool HasPostingParentFlag(TClusterId parent) {
    return bool(parent & PostingParentFlag);
}

// Note: if cluster id is too big, something is wrong with cluster enumeration
void EnsureNoPostingParentFlag(TClusterId parent) {
    Y_ENSURE(!HasPostingParentFlag(parent));
}

TClusterId SetPostingParentFlag(TClusterId parent) {
    EnsureNoPostingParentFlag(parent);
    return (parent | PostingParentFlag);
}

}

TString ToShortDebugString(const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansRequest& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansResponse& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    copy.ClearClusterSizes();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvSampleKResponse& record) {
    auto copy = record;
    TStringBuilder result;
    // rows are not human readable and can be large like 100Kb+
    copy.ClearRows();
    result << copy.ShortDebugString();
    result << " Rows: " << record.RowsSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvValidateUniqueIndexResponse& record) {
    auto copy = record;
    // keys are not human readable and contain user data
    copy.ClearFirstIndexKey();
    copy.ClearLastIndexKey();
    return copy.ShortDebugString();
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvFilterKMeansResponse& record) {
    auto copy = record;
    // keys are not human readable and contain user data
    copy.ClearFirstKeyRows();
    copy.ClearLastKeyRows();
    return copy.ShortDebugString();
}

}
