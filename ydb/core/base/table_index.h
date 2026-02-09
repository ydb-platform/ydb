#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>
#include <span>
#include <string_view>

namespace NKikimrTxDataShard {
    class TEvReshuffleKMeansRequest;
    class TEvRecomputeKMeansRequest;
    class TEvRecomputeKMeansResponse;
    class TEvSampleKResponse;
    class TEvValidateUniqueIndexResponse;
    class TEvFilterKMeansResponse;
}

namespace NKikimr {

inline constexpr const char* SYSTEM_COLUMN_PREFIX = "__ydb_";

namespace NTableIndex {

struct TTableColumns {
    THashSet<TString> Columns;
    TVector<TString> Keys;
};

struct TIndexColumns {
    TVector<TString> KeyColumns;
    TVector<TString> DataColumns;
};

inline constexpr const char* ImplTable = "indexImplTable";

bool IsCompatibleIndex(NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index, TString& explain);
TTableColumns CalcTableImplDescription(NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index);

bool DoesIndexSupportTTL(NKikimrSchemeOp::EIndexType indexType);

NKikimrSchemeOp::EIndexType GetIndexType(const NKikimrSchemeOp::TIndexCreationConfig& indexCreation);
TString InvalidIndexType(NKikimrSchemeOp::EIndexType indexType);
std::optional<NKikimrSchemeOp::EIndexType> TryConvertIndexType(Ydb::Table::TableIndex::TypeCase type);
NKikimrSchemeOp::EIndexType ConvertIndexType(Ydb::Table::TableIndex::TypeCase type);

std::span<const std::string_view> GetImplTables(
    NKikimrSchemeOp::EIndexType indexType,
    std::span<const TString> indexKeys);
std::span<const std::string_view> GetFulltextImplTables(Ydb::Table::FulltextIndexSettings::Layout layout);
bool IsImplTable(std::string_view tableName);
bool IsBuildImplTable(std::string_view tableName);

namespace NKMeans {

using TClusterId = ui64;
inline constexpr auto ClusterIdType = Ydb::Type::UINT64;
inline constexpr const char* ClusterIdTypeName = "Uint64";

// Level and Posting tables
inline constexpr const char* ParentColumn = "__ydb_parent";

// Level table
inline constexpr const char* LevelTable = "indexImplLevelTable";
inline constexpr const char* IdColumn = "__ydb_id";
inline constexpr const char* CentroidColumn = "__ydb_centroid";

// Posting table
inline constexpr const char* PostingTable = "indexImplPostingTable";

inline constexpr const char* BuildSuffix0 = "0build";
inline constexpr const char* BuildSuffix1 = "1build";
inline constexpr auto IsForeignType = Ydb::Type::BOOL;
inline constexpr auto IsForeignTypeName = "Bool";
inline constexpr const char* IsForeignColumn = "__ydb_foreign";
inline constexpr auto DistanceType = Ydb::Type::DOUBLE;
inline constexpr auto DistanceTypeName = "Double";
inline constexpr const char* DistanceColumn = "__ydb_distance";

// Prefix table
inline constexpr const char* PrefixTable = "indexImplPrefixTable";
inline constexpr const char* IdColumnSequence = "__ydb_id_sequence";

inline constexpr const int DefaultKMeansRounds = 3;
inline constexpr const int DefaultOverlapClusters = 1;
inline constexpr const double DefaultOverlapRatio = 0;

inline constexpr TClusterId PostingParentFlag = (1ull << 63ull);

bool HasPostingParentFlag(TClusterId parent);
void EnsureNoPostingParentFlag(TClusterId parent);
TClusterId SetPostingParentFlag(TClusterId parent);

}

namespace NFulltext {
    // Type for token frequency within a document - uint32 is OK
    using TTokenCount = ui32;
    inline constexpr auto TokenCountType = Ydb::Type::UINT32;
    inline constexpr const char* TokenCountTypeName = "Uint32";

    // Type for the global number of documents / number of documents with token
    using TDocCount = ui64;
    inline constexpr auto DocCountType = Ydb::Type::UINT64;
    inline constexpr const char* DocCountTypeName = "Uint64";

    inline constexpr const char* TokenColumn = "__ydb_token";
    inline constexpr const char* FreqColumn = "__ydb_freq";
    inline constexpr const char* IdColumn = "__ydb_id";

    inline constexpr const char* DocsTable = "indexImplDocsTable";
    inline constexpr const char* DocLengthColumn = "__ydb_length";

    inline constexpr const char* DictTable = "indexImplDictTable";

    inline constexpr const char* StatsTable = "indexImplStatsTable";
    inline constexpr const char* DocCountColumn = "__ydb_doc_count";
    inline constexpr const char* SumDocLengthColumn = "__ydb_sum_doc_length";

    inline constexpr const char* FullTextRelevanceColumn = "__ydb_full_text_relevance";

    enum class EDefaultOperator {
        Invalid,
        And,
        Or
    };

    EDefaultOperator DefaultOperatorFromString(const TString& mode, TString& explain);
    ui32 MinimumShouldMatchFromString(i32 wordsCount, EDefaultOperator defaultOperator, const TString& minimumShouldMatch, TString& explain);
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvSampleKResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvValidateUniqueIndexResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvFilterKMeansResponse& record);

}
}
