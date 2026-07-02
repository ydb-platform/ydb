#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/bitops.h>
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
NKikimrSchemeOp::EIndexType GetIndexType(const NKikimrSchemeOp::TIndexAlteringConfig& indexAlter);
TString InvalidIndexType(NKikimrSchemeOp::EIndexType indexType);
std::optional<NKikimrSchemeOp::EIndexType> TryConvertIndexType(Ydb::Table::TableIndex::TypeCase type);
NKikimrSchemeOp::EIndexType ConvertIndexType(Ydb::Table::TableIndex::TypeCase type);
bool IsLocalTableIndex(Ydb::Table::TableIndex::TypeCase type);

std::span<const std::string_view> GetImplTables(
    NKikimrSchemeOp::EIndexType indexType,
    std::span<const TString> indexKeys);
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

// Impl table positions in partitioning setting list
inline constexpr const int LevelTablePosition = 0;
inline constexpr const int PostingTablePosition = 1;
inline constexpr const int PrefixTablePosition = 2;

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

    // Synthetic doc_id column on the main table. When present together with a unique
    // secondary index over [RowIdColumn], the fulltext index uses this column as doc_id
    // instead of the main-table PK. At read time the unique index resolves __ydb_row_id -> PK.
    // The user may pre-create it, or the schemeshard auto-provisions it (column + unique
    // index, named RowIdUniqueIndexName, backed by RowIdSequenceName) when a fulltext index
    // is built on a table with a non-single-integer ("custom") primary key.
    inline constexpr const char* RowIdColumn = "__ydb_row_id";

    // Deterministic names of the auto-provisioned unique secondary index over [__ydb_row_id] and
    // of the sequence that generates __ydb_row_id values. Deterministic so a second fulltext index
    // on the same table reuses the same infrastructure instead of creating duplicates.
    inline constexpr const char* RowIdUniqueIndexName = "__ydb_unique_row_id";
    inline constexpr const char* RowIdSequenceName = "_seq___ydb_row_id";

    // __ydb_row_id value layout. A single Uint64 is derived from a dense per-table document counter
    // `seq` by bit-reversing its low RowIdSpreadBits into the high bits (a "spread bucket"), so that
    // consecutive seq values land in distant key ranges -> writes to the unique index / posting / docs
    // tables spread across shards instead of hot-spotting on a monotonic tail. Both compact and
    // non-compact fulltext indexes use the full spread __ydb_row_id as the doc id and resolve it to the
    // PK via the unique index. Generation (backfill scan + online-insert sequencer) calls RowIdFromSeq;
    // both must stay in lockstep so a backfilled and an online-inserted row share the same layout.
    inline constexpr int  RowIdSpreadBits = 16;                                    // R: 2^16 buckets, 2^48 docs
    inline constexpr ui64 RowIdSeqMask    = (ui64(1) << (64 - RowIdSpreadBits)) - 1;

    // seq (in [0, 2^(64-R))) -> spread __ydb_row_id. Bijective; consecutive seq spread across buckets.
    inline ui64 RowIdFromSeq(ui64 seq) {
        ui64 bucket = ReverseBits(seq) >> (64 - RowIdSpreadBits); // low R bits of seq, reversed, into top R bits
        return (bucket << (64 - RowIdSpreadBits)) | (seq & RowIdSeqMask);
    }
    // Recover the dense seq from a stored __ydb_row_id (cheap mask, no reverse).
    inline ui64 SeqFromRowId(ui64 rowId) {
        return rowId & RowIdSeqMask;
    }

    inline constexpr const char* DocsTable = "indexImplDocsTable";
    inline constexpr const char* DocLengthColumn = "__ydb_length";

    // Transient build table used only by the compact build in rowid mode: the main table re-keyed by
    // the full __ydb_row_id so the posting scan visits doc ids in ascending order (the order the compact
    // delta format requires). Built as a generic secondary index over __ydb_row_id, so it holds
    // (__ydb_row_id, <text column>, <data columns>); dropped after the build.
    inline constexpr const char* RowIdSrcBuildSuffix = "rowidsrc";

    inline constexpr const char* DictTable = "indexImplDictTable";

    inline constexpr const char* StatsTable = "indexImplStatsTable";
    inline constexpr const char* DocCountColumn = "__ydb_doc_count";
    inline constexpr const char* SumDocLengthColumn = "__ydb_sum_doc_length";

    inline constexpr const char* FullTextRelevanceColumn = "__ydb_full_text_relevance";

    using TGen = ui64;
    inline constexpr auto GenType = Ydb::Type::UINT64;
    inline constexpr const char* MaxIdColumn = "__ydb_max_id";
    inline constexpr const char* GenColumn = "__ydb_generation";
    inline constexpr const char* AddedColumn = "__ydb_added";
    inline constexpr const char* SegmentColumn = "__ydb_segment";

    inline constexpr const char* GenSequence = "__ydb_gen_sequence";

    // Impl table positions in partitioning setting list
    inline constexpr const int DictTablePosition = 0;
    inline constexpr const int DocsTablePosition = 1;
    inline constexpr const int StatsTablePosition = 2;
    inline constexpr const int PostingTablePosition = 3;

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
