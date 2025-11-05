#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <span>
#include <string_view>

namespace NKikimrTxDataShard {
    class TEvReshuffleKMeansRequest;
    class TEvRecomputeKMeansRequest;
    class TEvRecomputeKMeansResponse;
    class TEvSampleKResponse;
    class TEvValidateUniqueIndexResponse;
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

NKikimrSchemeOp::EIndexType GetIndexType(NKikimrSchemeOp::TIndexCreationConfig indexCreation);
TString InvalidIndexType(NKikimrSchemeOp::EIndexType indexType);

std::span<const std::string_view> GetImplTables(NKikimrSchemeOp::EIndexType indexType, std::span<const TString> indexKeys);
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

// Prefix table
inline constexpr const char* PrefixTable = "indexImplPrefixTable";
inline constexpr const char* IdColumnSequence = "__ydb_id_sequence";

inline constexpr const int DefaultKMeansRounds = 3;

inline constexpr TClusterId PostingParentFlag = (1ull << 63ull);

bool HasPostingParentFlag(TClusterId parent);
void EnsureNoPostingParentFlag(TClusterId parent);
TClusterId SetPostingParentFlag(TClusterId parent);

}

namespace NFulltext {
    inline constexpr const char* TokenColumn = "__ydb_token";
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvSampleKResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvValidateUniqueIndexResponse& record);

}
}
