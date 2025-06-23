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

bool IsCompatibleIndex(NKikimrSchemeOp::EIndexType type, const TTableColumns& table, const TIndexColumns& index, TString& explain);
TTableColumns CalcTableImplDescription(NKikimrSchemeOp::EIndexType type, const TTableColumns& table, const TIndexColumns& index);

std::span<const std::string_view> GetImplTables(NKikimrSchemeOp::EIndexType indexType, std::span<const TString> indexKeys);
bool IsImplTable(std::string_view tableName);
bool IsBuildImplTable(std::string_view tableName);

using TClusterId = ui64;
inline constexpr auto ClusterIdType = Ydb::Type::UINT64;
inline constexpr const char* ClusterIdTypeName = "Uint64";

void EnsureNoPostingParentFlag(TClusterId parent);

TClusterId SetPostingParentFlag(TClusterId parent);

TString ToShortDebugString(const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansRequest& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansResponse& record);
TString ToShortDebugString(const NKikimrTxDataShard::TEvSampleKResponse& record);

}
}
