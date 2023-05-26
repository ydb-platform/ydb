#pragma once

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/sharding/unboxed_reader.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/map.h>

namespace NKikimr {
namespace NKqp {

enum class ETableKind {
    Unknown = 0,
    Datashard,
    SysView,
    Olap
};

class TKqpTableKeys {
public:
    using TColumn = NSharding::TShardingBase::TColumn;

    struct TTable {
    public:
        TString Path;
        TMap<TString, TColumn> Columns;
        TVector<TString> KeyColumns;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
        ETableKind TableKind = ETableKind::Unknown;
        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> ColumnTableInfo;

        const TMap<TString, TColumn>& GetColumnsRemap() const {
            return Columns;
        }

        std::unique_ptr<NSharding::TShardingBase> BuildSharding() const {
            if (ColumnTableInfo) {
                auto result = NSharding::TShardingBase::BuildShardingOperator(ColumnTableInfo->Description.GetSharding());
                YQL_ENSURE(result);
                return result;
            } else {
                return nullptr;
            }
        }
    };

    TTable* FindTablePtr(const TTableId& id) {
        return TablesById.FindPtr(id);
    }

    const TTable* FindTablePtr(const TTableId& id) const {
        return TablesById.FindPtr(id);
    }

    TTable& GetTable(const TTableId& id) {
        auto table = TablesById.FindPtr(id);
        MKQL_ENSURE_S(table);
        return *table;
    }

    const TTable& GetTable(const TTableId& id) const {
        auto table = TablesById.FindPtr(id);
        MKQL_ENSURE_S(table);
        return *table;
    }

    TTable& GetOrAddTable(const TTableId& id, const TStringBuf path) {
        auto& table = TablesById[id];

        if (table.Path.empty()) {
            table.Path = path;
        } else {
            MKQL_ENSURE_S(table.Path == path);
        }

        return table;
    }

    size_t Size() const {
        return TablesById.size();
    }

    const THashMap<TTableId, TTable>& Get() const {
        return TablesById;
    }

private:
    THashMap<TTableId, TTable> TablesById;
};

TVector<TCell> MakeKeyCells(const NKikimr::NUdf::TUnboxedValue& value, const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const TVector<ui32>& keyColumnIndices, const NMiniKQL::TTypeEnvironment& typeEnv, bool copyValues);

template<typename TList, typename TRangeFunc>
size_t FindKeyPartitionIndex(const TVector<TCell>& key, const TList& partitions,
    const TVector<NScheme::TTypeInfo>& keyColumnTypes, const TRangeFunc& rangeFunc)
{
    auto it = std::lower_bound(partitions.begin(), partitions.end(), key,
        [&keyColumnTypes, &rangeFunc](const auto& partition, const auto& key) {
            const auto& range = rangeFunc(partition);
            const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                range.IsInclusive || range.IsPoint, true, keyColumnTypes);

            return (cmp < 0);
        });

    MKQL_ENSURE_S(it != partitions.end());

    return std::distance(partitions.begin(), it);
}

template<typename TList, typename TRangeFunc>
size_t FindKeyPartitionIndex(const NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NUdf::TUnboxedValue& value,
     const TList& partitions, const TVector<NScheme::TTypeInfo>& keyColumnTypes, const TVector<ui32>& keyColumnIndices,
     const TRangeFunc& rangeFunc)
{
    auto key = MakeKeyCells(value, keyColumnTypes, keyColumnIndices, typeEnv, /* copyValues */ true);

    return FindKeyPartitionIndex(key, partitions, keyColumnTypes, rangeFunc);
}

using TSerializedPointOrRange = std::variant<TSerializedCellVec, TSerializedTableRange>;

struct TPartitionWithRange {
    const TKeyDesc::TPartitionInfo* PartitionInfo;
    TSerializedPointOrRange PointOrRange;
    std::optional<TSerializedTableRange> FullRange;

    TPartitionWithRange(const TKeyDesc::TPartitionInfo* partitionInfo)
        : PartitionInfo(partitionInfo) {}
};

TVector<TPartitionWithRange> GetKeyRangePartitions(const TTableRange& range,
    const TVector<TKeyDesc::TPartitionInfo>& partitions, const TVector<NScheme::TTypeInfo>& keyColumnTypes);

template<typename TList, typename TRangeFunc>
void SortPartitions(TList& partitions, const TVector<NScheme::TTypeInfo>& keyColumnTypes, const TRangeFunc& rangeFunc) {
    std::sort(partitions.begin(), partitions.end(),
        [&keyColumnTypes, &rangeFunc](const auto& left, const auto& right) {
            const auto& leftRange = rangeFunc(left);
            const auto& rightRange = rangeFunc(right);

            const int cmp = CompareBorders<true, true>(leftRange.EndKeyPrefix.GetCells(),
                rightRange.EndKeyPrefix.GetCells(), leftRange.IsInclusive || leftRange.IsPoint,
                rightRange.IsInclusive || rightRange.IsPoint, keyColumnTypes);

            return (cmp < 0);
        });
}

TTableId MakeTableId(const NYql::NNodes::TKqpTable& node);
TTableId MakeTableId(const NKqpProto::TKqpPhyTableId& table);

} // namespace NKqp
} // namespace NKikimr
