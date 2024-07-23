#pragma once

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/sharding/unboxed_reader.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
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
    Olap,
    External
};


struct TTableConstInfo : public TAtomicRefCount<TTableConstInfo> {
    TString Path;
    TMap<TString, NSharding::IShardingBase::TColumn> Columns;
    TVector<TString> KeyColumns;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    ETableKind TableKind = ETableKind::Unknown;
    THashMap<TString, TString> Sequences;
    THashMap<TString, Ydb::TypedValue> DefaultFromLiteral;
    bool IsBuildInProgress = false;
    bool IsCheckingNotNullInProgress = false;

    TTableConstInfo() {}
    TTableConstInfo(const TString& path) : Path(path) {}

    void FillColumn(const NKqpProto::TKqpPhyColumn& phyColumn) {
        if (Columns.FindPtr(phyColumn.GetId().GetName())) {
            return;
        }

        NSharding::IShardingBase::TColumn column;
        column.Id = phyColumn.GetId().GetId();

        if (phyColumn.GetTypeId() != NScheme::NTypeIds::Pg) {
            column.Type = NScheme::TTypeInfo(phyColumn.GetTypeId());
        } else {
            column.Type = NScheme::TTypeInfo(phyColumn.GetTypeId(),
                NPg::TypeDescFromPgTypeName(phyColumn.GetPgTypeName()));
        }
        column.NotNull = phyColumn.GetNotNull();
        column.IsBuildInProgress = phyColumn.GetIsBuildInProgress();
        column.IsCheckingNotNullInProgress = phyColumn.GetIsCheckingNotNullInProgress();

        Columns.emplace(phyColumn.GetId().GetName(), std::move(column));
        if (!phyColumn.GetDefaultFromSequence().empty()) {
            TString seq = phyColumn.GetDefaultFromSequence();
            if (!seq.StartsWith("/")) {
                seq = Path + "/" + seq;
            }

            Sequences.emplace(phyColumn.GetId().GetName(), seq);
        }

        if (phyColumn.HasDefaultFromLiteral()) {
            DefaultFromLiteral.emplace(
                phyColumn.GetId().GetName(),
                phyColumn.GetDefaultFromLiteral());
        }
    }

    void AddColumn(const TString& columnName) {
        auto& sysColumns = GetSystemColumns();
        if (Columns.FindPtr(columnName)) {
            return;
        }

        auto* systemColumn = sysColumns.FindPtr(columnName);
        YQL_ENSURE(systemColumn, "Unknown table column"
            << ", table: " << Path
            << ", column: " << columnName);

        NSharding::IShardingBase::TColumn column;
        column.Id = systemColumn->ColumnId;
        column.Type = NScheme::TTypeInfo(systemColumn->TypeId);
        column.NotNull = false;
        Columns.emplace(columnName, std::move(column));
    }

    void FillTable(const NKqpProto::TKqpPhyTable& phyTable) {
        switch (phyTable.GetKind()) {
            case NKqpProto::TABLE_KIND_DS:
                TableKind = ETableKind::Datashard;
                break;
            case NKqpProto::TABLE_KIND_OLAP:
                TableKind = ETableKind::Olap;
                break;
            case NKqpProto::TABLE_KIND_SYS_VIEW:
                TableKind = ETableKind::SysView;
                break;
            case NKqpProto::TABLE_KIND_EXTERNAL:
                TableKind = ETableKind::External;
                return;
            default:
                YQL_ENSURE(false, "Unexpected phy table kind: " << (i64) phyTable.GetKind());
        }

        for (const auto& [_, phyColumn] : phyTable.GetColumns()) {
            FillColumn(phyColumn);
        }

        YQL_ENSURE(KeyColumns.empty());
        KeyColumns.reserve(phyTable.KeyColumnsSize());
        YQL_ENSURE(KeyColumnTypes.empty());
        KeyColumnTypes.reserve(phyTable.KeyColumnsSize());
        for (const auto& keyColumnId : phyTable.GetKeyColumns()) {
            const auto& column = Columns.FindPtr(keyColumnId.GetName());
            YQL_ENSURE(column);

            KeyColumns.push_back(keyColumnId.GetName());
            KeyColumnTypes.push_back(column->Type);
        }
    }
};

class TKqpTableKeys {
public:
    struct TTable {
    private:
        TIntrusivePtr<TTableConstInfo> TableConstInfo;
        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> ColumnTableInfo;
        THashMap<TString, Ydb::TypedValue> DefaultFromLiteral;

    public:
        TTable() : TableConstInfo(MakeIntrusive<TTableConstInfo>()) {}

        TTable(TIntrusivePtr<TTableConstInfo> constInfoPtr) : TableConstInfo(constInfoPtr) {}

        const TString& GetPath() const {
            return TableConstInfo->Path;
        }

        const TMap<TString, NSharding::IShardingBase::TColumn>& GetColumns() const {
            return TableConstInfo->Columns;
        }

        const TVector<TString>& GetKeyColumns() const {
            return TableConstInfo->KeyColumns;
        }

        const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const {
            return TableConstInfo->KeyColumnTypes;
        }

        const ETableKind& GetTableKind() const {
            return TableConstInfo->TableKind;
        }

        const THashMap<TString, TString>& GetSequences() const {
            return TableConstInfo->Sequences;
        }

        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> GetColumnTableInfo() const {
            return ColumnTableInfo;
        }

        void SetColumnTableInfo(TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> columnTableInfo) {
            ColumnTableInfo = columnTableInfo;
        }

        void SetPath(const TStringBuf& path) {
            TableConstInfo->Path = path;
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

    TTable& GetOrAddTable(const TTableId& id, const TStringBuf& path) {
        auto& table = TablesById[id];

        if (table.GetPath().empty()) {
            table.SetPath(path);
        } else {
            MKQL_ENSURE_S(table.GetPath() == path);
        }

        return table;
    }

    size_t Size() const {
        return TablesById.size();
    }

    const THashMap<TTableId, TTable>& Get() const {
        return TablesById;
    }

    void AddTable(const TTableId& id, TIntrusivePtr<TTableConstInfo> info) {
        TablesById.insert_or_assign(id, info);
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
