#include "events.h"

#include <ydb/services/sql/codec/codec.h>
#include <ydb/services/sql/plan/plan.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_order.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/mapfindptr.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace NKikimr::NSql {

namespace {

constexpr ui32 kOverloadRetries = 16;

struct TColumn {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    NScheme::TTypeInfo Type;
    ui32 TypeOid = 25;
    i16 TypeSize = -1;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

struct TPgWire {
    ui32 Oid = 25;
    i16 Size = -1;
};

std::optional<TPgWire> PgWire(NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Bool:
            return TPgWire{16, 1};
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Uint8:
            return TPgWire{21, 2};
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Uint16:
            return TPgWire{23, 4};
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint32:
            return TPgWire{20, 8};
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Decimal:
            return TPgWire{1700, -1};
        case NScheme::NTypeIds::Utf8:
            return TPgWire{25, -1};
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            return TPgWire{17, -1};
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Date32:
            return TPgWire{1082, 4};
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Timestamp64:
            return TPgWire{1114, 8};
        case NScheme::NTypeIds::Interval:
        case NScheme::NTypeIds::Interval64:
            return TPgWire{1186, 16};
        case NScheme::NTypeIds::Uuid:
            return TPgWire{2950, 16};
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::JsonDocument:
            return TPgWire{114, -1};
        default:
            return std::nullopt;
    }
}

TString TablePath(const TString& database, const TSelectStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    path += "/" + statement.From.Name;
    return path;
}

struct TPlanStore {
    TAdaptiveLock Lock;
    TPlanCache Cache{TPlannerConfig{}};
};

TPlanStore& Plans() {
    static TPlanStore store;
    return store;
}

class TSelectActor : public NActors::TActorBootstrapped<TSelectActor> {
public:
    TSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries, std::optional<std::pair<ui64, ui64>> snapshot, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
        , Snapshot(std::move(snapshot))
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        if (Statement.Schema && (Statement.Schema->Name == "information_schema" || Statement.Schema->Name == "pg_catalog")) {
            Fail("0A000", "not supported");
            return;
        }
        Path = TablePath(Database, Statement);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TSelectActor::StateNavigate);
    }

private:
    void Fail(TString sqlState, TString message) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Succeed() {
        if (Replied) {
            return;
        }
        Replied = true;
        if (!SortSlots.empty() && !(PkOrdered && !ReadingIndex)) {
            TVector<size_t> order(Rows.size());
            for (size_t i = 0; i < order.size(); ++i) {
                order[i] = i;
            }
            std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
                return Less(SortKeys[left], SortKeys[right]);
            });
            TVector<TVector<TQueryCell>> sorted;
            sorted.reserve(Rows.size());
            for (size_t index : order) {
                sorted.push_back(std::move(Rows[index]));
            }
            Rows = std::move(sorted);
        }
        if (ReverseRead && !ReadingIndex) {
            AccessPath = "Reverse";
        }
        if (Statement.Limit && Rows.size() > *Statement.Limit) {
            Rows.resize(*Statement.Limit);
        }
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        for (const TColumn& column : Output) {
            result->Columns.push_back({column.Name, column.TypeOid, column.TypeSize});
        }
        result->Rows = std::move(Rows);
        result->Locks = std::move(Locks);
        result->AccessPath = AccessPath;
        Send(ReplyTo, result.Release());
        PassAway();
    }

    const TSysTables::TTableColumnInfo* FindColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& name) const {
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.Name == name) {
                return &column;
            }
        }
        return nullptr;
    }

    bool PrepareColumn(const TSysTables::TTableColumnInfo& info, TColumn& column) const {
        const auto wire = PgWire(info.PType.GetTypeId());
        if (!wire) {
            return false;
        }
        column.Name = info.Name;
        column.Id = info.Id;
        column.KeyOrder = info.KeyOrder;
        column.Type = info.PType;
        column.TypeOid = wire->Oid;
        column.TypeSize = wire->Size;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        const TCodecValue probe = EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale);
        return probe.Ok;
    }

    struct TShard {
        ui64 TabletId = 0;
        ui64 ReadId = 0;
        bool Done = false;
        ui32 InternalErrors = 0;
    };

    struct TFilter {
        TColumn Column;
        ECompareOp Op = ECompareOp::Eq;
        size_t CellIndex = 0;
        TString Literal;
    };

    struct TSortSlot {
        size_t Index = 0;
        bool Desc = false;
        bool NullsFirst = false;
        NScheme::TTypeInfo Type;
    };

    struct TSortBytes {
        TVector<TString> Bytes;
        TVector<bool> Nulls;
    };

    void Unlink() {
        for (const TShard& shard : Shards) {
            if (shard.TabletId != 0) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(shard.TabletId));
            }
        }
        Sent = false;
    }

    bool DecodeLiteral(const TColumn& column, const TLiteral& literal, TCell& cell) {
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState, decoded.Message);
            return false;
        }
        BoundBytes.push_back(decoded.Bytes);
        cell = TCell(BoundBytes.back().data(), BoundBytes.back().size());
        return true;
    }

    void TightenLower(const TColumn& column, const TCell& cell, bool inclusive) {
        if (!FromSet || CompareTypedCells(cell, FromCells[0], NScheme::TTypeInfoOrder(column.Type)) > 0
            || (CompareTypedCells(cell, FromCells[0], NScheme::TTypeInfoOrder(column.Type)) == 0 && !inclusive && FromInclusive)) {
            FromCells.assign(1, cell);
            FromInclusive = inclusive;
            FromSet = true;
        }
    }

    void TightenUpper(const TColumn& column, const TCell& cell, bool inclusive) {
        if (!ToSet || CompareTypedCells(cell, ToCells[0], NScheme::TTypeInfoOrder(column.Type)) < 0
            || (CompareTypedCells(cell, ToCells[0], NScheme::TTypeInfoOrder(column.Type)) == 0 && !inclusive && ToInclusive)) {
            ToCells.assign(1, cell);
            ToInclusive = inclusive;
            ToSet = true;
        }
    }

    bool BuildRead(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        TVector<const TSysTables::TTableColumnInfo*> keys;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.KeyOrder >= 0) {
                keys.push_back(&column);
            }
        }
        std::sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->KeyOrder < right->KeyOrder;
        });
        if (keys.empty()) {
            return false;
        }
        AllKeyNames.clear();
        AllKeyTypes.clear();
        for (const auto* info : keys) {
            AllKeyNames.push_back(info->Name);
            TColumn prepared;
            if (!PrepareColumn(*info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            AllKeyTypes.push_back(prepared.Type);
        }

        absl::flat_hash_map<TString, TColumn> columns;
        absl::flat_hash_map<TString, TVector<const TComparison*>> grouped;
        for (const TComparison& comparison : Statement.Where) {
            const auto* info = FindColumn(entry, comparison.Column.Name);
            if (!info) {
                Fail("42703", comparison.Column.Name);
                return false;
            }
            if (!MapFindPtr(columns, comparison.Column.Name)) {
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return false;
                }
                columns[comparison.Column.Name] = column;
            }
            grouped[comparison.Column.Name].push_back(&comparison);
        }

        BoundBytes.clear();
        BoundBytes.reserve(Statement.Where.size() * 2);
        FromCells.clear();
        ToCells.clear();
        FromSet = false;
        ToSet = false;
        FromInclusive = true;
        ToInclusive = true;
        Compares.clear();
        KeyNames.clear();
        KeyTypes.clear();
        bool ranged = false;
        size_t equalPrefix = 0;
        for (const auto* info : keys) {
            KeyNames.push_back(info->Name);
            TColumn prepared;
            if (!PrepareColumn(*info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            KeyTypes.push_back(prepared.Type);
            const auto* group = MapFindPtr(grouped, info->Name);
            if (!group || ranged) {
                break;
            }
            const bool onlyEqual = group->size() == 1 && (*group)[0]->Op == ECompareOp::Eq;
            if (onlyEqual) {
                TCell cell;
                if (!DecodeLiteral(prepared, (*group)[0]->Value, cell)) {
                    return false;
                }
                FromCells.push_back(cell);
                ToCells.push_back(cell);
                FromSet = true;
                ToSet = true;
                FromInclusive = true;
                ToInclusive = true;
                Compares.push_back({prepared.Name, true, false, false, BoundBytes.back(), {}});
                ++equalPrefix;
                grouped.erase(info->Name);
                continue;
            }
            bool pushed = false;
            TVector<const TComparison*> leftover;
            for (const TComparison* comparison : *group) {
                if (comparison->Op == ECompareOp::NotEq) {
                    leftover.push_back(comparison);
                    continue;
                }
                TCell cell;
                if (!DecodeLiteral(prepared, comparison->Value, cell)) {
                    return false;
                }
                pushed = true;
                switch (comparison->Op) {
                    case ECompareOp::Eq:
                        TightenLower(prepared, cell, true);
                        TightenUpper(prepared, cell, true);
                        break;
                    case ECompareOp::Gt:
                        TightenLower(prepared, cell, false);
                        break;
                    case ECompareOp::Ge:
                        TightenLower(prepared, cell, true);
                        break;
                    case ECompareOp::Lt:
                        TightenUpper(prepared, cell, false);
                        break;
                    case ECompareOp::Le:
                        TightenUpper(prepared, cell, true);
                        break;
                    case ECompareOp::NotEq:
                        break;
                }
            }
            if (!pushed) {
                break;
            }
            ranged = true;
            TKeyCompare compare;
            compare.Column = prepared.Name;
            compare.Greater = FromSet;
            compare.Less = ToSet;
            if (FromSet) {
                compare.Value = TString(FromCells[0].AsBuf());
            }
            if (ToSet) {
                compare.Upper = TString(ToCells[0].AsBuf());
                if (!FromSet) {
                    compare.Value = compare.Upper;
                }
            }
            Compares.push_back(std::move(compare));
            if (leftover.empty()) {
                grouped.erase(info->Name);
            } else {
                grouped[info->Name] = std::move(leftover);
            }
            break;
        }
        Point = !ranged && equalPrefix == keys.size();
        KeyPrefix = static_cast<ui32>(equalPrefix + (ranged ? 1 : 0));
        KeyPredicate = KeyPrefix > 0;
        if (KeyPredicate) {
            if (!FromSet) {
                // An empty start is +inf. -inf is a null cell for every key column.
                FromCells.assign(KeyTypes.size(), TCell());
                FromInclusive = true;
            }
            if (!ToSet) {
                ToCells.clear();
                ToInclusive = true;
            }
        }

        Filters.clear();
        for (const auto& [name, list] : grouped) {
            Y_UNUSED(name);
            for (const TComparison* comparison : list) {
                TFilter filter;
                filter.Column = *MapFindPtr(columns, comparison->Column.Name);
                filter.Op = comparison->Op;
                TCell cell;
                if (!DecodeLiteral(filter.Column, comparison->Value, cell)) {
                    return false;
                }
                filter.Literal = BoundBytes.back();
                Filters.push_back(std::move(filter));
            }
        }
        ReadColumns = Output;
        for (TFilter& filter : Filters) {
            size_t index = ReadColumns.size();
            for (size_t i = 0; i < ReadColumns.size(); ++i) {
                if (ReadColumns[i].Id == filter.Column.Id) {
                    index = i;
                    break;
                }
            }
            filter.CellIndex = index;
            if (index == ReadColumns.size()) {
                ReadColumns.push_back(filter.Column);
            }
        }
        FilterCells.clear();
        for (const TFilter& filter : Filters) {
            FilterCells.emplace_back(filter.Literal.data(), filter.Literal.size());
        }
        return BindOrder(entry);
    }

    bool BindOrder(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        SortSlots.clear();
        PkOrdered = false;
        ReverseRead = false;
        if (Statement.Order.empty()) {
            return true;
        }
        for (const TOrderKey& key : Statement.Order) {
            if (!key.Column.Qualifier.empty() && key.Column.Qualifier != Statement.From.Name && key.Column.Qualifier != Statement.Alias) {
                Fail("42703", key.Column.Qualifier);
                return false;
            }
            const auto* info = FindColumn(entry, key.Column.Name);
            if (!info) {
                Fail("42703", key.Column.Name);
                return false;
            }
            TColumn prepared;
            if (!PrepareColumn(*info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            size_t index = ReadColumns.size();
            for (size_t i = 0; i < ReadColumns.size(); ++i) {
                if (ReadColumns[i].Name == prepared.Name) {
                    index = i;
                    break;
                }
            }
            if (index == ReadColumns.size()) {
                ReadColumns.push_back(prepared);
            }
            TSortSlot slot;
            slot.Index = index;
            slot.Desc = key.Desc;
            slot.NullsFirst = key.NullsFirst;
            slot.Type = prepared.Type;
            SortSlots.push_back(slot);
        }
        PkOrdered = SortSlots.size() == AllKeyNames.size();
        for (size_t i = 0; i < SortSlots.size() && PkOrdered; ++i) {
            if (ReadColumns[SortSlots[i].Index].Name != AllKeyNames[i] || SortSlots[i].Desc != SortSlots[0].Desc) {
                PkOrdered = false;
            }
        }
        ReverseRead = PkOrdered && !SortSlots.empty() && SortSlots[0].Desc;
        return true;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = request->ResultSet[0];
        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            Fail("0A000", "not supported");
            return;
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            Fail("42P01", Path);
            return;
        }
        Table = entry.TableId;

        if (Statement.Star) {
            TVector<const TSysTables::TTableColumnInfo*> ordered;
            for (const auto& [id, column] : entry.Columns) {
                Y_UNUSED(id);
                ordered.push_back(&column);
            }
            std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
                return left->Id < right->Id;
            });
            for (const auto* info : ordered) {
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return;
                }
                Output.push_back(std::move(column));
            }
        } else {
            for (const TIdentifier& name : Statement.Columns) {
                if (name.Qualifier && name.Qualifier != Statement.From.Name && name.Qualifier != Statement.Alias) {
                    Fail("42703", name.Qualifier);
                    return;
                }
                const auto* info = FindColumn(entry, name.Name);
                if (!info) {
                    Fail("42703", name.Name);
                    return;
                }
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return;
                }
                Output.push_back(std::move(column));
            }
        }
        if (Output.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        if (!BuildRead(entry)) {
            Fail("0A000", "not supported");
            return;
        }
        MainTable = Table;
        MainReadColumns = ReadColumns;
        if (!CollectIndexes(entry)) {
            return;
        }
        if (!KeyPredicate && Indexes.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        if (!Indexes.empty()) {
            Exploring = true;
            if (KeyPredicate) {
                StartResolve();
            } else {
                StartIndex(0);
            }
            return;
        }

        {
            TGuard<TAdaptiveLock> guard(Plans().Lock);
            if (const auto plan = Plans().Cache.Lookup(Database, Text, {}, Table.SchemaVersion, TActivationContext::Now().MilliSeconds())) {
                if (PlanMatches(*plan)) {
                    Adopt(*plan);
                    SendAll();
                    return;
                }
            }
        }
        StartResolve();
    }

    bool PlanMatches(const TPlan& plan) const {
        if (plan.Reads.empty()) {
            return false;
        }
        for (const TReadTarget& read : plan.Reads) {
            if (Point && read.Shape != EReadShape::Keys) {
                return false;
            }
            if (!Point && read.Shape != EReadShape::Ranges) {
                return false;
            }
        }
        return true;
    }

    void Adopt(const TPlan& plan) {
        Shards.clear();
        for (const TReadTarget& read : plan.Reads) {
            TShard shard;
            shard.TabletId = read.TabletId;
            Shards.push_back(shard);
        }
    }

    void StartResolve() {
        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(ReadColumns.size());
        for (const TColumn& column : ReadColumns) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
            operation.InplaceUpdateMode = 0;
            operation.ImmediateUpdateSize = 0;
            columns.push_back(operation);
        }
        const TTableRange range = Point
            ? TTableRange(FromCells)
            : TTableRange(FromCells, FromInclusive, ToCells, ToInclusive);
        auto description = MakeHolder<TKeyDesc>(Table, range, TKeyDesc::ERowOperation::Read, KeyTypes, columns);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(description));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()));
        Become(&TSelectActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (IndexPhase) {
            if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                Indexes[IndexPos].Ready = false;
                StartIndex(IndexPos + 1);
                return;
            }
            OnIndexResolved(*request->ResultSet[0].KeyDescription);
            return;
        }
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Fail("42P01", Path);
            return;
        }
        const auto& partitions = request->ResultSet[0].KeyDescription->GetPartitions();
        if (partitions.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        if (MainLookup || ReadingIndex) {
            Shards.clear();
            for (const auto& partition : partitions) {
                TShard shard;
                shard.TabletId = partition.ShardId;
                Shards.push_back(shard);
            }
            SendAll();
            return;
        }
        TTableShape shape;
        shape.KeyColumns = KeyNames;
        shape.SchemaVersion = Table.SchemaVersion;
        for (const auto& partition : partitions) {
            shape.Partitions.push_back({partition.ShardId, {}, {}, true});
        }
        const TPlanResult chosen = ChoosePlan(shape, Compares, {});
        if (!chosen.Ok || !chosen.Plan || !PlanMatches(*chosen.Plan)) {
            Fail(chosen.SqlState ? chosen.SqlState : TString("0A000"), chosen.Message ? chosen.Message : TString("not supported"));
            return;
        }
        if (Exploring) {
            PkCost = chosen.Plan->Cost.Total;
            SavedPk = *chosen.Plan;
            StartIndex(0);
            return;
        }
        {
            TGuard<TAdaptiveLock> guard(Plans().Lock);
            Plans().Cache.Store(Database, Text, {}, *chosen.Plan, TActivationContext::Now().MilliSeconds());
        }
        Adopt(*chosen.Plan);
        SendAll();
    }

    void SendOne(TShard& shard) {
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        shard.ReadId = ++ReadId;
        record.SetReadId(shard.ReadId);
        record.MutableTableId()->SetOwnerId(Table.PathId.OwnerId);
        record.MutableTableId()->SetTableId(Table.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(Table.SchemaVersion);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        if (Snapshot) {
            record.MutableSnapshot()->SetStep(Snapshot->first);
            record.MutableSnapshot()->SetTxId(Snapshot->second);
        }
        if (LockTxId) {
            record.SetLockTxId(LockTxId);
            record.SetLockNodeId(SelfId().NodeId());
        }
        if (ReverseRead && !ReadingIndex && !MainLookup) {
            record.SetReverse(true);
        }
        for (const TColumn& column : ReadColumns) {
            record.AddColumns(column.Id);
        }
        if (Point) {
            request->Keys.push_back(TSerializedCellVec(FromCells));
        } else {
            request->Ranges.emplace_back(FromCells, FromInclusive, ToCells, ToInclusive);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shard.TabletId, true));
        Sent = true;
    }

    void SendAll() {
        Pending = Shards.size();
        for (TShard& shard : Shards) {
            shard.Done = false;
            SendOne(shard);
        }
        Become(&TSelectActor::StateRead);
    }

    void ForgetPlan() {
        TGuard<TAdaptiveLock> guard(Plans().Lock);
        Plans().Cache.ForgetSchema(Database, Text);
    }

    void RetryResolve() {
        if (!Rows.empty()) {
            Fail("08006", "pipe");
            return;
        }
        Unlink();
        ForgetPlan();
        if (ResolveErrors >= InternalErrorRetries) {
            Fail("08006", "pipe");
            return;
        }
        ++ResolveErrors;
        Shards.clear();
        Pending = 0;
        StartResolve();
    }

    TShard* FindShard(ui64 readId) {
        for (TShard& shard : Shards) {
            if (shard.ReadId == readId && !shard.Done) {
                return &shard;
            }
        }
        return nullptr;
    }

    TString StatusText(const NKikimrTxDataShard::TEvReadResult& record) const {
        const auto& status = record.GetStatus();
        if (status.IssuesSize() == 0) {
            return "read failed";
        }
        return status.GetIssues(0).message();
    }

    bool Matches(const TConstArrayRef<TCell>& cells) const {
        for (size_t i = 0; i < Filters.size(); ++i) {
            const TCell& cell = cells[Filters[i].CellIndex];
            if (cell.IsNull()) {
                return false;
            }
            const int compared = CompareTypedCells(cell, FilterCells[i], NScheme::TTypeInfoOrder(Filters[i].Column.Type));
            switch (Filters[i].Op) {
                case ECompareOp::Eq:
                    if (compared != 0) {
                        return false;
                    }
                    break;
                case ECompareOp::NotEq:
                    if (compared == 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Lt:
                    if (compared >= 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Gt:
                    if (compared <= 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Le:
                    if (compared > 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Ge:
                    if (compared < 0) {
                        return false;
                    }
                    break;
            }
        }
        return true;
    }

    bool TakeRows(TEvDataShard::TEvReadResult& result) {
        const size_t count = result.GetRowsCount();
        for (size_t row = 0; row < count; ++row) {
            const TConstArrayRef<TCell> cells = result.GetCells(row);
            if (cells.size() != ReadColumns.size()) {
                Fail("XX000", "read failed");
                return false;
            }
            if (!ReadingIndex && !Matches(cells)) {
                continue;
            }
            TVector<TQueryCell> line;
            line.reserve(Output.size());
            for (size_t i = 0; i < Output.size(); ++i) {
                const TCell& cell = cells[i];
                const TStringBuf bytes = cell.IsNull() ? TStringBuf() : cell.AsBuf();
                const TCodecValue encoded = EncodeCell(Output[i].Type.GetTypeId(), cell.IsNull(), bytes, EWireFormat::Text, Output[i].Precision, Output[i].Scale);
                if (!encoded.Ok) {
                    Fail(encoded.SqlState, encoded.Message);
                    return false;
                }
                line.push_back({encoded.Null, encoded.Bytes});
            }
            if (!SortSlots.empty() && !(PkOrdered && !ReadingIndex)) {
                TSortBytes held;
                held.Bytes.reserve(SortSlots.size());
                held.Nulls.reserve(SortSlots.size());
                for (const TSortSlot& slot : SortSlots) {
                    const TCell& cell = cells[slot.Index];
                    held.Nulls.push_back(cell.IsNull());
                    held.Bytes.push_back(cell.IsNull() ? TString() : TString(cell.AsBuf()));
                }
                SortKeys.push_back(std::move(held));
            }
            Rows.push_back(std::move(line));
        }
        return true;
    }

    bool Less(const TSortBytes& left, const TSortBytes& right) const {
        for (size_t i = 0; i < SortSlots.size(); ++i) {
            if (left.Nulls[i] || right.Nulls[i]) {
                if (left.Nulls[i] && right.Nulls[i]) {
                    continue;
                }
                if (SortSlots[i].NullsFirst) {
                    return left.Nulls[i];
                }
                return right.Nulls[i];
            }
            const TCell leftCell(left.Bytes[i].data(), left.Bytes[i].size());
            const TCell rightCell(right.Bytes[i].data(), right.Bytes[i].size());
            const int compared = CompareTypedCells(leftCell, rightCell, NScheme::TTypeInfoOrder(SortSlots[i].Type));
            if (compared == 0) {
                continue;
            }
            return SortSlots[i].Desc ? compared > 0 : compared < 0;
        }
        return false;
    }

    void FinishShard(TShard& shard) {
        shard.Done = true;
        if (Pending > 0) {
            --Pending;
        }
        if (Pending == 0) {
            if (ReadingIndex && !CoveringRead) {
                BeginMainLookup();
                return;
            }
            if (MainLookup && MainPos + 1 < MainKeyCells.size()) {
                ++MainPos;
                ReadNextMain();
                return;
            }
            Succeed();
        }
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr& ev) {
        TShard* shard = FindShard(ev->Get()->Record.GetReadId());
        if (!shard) {
            return;
        }
        const auto code = ev->Get()->Record.GetStatus().GetCode();
        if (code == Ydb::StatusIds::OVERLOADED) {
            if (Overloads >= kOverloadRetries) {
                Fail("53000", StatusText(ev->Get()->Record));
                return;
            }
            ++Overloads;
            WakeTablets.push_back(shard->TabletId);
            Schedule(TDuration::MilliSeconds(ev->Get()->Record.GetThrottleDelayMs()), new NActors::TEvents::TEvWakeup);
            return;
        }
        if (code == Ydb::StatusIds::INTERNAL_ERROR) {
            if (shard->InternalErrors >= InternalErrorRetries) {
                Fail("XX000", StatusText(ev->Get()->Record));
                return;
            }
            ++shard->InternalErrors;
            SendOne(*shard);
            return;
        }
        if (code == Ydb::StatusIds::NOT_FOUND) {
            RetryResolve();
            return;
        }
        if (code == Ydb::StatusIds::SCHEME_ERROR) {
            ForgetPlan();
            Fail("42P01", StatusText(ev->Get()->Record));
            return;
        }
        if (code == Ydb::StatusIds::BAD_REQUEST) {
            Fail("22023", StatusText(ev->Get()->Record));
            return;
        }
        if (code != Ydb::StatusIds::SUCCESS) {
            Fail("XX000", StatusText(ev->Get()->Record));
            return;
        }
        if (ev->Get()->Record.BrokenTxLocksSize() > 0) {
            Fail("40001", "could not serialize access");
            return;
        }
        for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
            Locks.push_back(lock);
        }
        if (ReadingIndex && !CoveringRead) {
            if (!TakeIndexKeys(*ev->Get())) {
                return;
            }
        } else if (!TakeRows(*ev->Get())) {
            return;
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(shard->ReadId);
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), shard->TabletId, false));
            return;
        }
        FinishShard(*shard);
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        TVector<ui64> tablets;
        tablets.swap(WakeTablets);
        for (const ui64 tabletId : tablets) {
            for (TShard& shard : Shards) {
                if (shard.TabletId == tabletId && !shard.Done) {
                    SendOne(shard);
                }
            }
        }
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        RetryResolve();
    }

    struct TIndexCand {
        TString Name;
        TString ImplPath;
        TVector<TString> Keys;
        TVector<TString> Data;
        TVector<TString> KeyNames;
        TVector<NScheme::TTypeInfo> KeyTypes;
        TVector<TColumn> ReadColumns;
        TVector<TString> BoundBytes;
        TVector<TCell> FromCells;
        TVector<TCell> ToCells;
        TVector<TKeyCompare> Compares;
        TVector<ui64> Tablets;
        TTableId Table;
        ui32 Prefix = 0;
        double Cost = 0;
        bool Covering = false;
        bool FullPoint = false;
        bool FromInclusive = true;
        bool ToInclusive = true;
        bool Ready = false;
    };

    struct THeldKey {
        TVector<TString> Bytes;
        TVector<bool> Nulls;
    };

    bool DecodeInto(TVector<TString>& storage, const TColumn& column, const TLiteral& literal, TCell& cell) {
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState, decoded.Message);
            return false;
        }
        storage.push_back(decoded.Bytes);
        cell = TCell(storage.back().data(), storage.back().size());
        return true;
    }

    ui32 IndexPrefix(const TVector<TString>& keys) const {
        ui32 count = 0;
        for (const TString& name : keys) {
            TVector<const TComparison*> group;
            for (const TComparison& comparison : Statement.Where) {
                if (comparison.Column.Name == name) {
                    group.push_back(&comparison);
                }
            }
            if (group.empty()) {
                break;
            }
            if (group.size() == 1 && group[0]->Op == ECompareOp::Eq) {
                ++count;
                continue;
            }
            bool range = false;
            for (const TComparison* comparison : group) {
                if (comparison->Op != ECompareOp::Eq && comparison->Op != ECompareOp::NotEq) {
                    range = true;
                }
            }
            if (range) {
                ++count;
            }
            break;
        }
        return count;
    }

    bool Covers(const TIndexCand& cand) const {
        for (const TColumn& column : Output) {
            bool found = false;
            for (const TString& name : cand.Keys) {
                found = found || name == column.Name;
            }
            for (const TString& name : cand.Data) {
                found = found || name == column.Name;
            }
            for (const TString& name : AllKeyNames) {
                found = found || name == column.Name;
            }
            if (!found) {
                return false;
            }
        }
        return !Output.empty();
    }

    bool BuildIndexBounds(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, TIndexCand& cand) {
        cand.BoundBytes.reserve(cand.Prefix);
        TVector<TCell> prefix;
        for (ui32 i = 0; i < cand.Prefix; ++i) {
            const TComparison* equal = nullptr;
            for (const TComparison& comparison : Statement.Where) {
                if (comparison.Column.Name == cand.Keys[i] && comparison.Op == ECompareOp::Eq) {
                    equal = &comparison;
                }
            }
            if (!equal) {
                cand.Prefix = 0;
                return true;
            }
            const auto* info = FindColumn(entry, cand.Keys[i]);
            if (!info) {
                Fail("42703", cand.Keys[i]);
                return false;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            TCell cell;
            if (!DecodeInto(cand.BoundBytes, column, equal->Value, cell)) {
                return false;
            }
            prefix.push_back(cell);
            cand.Compares.push_back({column.Name, true, false, false, cand.BoundBytes.back(), {}});
        }

        TVector<TString> implNames = cand.Keys;
        for (const TString& name : AllKeyNames) {
            bool present = false;
            for (const TString& key : cand.Keys) {
                present = present || key == name;
            }
            if (!present) {
                implNames.push_back(name);
            }
        }
        cand.KeyNames = implNames;
        for (const TString& name : implNames) {
            const auto* info = FindColumn(entry, name);
            if (!info) {
                Fail("42703", name);
                return false;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            cand.KeyTypes.push_back(column.Type);
        }
        if (prefix.size() == cand.KeyTypes.size()) {
            cand.FullPoint = true;
            cand.FromCells = prefix;
            cand.FromInclusive = true;
            cand.ToInclusive = true;
        } else {
            cand.FromCells = prefix;
            for (size_t i = prefix.size(); i < cand.KeyTypes.size(); ++i) {
                cand.FromCells.emplace_back();
            }
            cand.ToCells = prefix;
            cand.FromInclusive = true;
            cand.ToInclusive = true;
        }
        return true;
    }

    bool CollectIndexes(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        for (const auto& index : entry.Indexes) {
            if (index.GetState() != NKikimrSchemeOp::EIndexStateReady) {
                continue;
            }
            const auto type = index.GetType();
            if (type != NKikimrSchemeOp::EIndexTypeGlobal
                && type != NKikimrSchemeOp::EIndexTypeGlobalUnique
                && type != NKikimrSchemeOp::EIndexTypeGlobalAsync)
            {
                continue;
            }
            if (index.GetSpecializedIndexDescriptionCase() != NKikimrSchemeOp::TIndexDescription::SPECIALIZEDINDEXDESCRIPTION_NOT_SET) {
                continue;
            }
            TIndexCand cand;
            cand.Name = index.GetName();
            for (const auto& name : index.GetKeyColumnNames()) {
                cand.Keys.push_back(name);
            }
            for (const auto& name : index.GetDataColumnNames()) {
                cand.Data.push_back(name);
            }
            if (cand.Keys.empty()) {
                continue;
            }
            cand.Prefix = IndexPrefix(cand.Keys);
            if (cand.Prefix == 0 || cand.Prefix <= KeyPrefix) {
                continue;
            }
            cand.ImplPath = Path + "/" + cand.Name + "/indexImplTable";
            cand.Covering = Covers(cand);
            if (!BuildIndexBounds(entry, cand)) {
                return false;
            }
            if (cand.Prefix == 0) {
                continue;
            }
            Indexes.push_back(std::move(cand));
        }
        return true;
    }

    void StartIndex(size_t pos) {
        IndexPos = pos;
        IndexPhase = false;
        if (pos >= Indexes.size()) {
            PickPath();
            return;
        }
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Indexes[pos].ImplPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TSelectActor::StateIndexNavigate);
    }

    void HandleIndexNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Indexes[IndexPos].Ready = false;
            StartIndex(IndexPos + 1);
            return;
        }
        const auto& entry = request->ResultSet[0];
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            Indexes[IndexPos].Ready = false;
            StartIndex(IndexPos + 1);
            return;
        }
        TIndexCand& cand = Indexes[IndexPos];
        cand.Table = entry.TableId;
        if (cand.Covering) {
            for (const TColumn& out : Output) {
                const auto* info = FindColumn(entry, out.Name);
                if (!info) {
                    cand.Ready = false;
                    StartIndex(IndexPos + 1);
                    return;
                }
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return;
                }
                cand.ReadColumns.push_back(std::move(column));
            }
        } else {
            for (const TString& name : AllKeyNames) {
                const auto* info = FindColumn(entry, name);
                if (!info) {
                    cand.Ready = false;
                    StartIndex(IndexPos + 1);
                    return;
                }
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return;
                }
                cand.ReadColumns.push_back(std::move(column));
            }
        }
        if (cand.ReadColumns.empty()) {
            cand.Ready = false;
            StartIndex(IndexPos + 1);
            return;
        }
        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(cand.ReadColumns.size());
        for (const TColumn& column : cand.ReadColumns) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
            operation.InplaceUpdateMode = 0;
            operation.ImmediateUpdateSize = 0;
            columns.push_back(operation);
        }
        const TTableRange range = cand.FullPoint
            ? TTableRange(cand.FromCells)
            : TTableRange(cand.FromCells, cand.FromInclusive, cand.ToCells, cand.ToInclusive);
        auto description = MakeHolder<TKeyDesc>(cand.Table, range, TKeyDesc::ERowOperation::Read, cand.KeyTypes, columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        IndexPhase = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TSelectActor::StateResolve);
    }

    void OnIndexResolved(const TKeyDesc& description) {
        TIndexCand& cand = Indexes[IndexPos];
        const auto& partitions = description.GetPartitions();
        if (partitions.empty()) {
            cand.Ready = false;
            StartIndex(IndexPos + 1);
            return;
        }
        TTableShape shape;
        shape.KeyColumns = cand.Keys;
        shape.SchemaVersion = cand.Table.SchemaVersion;
        for (const auto& partition : partitions) {
            shape.Partitions.push_back({partition.ShardId, {}, {}, true});
            cand.Tablets.push_back(partition.ShardId);
        }
        const TPlanResult chosen = ChoosePlan(shape, cand.Compares, {});
        const TPlannerConfig config;
        const double rows = chosen.Ok && chosen.Plan ? chosen.Plan->Cost.Rows : static_cast<double>(cand.Tablets.size());
        cand.Cost = cand.Tablets.size() * config.CallCost + rows * config.RowCost + rows * config.ByteCost;
        cand.Ready = !cand.Tablets.empty();
        StartIndex(IndexPos + 1);
    }

    void PickPath() {
        Exploring = false;
        IndexPhase = false;
        double best = KeyPredicate ? PkCost : std::numeric_limits<double>::infinity();
        int bestAt = -1;
        const double followUp = TPlannerConfig{}.CallCost;
        for (size_t i = 0; i < Indexes.size(); ++i) {
            if (!Indexes[i].Ready) {
                continue;
            }
            const double cost = Indexes[i].Cost + (Indexes[i].Covering ? 0 : followUp);
            if (cost < best) {
                best = cost;
                bestAt = static_cast<int>(i);
            }
        }
        if (bestAt < 0) {
            if (!KeyPredicate || !SavedPk) {
                Fail("0A000", "not supported");
                return;
            }
            Adopt(*SavedPk);
            SendAll();
            return;
        }
        const TIndexCand& cand = Indexes[bestAt];
        ReadingIndex = true;
        CoveringRead = cand.Covering;
        AccessPath = cand.Covering ? "IndexOnlyScan" : "IndexScan";
        Table = cand.Table;
        ReadColumns = cand.ReadColumns;
        FromCells = cand.FromCells;
        ToCells = cand.ToCells;
        FromInclusive = cand.FromInclusive;
        ToInclusive = cand.ToInclusive;
        Point = cand.FullPoint;
        KeyTypes = cand.KeyTypes;
        Shards.clear();
        for (const ui64 tabletId : cand.Tablets) {
            TShard shard;
            shard.TabletId = tabletId;
            Shards.push_back(shard);
        }
        SendAll();
    }

    bool TakeIndexKeys(TEvDataShard::TEvReadResult& result) {
        const size_t count = result.GetRowsCount();
        for (size_t row = 0; row < count; ++row) {
            const TConstArrayRef<TCell> cells = result.GetCells(row);
            if (cells.size() != ReadColumns.size()) {
                Fail("XX000", "read failed");
                return false;
            }
            THeldKey held;
            held.Bytes.reserve(cells.size());
            held.Nulls.reserve(cells.size());
            for (const TCell& cell : cells) {
                held.Nulls.push_back(cell.IsNull());
                held.Bytes.push_back(cell.IsNull() ? TString() : TString(cell.AsBuf()));
            }
            MainKeys.push_back(std::move(held));
        }
        return true;
    }

    void BeginMainLookup() {
        ReadingIndex = false;
        MainLookup = true;
        Table = MainTable;
        ReadColumns = MainReadColumns;
        KeyTypes = AllKeyTypes;
        Point = true;
        FromInclusive = true;
        ToInclusive = true;
        MainKeyCells.clear();
        MainKeyCells.reserve(MainKeys.size());
        for (const THeldKey& held : MainKeys) {
            TVector<TCell> cells;
            cells.reserve(held.Bytes.size());
            for (size_t i = 0; i < held.Bytes.size(); ++i) {
                if (held.Nulls[i]) {
                    cells.emplace_back();
                } else {
                    cells.emplace_back(held.Bytes[i].data(), held.Bytes[i].size());
                }
            }
            MainKeyCells.push_back(std::move(cells));
        }
        MainPos = 0;
        ReadNextMain();
    }

    void ReadNextMain() {
        if (MainPos >= MainKeyCells.size()) {
            Succeed();
            return;
        }
        FromCells = MainKeyCells[MainPos];
        ToCells.clear();
        Point = true;
        Exploring = false;
        IndexPhase = false;
        StartResolve();
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateRead) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleRead);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateIndexNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndexNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TString Text;
    const TSelectStmt Statement;
    const ui32 InternalErrorRetries;
    const std::optional<std::pair<ui64, ui64>> Snapshot;
    const ui64 LockTxId;
    TString Path;
    TTableId Table;
    TVector<TColumn> Output;
    TVector<TColumn> ReadColumns;
    TVector<TString> KeyNames;
    TVector<NScheme::TTypeInfo> KeyTypes;
    TVector<TString> BoundBytes;
    TVector<TCell> FromCells;
    TVector<TCell> ToCells;
    TVector<TKeyCompare> Compares;
    TVector<TFilter> Filters;
    TVector<TCell> FilterCells;
    TVector<TShard> Shards;
    TVector<TVector<TQueryCell>> Rows;
    TVector<TSortSlot> SortSlots;
    TVector<TSortBytes> SortKeys;
    bool PkOrdered = false;
    bool ReverseRead = false;
    TVector<NKikimrDataEvents::TLock> Locks;
    TVector<ui64> WakeTablets;
    ui64 ReadId = 0;
    ui32 Pending = 0;
    ui32 ResolveErrors = 0;
    ui32 Overloads = 0;
    bool Point = false;
    bool FromSet = false;
    bool ToSet = false;
    bool FromInclusive = true;
    bool ToInclusive = true;
    bool Sent = false;
    bool Replied = false;
    bool KeyPredicate = false;
    bool Exploring = false;
    bool IndexPhase = false;
    bool ReadingIndex = false;
    bool CoveringRead = false;
    bool MainLookup = false;
    ui32 KeyPrefix = 0;
    size_t IndexPos = 0;
    size_t MainPos = 0;
    double PkCost = 0;
    TString AccessPath;
    TTableId MainTable;
    TVector<TColumn> MainReadColumns;
    TVector<TString> AllKeyNames;
    TVector<NScheme::TTypeInfo> AllKeyTypes;
    TVector<TIndexCand> Indexes;
    std::optional<TPlan> SavedPk;
    TVector<THeldKey> MainKeys;
    TVector<TVector<TCell>> MainKeyCells;
};

} // namespace

NActors::IActor* CreateSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries, std::optional<std::pair<ui64, ui64>> snapshot, ui64 lockTxId) {
    return new TSelectActor(replyTo, std::move(database), std::move(text), std::move(statement), internalErrorRetries, std::move(snapshot), lockTxId);
}

} // namespace NKikimr::NSql
