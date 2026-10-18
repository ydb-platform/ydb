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
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <algorithm>

namespace NKikimr::NSql {

namespace {

constexpr ui32 kOverloadRetries = 16;

struct TColumn {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    NScheme::TTypeInfo Type;
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

TString TablePath(const TString& database, const TUpdateStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    path += "/" + statement.Name.Name;
    return path;
}

class TUpdateActor : public NActors::TActorBootstrapped<TUpdateActor> {
public:
    TUpdateActor(const NActors::TActorId& replyTo, TString database, TUpdateStmt statement, ui32 internalErrorRetries, bool defer)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
        , Defer(defer)
    {
    }

    void Bootstrap() {
        Path = TablePath(Database, Statement);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TUpdateActor::StateNavigate);
    }

private:
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

    struct THeld {
        ui64 TabletId = 0;
        TVector<TString> Storage;
        TVector<TCell> Cells;
        THashMap<TString, TCell> Old;
    };

    struct TIndexPlan {
        TString Name;
        TString ImplPath;
        TVector<TString> Keys;
        TVector<TString> Data;
    };

    struct TIndexOp {
        TDeferredWrite Write;
        TVector<TString> Bytes;
        TVector<TCell> Key;
        TVector<NScheme::TTypeInfo> Types;
        TTableId Table;
    };

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

    void Succeed(ui32 count, TVector<TDeferredWrite> writes) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "UPDATE " + ToString(count);
        result->Columns = ResultColumns;
        if (count > 0) {
            result->Rows = ResultRows;
        }
        result->Deferred = std::move(writes);
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
        column.Name = info.Name;
        column.Id = info.Id;
        column.KeyOrder = info.KeyOrder;
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        const TCodecValue probe = EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale);
        return probe.Ok;
    }

    bool Decode(const TColumn& column, const TLiteral& literal, TCell& cell, TVector<TString>& storage) {
        if (literal.Kind == TLiteral::EKind::Null) {
            cell = TCell();
            return true;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("22023") : decoded.SqlState, decoded.Message.empty() ? TString("bad request") : decoded.Message);
            return false;
        }
        storage.push_back(decoded.Bytes);
        cell = TCell(storage.back().data(), storage.back().size());
        return true;
    }

    bool DecodeBound(const TColumn& column, const TLiteral& literal, TCell& cell) {
        return Decode(column, literal, cell, BoundBytes);
    }

    TCell CopyCell(const TCell& cell, TVector<TString>& storage) const {
        if (cell.IsNull()) {
            return TCell();
        }
        storage.push_back(TString(cell.AsBuf()));
        return TCell(storage.back().data(), storage.back().size());
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

    bool Build(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        for (const auto& index : entry.Indexes) {
            const auto type = index.GetType();
            const auto state = index.GetState();
            if (type == NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                continue;
            }
            const bool blocking = state == NKikimrSchemeOp::EIndexStateReady || state == NKikimrSchemeOp::EIndexStateWriteOnly;
            if (!blocking) {
                continue;
            }
            if (type != NKikimrSchemeOp::EIndexTypeGlobal && type != NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                Fail("0A000", "not supported");
                return false;
            }
            if (index.GetSpecializedIndexDescriptionCase() != NKikimrSchemeOp::TIndexDescription::SPECIALIZEDINDEXDESCRIPTION_NOT_SET) {
                Fail("0A000", "not supported");
                return false;
            }
            TIndexPlan plan;
            plan.Name = index.GetName();
            plan.ImplPath = Path + "/" + plan.Name + "/indexImplTable";
            for (const auto& column : index.GetKeyColumnNames()) {
                plan.Keys.push_back(column);
            }
            for (const auto& column : index.GetDataColumnNames()) {
                plan.Data.push_back(column);
            }
            IndexPlans.push_back(std::move(plan));
        }
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
        if (keys.empty() || Statement.Set.empty()) {
            Fail("0A000", "not supported");
            return false;
        }
        THashMap<TString, TColumn> byName;
        auto prepare = [&](const TString& name) -> const TColumn* {
            if (const TColumn* found = byName.FindPtr(name)) {
                return found;
            }
            const auto* info = FindColumn(entry, name);
            if (!info) {
                Fail("42703", name);
                return nullptr;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return nullptr;
            }
            byName[name] = column;
            return byName.FindPtr(name);
        };
        THashSet<TString> seenTarget;
        for (const TSetAssign& assign : Statement.Set) {
            const TColumn* target = prepare(assign.Column.Name);
            if (!target) {
                return false;
            }
            if (target->KeyOrder >= 0) {
                Fail("0A000", "not supported");
                return false;
            }
            if (assign.FromColumn && !prepare(assign.Source.Name)) {
                return false;
            }
            if (!seenTarget.contains(assign.Column.Name)) {
                seenTarget.insert(assign.Column.Name);
                WriteColumns.push_back(*target);
            }
        }
        TVector<TColumn> keyColumns;
        for (const auto* info : keys) {
            const TColumn* column = prepare(info->Name);
            if (!column) {
                return false;
            }
            keyColumns.push_back(*column);
            KeyNames.push_back(column->Name);
            KeyTypes.push_back(column->Type);
        }
        WriteColumns.insert(WriteColumns.begin(), keyColumns.begin(), keyColumns.end());

        THashMap<TString, TVector<const TComparison*>> grouped;
        for (const TComparison& comparison : Statement.Where) {
            if (!prepare(comparison.Column.Name)) {
                return false;
            }
            grouped[comparison.Column.Name].push_back(&comparison);
        }
        BoundBytes.clear();
        BoundBytes.reserve(Statement.Where.size() * 2);
        bool ranged = false;
        size_t equalPrefix = 0;
        for (const TColumn& key : keyColumns) {
            const auto* group = grouped.FindPtr(key.Name);
            if (!group || ranged) {
                break;
            }
            const bool onlyEqual = group->size() == 1 && (*group)[0]->Op == ECompareOp::Eq;
            if (onlyEqual) {
                TCell cell;
                if (!DecodeBound(key, (*group)[0]->Value, cell)) {
                    return false;
                }
                FromCells.push_back(cell);
                ToCells.push_back(cell);
                FromSet = true;
                ToSet = true;
                Compares.push_back({key.Name, true, false, false, BoundBytes.back(), {}});
                ++equalPrefix;
                grouped.erase(key.Name);
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
                if (!DecodeBound(key, comparison->Value, cell)) {
                    return false;
                }
                pushed = true;
                switch (comparison->Op) {
                    case ECompareOp::Eq:
                        TightenLower(key, cell, true);
                        TightenUpper(key, cell, true);
                        break;
                    case ECompareOp::Gt:
                        TightenLower(key, cell, false);
                        break;
                    case ECompareOp::Ge:
                        TightenLower(key, cell, true);
                        break;
                    case ECompareOp::Lt:
                        TightenUpper(key, cell, false);
                        break;
                    case ECompareOp::Le:
                        TightenUpper(key, cell, true);
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
            compare.Column = key.Name;
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
                grouped.erase(key.Name);
            } else {
                grouped[key.Name] = std::move(leftover);
            }
            break;
        }
        Point = !ranged && equalPrefix == keyColumns.size();
        if (!Point && !ranged && equalPrefix == 0) {
            Fail("0A000", "not supported");
            return false;
        }
        if (!FromSet) {
            FromCells.assign(KeyTypes.size(), TCell());
            FromInclusive = true;
        }
        if (!ToSet) {
            ToCells.clear();
            ToInclusive = true;
        }
        ReadColumns = keyColumns;
        for (const TColumn& column : WriteColumns) {
            bool listed = false;
            for (const TColumn& read : ReadColumns) {
                listed = listed || read.Id == column.Id;
            }
            if (!listed) {
                ReadColumns.push_back(column);
            }
        }
        for (const TSetAssign& assign : Statement.Set) {
            if (!assign.FromColumn) {
                continue;
            }
            const TColumn* source = byName.FindPtr(assign.Source.Name);
            if (!source) {
                Fail("42703", assign.Source.Name);
                return false;
            }
            bool listed = false;
            for (const TColumn& read : ReadColumns) {
                listed = listed || read.Id == source->Id;
            }
            if (!listed) {
                ReadColumns.push_back(*source);
            }
        }
        for (const auto& [name, list] : grouped) {
            Y_UNUSED(name);
            for (const TComparison* comparison : list) {
                TFilter filter;
                filter.Column = *byName.FindPtr(comparison->Column.Name);
                filter.Op = comparison->Op;
                TCell cell;
                if (!DecodeBound(filter.Column, comparison->Value, cell)) {
                    return false;
                }
                filter.Literal = BoundBytes.back();
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
                Filters.push_back(std::move(filter));
            }
        }
        FilterCells.clear();
        for (const TFilter& filter : Filters) {
            FilterCells.emplace_back(filter.Literal.data(), filter.Literal.size());
        }
        auto ensure = [&](const TString& name) -> bool {
            const TColumn* column = prepare(name);
            if (!column) {
                return false;
            }
            for (const TColumn& read : ReadColumns) {
                if (read.Id == column->Id) {
                    return true;
                }
            }
            ReadColumns.push_back(*column);
            return true;
        };
        for (const TIndexPlan& plan : IndexPlans) {
            for (const TString& name : plan.Keys) {
                if (!ensure(name)) {
                    return false;
                }
            }
            for (const TString& name : plan.Data) {
                if (!ensure(name)) {
                    return false;
                }
            }
        }
        if (!Statement.Returning.empty() && !PrepareReturning(entry, prepare)) {
            return false;
        }
        Table = entry.TableId;
        return true;
    }

    template <typename TPrepare>
    bool PrepareReturning(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TPrepare& prepare) {
        TVector<const TSysTables::TTableColumnInfo*> ordered;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            ordered.push_back(&column);
        }
        std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
            return left->Id < right->Id;
        });
        auto add = [&](const TColumn& column, const TString& name) -> bool {
            const auto wire = PgWire(column.Type.GetTypeId());
            if (!wire) {
                Fail("0A000", "not supported");
                return false;
            }
            ResultColumns.push_back({name, wire->Oid, wire->Size});
            Outputs.push_back(column);
            return true;
        };
        for (const TReturningItem& item : Statement.Returning) {
            if (item.Star) {
                for (const auto* info : ordered) {
                    const TColumn* column = prepare(info->Name);
                    if (!column || !add(*column, column->Name)) {
                        return false;
                    }
                }
                continue;
            }
            const TColumn* column = prepare(item.Column.Name);
            if (!column) {
                return false;
            }
            if (!add(*column, item.Alias ? item.Alias->Name : column->Name)) {
                return false;
            }
        }
        for (const TColumn& column : Outputs) {
            bool listed = false;
            for (const TColumn& read : ReadColumns) {
                listed = listed || read.Id == column.Id;
            }
            if (!listed) {
                ReadColumns.push_back(column);
            }
        }
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
        if (!Build(entry)) {
            return;
        }
        StartResolve();
    }

    void StartResolve() {
        TVector<TKeyDesc::TColumnOp> columns;
        for (const TColumn& column : ReadColumns) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
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
        Become(&TUpdateActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (IndexPhase) {
            if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                Fail("42P01", IndexPlans[IndexPos].ImplPath);
                return;
            }
            const auto& parts = request->ResultSet[0].KeyDescription->GetPartitions();
            if (parts.size() != 1) {
                Fail("0A000", "not supported");
                return;
            }
            IndexOps[OpPos].Write.TabletId = parts[0].ShardId;
            ++OpPos;
            ResolveNext();
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
        TTableShape shape;
        shape.KeyColumns = KeyNames;
        shape.SchemaVersion = Table.SchemaVersion;
        for (const auto& partition : partitions) {
            shape.Partitions.push_back({partition.ShardId, {}, {}, true});
        }
        const TPlanResult chosen = ChoosePlan(shape, Compares, {});
        if (!chosen.Ok || !chosen.Plan || chosen.Plan->Reads.empty()) {
            Fail(chosen.SqlState ? chosen.SqlState : TString("0A000"), chosen.Message ? chosen.Message : TString("not supported"));
            return;
        }
        Shards.clear();
        for (const TReadTarget& read : chosen.Plan->Reads) {
            if (Point && read.Shape != EReadShape::Keys) {
                Fail("0A000", "not supported");
                return;
            }
            if (!Point && read.Shape != EReadShape::Ranges) {
                Fail("0A000", "not supported");
                return;
            }
            Shards.push_back({read.TabletId, 0, false, 0});
        }
        Pending = Shards.size();
        for (TShard& shard : Shards) {
            SendRead(shard);
        }
        Become(&TUpdateActor::StateRead);
    }

    void SendRead(TShard& shard) {
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        shard.ReadId = ++ReadId;
        record.SetReadId(shard.ReadId);
        record.MutableTableId()->SetOwnerId(Table.PathId.OwnerId);
        record.MutableTableId()->SetTableId(Table.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(Table.SchemaVersion);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (const TColumn& column : ReadColumns) {
            record.AddColumns(column.Id);
        }
        if (Point) {
            request->Keys.push_back(TSerializedCellVec(FromCells));
        } else {
            request->Ranges.emplace_back(FromCells, FromInclusive, ToCells, ToInclusive);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shard.TabletId, true));
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

    const TColumn* ReadColumn(const TString& name) const {
        for (const TColumn& column : ReadColumns) {
            if (column.Name == name) {
                return &column;
            }
        }
        return nullptr;
    }

    size_t ReadIndex(ui32 id) const {
        for (size_t i = 0; i < ReadColumns.size(); ++i) {
            if (ReadColumns[i].Id == id) {
                return i;
            }
        }
        return ReadColumns.size();
    }

    bool TakeRows(TEvDataShard::TEvReadResult& result, ui64 tabletId) {
        THashMap<TString, const TSetAssign*> last;
        for (const TSetAssign& assign : Statement.Set) {
            last[assign.Column.Name] = &assign;
        }
        const size_t count = result.GetRowsCount();
        for (size_t row = 0; row < count; ++row) {
            const TConstArrayRef<TCell> cells = result.GetCells(row);
            if (cells.size() != ReadColumns.size()) {
                Fail("XX000", "read failed");
                return false;
            }
            if (!Matches(cells)) {
                continue;
            }
            THeld held;
            held.TabletId = tabletId;
            held.Storage.reserve(WriteColumns.size());
            for (const TColumn& column : WriteColumns) {
                TCell cell;
                if (column.KeyOrder >= 0) {
                    cell = CopyCell(cells[ReadIndex(column.Id)], held.Storage);
                    if (cell.IsNull()) {
                        Fail("23502", "null value in column " + column.Name);
                        return false;
                    }
                } else {
                    const TSetAssign* assign = last.FindPtr(column.Name) ? *last.FindPtr(column.Name) : nullptr;
                    if (!assign) {
                        Fail("XX000", "internal error");
                        return false;
                    }
                    if (assign->FromColumn) {
                        const TColumn* source = ReadColumn(assign->Source.Name);
                        if (!source) {
                            Fail("42703", assign->Source.Name);
                            return false;
                        }
                        cell = CopyCell(cells[ReadIndex(source->Id)], held.Storage);
                    } else if (!Decode(column, assign->Value, cell, held.Storage)) {
                        return false;
                    }
                }
                held.Cells.push_back(cell);
            }
            for (const TColumn& column : ReadColumns) {
                held.Old[column.Name] = CopyCell(cells[ReadIndex(column.Id)], held.Storage);
            }
            if (!AppendReturning(cells, held, true)) {
                return false;
            }
            Rows.push_back(std::move(held));
        }
        return true;
    }

    bool AppendReturning(const TConstArrayRef<TCell>& read, const THeld& held, bool overlay) {
        if (Outputs.empty()) {
            return true;
        }
        TVector<TQueryCell> line;
        line.reserve(Outputs.size());
        for (const TColumn& column : Outputs) {
            const TCell* cell = nullptr;
            TCell overlaid;
            if (overlay) {
                for (size_t i = 0; i < WriteColumns.size(); ++i) {
                    if (WriteColumns[i].Id == column.Id && WriteColumns[i].KeyOrder < 0) {
                        overlaid = held.Cells[i];
                        cell = &overlaid;
                    }
                }
            }
            TCell original;
            if (!cell) {
                const size_t index = ReadIndex(column.Id);
                if (index >= read.size()) {
                    Fail("XX000", "read failed");
                    return false;
                }
                original = read[index];
                cell = &original;
            }
            const bool null = cell->IsNull();
            const TStringBuf bytes = null ? TStringBuf() : cell->AsBuf();
            const TCodecValue encoded = EncodeCell(column.Type.GetTypeId(), null, bytes, EWireFormat::Text, column.Precision, column.Scale);
            if (!encoded.Ok) {
                Fail(encoded.SqlState.empty() ? TString("0A000") : encoded.SqlState, encoded.Message.empty() ? TString("not supported") : encoded.Message);
                return false;
            }
            line.push_back({encoded.Null, encoded.Bytes});
        }
        ResultRows.push_back(std::move(line));
        return true;
    }

    void FinishShard(TShard& shard) {
        shard.Done = true;
        if (Pending > 0) {
            --Pending;
        }
        if (Pending == 0) {
            Publish();
        }
    }

    void Publish() {
        THashMap<ui64, TVector<size_t>> groups;
        for (size_t i = 0; i < Rows.size(); ++i) {
            groups[Rows[i].TabletId].push_back(i);
        }
        TVector<TDeferredWrite> writes;
        for (const auto& [tablet, indexes] : groups) {
            TVector<TCell> cells;
            cells.reserve(indexes.size() * WriteColumns.size());
            for (const size_t index : indexes) {
                cells.insert(cells.end(), Rows[index].Cells.begin(), Rows[index].Cells.end());
            }
            TSerializedCellMatrix matrix(cells, indexes.size(), WriteColumns.size());
            TDeferredWrite write;
            write.OwnerId = Table.PathId.OwnerId;
            write.LocalPathId = Table.PathId.LocalPathId;
            write.SchemaVersion = Table.SchemaVersion;
            write.TabletId = tablet;
            write.Operation = static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
            for (const TColumn& column : WriteColumns) {
                write.ColumnIds.push_back(column.Id);
            }
            write.Payload = matrix.ReleaseBuffer();
            writes.push_back(std::move(write));
        }
        MainWrites = std::move(writes);
        const ui32 count = Rows.size();
        if (!IndexPlans.empty() && !Rows.empty()) {
            PendingWrites = count;
            IndexPos = 0;
            OpPos = 0;
            OpenIndex();
            return;
        }
        CommitReady(count);
    }

    const TCell* FindNew(const THeld& held, const TString& name) const {
        for (size_t i = 0; i < WriteColumns.size(); ++i) {
            if (WriteColumns[i].Name == name) {
                return &held.Cells[i];
            }
        }
        return held.Old.FindPtr(name);
    }

    bool SameCell(const TCell& left, const TCell& right) const {
        if (left.IsNull() || right.IsNull()) {
            return left.IsNull() && right.IsNull();
        }
        return left.AsBuf() == right.AsBuf();
    }

    bool AddIndexOp(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TVector<TString>& names, size_t keyCount, const THeld& held, bool newest, ui32 operation) {
        TIndexOp op;
        op.Bytes.reserve(names.size());
        for (const TString& name : names) {
            const TCell* source = newest ? FindNew(held, name) : held.Old.FindPtr(name);
            if (!source || source->IsNull()) {
                op.Bytes.push_back(TString());
            } else {
                op.Bytes.push_back(TString(source->AsBuf()));
            }
        }
        TVector<TCell> cells;
        cells.reserve(names.size());
        for (size_t i = 0; i < names.size(); ++i) {
            const auto* info = FindColumn(entry, names[i]);
            if (!info) {
                Fail("42703", names[i]);
                return false;
            }
            const TCell cell = op.Bytes[i].empty() && (newest ? !FindNew(held, names[i]) || FindNew(held, names[i])->IsNull() : !held.Old.FindPtr(names[i]) || held.Old.FindPtr(names[i])->IsNull())
                ? TCell()
                : TCell(op.Bytes[i].data(), op.Bytes[i].size());
            cells.push_back(cell);
            op.Write.ColumnIds.push_back(info->Id);
            if (i < keyCount) {
                op.Key.push_back(cell);
                op.Types.push_back(info->PType);
            }
        }
        TSerializedCellMatrix matrix(cells, 1, static_cast<ui16>(cells.size()));
        op.Write.OwnerId = entry.TableId.PathId.OwnerId;
        op.Write.LocalPathId = entry.TableId.PathId.LocalPathId;
        op.Write.SchemaVersion = entry.TableId.SchemaVersion;
        op.Write.Operation = operation;
        op.Write.Payload = matrix.ReleaseBuffer();
        op.Table = entry.TableId;
        IndexOps.push_back(std::move(op));
        return true;
    }

    void OpenIndex() {
        IndexPhase = false;
        if (IndexPos >= IndexPlans.size()) {
            CommitReady(PendingWrites);
            return;
        }
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(IndexPlans[IndexPos].ImplPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TUpdateActor::StateIndexNavigate);
    }

    void HandleIndexNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || request->ResultSet[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindTable)
        {
            Fail("42P01", IndexPlans[IndexPos].ImplPath);
            return;
        }
        const auto& entry = request->ResultSet[0];
        const TIndexPlan& plan = IndexPlans[IndexPos];
        TVector<TString> keyNames = plan.Keys;
        for (const TColumn& column : WriteColumns) {
            if (column.KeyOrder < 0) {
                continue;
            }
            bool present = false;
            for (const TString& key : plan.Keys) {
                present = present || key == column.Name;
            }
            if (!present) {
                keyNames.push_back(column.Name);
            }
        }
        TVector<TString> insertNames = keyNames;
        for (const TString& column : plan.Data) {
            bool present = false;
            for (const TString& name : insertNames) {
                present = present || name == column;
            }
            if (!present) {
                insertNames.push_back(column);
            }
        }
        OpPos = IndexOps.size();
        for (const THeld& held : Rows) {
            bool changed = false;
            for (const TString& name : plan.Keys) {
                const TCell* previous = held.Old.FindPtr(name);
                const TCell* next = FindNew(held, name);
                if (!previous || !next || !SameCell(*previous, *next)) {
                    changed = true;
                }
            }
            if (!changed) {
                continue;
            }
            if (!AddIndexOp(entry, keyNames, keyNames.size(), held, false, static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE))
                || !AddIndexOp(entry, insertNames, keyNames.size(), held, true, static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT)))
            {
                return;
            }
        }
        ResolveNext();
    }

    void ResolveNext() {
        if (OpPos >= IndexOps.size()) {
            ++IndexPos;
            OpenIndex();
            return;
        }
        TIndexOp& op = IndexOps[OpPos];
        TVector<TKeyDesc::TColumnOp> columns;
        for (size_t i = 0; i < op.Key.size(); ++i) {
            TKeyDesc::TColumnOp operation;
            operation.Column = op.Write.ColumnIds[i];
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = op.Types[i];
            columns.push_back(operation);
        }
        auto description = MakeHolder<TKeyDesc>(op.Table, TTableRange(op.Key), TKeyDesc::ERowOperation::Read, op.Types, columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        IndexPhase = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TUpdateActor::StateResolve);
    }

    void CommitReady(ui32 count) {
        for (TIndexOp& op : IndexOps) {
            MainWrites.push_back(std::move(op.Write));
        }
        if (Defer || MainWrites.empty()) {
            Succeed(count, std::move(MainWrites));
            return;
        }
        PendingWrites = count;
        Register(CreateCommitActor(SelfId(), 0, std::move(MainWrites), {}, InternalErrorRetries));
        Become(&TUpdateActor::StateCommit);
    }

    void HandleCommit(TEvQueryResult::TPtr& ev) {
        const TEvQueryResult& result = *ev->Get();
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("XX000") : result.SqlState,
                result.Message.empty() ? TString("internal error") : result.Message);
            return;
        }
        Succeed(PendingWrites, {});
    }

    TString StatusText(const NKikimrTxDataShard::TEvReadResult& record) const {
        const auto& status = record.GetStatus();
        if (status.IssuesSize() == 0) {
            return "read failed";
        }
        return status.GetIssues(0).message();
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr& ev) {
        TShard* shard = nullptr;
        for (TShard& candidate : Shards) {
            if (candidate.ReadId == ev->Get()->Record.GetReadId() && !candidate.Done) {
                shard = &candidate;
                break;
            }
        }
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
            WakeTablet = shard->TabletId;
            Schedule(TDuration::MilliSeconds(50), new NActors::TEvents::TEvWakeup());
            return;
        }
        if (code == Ydb::StatusIds::INTERNAL_ERROR) {
            if (shard->InternalErrors >= InternalErrorRetries) {
                Fail("XX000", StatusText(ev->Get()->Record));
                return;
            }
            ++shard->InternalErrors;
            SendRead(*shard);
            return;
        }
        if (code == Ydb::StatusIds::SCHEME_ERROR) {
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
        if (!TakeRows(*ev->Get(), shard->TabletId)) {
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
        for (TShard& shard : Shards) {
            if (shard.TabletId == WakeTablet && !shard.Done) {
                SendRead(shard);
            }
        }
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    void Unlink() {
        for (const TShard& shard : Shards) {
            if (shard.TabletId != 0) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(shard.TabletId));
            }
        }
    }

    STATEFN(StateIndexNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndexNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
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

    STATEFN(StateCommit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQueryResult, HandleCommit);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TUpdateStmt Statement;
    const ui32 InternalErrorRetries;
    const bool Defer;
    TString Path;
    TTableId Table;
    TVector<TColumn> ReadColumns;
    TVector<TColumn> WriteColumns;
    TVector<TColumn> Outputs;
    TVector<TQueryColumn> ResultColumns;
    TVector<TVector<TQueryCell>> ResultRows;
    TVector<TString> KeyNames;
    TVector<NScheme::TTypeInfo> KeyTypes;
    TVector<TString> BoundBytes;
    TVector<TCell> FromCells;
    TVector<TCell> ToCells;
    TVector<TKeyCompare> Compares;
    TVector<TFilter> Filters;
    TVector<TCell> FilterCells;
    TVector<TShard> Shards;
    TVector<THeld> Rows;
    ui64 ReadId = 0;
    ui32 Pending = 0;
    ui32 PendingWrites = 0;
    ui32 Overloads = 0;
    ui64 WakeTablet = 0;
    bool Point = false;
    bool FromSet = false;
    bool ToSet = false;
    bool FromInclusive = true;
    bool ToInclusive = true;
    bool Replied = false;
    bool IndexPhase = false;
    size_t IndexPos = 0;
    size_t OpPos = 0;
    TVector<TIndexPlan> IndexPlans;
    TVector<TIndexOp> IndexOps;
    TVector<TDeferredWrite> MainWrites;
};

} // namespace

NActors::IActor* CreateUpdateActor(const NActors::TActorId& replyTo, TString database, TUpdateStmt statement, ui32 internalErrorRetries, bool defer) {
    return new TUpdateActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, defer);
}

} // namespace NKikimr::NSql
