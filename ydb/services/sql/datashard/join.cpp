#include "events.h"

#include <ydb/services/sql/codec/codec.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/hash_set.h>
#include <util/generic/mapfindptr.h>

namespace NKikimr::NSql {

namespace {

constexpr ui32 kOverloadRetries = 16;

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

TString MakePath(const TString& database, const std::optional<TIdentifier>& schema, const TIdentifier& name) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (schema) {
        path += "/" + schema->Name;
    }
    path += "/" + name.Name;
    return path;
}

struct TColumn {
    TString Name;
    ui32 Id = 0;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

struct TOutput {
    TString Name;
    int Side = 0;
    TString Source;
    ui32 TypeOid = 25;
    i16 TypeSize = -1;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
    size_t Index = 0;
};

struct THeld {
    TVector<TString> Bytes;
    TVector<bool> Nulls;
};

class TJoinActor : public NActors::TActorBootstrapped<TJoinActor> {
public:
    TJoinActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui32 internalErrorRetries, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        if (!Statement.Join || Statement.Star || !Statement.Where.empty() || Statement.Join->Kind != EJoinKind::Inner) {
            Fail("0A000", "not supported");
            return;
        }
        if ((Statement.Schema && (Statement.Schema->Name == "information_schema" || Statement.Schema->Name == "pg_catalog"))
            || (Statement.Join->Schema && (Statement.Join->Schema->Name == "information_schema" || Statement.Join->Schema->Name == "pg_catalog")))
        {
            Fail("0A000", "not supported");
            return;
        }
        LeftPath = MakePath(Database, Statement.Schema, Statement.From);
        RightPath = MakePath(Database, Statement.Join->Schema, Statement.Join->Name);
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));
        Become(&TJoinActor::StateSnapshot);
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
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "SELECT " + ToString(Rows.size());
        result->AccessPath = "HashJoin " + ToString(RightCalls);
        for (const TOutput& column : Output) {
            result->Columns.push_back({column.Name, column.TypeOid, column.TypeSize});
        }
        result->Rows = std::move(Rows);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Unlink() {
        for (ui64 tablet : Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(tablet));
        }
        Linked.clear();
    }

    bool NamesSide(const TString& qualifier, const TString& table, const TString& alias) const {
        return qualifier == table || (!alias.empty() && qualifier == alias);
    }

    bool PrepareOutputs() {
        const TJoin& join = *Statement.Join;
        for (const TIdentifier& column : Statement.Columns) {
            TOutput out;
            out.Name = column.Name;
            out.Source = column.Name;
            const bool left = column.Qualifier.empty() || NamesSide(column.Qualifier, Statement.From.Name, Statement.Alias);
            const bool right = column.Qualifier.empty() || NamesSide(column.Qualifier, join.Name.Name, join.Alias);
            if (column.Qualifier.empty()) {
                LeftNames.insert(column.Name);
                RightNames.insert(column.Name);
                out.Side = -1;
            } else if (left && !right) {
                out.Side = 0;
                LeftNames.insert(column.Name);
            } else if (right && !left) {
                out.Side = 1;
                RightNames.insert(column.Name);
            } else {
                Fail("42703", column.Qualifier);
                return false;
            }
            Output.push_back(std::move(out));
        }
        const auto own = [&](const TIdentifier& column, int preferred) {
            if (!column.Qualifier.empty()) {
                if (NamesSide(column.Qualifier, Statement.From.Name, Statement.Alias)) {
                    return 0;
                }
                if (NamesSide(column.Qualifier, join.Name.Name, join.Alias)) {
                    return 1;
                }
                return -1;
            }
            return preferred;
        };
        const int leftKey = own(join.LeftColumn, 0);
        const int rightKey = own(join.RightColumn, 1);
        if (leftKey < 0 || rightKey < 0 || leftKey == rightKey) {
            Fail("42703", join.LeftColumn.Name);
            return false;
        }
        KeySide[0] = leftKey == 0 ? join.LeftColumn.Name : join.RightColumn.Name;
        KeySide[1] = leftKey == 1 ? join.LeftColumn.Name : join.RightColumn.Name;
        LeftNames.insert(KeySide[0]);
        RightNames.insert(KeySide[1]);
        return true;
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
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        const TCodecValue probe = EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale);
        return probe.Ok;
    }

    void OpenSide(int side) {
        Side = side;
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(side == 0 ? LeftPath : RightPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TJoinActor::StateNavigate);
    }

    void HandleSnapshot(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Fail("XX000", "internal error");
            return;
        }
        SnapshotStep = ev->Get()->Snapshot.Step;
        SnapshotTxId = ev->Get()->Snapshot.TxId;
        SnapshotHandle = std::move(ev->Get()->SnapshotHandle);
        if (!PrepareOutputs()) {
            return;
        }
        OpenSide(0);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request->ResultSet;
        const TString& path = Side == 0 ? LeftPath : RightPath;
        if (result.empty() || result[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", path);
            return;
        }
        if (result[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            Fail("0A000", "not supported");
            return;
        }
        if (result[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            Fail("42P01", path);
            return;
        }
        if (!BuildSide(result[0])) {
            return;
        }
        Resolve();
    }

    bool HasColumn(const TVector<TColumn>& columns, const TString& name) const {
        for (const TColumn& column : columns) {
            if (column.Name == name) {
                return true;
            }
        }
        return false;
    }

    bool Bind(TOutput& out, const TVector<TColumn>& columns) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].Name != out.Source) {
                continue;
            }
            const auto wire = PgWire(columns[i].Type.GetTypeId());
            if (!wire) {
                Fail("0A000", "not supported");
                return false;
            }
            out.Index = i;
            out.Type = columns[i].Type;
            out.Precision = columns[i].Precision;
            out.Scale = columns[i].Scale;
            out.TypeOid = wire->Oid;
            out.TypeSize = wire->Size;
            return true;
        }
        Fail("42703", out.Source);
        return false;
    }

    bool BuildSide(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
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
            Fail("0A000", "not supported");
            return false;
        }
        auto& columns = Side == 0 ? LeftColumns : RightColumns;
        auto& types = Side == 0 ? LeftTypes : RightTypes;
        columns.clear();
        types.clear();
        const auto& names = Side == 0 ? LeftNames : RightNames;
        for (const TString& name : names) {
            const auto* info = FindColumn(entry, name);
            if (!info) {
                continue;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            columns.push_back(std::move(column));
        }
        if (!HasColumn(columns, KeySide[Side])) {
            Fail("42703", KeySide[Side]);
            return false;
        }
        if (Side == 1) {
            for (TOutput& out : Output) {
                const bool left = HasColumn(LeftColumns, out.Source);
                const bool right = HasColumn(RightColumns, out.Source);
                if (out.Side == 0 && !left) {
                    Fail("42703", out.Source);
                    return false;
                }
                if (out.Side == 1 && !right) {
                    Fail("42703", out.Source);
                    return false;
                }
                if (out.Side < 0) {
                    if (left && right) {
                        Fail("42702", out.Source);
                        return false;
                    }
                    if (!left && !right) {
                        Fail("42703", out.Source);
                        return false;
                    }
                    out.Side = left ? 0 : 1;
                }
                if (!Bind(out, out.Side == 0 ? LeftColumns : RightColumns)) {
                    return false;
                }
            }
        }
        Table = entry.TableId;
        types.reserve(keys.size());
        FromCells.clear();
        for (const auto* info : keys) {
            types.push_back(info->PType);
            FromCells.emplace_back();
        }
        ToCells.clear();
        return true;
    }

    void Resolve() {
        auto& columns = Side == 0 ? LeftColumns : RightColumns;
        auto& types = Side == 0 ? LeftTypes : RightTypes;
        TVector<TKeyDesc::TColumnOp> ops;
        for (const TColumn& column : columns) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
            ops.push_back(operation);
        }
        auto description = MakeHolder<TKeyDesc>(Table, TTableRange(FromCells, true, ToCells, true), TKeyDesc::ERowOperation::Read, types, ops);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(description));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()));
        Become(&TJoinActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const TString& path = Side == 0 ? LeftPath : RightPath;
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Fail("42P01", path);
            return;
        }
        const auto& partitions = request->ResultSet[0].KeyDescription->GetPartitions();
        if (partitions.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        Tablets.clear();
        for (const auto& partition : partitions) {
            Tablets.push_back(partition.ShardId);
        }
        if (Side == 1) {
            RightCalls += Tablets.size();
        }
        Pending = Tablets.size();
        TabletPos = 0;
        SendRead(Tablets[TabletPos]);
    }

    void SendRead(ui64 tablet) {
        auto& columns = Side == 0 ? LeftColumns : RightColumns;
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        request->Record.SetReadId(++ReadId);
        request->Record.MutableTableId()->SetOwnerId(Table.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(Table.PathId.LocalPathId);
        request->Record.MutableTableId()->SetSchemaVersion(Table.SchemaVersion);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        request->Record.MutableSnapshot()->SetStep(SnapshotStep);
        request->Record.MutableSnapshot()->SetTxId(SnapshotTxId);
        if (LockTxId) {
            request->Record.SetLockTxId(LockTxId);
            request->Record.SetLockNodeId(SelfId().NodeId());
        }
        for (const TColumn& column : columns) {
            request->Record.AddColumns(column.Id);
        }
        request->Ranges.emplace_back(FromCells, true, ToCells, true);
        CurrentTablet = tablet;
        Linked.push_back(tablet);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), tablet, true));
        Become(&TJoinActor::StateRead);
    }

    bool Take(TEvDataShard::TEvReadResult& result) {
        auto& columns = Side == 0 ? LeftColumns : RightColumns;
        auto& rows = Side == 0 ? LeftRows : RightRows;
        for (size_t row = 0; row < result.GetRowsCount(); ++row) {
            const TConstArrayRef<TCell> cells = result.GetCells(row);
            if (cells.size() != columns.size()) {
                Fail("XX000", "read failed");
                return false;
            }
            THeld held;
            held.Bytes.reserve(cells.size());
            held.Nulls.reserve(cells.size());
            for (const TCell& cell : cells) {
                held.Nulls.push_back(cell.IsNull());
                held.Bytes.push_back(cell.IsNull() ? TString() : TString(cell.AsBuf()));
            }
            rows.push_back(std::move(held));
        }
        return true;
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto code = ev->Get()->Record.GetStatus().GetCode();
        if (code == Ydb::StatusIds::OVERLOADED) {
            if (OverloadRetries >= kOverloadRetries) {
                Fail("53000", "overloaded");
                return;
            }
            ++OverloadRetries;
            Schedule(TDuration::MilliSeconds(50), new NActors::TEvents::TEvWakeup());
            return;
        }
        if (code == Ydb::StatusIds::INTERNAL_ERROR) {
            if (InternalRetries >= InternalErrorRetries) {
                Fail("XX000", "internal error");
                return;
            }
            ++InternalRetries;
            SendRead(CurrentTablet);
            return;
        }
        if (code != Ydb::StatusIds::SUCCESS) {
            Fail(code == Ydb::StatusIds::SCHEME_ERROR ? TString("42P01") : TString("XX000"), Side == 0 ? LeftPath : RightPath);
            return;
        }
        if (!Take(*ev->Get())) {
            return;
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(ev->Get()->Record.GetReadId());
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), CurrentTablet, false));
            return;
        }
        ++TabletPos;
        if (TabletPos < Tablets.size()) {
            SendRead(Tablets[TabletPos]);
            return;
        }
        if (Side == 0) {
            OpenSide(1);
            return;
        }
        Emit();
    }

    size_t KeyIndex(int side) const {
        const auto& columns = side == 0 ? LeftColumns : RightColumns;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].Name == KeySide[side]) {
                return i;
            }
        }
        return columns.size();
    }

    void Emit() {
        const size_t rightKey = KeyIndex(1);
        const size_t leftKey = KeyIndex(0);
        if (rightKey >= RightColumns.size() || leftKey >= LeftColumns.size()) {
            Fail("42703", KeySide[0]);
            return;
        }
        absl::flat_hash_map<TString, TVector<size_t>> bucket;
        for (size_t i = 0; i < RightRows.size(); ++i) {
            if (RightRows[i].Nulls[rightKey]) {
                continue;
            }
            bucket[RightRows[i].Bytes[rightKey]].push_back(i);
        }
        for (const THeld& left : LeftRows) {
            if (left.Nulls[leftKey]) {
                continue;
            }
            const TVector<size_t>* hits = MapFindPtr(bucket, left.Bytes[leftKey]);
            if (!hits) {
                continue;
            }
            for (size_t index : *hits) {
                if (Statement.Limit && Rows.size() >= *Statement.Limit) {
                    Succeed();
                    return;
                }
                if (!Append(left, RightRows[index])) {
                    return;
                }
            }
        }
        Succeed();
    }

    bool Append(const THeld& left, const THeld& right) {
        TVector<TQueryCell> line;
        line.reserve(Output.size());
        for (const TOutput& column : Output) {
            if (column.Side < 0) {
                Fail("42702", column.Source);
                return false;
            }
            const THeld& row = column.Side == 0 ? left : right;
            const bool null = row.Nulls[column.Index];
            const TStringBuf bytes = null ? TStringBuf() : TStringBuf(row.Bytes[column.Index]);
            const TCodecValue encoded = EncodeCell(column.Type.GetTypeId(), null, bytes, EWireFormat::Text, column.Precision, column.Scale);
            if (!encoded.Ok) {
                Fail(encoded.SqlState.empty() ? TString("0A000") : encoded.SqlState, encoded.Message.empty() ? TString("not supported") : encoded.Message);
                return false;
            }
            line.push_back({encoded.Null, encoded.Bytes});
        }
        Rows.push_back(std::move(line));
        return true;
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        SendRead(CurrentTablet);
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    STATEFN(StateSnapshot) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, HandleSnapshot);
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSelectStmt Statement;
    const ui32 InternalErrorRetries;
    const ui64 LockTxId;
    TString LeftPath;
    TString RightPath;
    TString KeySide[2];
    THashSet<TString> LeftNames;
    THashSet<TString> RightNames;
    TVector<TColumn> LeftColumns;
    TVector<TColumn> RightColumns;
    TVector<NScheme::TTypeInfo> LeftTypes;
    TVector<NScheme::TTypeInfo> RightTypes;
    TVector<THeld> LeftRows;
    TVector<THeld> RightRows;
    TVector<TOutput> Output;
    TVector<TVector<TQueryCell>> Rows;
    TTableId Table;
    TVector<TCell> FromCells;
    TVector<TCell> ToCells;
    TVector<ui64> Tablets;
    TVector<ui64> Linked;
    size_t TabletPos = 0;
    size_t Pending = 0;
    ui64 CurrentTablet = 0;
    ui64 ReadId = 0;
    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;
    NKqp::TSnapshotHandle SnapshotHandle;
    int Side = 0;
    ui32 RightCalls = 0;
    ui32 OverloadRetries = 0;
    ui32 InternalRetries = 0;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateJoinActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui32 internalErrorRetries, ui64 lockTxId) {
    return new TJoinActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, lockTxId);
}

} // namespace NKikimr::NSql
