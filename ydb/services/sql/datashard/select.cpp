#include "events.h"

#include <ydb/services/sql/codec/codec.h>
#include <ydb/services/sql/plan/plan.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <algorithm>
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
    TSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
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
        if (Sent) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Sent = false;
        }
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
        if (Sent) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Sent = false;
        }
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        for (const TColumn& column : Output) {
            result->Columns.push_back({column.Name, column.TypeOid, column.TypeSize});
        }
        result->Rows = std::move(Rows);
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

    bool BuildPoint(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
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

        THashMap<TString, const TComparison*> equals;
        for (const TComparison& comparison : Statement.Where) {
            if (comparison.Op != ECompareOp::Eq || equals.FindPtr(comparison.Column.Name)) {
                return false;
            }
            equals[comparison.Column.Name] = &comparison;
        }
        if (equals.size() != keys.size()) {
            return false;
        }

        KeyBytes.clear();
        KeyBytes.reserve(keys.size());
        KeyCells.clear();
        Compares.clear();
        KeyNames.clear();
        TVector<NScheme::TTypeInfo> keyTypes;
        for (const auto* info : keys) {
            const TComparison* comparison = equals.Value(info->Name, nullptr);
            if (!comparison) {
                return false;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, comparison->Value.Text, EWireFormat::Text, column.Precision, column.Scale);
            if (!decoded.Ok) {
                Fail(decoded.SqlState, decoded.Message);
                return false;
            }
            KeyBytes.push_back(decoded.Bytes);
            KeyCells.emplace_back(KeyBytes.back().data(), KeyBytes.back().size());
            keyTypes.push_back(column.Type);
            KeyNames.push_back(column.Name);
            Compares.push_back({column.Name, true, false, false, decoded.Bytes, {}});
        }
        KeyTypes = std::move(keyTypes);
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
        if (!BuildPoint(entry)) {
            Fail("0A000", "not supported");
            return;
        }

        {
            TGuard<TAdaptiveLock> guard(Plans().Lock);
            if (const auto plan = Plans().Cache.Lookup(Database, Text, {}, Table.SchemaVersion, TActivationContext::Now().MilliSeconds())) {
                if (plan->Reads.size() == 1 && plan->Reads[0].Shape == EReadShape::Keys) {
                    TabletId = plan->Reads[0].TabletId;
                    SendRead();
                    return;
                }
            }
        }
        StartResolve();
    }

    void StartResolve() {
        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(Output.size());
        for (const TColumn& column : Output) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
            operation.InplaceUpdateMode = 0;
            operation.ImmediateUpdateSize = 0;
            columns.push_back(operation);
        }
        const TTableRange range(KeyCells);
        auto description = MakeHolder<TKeyDesc>(Table, range, TKeyDesc::ERowOperation::Read, KeyTypes, columns);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(description));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()));
        Become(&TSelectActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Fail("42P01", Path);
            return;
        }
        const auto& partitions = request->ResultSet[0].KeyDescription->GetPartitions();
        if (partitions.size() != 1) {
            Fail("0A000", "not supported");
            return;
        }
        TabletId = partitions[0].ShardId;

        TTableShape shape;
        shape.KeyColumns = KeyNames;
        shape.SchemaVersion = Table.SchemaVersion;
        shape.Partitions.push_back({TabletId, {}, {}, true});
        const TPlanResult chosen = ChoosePlan(shape, Compares, {});
        if (!chosen.Ok || !chosen.Plan || chosen.Plan->Reads.size() != 1 || chosen.Plan->Reads[0].Shape != EReadShape::Keys) {
            Fail(chosen.SqlState ? chosen.SqlState : TString("0A000"), chosen.Message ? chosen.Message : TString("not supported"));
            return;
        }
        TabletId = chosen.Plan->Reads[0].TabletId;
        {
            TGuard<TAdaptiveLock> guard(Plans().Lock);
            Plans().Cache.Store(Database, Text, {}, *chosen.Plan, TActivationContext::Now().MilliSeconds());
        }
        SendRead();
    }

    void SendRead() {
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        record.SetReadId(++ReadId);
        record.MutableTableId()->SetOwnerId(Table.PathId.OwnerId);
        record.MutableTableId()->SetTableId(Table.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(Table.SchemaVersion);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (const TColumn& column : Output) {
            record.AddColumns(column.Id);
        }
        request->Keys.push_back(KeySerialized());
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), TabletId, true));
        Sent = true;
        Become(&TSelectActor::StateRead);
    }

    TSerializedCellVec KeySerialized() const {
        return TSerializedCellVec(KeyCells);
    }

    void ForgetPlan() {
        TGuard<TAdaptiveLock> guard(Plans().Lock);
        Plans().Cache.ForgetSchema(Database, Text);
    }

    void RetryResolve() {
        if (Sent) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Sent = false;
        }
        ForgetPlan();
        if (ResolveErrors >= InternalErrorRetries) {
            Fail("08006", "pipe");
            return;
        }
        ++ResolveErrors;
        StartResolve();
    }

    TString StatusText(const NKikimrTxDataShard::TEvReadResult& record) const {
        const auto& status = record.GetStatus();
        if (status.IssuesSize() == 0) {
            return "read failed";
        }
        return status.GetIssues(0).message();
    }

    bool TakeRows(TEvDataShard::TEvReadResult& result) {
        const size_t count = result.GetRowsCount();
        for (size_t row = 0; row < count; ++row) {
            const TConstArrayRef<TCell> cells = result.GetCells(row);
            if (cells.size() != Output.size()) {
                Fail("XX000", "read failed");
                return false;
            }
            TVector<TQueryCell> line;
            line.reserve(cells.size());
            for (size_t i = 0; i < cells.size(); ++i) {
                const TCell& cell = cells[i];
                const TStringBuf bytes = cell.IsNull() ? TStringBuf() : cell.AsBuf();
                const TCodecValue encoded = EncodeCell(Output[i].Type.GetTypeId(), cell.IsNull(), bytes, EWireFormat::Text, Output[i].Precision, Output[i].Scale);
                if (!encoded.Ok) {
                    Fail(encoded.SqlState, encoded.Message);
                    return false;
                }
                line.push_back({encoded.Null, encoded.Bytes});
            }
            Rows.push_back(std::move(line));
        }
        return true;
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto code = ev->Get()->Record.GetStatus().GetCode();
        if (code == Ydb::StatusIds::OVERLOADED) {
            if (Overloads >= kOverloadRetries) {
                Fail("53000", StatusText(ev->Get()->Record));
                return;
            }
            ++Overloads;
            const ui64 delay = ev->Get()->Record.GetThrottleDelayMs();
            Schedule(TDuration::MilliSeconds(delay), new NActors::TEvents::TEvWakeup);
            return;
        }
        if (code == Ydb::StatusIds::INTERNAL_ERROR) {
            if (InternalErrors >= InternalErrorRetries) {
                Fail("XX000", StatusText(ev->Get()->Record));
                return;
            }
            ++InternalErrors;
            SendRead();
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
        if (!TakeRows(*ev->Get())) {
            return;
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(ReadId);
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), TabletId, false));
            return;
        }
        Succeed();
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        SendRead();
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        RetryResolve();
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
    const TString Text;
    const TSelectStmt Statement;
    const ui32 InternalErrorRetries;
    TString Path;
    TTableId Table;
    TVector<TColumn> Output;
    TVector<TString> KeyNames;
    TVector<NScheme::TTypeInfo> KeyTypes;
    TVector<TString> KeyBytes;
    TVector<TCell> KeyCells;
    TVector<TKeyCompare> Compares;
    TVector<TVector<TQueryCell>> Rows;
    ui64 TabletId = 0;
    ui64 ReadId = 0;
    ui32 InternalErrors = 0;
    ui32 ResolveErrors = 0;
    ui32 Overloads = 0;
    bool Sent = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries) {
    return new TSelectActor(replyTo, std::move(database), std::move(text), std::move(statement), internalErrorRetries);
}

} // namespace NKikimr::NSql
