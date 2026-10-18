#include "events.h"

#include <ydb/services/sql/codec/codec.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/mapfindptr.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NSql {

namespace {

TString TablePath(const TString& database, const TInsertStmt& statement) {
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

struct TPrepared {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

class TInsertSelectActor : public NActors::TActorBootstrapped<TInsertSelectActor> {
public:
    TInsertSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TInsertStmt statement, ui32 internalErrorRetries)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
    {
    }

    void Bootstrap() {
        if (!Statement.Source) {
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
        Become(&TInsertSelectActor::StateNavigate);
    }

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
        result->CommandTag = "INSERT 0 " + ToString(RowCount);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    bool Prepare(const TSysTables::TTableColumnInfo& info, TPrepared& column) const {
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

    bool Build(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        for (const auto& index : entry.Indexes) {
            const bool async = index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalAsync;
            const bool blocking = index.GetState() == NKikimrSchemeOp::EIndexStateReady
                || index.GetState() == NKikimrSchemeOp::EIndexStateWriteOnly;
            if (!async && blocking) {
                Fail("0A000", "not supported");
                return false;
            }
        }
        absl::flat_hash_map<TString, TPrepared> byName;
        TVector<TPrepared> ordered;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            TPrepared prepared;
            if (!Prepare(column, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            byName[column.Name] = prepared;
            ordered.push_back(prepared);
            if (prepared.KeyOrder >= 0) {
                Keys.push_back(prepared);
            }
        }
        std::sort(ordered.begin(), ordered.end(), [](const TPrepared& left, const TPrepared& right) {
            return left.Id < right.Id;
        });
        std::sort(Keys.begin(), Keys.end(), [](const TPrepared& left, const TPrepared& right) {
            return left.KeyOrder < right.KeyOrder;
        });
        if (Keys.empty()) {
            Fail("0A000", "not supported");
            return false;
        }
        if (Statement.Columns.empty()) {
            Targets = ordered;
        } else {
            for (const TIdentifier& name : Statement.Columns) {
                const TPrepared* column = MapFindPtr(byName, name.Name);
                if (!column) {
                    Fail("42703", name.Name);
                    return false;
                }
                Targets.push_back(*column);
            }
        }
        THashSet<TString> listed;
        for (const TPrepared& column : Targets) {
            listed.insert(column.Name);
        }
        for (const TPrepared& key : Keys) {
            if (!listed.contains(key.Name)) {
                Fail("23502", "null value in column " + key.Name);
                return false;
            }
        }
        WriteOrder = Keys;
        for (const TPrepared& column : Targets) {
            if (column.KeyOrder < 0) {
                WriteOrder.push_back(column);
            }
        }
        Table = entry.TableId;
        return true;
    }

    void Acquire() {
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));
        Become(&TInsertSelectActor::StateSnapshot);
    }

    void HandleSnapshot(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Fail("XX000", "internal error");
            return;
        }
        SnapshotStep = ev->Get()->Snapshot.Step;
        SnapshotTxId = ev->Get()->Snapshot.TxId;
        SnapshotHandle = std::move(ev->Get()->SnapshotHandle);
        Register(CreateSelectActor(SelfId(), Database, Text, *Statement.Source, InternalErrorRetries, std::make_pair(SnapshotStep, SnapshotTxId)));
        Become(&TInsertSelectActor::StateSelect);
    }

    bool DecodeRow(const TVector<TQueryCell>& line, TVector<TCell>& cells) {
        if (line.size() != Targets.size()) {
            Fail("42601", "column count");
            return false;
        }
        absl::flat_hash_map<TString, TCell> byName;
        for (size_t i = 0; i < Targets.size(); ++i) {
            TCell cell;
            if (line[i].Null) {
                cell = TCell();
            } else {
                const TCodecValue decoded = DecodeCell(Targets[i].Type.GetTypeId(), false, line[i].Text, EWireFormat::Text, Targets[i].Precision, Targets[i].Scale);
                if (!decoded.Ok) {
                    Fail(decoded.SqlState.empty() ? TString("22023") : decoded.SqlState, decoded.Message.empty() ? TString("bad request") : decoded.Message);
                    return false;
                }
                Storage.push_back(decoded.Bytes);
                cell = TCell(Storage.back().data(), Storage.back().size());
            }
            if (Targets[i].KeyOrder >= 0 && cell.IsNull()) {
                Fail("23502", "null value in column " + Targets[i].Name);
                return false;
            }
            byName[Targets[i].Name] = cell;
        }
        cells.clear();
        for (const TPrepared& column : WriteOrder) {
            cells.push_back(byName[column.Name]);
        }
        return true;
    }

    void HandleSelect(TEvQueryResult::TPtr& ev) {
        const TEvQueryResult& result = *ev->Get();
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("0A000") : result.SqlState, result.Message.empty() ? TString("not supported") : result.Message);
            return;
        }
        RowCount = result.Rows.size();
        if (RowCount == 0) {
            Succeed();
            return;
        }
        Storage.reserve(RowCount * Targets.size() + 8);
        Rows.reserve(RowCount);
        for (const TVector<TQueryCell>& line : result.Rows) {
            TVector<TCell> cells;
            if (!DecodeRow(line, cells)) {
                return;
            }
            Rows.push_back(std::move(cells));
        }
        Resolve();
    }

    void Resolve() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        TVector<NScheme::TTypeInfo> keyTypes;
        for (const TPrepared& key : Keys) {
            keyTypes.push_back(key.Type);
        }
        TVector<TKeyDesc::TColumnOp> columns;
        for (const TPrepared& column : WriteOrder) {
            TKeyDesc::TColumnOp operation;
            operation.Column = column.Id;
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = column.Type;
            columns.push_back(operation);
        }
        for (const TVector<TCell>& row : Rows) {
            TVector<TCell> key;
            key.insert(key.end(), row.begin(), row.begin() + Keys.size());
            KeyStorage.push_back(std::move(key));
            auto description = MakeHolder<TKeyDesc>(Table, TTableRange(KeyStorage.back()), TKeyDesc::ERowOperation::Read, keyTypes, columns);
            request->ResultSet.emplace_back(std::move(description));
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()));
        Become(&TInsertSelectActor::StateResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != Rows.size()) {
            Fail("42P01", Path);
            return;
        }
        absl::flat_hash_map<ui64, TVector<size_t>> groups;
        for (size_t i = 0; i < request->ResultSet.size(); ++i) {
            if (request->ResultSet[i].Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                Fail("42P01", Path);
                return;
            }
            const auto& partitions = request->ResultSet[i].KeyDescription->GetPartitions();
            if (partitions.size() != 1) {
                Fail("0A000", "not supported");
                return;
            }
            groups[partitions[0].ShardId].push_back(i);
        }
        for (auto& [tablet, indexes] : groups) {
            Groups.push_back({tablet, std::move(indexes)});
        }
        WriteNext();
    }

    void WriteNext() {
        if (NextGroup >= Groups.size()) {
            Succeed();
            return;
        }
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        Become(&TInsertSelectActor::StateAllocate);
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        const ui64 txId = ev->Get()->TxId;
        if (!txId) {
            Fail("XX000", "internal error");
            return;
        }
        const TGroup& group = Groups[NextGroup];
        std::vector<ui32> ids;
        for (const TPrepared& column : WriteOrder) {
            ids.push_back(column.Id);
        }
        TVector<TCell> cells;
        cells.reserve(group.Rows.size() * WriteOrder.size());
        for (const size_t index : group.Rows) {
            cells.insert(cells.end(), Rows[index].begin(), Rows[index].end());
        }
        TSerializedCellMatrix matrix(cells, static_cast<ui32>(group.Rows.size()), static_cast<ui16>(WriteOrder.size()));
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        event->Record.MutableMvccSnapshot()->SetStep(SnapshotStep);
        event->Record.MutableMvccSnapshot()->SetTxId(SnapshotTxId);
        const ui64 payload = NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*event).AddDataToPayload(matrix.ReleaseBuffer());
        event->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, Table, ids, payload, NKikimrDataEvents::FORMAT_CELLVEC);
        TabletId = group.TabletId;
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(event.release(), TabletId, true));
        Linked = true;
        Become(&TInsertSelectActor::StateWrite);
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto status = ev->Get()->Record.GetStatus();
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_COMPLETED:
                ++NextGroup;
                WriteNext();
                return;
            case EStatus::STATUS_CONSTRAINT_VIOLATION:
                Fail("23505", "duplicate key value violates unique constraint");
                return;
            case EStatus::STATUS_OVERLOADED:
                Fail("53000", "overloaded");
                return;
            case EStatus::STATUS_BAD_REQUEST:
                Fail("22023", "bad request");
                return;
            default:
                Fail("XX000", "internal error");
                return;
        }
    }

    void Unlink() {
        if (Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Linked = false;
        }
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
        Acquire();
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateSnapshot) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, HandleSnapshot);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateSelect) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQueryResult, HandleSelect);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateAllocate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleAllocate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateWrite) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvents::TDataEvents::TEvWriteResult, HandleWrite);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    struct TGroup {
        ui64 TabletId = 0;
        TVector<size_t> Rows;
    };

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TString Text;
    const TInsertStmt Statement;
    const ui32 InternalErrorRetries;
    TString Path;
    TTableId Table;
    TVector<TPrepared> Targets;
    TVector<TPrepared> Keys;
    TVector<TPrepared> WriteOrder;
    TVector<TString> Storage;
    TVector<TVector<TCell>> Rows;
    TVector<TVector<TCell>> KeyStorage;
    TVector<TGroup> Groups;
    size_t NextGroup = 0;
    ui32 RowCount = 0;
    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;
    NKqp::TSnapshotHandle SnapshotHandle;
    ui64 TabletId = 0;
    bool Linked = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateInsertSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TInsertStmt statement, ui32 internalErrorRetries) {
    return new TInsertSelectActor(replyTo, std::move(database), std::move(text), std::move(statement), internalErrorRetries);
}

} // namespace NKikimr::NSql
