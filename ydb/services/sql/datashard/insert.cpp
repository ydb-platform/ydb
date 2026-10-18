#include "events.h"

#include <ydb/services/sql/codec/codec.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/hash.h>
#include <util/string/cast.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NSql {

namespace {

constexpr ui32 kOverloadRetries = 16;

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

TString LiteralText(const Ydb::TypedValue& typed) {
    const auto& value = typed.value();
    switch (typed.type().type_id()) {
        case Ydb::Type::INT8:
        case Ydb::Type::INT16:
        case Ydb::Type::INT32:
        case Ydb::Type::UINT8:
        case Ydb::Type::UINT16:
            return ToString(value.int32_value());
        case Ydb::Type::INT64:
        case Ydb::Type::UINT32:
        case Ydb::Type::UINT64:
            return ToString(value.int64_value());
        case Ydb::Type::UTF8:
        case Ydb::Type::JSON:
        case Ydb::Type::JSON_DOCUMENT:
            return value.text_value();
        case Ydb::Type::STRING:
            return value.bytes_value();
        default:
            return {};
    }
}

struct TPrepared {
    TString Name;
    ui32 Id = 0;
    i32 KeyOrder = -1;
    bool NotNull = false;
    NScheme::TTypeInfo Type;
    ui8 Precision = 0;
    ui8 Scale = 0;
};

class TInsertActor : public NActors::TActorBootstrapped<TInsertActor> {
public:
    TInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries)
        : ReplyTo(replyTo)
        , Database(std::move(database))
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
        request->ResultSet.push_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TInsertActor::StateNavigate);
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
        result->CommandTag = "INSERT 0 1";
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

    bool Prepare(const TSysTables::TTableColumnInfo& info, TPrepared& column) const {
        column.Name = info.Name;
        column.Id = info.Id;
        column.KeyOrder = info.KeyOrder;
        column.NotNull = info.IsNotNullColumn;
        column.Type = info.PType;
        if (info.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
            column.Precision = static_cast<ui8>(info.PType.GetDecimalType().GetPrecision());
            column.Scale = static_cast<ui8>(info.PType.GetDecimalType().GetScale());
        }
        const TCodecValue probe = EncodeCell(column.Type.GetTypeId(), true, {}, EWireFormat::Text, column.Precision, column.Scale);
        return probe.Ok;
    }

    bool Decode(const TPrepared& column, const TLiteral& literal, TCell& cell) {
        if (literal.Kind == TLiteral::EKind::Null) {
            if (column.NotNull) {
                Fail("23502", "null value in column " + column.Name);
                return false;
            }
            cell = TCell();
            return true;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("22023") : decoded.SqlState, decoded.Message.empty() ? TString("bad request") : decoded.Message);
            return false;
        }
        Storage.push_back(decoded.Bytes);
        cell = TCell(Storage.back().data(), Storage.back().size());
        return true;
    }

    bool Build(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        for (const auto& index : entry.Indexes) {
            const auto type = index.GetType();
            const auto state = index.GetState();
            const bool async = type == NKikimrSchemeOp::EIndexTypeGlobalAsync;
            const bool blocking = state == NKikimrSchemeOp::EIndexStateReady || state == NKikimrSchemeOp::EIndexStateWriteOnly;
            if (!async && blocking) {
                Fail("0A000", "not supported");
                return false;
            }
        }

        THashMap<TString, const TLiteral*> given;
        if (Statement.Columns.empty()) {
            TVector<const TSysTables::TTableColumnInfo*> ordered;
            for (const auto& [id, column] : entry.Columns) {
                Y_UNUSED(id);
                ordered.push_back(&column);
            }
            std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
                return left->Id < right->Id;
            });
            if (ordered.size() != Statement.Values.size()) {
                Fail("42601", "column count");
                return false;
            }
            for (size_t i = 0; i < ordered.size(); ++i) {
                given[ordered[i]->Name] = &Statement.Values[i];
            }
        } else {
            if (Statement.Columns.size() != Statement.Values.size()) {
                Fail("42601", "column count");
                return false;
            }
            for (size_t i = 0; i < Statement.Columns.size(); ++i) {
                if (given.contains(Statement.Columns[i].Name)) {
                    Fail("42701", Statement.Columns[i].Name);
                    return false;
                }
                if (!FindColumn(entry, Statement.Columns[i].Name)) {
                    Fail("42703", Statement.Columns[i].Name);
                    return false;
                }
                given[Statement.Columns[i].Name] = &Statement.Values[i];
            }
        }

        struct TSlot {
            TPrepared Column;
            TCell Cell;
        };
        TVector<TSlot> slots;
        Storage.clear();
        Storage.reserve(Statement.Values.size() + entry.Columns.size());
        KeyTypes.clear();
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

        auto take = [&](const TSysTables::TTableColumnInfo& info, const TLiteral* literal) -> bool {
            TPrepared prepared;
            if (!Prepare(info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            TCell cell;
            if (literal) {
                if (!Decode(prepared, *literal, cell)) {
                    return false;
                }
            } else if (info.IsDefaultFromLiteral()) {
                const auto typeId = info.PType.GetTypeId();
                const bool text = typeId == NScheme::NTypeIds::Utf8 || typeId == NScheme::NTypeIds::Json
                    || typeId == NScheme::NTypeIds::JsonDocument || typeId == NScheme::NTypeIds::String;
                TLiteral fallback{text ? TLiteral::EKind::String : TLiteral::EKind::Integer, LiteralText(info.DefaultFromLiteral)};
                if (fallback.Text.empty() && !text) {
                    Fail("0A000", "not supported");
                    return false;
                }
                if (!Decode(prepared, fallback, cell)) {
                    return false;
                }
            } else if (info.IsDefaultFromSequence() || info.IsDefaultFromExpression()) {
                Fail("0A000", "not supported");
                return false;
            } else if (prepared.NotNull) {
                Fail("23502", "null value in column " + prepared.Name);
                return false;
            } else {
                return true;
            }
            if (prepared.KeyOrder >= 0) {
                KeyTypes.push_back(prepared.Type);
            }
            slots.push_back({prepared, cell});
            return true;
        };

        for (const auto* info : keys) {
            const auto* literal = given.FindPtr(info->Name);
            if (!take(*info, literal ? *literal : nullptr)) {
                return false;
            }
            given.erase(info->Name);
        }
        TVector<const TSysTables::TTableColumnInfo*> rest;
        for (const auto& [name, literal] : given) {
            Y_UNUSED(literal);
            const auto* info = FindColumn(entry, name);
            if (!info) {
                Fail("42703", name);
                return false;
            }
            rest.push_back(info);
        }
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.KeyOrder < 0 && !given.contains(column.Name) && (column.IsDefaultFromLiteral() || column.IsNotNullColumn)) {
                rest.push_back(&column);
            }
        }
        std::sort(rest.begin(), rest.end(), [](const auto* left, const auto* right) {
            return left->Id < right->Id;
        });
        rest.erase(std::unique(rest.begin(), rest.end()), rest.end());
        for (const auto* info : rest) {
            const auto* literal = given.FindPtr(info->Name);
            if (!take(*info, literal ? *literal : nullptr)) {
                return false;
            }
        }

        std::sort(slots.begin(), slots.end(), [](const TSlot& left, const TSlot& right) {
            const bool leftKey = left.Column.KeyOrder >= 0;
            const bool rightKey = right.Column.KeyOrder >= 0;
            if (leftKey != rightKey) {
                return leftKey;
            }
            if (leftKey) {
                return left.Column.KeyOrder < right.Column.KeyOrder;
            }
            return left.Column.Id < right.Column.Id;
        });
        TVector<TCell> cells;
        ColumnIds.clear();
        ColumnTypes.clear();
        KeyCells.clear();
        for (const TSlot& slot : slots) {
            ColumnIds.push_back(slot.Column.Id);
            ColumnTypes.push_back(slot.Column.Type);
            cells.push_back(slot.Cell);
            if (slot.Column.KeyOrder >= 0) {
                KeyCells.push_back(slot.Cell);
            }
        }
        if (KeyCells.size() != keys.size()) {
            Fail("23502", "null value in column");
            return false;
        }
        TSerializedCellMatrix matrix(cells, 1, static_cast<ui16>(cells.size()));
        Payload = matrix.ReleaseBuffer();
        Table = entry.TableId;
        return true;
    }

    void Resolve() {
        TVector<TKeyDesc::TColumnOp> columns;
        for (size_t i = 0; i < ColumnIds.size(); ++i) {
            TKeyDesc::TColumnOp operation;
            operation.Column = ColumnIds[i];
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = ColumnTypes[i];
            columns.push_back(operation);
        }
        auto description = MakeHolder<TKeyDesc>(Table, TTableRange(KeyCells), TKeyDesc::ERowOperation::Read, KeyTypes, columns);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(description));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()));
        Become(&TInsertActor::StateResolve);
    }

    void Allocate() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        Become(&TInsertActor::StateAllocate);
    }

    void Write() {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        const ui64 index = NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*event).AddDataToPayload(TString(Payload));
        event->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, Table, ColumnIds, index, NKikimrDataEvents::FORMAT_CELLVEC);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(event.release(), TabletId, true));
        Linked = true;
        Become(&TInsertActor::StateWrite);
    }

    void Unlink() {
        if (Linked) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
            Linked = false;
        }
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request->ResultSet;
        if (result.empty() || result[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = result[0];
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
        Resolve();
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
        Allocate();
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        if (!TxId) {
            Fail("XX000", "internal error");
            return;
        }
        Write();
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto status = ev->Get()->Record.GetStatus();
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_COMPLETED:
                Succeed();
                return;
            case EStatus::STATUS_OVERLOADED:
                if (OverloadRetries >= kOverloadRetries) {
                    Fail("53000", "overloaded");
                    return;
                }
                ++OverloadRetries;
                Schedule(TDuration::MilliSeconds(50), new NActors::TEvents::TEvWakeup());
                return;
            case EStatus::STATUS_INTERNAL_ERROR:
                if (InternalRetries >= InternalErrorRetries) {
                    Fail("XX000", "internal error");
                    return;
                }
                ++InternalRetries;
                Write();
                return;
            case EStatus::STATUS_CONSTRAINT_VIOLATION:
                Fail("23505", "duplicate key value violates unique constraint");
                return;
            case EStatus::STATUS_BAD_REQUEST:
                Fail("22023", "bad request");
                return;
            case EStatus::STATUS_SCHEME_CHANGED:
                Fail("42P01", Path);
                return;
            default:
                Fail("XX000", "internal error");
                return;
        }
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        Write();
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
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TInsertStmt Statement;
    const ui32 InternalErrorRetries;
    TString Path;
    TTableId Table;
    ui64 TabletId = 0;
    ui64 TxId = 0;
    std::vector<ui32> ColumnIds;
    TVector<NScheme::TTypeInfo> ColumnTypes;
    TString Payload;
    TVector<TString> Storage;
    TVector<TCell> KeyCells;
    TVector<NScheme::TTypeInfo> KeyTypes;
    bool Linked = false;
    bool Replied = false;
    ui32 OverloadRetries = 0;
    ui32 InternalRetries = 0;
};

} // namespace

NActors::IActor* CreateInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries) {
    return new TInsertActor(replyTo, std::move(database), std::move(statement), internalErrorRetries);
}

} // namespace NKikimr::NSql
