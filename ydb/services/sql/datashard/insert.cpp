#include "events.h"

#include <ydb/services/sql/codec/codec.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_order.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/mapfindptr.h>

#include <util/generic/hash_set.h>
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
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::JsonDocument:
            return TPgWire{114, -1};
        default:
            return std::nullopt;
    }
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
    TInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , InternalErrorRetries(internalErrorRetries)
        , Defer(defer)
    {
    }

    struct TIndexPlan {
        TString Name;
        TString ImplPath;
        TVector<TString> Keys;
        TVector<TString> Data;
        bool Unique = false;
    };

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

    void Succeed(ui32 count) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "INSERT 0 " + ToString(count);
        if (!ResultColumns.empty()) {
            result->Columns = ResultColumns;
            if (count > 0) {
                result->Rows = ResultRows;
            }
        }
        if (HoldWrite && count > 0) {
            result->Deferred.push_back(MainWrite());
            for (TDeferredWrite& write : IndexWrites) {
                result->Deferred.push_back(std::move(write));
            }
        }
        Send(ReplyTo, result.Release());
        PassAway();
    }

    bool PrepareReturning() {
        ResultColumns.clear();
        if (Statement.Returning.empty()) {
            return true;
        }
        TVector<TPrepared> ordered;
        for (const auto& [name, column] : PreparedColumns) {
            Y_UNUSED(name);
            ordered.push_back(column);
        }
        std::sort(ordered.begin(), ordered.end(), [](const TPrepared& left, const TPrepared& right) {
            return left.Id < right.Id;
        });
        auto add = [&](const TPrepared& column, const TString& name) -> bool {
            const auto wire = PgWire(column.Type.GetTypeId());
            if (!wire) {
                Fail("0A000", "not supported");
                return false;
            }
            ResultColumns.push_back({name, wire->Oid, wire->Size});
            ResultSources.push_back(column);
            return true;
        };
        for (const TReturningItem& item : Statement.Returning) {
            if (item.Star) {
                for (const TPrepared& column : ordered) {
                    if (!add(column, column.Name)) {
                        return false;
                    }
                }
                continue;
            }
            const TPrepared* column = MapFindPtr(PreparedColumns, item.Column.Name);
            if (!column) {
                Fail("42703", item.Column.Name);
                return false;
            }
            if (!add(*column, item.Alias ? item.Alias->Name : column->Name)) {
                return false;
            }
        }
        return true;
    }

    bool RememberImage(const absl::flat_hash_map<TString, TCell>& image) {
        if (Statement.Returning.empty()) {
            return true;
        }
        TVector<TQueryCell> row;
        for (const TPrepared& column : ResultSources) {
            const TCell* cell = MapFindPtr(image, column.Name);
            const bool null = !cell || cell->IsNull();
            const TStringBuf bytes = null ? TStringBuf() : cell->AsBuf();
            const TCodecValue encoded = EncodeCell(column.Type.GetTypeId(), null, bytes, EWireFormat::Text, column.Precision, column.Scale);
            if (!encoded.Ok) {
                Fail(encoded.SqlState.empty() ? TString("0A000") : encoded.SqlState, encoded.Message.empty() ? TString("not supported") : encoded.Message);
                return false;
            }
            row.push_back({encoded.Null, encoded.Bytes});
        }
        ResultRows = {std::move(row)};
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
            plan.Unique = type == NKikimrSchemeOp::EIndexTypeGlobalUnique;
            IndexPlans.push_back(std::move(plan));
        }

        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            TPrepared prepared;
            if (!Prepare(column, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            PreparedColumns[column.Name] = prepared;
        }

        absl::flat_hash_map<TString, const TLiteral*> given;
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
        Storage.reserve(Statement.Values.size() + entry.Columns.size() + 8);
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
        for (const auto* info : keys) {
            PkNames.push_back(info->Name);
        }
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
            Proposed[prepared.Name] = cell;
            PreparedColumns[prepared.Name] = prepared;
            return true;
        };

        if (Statement.Conflict != EConflictAction::None) {
            THashSet<TString> target;
            for (const TIdentifier& name : Statement.ConflictTarget) {
                target.insert(name.Name);
            }
            bool primary = target.size() == keys.size();
            for (const auto* info : keys) {
                primary = primary && target.contains(info->Name);
            }
            if (!primary) {
                for (size_t i = 0; i < IndexPlans.size(); ++i) {
                    const TIndexPlan& plan = IndexPlans[i];
                    if (!plan.Unique || plan.Keys.size() != Statement.ConflictTarget.size()) {
                        continue;
                    }
                    bool match = true;
                    for (size_t column = 0; column < plan.Keys.size(); ++column) {
                        match = match && plan.Keys[column] == Statement.ConflictTarget[column].Name;
                    }
                    if (match) {
                        Arbiter = static_cast<int>(i);
                    }
                }
                if (Arbiter < 0) {
                    Fail("42P10", "invalid column reference");
                    return false;
                }
            }
        }
        if (Statement.ConflictWhere && !Statement.ConflictWhere->Excluded) {
            const auto* info = FindColumn(entry, Statement.ConflictWhere->Column.Name);
            if (!info) {
                Fail("42703", Statement.ConflictWhere->Column.Name);
                return false;
            }
            TPrepared prepared;
            if (!Prepare(*info, prepared)) {
                Fail("0A000", "not supported");
                return false;
            }
            PreparedColumns[prepared.Name] = prepared;
        }

        for (const auto* info : keys) {
            const auto* literal = MapFindPtr(given, info->Name);
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
            const auto* literal = MapFindPtr(given, info->Name);
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
        InsertPayload = Payload;
        InsertColumnIds = ColumnIds;
        Table = entry.TableId;
        ReadColumns.clear();
        for (const auto* info : keys) {
            const auto* prepared = MapFindPtr(PreparedColumns, info->Name);
            if (!prepared) {
                Fail("0A000", "not supported");
                return false;
            }
            ReadColumns.push_back(*prepared);
        }
        if (Statement.ConflictWhere && !Statement.ConflictWhere->Excluded) {
            bool listed = false;
            for (const TPrepared& column : ReadColumns) {
                if (column.Name == Statement.ConflictWhere->Column.Name) {
                    listed = true;
                }
            }
            if (!listed) {
                ReadColumns.push_back(PreparedColumns[Statement.ConflictWhere->Column.Name]);
            }
        }
        if (Statement.Conflict != EConflictAction::None) {
            for (const TIndexPlan& plan : IndexPlans) {
                auto ensure = [&](const TString& name) {
                    for (const TPrepared& column : ReadColumns) {
                        if (column.Name == name) {
                            return;
                        }
                    }
                    if (const TPrepared* column = MapFindPtr(PreparedColumns, name)) {
                        ReadColumns.push_back(*column);
                    }
                };
                for (const TString& name : plan.Keys) {
                    ensure(name);
                }
                for (const TString& name : plan.Data) {
                    ensure(name);
                }
            }
        }
        if (Statement.Conflict == EConflictAction::Update && !Statement.Returning.empty()) {
            ReadColumns.clear();
            for (const auto& [name, column] : PreparedColumns) {
                Y_UNUSED(name);
                ReadColumns.push_back(column);
            }
            std::sort(ReadColumns.begin(), ReadColumns.end(), [](const TPrepared& left, const TPrepared& right) {
                return left.Id < right.Id;
            });
        }
        return PrepareReturning();
    }

    void UseInsert() {
        Payload = InsertPayload;
        ColumnIds = InsertColumnIds;
        Operation = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
    }

    bool WhereHolds(const absl::flat_hash_map<TString, TCell>& existing) {
        if (!Statement.ConflictWhere) {
            return true;
        }
        const TConflictWhere& where = *Statement.ConflictWhere;
        const TPrepared* column = MapFindPtr(PreparedColumns, where.Column.Name);
        if (!column) {
            Fail("42703", where.Column.Name);
            return false;
        }
        TCell left;
        if (where.Excluded) {
            if (const TCell* proposed = MapFindPtr(Proposed, where.Column.Name)) {
                left = *proposed;
            }
        } else if (const TCell* found = MapFindPtr(existing, where.Column.Name)) {
            left = *found;
        }
        if (left.IsNull() || where.Value.Kind == TLiteral::EKind::Null) {
            return false;
        }
        TCell right;
        if (!Decode(*column, where.Value, right)) {
            return false;
        }
        const int compared = CompareTypedCells(left, right, NScheme::TTypeInfoOrder(column->Type));
        switch (where.Op) {
            case ECompareOp::Eq:
                return compared == 0;
            case ECompareOp::NotEq:
                return compared != 0;
            case ECompareOp::Lt:
                return compared < 0;
            case ECompareOp::Gt:
                return compared > 0;
            case ECompareOp::Le:
                return compared <= 0;
            case ECompareOp::Ge:
                return compared >= 0;
        }
        return false;
    }

    bool BuildUpdate() {
        TVector<TCell> cells;
        std::vector<ui32> ids;
        TVector<NScheme::TTypeInfo> types;
        for (const TPrepared& column : ReadColumns) {
            if (column.KeyOrder < 0) {
                continue;
            }
            const TCell* key = MapFindPtr(Arbiter >= 0 ? FoundCells : Proposed, column.Name);
            if (!key || key->IsNull()) {
                Fail("23502", "null value in column " + column.Name);
                return false;
            }
            ids.push_back(column.Id);
            types.push_back(column.Type);
            cells.push_back(*key);
        }
        if (Statement.Update.empty()) {
            Fail("0A000", "not supported");
            return false;
        }
        for (const TUpdateAssign& assign : Statement.Update) {
            const TPrepared* column = MapFindPtr(PreparedColumns, assign.Column.Name);
            if (!column) {
                Fail("42703", assign.Column.Name);
                return false;
            }
            if (column->KeyOrder >= 0) {
                Fail("0A000", "not supported");
                return false;
            }
            TCell cell;
            if (assign.Excluded) {
                if (!PreparedColumns.contains(assign.ExcludedColumn.Name)) {
                    Fail("42703", assign.ExcludedColumn.Name);
                    return false;
                }
                if (const TCell* proposed = MapFindPtr(Proposed, assign.ExcludedColumn.Name)) {
                    cell = *proposed;
                }
            } else if (!Decode(*column, assign.Value, cell)) {
                return false;
            }
            ids.push_back(column->Id);
            types.push_back(column->Type);
            cells.push_back(cell);
            Assigned[column->Name] = cell;
        }
        TSerializedCellMatrix matrix(cells, 1, static_cast<ui16>(cells.size()));
        Payload = matrix.ReleaseBuffer();
        ColumnIds = std::move(ids);
        ColumnTypes = std::move(types);
        Operation = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE;
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
        if (Defer) {
            HoldWrite = true;
            Succeed(1);
            return;
        }
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        Become(&TInsertActor::StateAllocate);
    }

    void Write() {
        auto event = std::make_unique<NEvents::TDataEvents::TEvWrite>(TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        const ui64 index = NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*event).AddDataToPayload(TString(Payload));
        Reading = false;
        event->AddOperation(Operation, Table, ColumnIds, index, NKikimrDataEvents::FORMAT_CELLVEC);
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
        if (ArbiterResolve) {
            ArbiterResolve = false;
            ArbiterTablet = TabletId;
            ReadArbiter();
            return;
        }
        if (UpdatingResolve) {
            UpdatingResolve = false;
            IndexWrites[PendingPos].TabletId = TabletId;
            ++PendingPos;
            ResolvePending();
            return;
        }
        if (IndexResolve) {
            IndexWrites.back().TabletId = partitions[0].ShardId;
            if (IndexPlans[IndexPos].Unique) {
                ReadUnique();
                return;
            }
            ++IndexPos;
            StartIndex();
            return;
        }
        MainTabletId = TabletId;
        if (Statement.Conflict != EConflictAction::None && Arbiter >= 0 && !ArbiterDone) {
            OpenArbiter();
            return;
        }
        if (Statement.Conflict == EConflictAction::None) {
            UseInsert();
            if (!RememberImage(Proposed)) {
                return;
            }
            if (!IndexPlans.empty()) {
                IndexPos = 0;
                StartIndex();
                return;
            }
            Allocate();
            return;
        }
        Read();
    }

    void Read() {
        Reading = true;
        Found = false;
        FoundCells.clear();
        FoundStorage.clear();
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        request->Record.SetReadId(++ReadId);
        request->Record.MutableTableId()->SetOwnerId(Table.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(Table.PathId.LocalPathId);
        request->Record.MutableTableId()->SetSchemaVersion(Table.SchemaVersion);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (const TPrepared& column : ReadColumns) {
            request->Record.AddColumns(column.Id);
        }
        request->Keys.push_back(TSerializedCellVec(KeyCells));
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), TabletId, true));
        Linked = true;
        Become(&TInsertActor::StateRead);
    }

    void ApplyFound() {
        if (!Found) {
            UseInsert();
            if (!RememberImage(Proposed)) {
                return;
            }
            if (!IndexPlans.empty()) {
                IndexPos = 0;
                StartIndex();
                return;
            }
            Allocate();
            return;
        }
        if (Statement.Conflict == EConflictAction::Nothing || !WhereHolds(FoundCells)) {
            Succeed(0);
            return;
        }
        if (!BuildUpdate()) {
            return;
        }
        absl::flat_hash_map<TString, TCell> image = FoundCells;
        for (const auto& [name, cell] : Assigned) {
            image[name] = cell;
        }
        if (!RememberImage(image)) {
            return;
        }
        if (IndexPlans.empty() || !IndexKeyChanged()) {
            Allocate();
            return;
        }
        IndexPos = 0;
        IndexWrites.clear();
        PendingKeys.clear();
        PendingTypes.clear();
        IndexByteStorage.clear();
        IndexByteStorage.reserve(64);
        UpdatingIndexes = true;
        StartIndexUpdate();
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        if (!TxId) {
            Fail("XX000", "internal error");
            return;
        }
        if (IndexWrites.empty()) {
            Write();
            return;
        }
        TVector<TDeferredWrite> writes;
        writes.push_back(MainWrite());
        for (const TDeferredWrite& write : IndexWrites) {
            writes.push_back(write);
        }
        Become(&TInsertActor::StateCommit);
        Register(CreateCommitActor(SelfId(), TxId, std::move(writes), {}, InternalErrorRetries));
    }

    void HandleCommit(TEvQueryResult::TPtr& ev) {
        const TEvQueryResult& result = *ev->Get();
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("XX000") : result.SqlState,
                result.Message.empty() ? TString("internal error") : result.Message);
            return;
        }
        Succeed(1);
    }

    void HandleWrite(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto status = ev->Get()->Record.GetStatus();
        using EStatus = NKikimrDataEvents::TEvWriteResult;
        switch (status) {
            case EStatus::STATUS_COMPLETED:
                Succeed(1);
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
                if (Statement.Conflict == EConflictAction::Nothing) {
                    Succeed(0);
                    return;
                }
                if (Statement.Conflict == EConflictAction::Update && Operation == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT) {
                    if (Reread) {
                        Fail("23505", "duplicate key value violates unique constraint");
                        return;
                    }
                    Reread = true;
                    Read();
                    return;
                }
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
        if (Reading) {
            Read();
            return;
        }
        Write();
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
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
            Read();
            return;
        }
        if (code != Ydb::StatusIds::SUCCESS) {
            Fail(code == Ydb::StatusIds::SCHEME_ERROR ? TString("42P01") : TString("XX000"), Path);
            return;
        }
        auto& result = *ev->Get();
        if (result.GetRowsCount() > 0) {
            const TConstArrayRef<TCell> cells = result.GetCells(0);
            if (cells.size() != ReadColumns.size()) {
                Fail("XX000", "internal error");
                return;
            }
            Found = true;
            for (size_t i = 0; i < cells.size(); ++i) {
                if (cells[i].IsNull()) {
                    FoundCells[ReadColumns[i].Name] = TCell();
                    continue;
                }
                FoundStorage.push_back(TString(cells[i].AsBuf()));
                FoundCells[ReadColumns[i].Name] = TCell(FoundStorage.back().data(), FoundStorage.back().size());
            }
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(ev->Get()->Record.GetReadId());
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), TabletId, false));
            return;
        }
        ApplyFound();
    }

    TDeferredWrite MainWrite() const {
        TDeferredWrite write;
        write.OwnerId = Table.PathId.OwnerId;
        write.LocalPathId = Table.PathId.LocalPathId;
        write.SchemaVersion = Table.SchemaVersion;
        write.TabletId = MainTabletId;
        write.Operation = static_cast<ui32>(Operation);
        write.ColumnIds = ColumnIds;
        write.Payload = Payload;
        return write;
    }

    void ReadUnique() {
        const TIndexPlan& plan = IndexPlans[IndexPos];
        UniqueFrom.clear();
        UniqueTo.clear();
        for (size_t i = 0; i < IndexKeyCells.size(); ++i) {
            if (i < plan.Keys.size()) {
                UniqueFrom.push_back(IndexKeyCells[i]);
                UniqueTo.push_back(IndexKeyCells[i]);
            } else {
                UniqueFrom.emplace_back();
            }
        }
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        request->Record.SetReadId(++ReadId);
        request->Record.MutableTableId()->SetOwnerId(IndexTable.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(IndexTable.PathId.LocalPathId);
        request->Record.MutableTableId()->SetSchemaVersion(IndexTable.SchemaVersion);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        request->Record.AddColumns(IndexWrites.back().ColumnIds.front());
        if (plan.Keys.size() == IndexKeyCells.size()) {
            request->Keys.push_back(TSerializedCellVec(IndexKeyCells));
        } else {
            request->Ranges.emplace_back(UniqueFrom, true, UniqueTo, true);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), IndexWrites.back().TabletId, true));
        Linked = true;
        Become(&TInsertActor::StateUnique);
    }

    void HandleUnique(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto code = ev->Get()->Record.GetStatus().GetCode();
        if (code != Ydb::StatusIds::SUCCESS) {
            Fail(code == Ydb::StatusIds::SCHEME_ERROR ? TString("42P01") : TString("XX000"), IndexPlans[IndexPos].Name);
            return;
        }
        if (ev->Get()->GetRowsCount() > 0) {
            Fail("23505", "duplicate key");
            return;
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(ev->Get()->Record.GetReadId());
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), IndexWrites.back().TabletId, false));
            return;
        }
        ++IndexPos;
        StartIndex();
    }

    void FinishIndexes() {
        IndexResolve = false;
        if (Defer) {
            HoldWrite = true;
            Succeed(1);
            return;
        }
        Allocate();
    }

    bool SameCell(const TCell* left, const TCell* right) const {
        if (!left || !right || left->IsNull() || right->IsNull()) {
            return (!left || left->IsNull()) && (!right || right->IsNull());
        }
        return left->AsBuf() == right->AsBuf();
    }

    const TCell* NewCell(const TString& name) const {
        if (const TCell* assigned = MapFindPtr(Assigned, name)) {
            return assigned;
        }
        return MapFindPtr(FoundCells, name);
    }

    bool IndexKeyChanged() const {
        for (const TIndexPlan& plan : IndexPlans) {
            for (const TString& name : plan.Keys) {
                if (!SameCell(MapFindPtr(FoundCells, name), NewCell(name))) {
                    return true;
                }
            }
        }
        return false;
    }

    void OpenArbiter() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(IndexPlans[Arbiter].ImplPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TInsertActor::StateArbiterNavigate);
    }

    void HandleArbiterNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || request->ResultSet[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindTable)
        {
            Fail("42P01", IndexPlans[Arbiter].ImplPath);
            return;
        }
        const auto& entry = request->ResultSet[0];
        const TIndexPlan& plan = IndexPlans[Arbiter];
        ArbiterNames = plan.Keys;
        for (const TString& name : PkNames) {
            bool present = false;
            for (const TString& key : plan.Keys) {
                present = present || key == name;
            }
            if (!present) {
                ArbiterNames.push_back(name);
            }
        }
        ArbiterIds.clear();
        ArbiterTypes.clear();
        ArbiterFrom.clear();
        ArbiterTo.clear();
        bool nullKey = false;
        for (size_t i = 0; i < ArbiterNames.size(); ++i) {
            const auto* info = FindColumn(entry, ArbiterNames[i]);
            if (!info) {
                Fail("42703", ArbiterNames[i]);
                return;
            }
            ArbiterIds.push_back(info->Id);
            ArbiterTypes.push_back(info->PType);
            if (i < plan.Keys.size()) {
                const TCell* proposed = MapFindPtr(Proposed, ArbiterNames[i]);
                if (!proposed || proposed->IsNull()) {
                    nullKey = true;
                    ArbiterFrom.emplace_back();
                    ArbiterTo.emplace_back();
                } else {
                    ArbiterFrom.push_back(*proposed);
                    ArbiterTo.push_back(*proposed);
                }
            }
        }
        ArbiterTable = entry.TableId;
        if (nullKey) {
            ArbiterDone = true;
            TabletId = MainTabletId;
            UseInsert();
            if (!RememberImage(Proposed)) {
                return;
            }
            IndexPos = 0;
            StartIndex();
            return;
        }
        TVector<TCell> from = ArbiterFrom;
        for (size_t i = plan.Keys.size(); i < ArbiterNames.size(); ++i) {
            from.emplace_back();
        }
        ArbiterFrom = std::move(from);
        TVector<TKeyDesc::TColumnOp> columns;
        for (size_t i = 0; i < ArbiterIds.size(); ++i) {
            TKeyDesc::TColumnOp operation;
            operation.Column = ArbiterIds[i];
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = ArbiterTypes[i];
            columns.push_back(operation);
        }
        const bool point = plan.Keys.size() == ArbiterNames.size();
        auto description = MakeHolder<TKeyDesc>(
            ArbiterTable,
            point ? TTableRange(ArbiterFrom) : TTableRange(ArbiterFrom, true, ArbiterTo, true),
            TKeyDesc::ERowOperation::Read,
            ArbiterTypes,
            columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        ArbiterResolve = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TInsertActor::StateResolve);
    }

    void ReadArbiter() {
        auto request = MakeHolder<TEvDataShard::TEvRead>();
        request->Record.SetReadId(++ReadId);
        request->Record.MutableTableId()->SetOwnerId(ArbiterTable.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(ArbiterTable.PathId.LocalPathId);
        request->Record.MutableTableId()->SetSchemaVersion(ArbiterTable.SchemaVersion);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (ui32 id : ArbiterIds) {
            request->Record.AddColumns(id);
        }
        if (IndexPlans[Arbiter].Keys.size() == ArbiterNames.size()) {
            request->Keys.push_back(TSerializedCellVec(ArbiterFrom));
        } else {
            request->Ranges.emplace_back(ArbiterFrom, true, ArbiterTo, true);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), ArbiterTablet, true));
        Linked = true;
        Become(&TInsertActor::StateArbiter);
    }

    void MissArbiter() {
        Unlink();
        ArbiterDone = true;
        TabletId = MainTabletId;
        UseInsert();
        if (!RememberImage(Proposed)) {
            return;
        }
        if (!IndexPlans.empty()) {
            IndexPos = 0;
            StartIndex();
            return;
        }
        Allocate();
    }

    void HandleArbiter(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto code = ev->Get()->Record.GetStatus().GetCode();
        if (code != Ydb::StatusIds::SUCCESS) {
            Fail(code == Ydb::StatusIds::SCHEME_ERROR ? TString("42P01") : TString("XX000"), IndexPlans[Arbiter].Name);
            return;
        }
        if (ev->Get()->GetRowsCount() > 0) {
            const TConstArrayRef<TCell> cells = ev->Get()->GetCells(0);
            if (cells.size() != ArbiterNames.size()) {
                Fail("XX000", "internal error");
                return;
            }
            ArbiterStorage.clear();
            ArbiterStorage.reserve(cells.size());
            absl::flat_hash_map<TString, TCell> byName;
            for (size_t i = 0; i < cells.size(); ++i) {
                if (cells[i].IsNull()) {
                    byName[ArbiterNames[i]] = TCell();
                    continue;
                }
                ArbiterStorage.push_back(TString(cells[i].AsBuf()));
                byName[ArbiterNames[i]] = TCell(ArbiterStorage.back().data(), ArbiterStorage.back().size());
            }
            KeyCells.clear();
            for (const TString& name : PkNames) {
                const TCell* cell = MapFindPtr(byName, name);
                if (!cell || cell->IsNull()) {
                    Fail("XX000", "internal error");
                    return;
                }
                KeyCells.push_back(*cell);
            }
            Unlink();
            ArbiterDone = true;
            Resolve();
            return;
        }
        if (!ev->Get()->Record.GetFinished()) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(ev->Get()->Record.GetReadId());
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ack.Release(), ArbiterTablet, false));
            return;
        }
        MissArbiter();
    }

    bool AddImplOp(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TIndexPlan& plan, bool newest, ui32 operation) {
        TVector<TString> names = plan.Keys;
        for (const TString& name : PkNames) {
            bool present = false;
            for (const TString& key : plan.Keys) {
                present = present || key == name;
            }
            if (!present) {
                names.push_back(name);
            }
        }
        const size_t keyCount = names.size();
        if (operation == static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT)) {
            for (const TString& column : plan.Data) {
                bool present = false;
                for (const TString& name : names) {
                    present = present || name == column;
                }
                if (!present) {
                    names.push_back(column);
                }
            }
        }
        TVector<TCell> cells;
        std::vector<ui32> ids;
        TVector<TCell> key;
        TVector<NScheme::TTypeInfo> keyTypes;
        for (size_t i = 0; i < names.size(); ++i) {
            const auto* info = FindColumn(entry, names[i]);
            if (!info) {
                Fail("42703", names[i]);
                return false;
            }
            const TCell* source = newest ? NewCell(names[i]) : MapFindPtr(FoundCells, names[i]);
            TCell cell;
            if (!source || source->IsNull()) {
                cell = TCell();
            } else {
                IndexByteStorage.push_back(TString(source->AsBuf()));
                cell = TCell(IndexByteStorage.back().data(), IndexByteStorage.back().size());
            }
            cells.push_back(cell);
            ids.push_back(info->Id);
            if (i < keyCount) {
                key.push_back(cell);
                keyTypes.push_back(info->PType);
            }
        }
        TSerializedCellMatrix matrix(cells, 1, static_cast<ui16>(cells.size()));
        TDeferredWrite write;
        write.OwnerId = entry.TableId.PathId.OwnerId;
        write.LocalPathId = entry.TableId.PathId.LocalPathId;
        write.SchemaVersion = entry.TableId.SchemaVersion;
        write.Operation = operation;
        write.ColumnIds = std::move(ids);
        write.Payload = matrix.ReleaseBuffer();
        IndexWrites.push_back(std::move(write));
        IndexTable = entry.TableId;
        IndexKeyCells = std::move(key);
        IndexKeyTypes = std::move(keyTypes);
        return true;
    }

    void StartIndexUpdate() {
        if (IndexPos >= IndexPlans.size()) {
            UpdatingIndexes = false;
            Allocate();
            return;
        }
        const TIndexPlan& plan = IndexPlans[IndexPos];
        bool changed = false;
        for (const TString& name : plan.Keys) {
            if (!SameCell(MapFindPtr(FoundCells, name), NewCell(name))) {
                changed = true;
            }
        }
        if (!changed) {
            ++IndexPos;
            StartIndexUpdate();
            return;
        }
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(plan.ImplPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TInsertActor::StateIndexUpdate);
    }

    void HandleIndexUpdate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || request->ResultSet[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindTable)
        {
            Fail("42P01", IndexPlans[IndexPos].ImplPath);
            return;
        }
        const auto& entry = request->ResultSet[0];
        const size_t before = IndexWrites.size();
        if (!AddImplOp(entry, IndexPlans[IndexPos], false, static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE))) {
            return;
        }
        PendingKeys.push_back(IndexKeyCells);
        PendingTypes.push_back(IndexKeyTypes);
        if (!AddImplOp(entry, IndexPlans[IndexPos], true, static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT))) {
            return;
        }
        PendingKeys.push_back(IndexKeyCells);
        PendingTypes.push_back(IndexKeyTypes);
        PendingPos = before;
        ResolvePending();
    }

    void ResolvePending() {
        if (PendingPos >= IndexWrites.size()) {
            ++IndexPos;
            StartIndexUpdate();
            return;
        }
        IndexKeyCells = PendingKeys[PendingPos];
        IndexKeyTypes = PendingTypes[PendingPos];
        TVector<TKeyDesc::TColumnOp> columns;
        const size_t count = std::min(IndexWrites[PendingPos].ColumnIds.size(), IndexKeyTypes.size());
        for (size_t i = 0; i < count; ++i) {
            TKeyDesc::TColumnOp operation;
            operation.Column = IndexWrites[PendingPos].ColumnIds[i];
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = IndexKeyTypes[i];
            columns.push_back(operation);
        }
        auto description = MakeHolder<TKeyDesc>(IndexTable, TTableRange(IndexKeyCells), TKeyDesc::ERowOperation::Read, IndexKeyTypes, columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        UpdatingResolve = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TInsertActor::StateResolve);
    }

    void StartIndex() {
        IndexResolve = false;
        if (IndexPos >= IndexPlans.size()) {
            FinishIndexes();
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
        Become(&TInsertActor::StateIndexNavigate);
    }

    bool BuildImplWrite(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TIndexPlan& plan) {
        TVector<TString> keyNames = plan.Keys;
        TVector<std::pair<i32, TString>> primary;
        for (const auto& [name, column] : PreparedColumns) {
            if (column.KeyOrder >= 0) {
                primary.push_back({column.KeyOrder, name});
            }
        }
        std::sort(primary.begin(), primary.end());
        for (const auto& [order, name] : primary) {
            Y_UNUSED(order);
            bool present = false;
            for (const TString& key : plan.Keys) {
                present = present || key == name;
            }
            if (!present) {
                keyNames.push_back(name);
            }
        }
        TVector<TString> names = keyNames;
        for (const TString& column : plan.Data) {
            bool present = false;
            for (const TString& name : names) {
                present = present || name == column;
            }
            if (!present) {
                names.push_back(column);
            }
        }
        TVector<TCell> cells;
        std::vector<ui32> ids;
        TVector<NScheme::TTypeInfo> types;
        IndexKeyCells.clear();
        IndexKeyTypes.clear();
        for (size_t i = 0; i < names.size(); ++i) {
            const auto* info = FindColumn(entry, names[i]);
            if (!info) {
                Fail("42703", names[i]);
                return false;
            }
            const TCell* proposed = MapFindPtr(Proposed, names[i]);
            const TCell cell = proposed ? *proposed : TCell();
            if (i < keyNames.size() && cell.IsNull() && PreparedColumns.contains(names[i]) && PreparedColumns[names[i]].NotNull) {
                Fail("23502", "null value in column " + names[i]);
                return false;
            }
            ids.push_back(info->Id);
            types.push_back(info->PType);
            cells.push_back(cell);
            if (i < keyNames.size()) {
                IndexKeyCells.push_back(cell);
                IndexKeyTypes.push_back(info->PType);
            }
        }
        TSerializedCellMatrix matrix(cells, 1, static_cast<ui16>(cells.size()));
        TDeferredWrite write;
        write.OwnerId = entry.TableId.PathId.OwnerId;
        write.LocalPathId = entry.TableId.PathId.LocalPathId;
        write.SchemaVersion = entry.TableId.SchemaVersion;
        write.Operation = static_cast<ui32>(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT);
        write.ColumnIds = std::move(ids);
        write.Payload = matrix.ReleaseBuffer();
        IndexWrites.push_back(std::move(write));
        IndexTable = entry.TableId;
        IndexColumnTypes = std::move(types);
        return true;
    }

    void HandleIndexNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || request->ResultSet[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindTable)
        {
            Fail("42P01", IndexPlans[IndexPos].ImplPath);
            return;
        }
        if (!BuildImplWrite(request->ResultSet[0], IndexPlans[IndexPos])) {
            return;
        }
        TVector<TKeyDesc::TColumnOp> columns;
        for (size_t i = 0; i < IndexWrites.back().ColumnIds.size() && i < IndexColumnTypes.size(); ++i) {
            TKeyDesc::TColumnOp operation;
            operation.Column = IndexWrites.back().ColumnIds[i];
            operation.Operation = TKeyDesc::EColumnOperation::Read;
            operation.ExpectedType = IndexColumnTypes[i];
            columns.push_back(operation);
        }
        auto description = MakeHolder<TKeyDesc>(IndexTable, TTableRange(IndexKeyCells), TKeyDesc::ERowOperation::Read, IndexKeyTypes, columns);
        auto resolve = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        resolve->DatabaseName = Database;
        resolve->ResultSet.emplace_back(std::move(description));
        IndexResolve = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolve.Release()));
        Become(&TInsertActor::StateResolve);
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

    STATEFN(StateUnique) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleUnique);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateIndexNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndexNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateArbiterNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleArbiterNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateArbiter) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvReadResult, HandleArbiter);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateIndexUpdate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndexUpdate);
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
    const TInsertStmt Statement;
    const ui32 InternalErrorRetries;
    const bool Defer;
    TString Path;
    TTableId Table;
    ui64 TabletId = 0;
    ui64 MainTabletId = 0;
    ui64 TxId = 0;
    std::vector<ui32> ColumnIds;
    TVector<NScheme::TTypeInfo> ColumnTypes;
    TString Payload;
    TString InsertPayload;
    std::vector<ui32> InsertColumnIds;
    NKikimrDataEvents::TEvWrite::TOperation::EOperationType Operation = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
    absl::flat_hash_map<TString, TPrepared> PreparedColumns;
    absl::flat_hash_map<TString, TCell> Proposed;
    absl::flat_hash_map<TString, TCell> Assigned;
    TVector<TQueryColumn> ResultColumns;
    TVector<TPrepared> ResultSources;
    TVector<TVector<TQueryCell>> ResultRows;
    TVector<TPrepared> ReadColumns;
    absl::flat_hash_map<TString, TCell> FoundCells;
    TVector<TString> FoundStorage;
    bool Found = false;
    bool Reading = false;
    bool Reread = false;
    ui64 ReadId = 0;
    TVector<TString> Storage;
    TVector<TCell> KeyCells;
    TVector<NScheme::TTypeInfo> KeyTypes;
    bool HoldWrite = false;
    bool IndexResolve = false;
    bool ArbiterResolve = false;
    bool ArbiterDone = false;
    bool UpdatingIndexes = false;
    bool UpdatingResolve = false;
    int Arbiter = -1;
    bool Linked = false;
    size_t IndexPos = 0;
    TVector<TString> PkNames;
    TVector<TIndexPlan> IndexPlans;
    TVector<TString> ArbiterNames;
    TVector<ui32> ArbiterIds;
    TVector<NScheme::TTypeInfo> ArbiterTypes;
    TVector<TCell> ArbiterFrom;
    TVector<TCell> ArbiterTo;
    TVector<TString> ArbiterStorage;
    TTableId ArbiterTable;
    ui64 ArbiterTablet = 0;
    TVector<TString> IndexByteStorage;
    TVector<TVector<TCell>> PendingKeys;
    TVector<TVector<NScheme::TTypeInfo>> PendingTypes;
    size_t PendingPos = 0;
    TVector<TDeferredWrite> IndexWrites;
    TTableId IndexTable;
    TVector<TCell> IndexKeyCells;
    TVector<TCell> UniqueFrom;
    TVector<TCell> UniqueTo;
    TVector<NScheme::TTypeInfo> IndexKeyTypes;
    TVector<NScheme::TTypeInfo> IndexColumnTypes;
    bool Replied = false;
    ui32 OverloadRetries = 0;
    ui32 InternalRetries = 0;
};

} // namespace

NActors::IActor* CreateInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer) {
    return new TInsertActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, defer);
}

} // namespace NKikimr::NSql
