#include "events.h"

#include <ydb/services/sql/codec/codec.h>
#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_order.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/formats/arrow/protos/ssa.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <cstring>
#include <algorithm>

namespace NKikimr::NSql {
namespace {

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

struct TRawRow {
    TVector<TString> Bytes;
    TVector<bool> Nulls;
};

ui32 CompareFunction(ECompareOp op) {
    switch (op) {
        case ECompareOp::Eq:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_EQUAL;
        case ECompareOp::NotEq:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_NOT_EQUAL;
        case ECompareOp::Lt:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_LESS;
        case ECompareOp::Le:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_LESS_EQUAL;
        case ECompareOp::Gt:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_GREATER;
        case ECompareOp::Ge:
            return NKikimrSSA::TProgram::TAssignment::FUNC_CMP_GREATER_EQUAL;
    }
    return 0;
}

bool SetConstant(NKikimrSSA::TProgram::TConstant& constant, NScheme::TTypeInfo type, TStringBuf bytes) {
    switch (type.GetTypeId()) {
        case NScheme::NTypeIds::Bool: {
            if (bytes.size() != 1) {
                return false;
            }
            constant.SetBool(bytes[0] != 0);
            return true;
        }
        case NScheme::NTypeIds::Int8: {
            i8 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetInt8(value);
            return true;
        }
        case NScheme::NTypeIds::Int16: {
            i16 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetInt16(value);
            return true;
        }
        case NScheme::NTypeIds::Int32: {
            i32 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetInt32(value);
            return true;
        }
        case NScheme::NTypeIds::Int64: {
            i64 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetInt64(value);
            return true;
        }
        case NScheme::NTypeIds::Uint8: {
            ui8 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetUint8(value);
            return true;
        }
        case NScheme::NTypeIds::Uint16: {
            ui16 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetUint16(value);
            return true;
        }
        case NScheme::NTypeIds::Uint32: {
            ui32 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetUint32(value);
            return true;
        }
        case NScheme::NTypeIds::Uint64: {
            ui64 value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetUint64(value);
            return true;
        }
        case NScheme::NTypeIds::Float: {
            float value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetFloat(value);
            return true;
        }
        case NScheme::NTypeIds::Double: {
            double value;
            if (bytes.size() != sizeof(value)) {
                return false;
            }
            memcpy(&value, bytes.data(), sizeof(value));
            constant.SetDouble(value);
            return true;
        }
        case NScheme::NTypeIds::Utf8:
            constant.SetText(TString(bytes));
            return true;
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            constant.SetBytes(TString(bytes));
            return true;
        default:
            return false;
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

class TColumnSelectActor : public NActors::TActorBootstrapped<TColumnSelectActor> {
public:
    TColumnSelectActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui32 internalErrorRetries, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , LockTxId(lockTxId)
    {
        Y_UNUSED(internalErrorRetries);
    }

    void Bootstrap() {
        if (LockTxId || Statement.Lock != ERowLock::None || Statement.Join || !Statement.Windows.empty()
            || !Statement.Aggs.empty() || !Statement.Group.empty() || !Statement.Having.empty()
            || Statement.Distinct || !Statement.DistinctOn.empty() || Statement.FromQuery)
        {
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
        Become(&TColumnSelectActor::StateNavigate);
    }

private:
    struct TShard {
        ui64 TabletId = 0;
        TActorId ScanActor;
        ui32 Generation = 1;
        bool Started = false;
        bool Finished = false;
    };

    struct TSortSlot {
        size_t Index = 0;
        bool Desc = false;
    };

    struct TBound {
        const TComparison* Comparison = nullptr;
        TString Bytes;
    };

    void Fail(const TString& state, const TString& message) {
        if (Replied) {
            return;
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = false;
        result->SqlState = state;
        result->Message = message;
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Unlink() {
        for (const auto& shard : Shards) {
            if (shard.TabletId != 0) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(shard.TabletId));
            }
        }
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
        return true;
    }

    bool DecodeLiteral(const TColumn& column, const TLiteral& literal, TString& bytes) {
        if (literal.Kind == TLiteral::EKind::Parameter || literal.Kind == TLiteral::EKind::Null) {
            Fail("0A000", "not supported");
            return false;
        }
        const TCodecValue decoded = DecodeCell(column.Type.GetTypeId(), false, literal.Text, EWireFormat::Text, column.Precision, column.Scale);
        if (!decoded.Ok) {
            Fail(decoded.SqlState.empty() ? TString("0A000") : decoded.SqlState, decoded.Message.empty() ? TString("not supported") : decoded.Message);
            return false;
        }
        bytes = decoded.Bytes;
        return true;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = request->ResultSet[0];
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable || !entry.ColumnTableInfo) {
            Fail("42P01", Path);
            return;
        }
        Table = entry.TableId;
        for (ui64 tablet : entry.ColumnTableInfo->Description.GetSharding().GetColumnShards()) {
            TShard shard;
            shard.TabletId = tablet;
            Shards.push_back(shard);
        }
        if (Shards.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        if (!Build(entry)) {
            return;
        }
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
            new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database, {Table}));
        Become(&TColumnSelectActor::StateSnapshot);
    }

    bool Build(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        TVector<const TSysTables::TTableColumnInfo*> ordered;
        TVector<const TSysTables::TTableColumnInfo*> keys;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            ordered.push_back(&column);
            if (column.KeyOrder >= 0) {
                keys.push_back(&column);
            }
        }
        std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
            return left->Id < right->Id;
        });
        std::sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->KeyOrder < right->KeyOrder;
        });
        if (keys.empty()) {
            Fail("0A000", "not supported");
            return false;
        }
        if (Statement.Star) {
            for (const auto* info : ordered) {
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return false;
                }
                Output.push_back(std::move(column));
            }
        } else {
            for (const TIdentifier& name : Statement.Columns) {
                if (name.Qualifier && name.Qualifier != Statement.From.Name && name.Qualifier != Statement.Alias) {
                    Fail("42703", name.Qualifier);
                    return false;
                }
                const auto* info = FindColumn(entry, name.Name);
                if (!info) {
                    Fail("42703", name.Name);
                    return false;
                }
                TColumn column;
                if (!PrepareColumn(*info, column)) {
                    Fail("0A000", "not supported");
                    return false;
                }
                Output.push_back(std::move(column));
            }
        }
        if (Output.empty()) {
            Fail("0A000", "not supported");
            return false;
        }

        TVector<TBound> leftover;
        size_t equalPrefix = 0;
        bool ranged = false;
        FromInclusive = true;
        ToInclusive = true;
        for (const auto* info : keys) {
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            TVector<const TComparison*> group;
            for (const TComparison& comparison : Statement.Where) {
                if (comparison.Column.Name == info->Name) {
                    group.push_back(&comparison);
                }
            }
            if (group.empty() || ranged) {
                for (const TComparison* comparison : group) {
                    TBound bound;
                    bound.Comparison = comparison;
                    if (!DecodeLiteral(column, comparison->Value, bound.Bytes)) {
                        return false;
                    }
                    leftover.push_back(std::move(bound));
                }
                break;
            }
            const bool onlyEqual = group.size() == 1 && group[0]->Op == ECompareOp::Eq;
            if (!onlyEqual && equalPrefix > 0) {
                Fail("0A000", "not supported");
                return false;
            }
            if (onlyEqual) {
                TString bytes;
                if (!DecodeLiteral(column, group[0]->Value, bytes)) {
                    return false;
                }
                FromBytes.push_back(std::move(bytes));
                ToBytes.push_back(FromBytes.back());
                ++equalPrefix;
                continue;
            }
            bool pushed = false;
            for (const TComparison* comparison : group) {
                if (comparison->Op == ECompareOp::NotEq) {
                    TBound bound;
                    bound.Comparison = comparison;
                    if (!DecodeLiteral(column, comparison->Value, bound.Bytes)) {
                        return false;
                    }
                    leftover.push_back(std::move(bound));
                    continue;
                }
                TString bytes;
                if (!DecodeLiteral(column, comparison->Value, bytes)) {
                    return false;
                }
                pushed = true;
                switch (comparison->Op) {
                    case ECompareOp::Eq:
                        FromBytes = {bytes};
                        ToBytes = {bytes};
                        FromInclusive = true;
                        ToInclusive = true;
                        break;
                    case ECompareOp::Gt:
                        FromBytes = {bytes};
                        FromInclusive = false;
                        break;
                    case ECompareOp::Ge:
                        FromBytes = {bytes};
                        FromInclusive = true;
                        break;
                    case ECompareOp::Lt:
                        ToBytes = {bytes};
                        ToInclusive = false;
                        break;
                    case ECompareOp::Le:
                        ToBytes = {bytes};
                        ToInclusive = true;
                        break;
                    case ECompareOp::NotEq:
                        break;
                }
            }
            if (!pushed) {
                break;
            }
            ranged = true;
            break;
        }
        for (const TComparison& comparison : Statement.Where) {
            bool key = false;
            for (const auto* info : keys) {
                if (info->Name == comparison.Column.Name && info->KeyOrder < static_cast<i32>(equalPrefix + (ranged ? 1 : 0))) {
                    key = true;
                    break;
                }
            }
            if (key) {
                continue;
            }
            bool already = false;
            for (const TBound& bound : leftover) {
                if (bound.Comparison == &comparison) {
                    already = true;
                    break;
                }
            }
            if (already) {
                continue;
            }
            const auto* info = FindColumn(entry, comparison.Column.Name);
            if (!info) {
                Fail("42703", comparison.Column.Name);
                return false;
            }
            TColumn column;
            if (!PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            TBound bound;
            bound.Comparison = &comparison;
            if (!DecodeLiteral(column, comparison.Value, bound.Bytes)) {
                return false;
            }
            leftover.push_back(std::move(bound));
        }
        Point = !ranged && equalPrefix == keys.size();
        KeyBound = equalPrefix > 0 || ranged;
        if (!BuildProgram(entry, leftover)) {
            return false;
        }
        for (const TOrderKey& key : Statement.Order) {
            size_t index = Output.size();
            for (size_t i = 0; i < Output.size(); ++i) {
                if (Output[i].Name == key.Column.Name) {
                    index = i;
                    break;
                }
            }
            if (index == Output.size()) {
                Fail("42703", key.Column.Name);
                return false;
            }
            Sort.push_back({index, key.Desc});
        }
        return true;
    }

    bool BuildProgram(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TVector<TBound>& filters) {
        NKikimrSSA::TProgram program;
        TVector<ui32> source;
        auto addSource = [&](ui32 id) {
            for (ui32 have : source) {
                if (have == id) {
                    return;
                }
            }
            source.push_back(id);
        };
        for (const TColumn& column : Output) {
            addSource(column.Id);
        }
        for (const TBound& filter : filters) {
            const auto* info = FindColumn(entry, filter.Comparison->Column.Name);
            if (!info) {
                Fail("42703", filter.Comparison->Column.Name);
                return false;
            }
            addSource(info->Id);
        }
        ui32 nextId = 1000;
        for (const TBound& filter : filters) {
            const auto* info = FindColumn(entry, filter.Comparison->Column.Name);
            TColumn column;
            if (!info || !PrepareColumn(*info, column)) {
                Fail("0A000", "not supported");
                return false;
            }
            const ui32 constantId = nextId++;
            const ui32 predicateId = nextId++;
            auto* constant = program.AddCommand()->MutableAssign();
            constant->MutableColumn()->SetId(constantId);
            if (!SetConstant(*constant->MutableConstant(), column.Type, filter.Bytes)) {
                Fail("0A000", "not supported");
                return false;
            }
            auto* compare = program.AddCommand()->MutableAssign();
            compare->MutableColumn()->SetId(predicateId);
            auto* function = compare->MutableFunction();
            function->SetId(CompareFunction(filter.Comparison->Op));
            function->AddArguments()->SetId(column.Id);
            function->AddArguments()->SetId(constantId);
            program.AddCommand()->MutableFilter()->MutablePredicate()->SetId(predicateId);
        }
        {
            auto* command = program.AddCommand();
            auto* projection = command->MutableProjection();
            for (const TColumn& column : Output) {
                projection->AddColumns()->SetId(column.Id);
            }
        }
        TString serialized;
        if (!program.SerializeToString(&serialized)) {
            Fail("XX000", "program");
            return false;
        }
        NKikimrSSA::TOlapProgram wrapped;
        wrapped.SetProgram(serialized);
        if (!wrapped.SerializeToString(&Program)) {
            Fail("XX000", "program");
            return false;
        }
        ReadIds = std::move(source);
        return true;
    }

    void HandleSnapshot(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Fail("08006", ev->Get()->Issues.ToOneLineString());
            return;
        }
        SnapshotStep = ev->Get()->Snapshot.Step;
        SnapshotTxId = ev->Get()->Snapshot.TxId;
        SnapshotHandle.emplace(std::move(ev->Get()->SnapshotHandle));
        for (auto& shard : Shards) {
            SendOne(shard);
        }
        if (Shards.empty()) {
            Finish();
            return;
        }
        Become(&TColumnSelectActor::StateRead);
    }

    void SendOne(TShard& shard) {
        auto request = MakeHolder<TEvDataShard::TEvKqpScan>();
        auto& record = request->Record;
        record.SetTxId(SnapshotTxId);
        record.SetScanId(1);
        record.SetLocalPathId(Table.PathId.LocalPathId);
        record.SetTablePath(Path);
        record.SetSchemaVersion(Table.SchemaVersion);
        record.SetGeneration(1);
        record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
        record.MutableSnapshot()->SetStep(SnapshotStep);
        record.MutableSnapshot()->SetTxId(SnapshotTxId);
        for (ui32 id : ReadIds) {
            record.AddColumnTags(id);
        }
        TVector<TCell> from;
        TVector<TCell> to;
        for (const TString& bytes : FromBytes) {
            from.emplace_back(bytes.data(), bytes.size());
        }
        for (const TString& bytes : ToBytes) {
            to.emplace_back(bytes.data(), bytes.size());
        }
        TSerializedTableRange range(from, FromInclusive, to, ToInclusive);
        range.Point = Point;
        range.Serialize(*record.AddRanges());
        record.SetOlapProgram(Program);
        record.SetOlapProgramType(NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), shard.TabletId, true));
    }

    TShard* FindTablet(ui64 tabletId) {
        for (auto& shard : Shards) {
            if (shard.TabletId == tabletId) {
                return &shard;
            }
        }
        return nullptr;
    }

    TShard* FindScan(const TActorId& scanActor) {
        for (auto& shard : Shards) {
            if (shard.ScanActor == scanActor) {
                return &shard;
            }
        }
        return nullptr;
    }

    void Ack(const TShard& shard) {
        Send(shard.ScanActor, new NKqp::TEvKqpCompute::TEvScanDataAck(8 * 1024 * 1024, shard.Generation, 1));
    }

    void HandleInit(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        auto* shard = FindTablet(ev->Get()->Record.GetTabletId());
        if (!shard || shard->Finished) {
            return;
        }
        shard->ScanActor = ActorIdFromProto(ev->Get()->Record.GetScanActorId());
        shard->Generation = ev->Get()->Record.GetGeneration();
        shard->Started = true;
        Ack(*shard);
    }

    bool ReadCell(const arrow::Array& array, i64 row, NScheme::TTypeInfo type, TString& bytes) const {
        switch (array.type_id()) {
            case arrow::Type::INT64: {
                if (type.GetTypeId() != NScheme::NTypeIds::Int64 && type.GetTypeId() != NScheme::NTypeIds::Uint32
                    && type.GetTypeId() != NScheme::NTypeIds::Datetime && type.GetTypeId() != NScheme::NTypeIds::Timestamp
                    && type.GetTypeId() != NScheme::NTypeIds::Interval)
                {
                    return false;
                }
                const i64 value = static_cast<const arrow::Int64Array&>(array).Value(row);
                bytes.assign(reinterpret_cast<const char*>(&value), sizeof(value));
                return true;
            }
            case arrow::Type::UINT64: {
                const ui64 value = static_cast<const arrow::UInt64Array&>(array).Value(row);
                bytes.assign(reinterpret_cast<const char*>(&value), sizeof(value));
                return true;
            }
            case arrow::Type::INT32: {
                const i32 value = static_cast<const arrow::Int32Array&>(array).Value(row);
                bytes.assign(reinterpret_cast<const char*>(&value), sizeof(value));
                return true;
            }
            case arrow::Type::STRING:
            case arrow::Type::BINARY: {
                const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(row);
                bytes.assign(view.data(), view.size());
                return true;
            }
            case arrow::Type::BOOL: {
                const char value = static_cast<const arrow::BooleanArray&>(array).Value(row) ? 1 : 0;
                bytes.assign(&value, 1);
                return true;
            }
            default:
                return false;
        }
    }

    bool Absorb(const std::shared_ptr<arrow::Table>& table) {
        if (!table) {
            return true;
        }
        ProgramRows += static_cast<ui64>(table->num_rows());
        if (table->num_columns() != static_cast<int>(Output.size())) {
            Fail("XX000", "scan schema");
            return false;
        }
        for (i64 row = 0; row < table->num_rows(); ++row) {
            TRawRow raw;
            raw.Bytes.resize(Output.size());
            raw.Nulls.resize(Output.size(), false);
            for (int column = 0; column < table->num_columns(); ++column) {
                const auto& chunked = *table->column(column);
                i64 left = row;
                std::shared_ptr<arrow::Array> array;
                for (const auto& chunk : chunked.chunks()) {
                    if (left < chunk->length()) {
                        array = chunk;
                        break;
                    }
                    left -= chunk->length();
                }
                if (!array) {
                    Fail("XX000", "scan schema");
                    return false;
                }
                if (array->IsNull(left)) {
                    raw.Nulls[column] = true;
                    continue;
                }
                if (!ReadCell(*array, left, Output[column].Type, raw.Bytes[column])) {
                    Fail("0A000", "not supported");
                    return false;
                }
            }
            Raw.push_back(std::move(raw));
        }
        return true;
    }

    void HandleData(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
        auto* shard = FindScan(ev->Sender);
        if (!shard || shard->Finished) {
            return;
        }
        if (!Absorb(ev->Get()->ArrowBatch)) {
            return;
        }
        if (ev->Get()->Finished) {
            shard->Finished = true;
            if (std::all_of(Shards.begin(), Shards.end(), [](const TShard& item) { return item.Finished; })) {
                Finish();
            }
            return;
        }
        Ack(*shard);
    }

    void HandleScanError(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
        TString message = "scan failed";
        if (ev->Get()->Record.IssuesSize() > 0) {
            message = ev->Get()->Record.GetIssues(0).message();
        }
        Fail("XX000", message);
    }

    void HandleDelivery(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Fail("08006", "connection failure");
    }

    bool Less(const TRawRow& left, const TRawRow& right) const {
        for (const auto& slot : Sort) {
            const bool leftNull = left.Nulls[slot.Index];
            const bool rightNull = right.Nulls[slot.Index];
            if (leftNull || rightNull) {
                if (leftNull != rightNull) {
                    return slot.Desc ? !leftNull : leftNull;
                }
                continue;
            }
            const TCell leftCell(left.Bytes[slot.Index].data(), left.Bytes[slot.Index].size());
            const TCell rightCell(right.Bytes[slot.Index].data(), right.Bytes[slot.Index].size());
            const int compared = CompareTypedCells(leftCell, rightCell, NScheme::TTypeInfoOrder(Output[slot.Index].Type));
            if (compared != 0) {
                return slot.Desc ? compared > 0 : compared < 0;
            }
        }
        return false;
    }

    void Finish() {
        if (Replied) {
            return;
        }
        if (!Sort.empty()) {
            std::sort(Raw.begin(), Raw.end(), [&](const TRawRow& left, const TRawRow& right) {
                return Less(left, right);
            });
        }
        TVector<TVector<TQueryCell>> rows;
        rows.reserve(Raw.size());
        for (const TRawRow& raw : Raw) {
            TVector<TQueryCell> line;
            line.reserve(Output.size());
            for (size_t i = 0; i < Output.size(); ++i) {
                const TCodecValue encoded = EncodeCell(Output[i].Type.GetTypeId(), raw.Nulls[i], raw.Bytes[i], EWireFormat::Text, Output[i].Precision, Output[i].Scale);
                if (!encoded.Ok) {
                    Fail(encoded.SqlState.empty() ? TString("XX000") : encoded.SqlState, encoded.Message.empty() ? TString("encode") : encoded.Message);
                    return;
                }
                line.push_back({encoded.Null, encoded.Bytes});
            }
            rows.push_back(std::move(line));
        }
        if (Statement.Offset && *Statement.Offset > 0) {
            if (*Statement.Offset >= rows.size()) {
                rows.clear();
            } else {
                rows.erase(rows.begin(), rows.begin() + static_cast<size_t>(*Statement.Offset));
            }
        }
        if (Statement.Limit && rows.size() > *Statement.Limit) {
            rows.resize(*Statement.Limit);
        }
        Replied = true;
        Unlink();
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        for (const TColumn& column : Output) {
            result->Columns.push_back({column.Name, column.TypeOid, column.TypeSize});
        }
        result->Rows = std::move(rows);
        result->AccessPath = TStringBuilder() << (Point ? "Keys" : (KeyBound ? "Ranges" : "Ranges"))
            << " Shards " << Shards.size() << " ProgramRows " << ProgramRows;
        Send(ReplyTo, result.Release());
        PassAway();
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

    STATEFN(StateRead) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleInit);
            hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleData);
            hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleScanError);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDelivery);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSelectStmt Statement;
    const ui64 LockTxId;
    TString Path;
    TTableId Table;
    TVector<TColumn> Output;
    TVector<ui32> ReadIds;
    TVector<TString> FromBytes;
    TVector<TString> ToBytes;
    bool FromInclusive = true;
    bool ToInclusive = true;
    bool Point = false;
    bool KeyBound = false;
    TString Program;
    TVector<TShard> Shards;
    TVector<TRawRow> Raw;
    TVector<TSortSlot> Sort;
    ui64 ProgramRows = 0;
    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;
    std::optional<NKqp::TSnapshotHandle> SnapshotHandle;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateColumnSelectActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui32 internalErrorRetries, ui64 lockTxId) {
    Y_UNUSED(internalErrorRetries);
    return new TColumnSelectActor(replyTo, std::move(database), std::move(statement), internalErrorRetries, lockTxId);
}

} // namespace NKikimr::NSql
