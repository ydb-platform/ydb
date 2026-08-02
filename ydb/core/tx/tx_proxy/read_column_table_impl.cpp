#include "read_column_table.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>

#include <util/generic/algorithm.h>
#include <util/stream/format.h>

namespace NKikimr {
namespace NTxProxy {

namespace {

#define TXLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)

constexpr ui64 DEFAULT_ACK_FREE_SPACE = 8ull << 20;
constexpr ui64 MEMORY_TASK_ID = 1;
constexpr ui64 MEMORY_TASK_PRIORITY = 0;

class TCellRowsWriter : public NArrow::IRowWriter {
public:
    explicit TCellRowsWriter(TVector<TOwnedCellVec>& rows)
        : Rows(rows)
    {
    }

    void AddRow(const TConstArrayRef<TCell>& cells) override {
        Rows.emplace_back(cells);
    }

private:
    TVector<TOwnedCellVec>& Rows;
};

Y_FORCE_INLINE bool AddCellToYdbRow(Ydb::Value& row, NScheme::TTypeInfo type, const TCell& cell, TString& err) {
    auto& val = *row.add_items();
    if (cell.IsNull()) {
        val.set_null_flag_value(::google::protobuf::NULL_VALUE);
        return true;
    }

    switch (type.GetTypeId()) {
        case NScheme::NTypeIds::Bool: {
            bool value;
            if (!cell.ToValue(value, err)) return false;
            val.set_bool_value(value);
            return true;
        }
        case NScheme::NTypeIds::Int8: {
            i8 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_int32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Uint8: {
            ui8 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Int16: {
            i16 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_int32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Uint16: {
            ui16 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Int32: {
            i32 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_int32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Uint32: {
            ui32 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Int64: {
            i64 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_int64_value(value);
            return true;
        }
        case NScheme::NTypeIds::Uint64: {
            ui64 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint64_value(value);
            return true;
        }
        case NScheme::NTypeIds::Float: {
            float value;
            if (!cell.ToValue(value, err)) return false;
            val.set_float_value(value);
            return true;
        }
        case NScheme::NTypeIds::Double: {
            double value;
            if (!cell.ToValue(value, err)) return false;
            val.set_double_value(value);
            return true;
        }
        case NScheme::NTypeIds::Date: {
            ui16 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Datetime: {
            ui32 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint32_value(value);
            return true;
        }
        case NScheme::NTypeIds::Timestamp: {
            ui64 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_uint64_value(value);
            return true;
        }
        case NScheme::NTypeIds::Interval: {
            i64 value;
            if (!cell.ToValue(value, err)) return false;
            val.set_int64_value(value);
            return true;
        }
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
            val.set_text_value(cell.Data(), cell.Size());
            return true;
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::JsonDocument:
        case NScheme::NTypeIds::DyNumber:
            val.set_bytes_value(cell.Data(), cell.Size());
            return true;
        case NScheme::NTypeIds::Pg: {
            auto convert = NPg::PgNativeTextFromNativeBinary(TStringBuf(cell.Data(), cell.Size()), type.GetPgTypeDesc());
            if (convert.Error) {
                err = *convert.Error;
                return false;
            }
            val.set_text_value(convert.Str);
            return true;
        }
        default:
            val.set_bytes_value(cell.Data(), cell.Size());
            return true;
    }
}

class TReadColumnTableWorker : public TActorBootstrapped<TReadColumnTableWorker> {
private:
    using TBase = TActorBootstrapped<TReadColumnTableWorker>;

    enum class EShardState {
        Idle,
        Opening,
        Active,
        Finished,
        Error,
    };

    struct TShardState {
        ui64 TabletId = 0;
        TSerializedTableRange Range;
        EShardState State = EShardState::Idle;
        TActorId ScanActorId;
        ui32 Generation = 0;
        TVector<TOwnedCellVec> PendingRows;
        size_t PendingRowIndex = 0;
        bool NeedAck = false;
        bool Finished = false;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_ACTOR;
    }

    explicit TReadColumnTableWorker(TReadColumnTableParams&& params)
        : Params(std::move(params))
        , Settings(Params.Settings)
        , TxId(Params.TxId)
        , Services(Params.Services)
        , TxProxyMon(Params.TxProxyMon)
        , TableId(Params.TableId)
        , Columns(std::move(Params.Columns))
        , KeyDesc(std::move(Params.KeyDesc))
        , Parent(Params.Parent)
        , RemainingRows(Settings.MaxRows)
    {
        BuildColumnMeta();
    }

    void Bootstrap(const TActorContext& ctx) {
        LogPrefix = TStringBuilder() << "[ReadColumnTable " << SelfId() << " TxId# " << TxId << "] ";
        Become(&TThis::StateWork);

        Y_ABORT_UNLESS(KeyDesc);
        if (KeyDesc->GetPartitions().empty()) {
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest,
                NKikimrIssues::TStatusIds::BAD_REQUEST, "No partitions to read from column table", ctx);
        }

        BuildShards();
        AcquireSnapshot(ctx);
    }

private:
    void BuildColumnMeta() {
        YdbSchema.clear();
        KeyColumnIndexes.clear();
        KeyTypes.clear();

        TVector<std::pair<i32, size_t>> keyOrder;
        for (size_t i = 0; i < Columns.size(); ++i) {
            const auto& col = Columns[i];
            YdbSchema.emplace_back(col.Name, col.PType);
            if (col.KeyOrder >= 0) {
                keyOrder.emplace_back(col.KeyOrder, i);
            }
        }
        Sort(keyOrder);
        for (const auto& [order, idx] : keyOrder) {
            Y_UNUSED(order);
            KeyColumnIndexes.push_back(idx);
            KeyTypes.push_back(Columns[idx].PType);
        }

        AllowNotNull = (Settings.DataFormat == EReadTableFormat::YdbResultSetWithNotNullSupport);
        BuildResultCommon();
    }

    void BuildResultCommon() {
        Ydb::ResultSet res;
        for (const auto& col : Columns) {
            auto* meta = res.add_columns();
            meta->set_name(col.Name);
            if (col.PType.GetTypeId() == NScheme::NTypeIds::Pg) {
                auto* pg = meta->mutable_type()->mutable_pg_type();
                auto typeDesc = col.PType.GetPgTypeDesc();
                pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
                pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
            } else {
                bool notNullResp = AllowNotNull && col.IsNotNullColumn;
                auto* xType = notNullResp ? meta->mutable_type()
                                         : meta->mutable_type()->mutable_optional_type()->mutable_item();
                auto id = static_cast<NYql::NProto::TypeIds>(col.PType.GetTypeId());
                if (id == NYql::NProto::Decimal) {
                    NScheme::ProtoFromDecimalType(col.PType.GetDecimalType(), *xType->mutable_decimal_type());
                } else {
                    xType->set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(id));
                }
            }
        }
        res.set_truncated(true);
        Y_PROTOBUF_SUPPRESS_NODISCARD res.SerializeToString(&ResultCommon);
    }

    void BuildShards() {
        Shards.clear();
        Shards.reserve(KeyDesc->GetPartitions().size());
        // Column tables are hash-sharded: every shard receives the full requested key range
        // and filters locally. Partition EndKeyPrefix boundaries are not PK ranges.
        for (const auto& partition : KeyDesc->GetPartitions()) {
            TShardState state;
            state.TabletId = partition.ShardId;

            auto& range = state.Range;
            range.From = TSerializedCellVec(KeyDesc->Range.From);
            range.FromInclusive = KeyDesc->Range.InclusiveFrom;
            range.To = TSerializedCellVec(KeyDesc->Range.To);
            range.ToInclusive = KeyDesc->Range.InclusiveTo;

            if (range.From.GetBuffer().empty() && !range.FromInclusive) {
                range.FromInclusive = true;
            }
            if (range.To.GetBuffer().empty() && !range.ToInclusive) {
                range.ToInclusive = true;
            }

            Shards.push_back(std::move(state));
        }
    }

    TString ResolveDatabaseName() const {
        if (Settings.DatabaseName) {
            return Settings.DatabaseName;
        }
        // Fallback for clients that omit database (e.g. some local tests / legacy SDK paths).
        return CanonizePath(TString(ExtractDomain(Settings.TablePath)));
    }

    void AcquireSnapshot(const TActorContext& ctx) {
        if (!Settings.ReadVersion.IsMax()) {
            Snapshot = Settings.ReadVersion;
            return OnSnapshotReady(ctx);
        }

        const TString databaseName = ResolveDatabaseName();
        if (!databaseName || databaseName == "/") {
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest,
                NKikimrIssues::TStatusIds::BAD_REQUEST,
                "Database name is required for column table ReadTable snapshot", ctx);
        }

        TXLOG_D("Acquiring LongTx read snapshot Database# " << databaseName);
        Send(NLongTxService::MakeLongTxServiceID(ctx.SelfID.NodeId()),
            new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(
                databaseName, TVector<TTableId>{TableId}));
    }

    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev, const TActorContext& ctx) {
        auto* msg = ev->Get();
        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            IssueManager.RaiseIssues(msg->Issues);
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError,
                NKikimrIssues::TStatusIds::ERROR, "Failed to acquire read snapshot", ctx);
        }

        Snapshot = msg->Snapshot;
        SnapshotHandle = std::move(msg->SnapshotHandle);
        TXLOG_D("Acquired snapshot Step# " << Snapshot.Step << " TxId# " << Snapshot.TxId);
        RequestResourceBroker(ctx);
    }

    void OnSnapshotReady(const TActorContext& ctx) {
        RequestResourceBroker(ctx);
    }

    void RequestResourceBroker(const TActorContext& ctx) {
        const auto& cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();
        const ui64 messageSize = cfg.GetMessageSizeLimit() ? cfg.GetMessageSizeLimit() : DEFAULT_ACK_FREE_SPACE;
        AckFreeSpace = Min(messageSize, DEFAULT_ACK_FREE_SPACE);
        if (Settings.MaxBatchSizeBytes != Max<ui64>()) {
            AckFreeSpace = Min(AckFreeSpace, Settings.MaxBatchSizeBytes);
        }

        MaxParallelShards = Settings.Ordered
            ? Shards.size()
            : Min<size_t>(Shards.size(), Max<size_t>(1, cfg.GetMaxStreamingShards()));
        RequiredMemoryBytes = MaxParallelShards * AckFreeSpace;

        TXLOG_D("Requesting ResourceBroker for sync memory allocate Bytes# " << RequiredMemoryBytes);
        Send(NResourceBroker::MakeResourceBrokerID(), new NResourceBroker::TEvResourceBroker::TEvResourceBrokerRequest);
    }

    void Handle(NResourceBroker::TEvResourceBroker::TEvResourceBrokerResponse::TPtr& ev, const TActorContext& ctx) {
        ResourceBroker = ev->Get()->ResourceBroker;
        if (!ResourceBroker) {
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater,
                NKikimrIssues::TStatusIds::REJECTED,
                "ResourceBroker is not available", ctx);
        }

        // Synchronous allocate via ResourceBroker (same mechanism KQP RM uses). Fail hard if no memory.
        const TString taskName = TStringBuilder() << "readtable-column-" << TxId;
        NResourceBroker::TResourceValues resources{};
        resources[NKikimrResourceBroker::MEMORY] = RequiredMemoryBytes;
        const bool allocated = ResourceBroker->SubmitTaskInstant(
            NResourceBroker::TEvResourceBroker::TEvSubmitTask(
                MEMORY_TASK_ID, taskName, resources, "kqp_query", MEMORY_TASK_PRIORITY, {}),
            SelfId());
        if (!allocated) {
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded,
                NKikimrIssues::TStatusIds::OVERLOADED,
                TStringBuilder() << "Not enough memory for column ReadTable merge buffers, requested "
                    << RequiredMemoryBytes << " bytes", ctx);
        }

        MemoryAllocated = true;
        TXLOG_D("Allocated merge memory Bytes# " << RequiredMemoryBytes
            << " MaxParallelShards# " << MaxParallelShards
            << " Ordered# " << Settings.Ordered);
        ProcessOpenQueue(ctx);
    }

    void ProcessOpenQueue(const TActorContext& ctx) {
        size_t active = 0;
        for (const auto& shard : Shards) {
            if (shard.State == EShardState::Opening || shard.State == EShardState::Active) {
                ++active;
            }
        }

        for (size_t i = 0; i < Shards.size() && active < MaxParallelShards; ++i) {
            auto& shard = Shards[i];
            if (shard.State != EShardState::Idle) {
                continue;
            }
            OpenShard(i, ctx);
            ++active;
        }

        MaybeEmit(ctx);
        MaybeFinish(ctx);
    }

    void OpenShard(size_t idx, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto& shard = Shards[idx];
        Y_ABORT_UNLESS(shard.State == EShardState::Idle);

        auto ev = MakeHolder<TEvDataShard::TEvKqpScan>();
        auto& record = ev->Record;
        record.SetLocalPathId(TableId.PathId.LocalPathId);
        record.SetTablePath(Settings.TablePath);
        record.SetSchemaVersion(TableId.SchemaVersion);
        record.SetScanId(TableId.PathId.LocalPathId);
        record.SetTxId(TxId);
        record.SetGeneration(1);
        record.SetReverse(false);
        record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
        record.SetCSScanPolicy("PLAIN");

        record.MutableSnapshot()->SetStep(Snapshot.Step);
        record.MutableSnapshot()->SetTxId(Snapshot.TxId);

        if (RemainingRows != Max<ui64>()) {
            record.SetItemsLimit(RemainingRows);
        }

        for (const auto& col : Columns) {
            record.AddColumnTags(col.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(col.PType, col.PTypeMod);
            record.AddColumnTypes(columnType.TypeId);
            if (columnType.TypeInfo) {
                *record.AddColumnTypeInfos() = *columnType.TypeInfo;
            } else {
                record.AddColumnTypeInfos();
            }
        }

        shard.Range.Serialize(*record.AddRanges());

        TXLOG_D("Sending TEvKqpScan to TabletId# " << shard.TabletId);
        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.Release(), shard.TabletId, true),
            IEventHandle::FlagTrackDelivery);

        shard.State = EShardState::Opening;
        shard.Generation = 1;
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const ui64 tabletId = record.GetTabletId();
        auto* shard = FindShard(tabletId);
        if (!shard || shard->State != EShardState::Opening) {
            TXLOG_D("Ignoring unexpected TEvScanInitActor from TabletId# " << tabletId);
            return;
        }

        shard->ScanActorId = ActorIdFromProto(record.GetScanActorId());
        shard->Generation = record.GetGeneration();
        shard->State = EShardState::Active;
        shard->NeedAck = true;

        TXLOG_D("Scan initialized TabletId# " << tabletId << " ScanActor# " << shard->ScanActorId);
        SendAck(*shard, ctx);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev, const TActorContext& ctx) {
        auto* msg = ev->Get();
        auto* shard = FindShardByScanActor(ev->Sender);
        if (!shard) {
            TXLOG_D("Ignoring TEvScanData from unknown scan actor " << ev->Sender);
            return;
        }

        if (msg->Generation != shard->Generation) {
            TXLOG_D("Ignoring outdated TEvScanData Generation# " << msg->Generation
                << " expected " << shard->Generation);
            return;
        }

        if (msg->ArrowBatch && msg->ArrowBatch->num_rows() > 0) {
            auto batch = NArrow::ToBatch(msg->ArrowBatch);
            if (!batch) {
                return FailShard(*shard, "Failed to convert arrow table to record batch", ctx);
            }

            TVector<TOwnedCellVec> rows;
            rows.reserve(batch->num_rows());
            TCellRowsWriter writer(rows);
            NArrow::TArrowToYdbConverter converter(YdbSchema, writer, false, false);
            TString error;
            if (!converter.Process(*batch, error)) {
                return FailShard(*shard, error ? error : "Arrow to YDB conversion failed", ctx);
            }
            for (auto& row : rows) {
                shard->PendingRows.push_back(std::move(row));
            }
        }

        shard->Finished = msg->Finished;
        shard->NeedAck = true;

        if (shard->PendingRows.empty() && shard->Finished) {
            FinishShard(*shard, ctx);
        }

        MaybeEmit(ctx);
        ProcessOpenQueue(ctx);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        auto* shard = FindShard(record.GetTabletId());
        TString issues;
        NYql::TIssues parsed;
        NYql::IssuesFromMessage(record.GetIssues(), parsed);
        issues = parsed.ToString();
        if (shard) {
            FailShard(*shard, issues ? issues : "ColumnShard scan failed", ctx);
        } else {
            ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable,
                NKikimrIssues::TStatusIds::REJECTED, issues ? issues : "ColumnShard scan failed", ctx);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const ui64 tabletId = ev->Get()->TabletId;
        auto* shard = FindShard(tabletId);
        if (!shard || shard->State == EShardState::Finished || shard->State == EShardState::Error) {
            return;
        }
        FailShard(*shard, TStringBuilder() << "Delivery problem to ColumnShard " << tabletId, ctx);
    }

    void SendAck(TShardState& shard, const TActorContext&) {
        if (!shard.NeedAck || !shard.ScanActorId || shard.State != EShardState::Active) {
            return;
        }
        if (!shard.PendingRows.empty() && shard.PendingRowIndex < shard.PendingRows.size()) {
            // Wait until pending rows are consumed before asking for more.
            return;
        }

        Send(shard.ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(AckFreeSpace, shard.Generation, /*maxChunks*/ 1));
        shard.NeedAck = false;
    }

    void MaybeEmit(const TActorContext& ctx) {
        if (Settings.Ordered) {
            EmitOrdered(ctx);
        } else {
            EmitUnordered(ctx);
        }
    }

    void EmitUnordered(const TActorContext& ctx) {
        for (auto& shard : Shards) {
            if (shard.State != EShardState::Active && shard.State != EShardState::Finished) {
                continue;
            }
            while (shard.PendingRowIndex < shard.PendingRows.size()) {
                if (!AppendRow(shard.PendingRows[shard.PendingRowIndex], ctx)) {
                    return;
                }
                ++shard.PendingRowIndex;
            }
            if (shard.PendingRowIndex >= shard.PendingRows.size()) {
                shard.PendingRows.clear();
                shard.PendingRowIndex = 0;
                if (shard.Finished && shard.State == EShardState::Active) {
                    FinishShard(shard, ctx);
                } else if (shard.NeedAck) {
                    SendAck(shard, ctx);
                }
            }
        }
        FlushBuffer(false, ctx);
    }

    void EmitOrdered(const TActorContext& ctx) {
        // Wait until every non-finished shard has a head row (or is still opening).
        for (const auto& shard : Shards) {
            if (shard.State == EShardState::Opening) {
                return;
            }
            if (shard.State == EShardState::Active &&
                shard.PendingRowIndex >= shard.PendingRows.size() &&
                !shard.Finished)
            {
                return;
            }
        }

        while (true) {
            size_t best = Max<size_t>();
            for (size_t i = 0; i < Shards.size(); ++i) {
                auto& shard = Shards[i];
                if (shard.PendingRowIndex >= shard.PendingRows.size()) {
                    continue;
                }
                if (best == Max<size_t>()) {
                    best = i;
                    continue;
                }
                if (CompareRows(Shards[i].PendingRows[shard.PendingRowIndex],
                        Shards[best].PendingRows[Shards[best].PendingRowIndex]) < 0)
                {
                    best = i;
                }
            }

            if (best == Max<size_t>()) {
                break;
            }

            auto& shard = Shards[best];
            if (!AppendRow(shard.PendingRows[shard.PendingRowIndex], ctx)) {
                return;
            }
            ++shard.PendingRowIndex;
            if (shard.PendingRowIndex >= shard.PendingRows.size()) {
                shard.PendingRows.clear();
                shard.PendingRowIndex = 0;
                if (shard.Finished) {
                    FinishShard(shard, ctx);
                } else if (shard.NeedAck) {
                    SendAck(shard, ctx);
                }
            }
        }

        FlushBuffer(false, ctx);
    }

    int CompareRows(const TOwnedCellVec& a, const TOwnedCellVec& b) const {
        if (KeyColumnIndexes.empty()) {
            return 0;
        }
        TVector<TCell> keyA(Reserve(KeyColumnIndexes.size()));
        TVector<TCell> keyB(Reserve(KeyColumnIndexes.size()));
        for (size_t idx : KeyColumnIndexes) {
            keyA.push_back(idx < a.size() ? a[idx] : TCell());
            keyB.push_back(idx < b.size() ? b[idx] : TCell());
        }
        return CompareTypedCellVectors(keyA.data(), keyB.data(), KeyTypes.data(), KeyTypes.size());
    }

    bool AppendRow(const TOwnedCellVec& row, const TActorContext& ctx) {
        if (RemainingRows == 0) {
            return false;
        }

        Ydb::ResultSet oneRow;
        auto& protoRow = *oneRow.add_rows();
        TString err;
        for (size_t col = 0; col < Columns.size(); ++col) {
            const TCell cell = col < row.size() ? row[col] : TCell();
            if (!AddCellToYdbRow(protoRow, Columns[col].PType, cell, err)) {
                ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable,
                    NKikimrIssues::TStatusIds::ERROR, err, ctx);
                return false;
            }
        }

        if (Buffer.empty()) {
            Buffer = ResultCommon;
        }
        {
            TString rowBytes;
            TStringOutput out(rowBytes);
            oneRow.SerializeToArcadiaStream(&out);
            Buffer += rowBytes;
        }
        ++BufferedRows;

        if (RemainingRows != Max<ui64>()) {
            --RemainingRows;
        }

        const bool sizeLimit = Buffer.size() >= AckFreeSpace;
        const bool rowsLimit = Settings.MaxBatchSizeRows != Max<ui64>() && BufferedRows >= Settings.MaxBatchSizeRows;
        if (sizeLimit || rowsLimit || RemainingRows == 0) {
            FlushBuffer(RemainingRows == 0, ctx);
            if (RemainingRows == 0) {
                FinishAllAndComplete(ctx);
                return false;
            }
        }
        return true;
    }

    void FlushBuffer(bool last, const TActorContext& ctx) {
        if (Buffer.empty() && !last) {
            return;
        }
        if (Buffer.empty() && last && !SentAnyData) {
            // Empty table: send schema-only result set.
            Ydb::ResultSet res;
            Y_PROTOBUF_SUPPRESS_NODISCARD res.ParseFromString(ResultCommon);
            res.set_truncated(false);
            Y_PROTOBUF_SUPPRESS_NODISCARD res.SerializeToString(&Buffer);
        }
        if (Buffer.empty()) {
            return;
        }

        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
            TEvTxUserProxy::TResultStatus::ExecResponseData);
        x->Record.SetStatusCode(NKikimrIssues::TStatusIds::TRANSIENT);
        x->Record.SetStep(Snapshot.Step);
        x->Record.SetTxId(Snapshot.TxId ? Snapshot.TxId : TxId);
        x->Record.SetSerializedReadTableResponse(Buffer);
        x->Record.SetReadTableResponseVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
        x->Record.SetDataShardTabletId(0);

        if (TxProxyMon) {
            TxProxyMon->ReportStatusStreamData->Inc();
        }
        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);

        Buffer.clear();
        BufferedRows = 0;
        SentAnyData = true;
        Y_UNUSED(last);
    }

    void FinishShard(TShardState& shard, const TActorContext& ctx) {
        if (shard.State == EShardState::Finished || shard.State == EShardState::Error) {
            return;
        }
        shard.State = EShardState::Finished;
        AbortScan(shard);
        ProcessOpenQueue(ctx);
        MaybeFinish(ctx);
    }

    void FailShard(TShardState& shard, const TString& error, const TActorContext& ctx) {
        shard.State = EShardState::Error;
        AbortScan(shard);
        ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable,
            NKikimrIssues::TStatusIds::REJECTED, error, ctx);
    }

    void FinishAllAndComplete(const TActorContext& ctx) {
        for (auto& shard : Shards) {
            if (shard.State != EShardState::Finished && shard.State != EShardState::Error) {
                shard.State = EShardState::Finished;
                AbortScan(shard);
            }
        }
        FlushBuffer(true, ctx);
        ReplyComplete(ctx);
    }

    void MaybeFinish(const TActorContext& ctx) {
        for (const auto& shard : Shards) {
            if (shard.State != EShardState::Finished) {
                return;
            }
        }
        FlushBuffer(true, ctx);
        ReplyComplete(ctx);
    }

    void ReplyComplete(const TActorContext& ctx) {
        if (Completed) {
            return;
        }
        Completed = true;

        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
            TEvTxUserProxy::TResultStatus::ExecComplete);
        x->Record.SetStatusCode(NKikimrIssues::TStatusIds::SUCCESS);
        x->Record.SetStep(Snapshot.Step);
        x->Record.SetTxId(Snapshot.TxId ? Snapshot.TxId : TxId);
        if (TxProxyMon) {
            TxProxyMon->ReportStatusOK->Inc();
        }
        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);
        Die(ctx);
    }

    void ReplyAndDie(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status,
        NKikimrIssues::TStatusIds::EStatusCode code,
        const TString& error,
        const TActorContext& ctx)
    {
        if (Completed) {
            return;
        }
        Completed = true;

        if (error) {
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, error));
        }
        TXLOG_E("Failing column ReadTable: " << error);

        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(status);
        x->Record.SetStatusCode(code);
        IssuesToMessage(IssueManager.GetIssues(), x->Record.MutableIssues());
        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);
        Die(ctx);
    }

    TShardState* FindShard(ui64 tabletId) {
        for (auto& shard : Shards) {
            if (shard.TabletId == tabletId) {
                return &shard;
            }
        }
        return nullptr;
    }

    TShardState* FindShardByScanActor(const TActorId& actorId) {
        for (auto& shard : Shards) {
            if (shard.ScanActorId == actorId) {
                return &shard;
            }
        }
        return nullptr;
    }

    void AbortScan(TShardState& shard) {
        if (shard.ScanActorId) {
            Send(shard.ScanActorId, new NKqp::TEvKqp::TEvAbortExecution(
                NYql::NDqProto::StatusIds::CANCELLED, TString("ReadTable finished")));
            shard.ScanActorId = {};
        }
    }

    void Die(const TActorContext& ctx) override {
        for (auto& shard : Shards) {
            AbortScan(shard);
        }
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

        if (MemoryAllocated && ResourceBroker) {
            ResourceBroker->FinishTaskInstant(
                NResourceBroker::TEvResourceBroker::TEvFinishTask(MEMORY_TASK_ID), SelfId());
            MemoryAllocated = false;
        }

        if (Parent) {
            // Wakeup is used as a lightweight "done" signal to the parent ReadTable worker.
            Send(Parent, new TEvents::TEvWakeup);
            Parent = {};
        }

        TBase::Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        Die(ctx);
    }

    STRICT_STFUNC(StateWork,
        HFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
        HFunc(NResourceBroker::TEvResourceBroker::TEvResourceBrokerResponse, Handle);
        HFunc(NKqp::TEvKqpCompute::TEvScanInitActor, Handle);
        HFunc(NKqp::TEvKqpCompute::TEvScanData, Handle);
        HFunc(NKqp::TEvKqpCompute::TEvScanError, Handle);
        HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
    )

private:
    TReadColumnTableParams Params;
    TReadTableSettings Settings;
    ui64 TxId = 0;
    TTxProxyServices Services;
    TIntrusivePtr<TTxProxyMon> TxProxyMon;
    TTableId TableId;
    TVector<TSysTables::TTableColumnInfo> Columns;
    THolder<TKeyDesc> KeyDesc;
    TActorId Parent;
    TString LogPrefix;

    TRowVersion Snapshot = TRowVersion::Min();
    NKqp::TSnapshotHandle SnapshotHandle;

    TIntrusivePtr<NResourceBroker::IResourceBroker> ResourceBroker;
    bool MemoryAllocated = false;
    ui64 RequiredMemoryBytes = 0;

    TVector<TShardState> Shards;
    size_t MaxParallelShards = 1;
    ui64 AckFreeSpace = DEFAULT_ACK_FREE_SPACE;
    ui64 RemainingRows = Max<ui64>();

    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    TVector<size_t> KeyColumnIndexes;
    TVector<NScheme::TTypeInfo> KeyTypes;
    bool AllowNotNull = false;
    TString ResultCommon;
    TString Buffer;
    ui64 BufferedRows = 0;
    bool SentAnyData = false;
    bool Completed = false;

    NYql::TIssueManager IssueManager;
};

} // namespace

IActor* CreateReadColumnTableWorker(TReadColumnTableParams&& params) {
    return new TReadColumnTableWorker(std::move(params));
}

} // namespace NTxProxy
} // namespace NKikimr
