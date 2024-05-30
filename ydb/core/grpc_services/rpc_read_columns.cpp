#include "service_coordination.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "rpc_kh_snapshots.h"
#include "resolve_local_db_table.h"
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/core/formats/factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/public/api/protos/ydb_clickhouse_internal.pb.h>

#include <util/string/vector.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvReadColumnsRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::ScanRequest,
    Ydb::ClickhouseInternal::ScanResponse>;

class TReadColumnsRPC : public TActorBootstrapped<TReadColumnsRPC> {
    using TBase = TActorBootstrapped<TReadColumnsRPC>;

private:

    static constexpr ui32 DEFAULT_TIMEOUT_SEC = 5*60;

    std::unique_ptr<IRequestOpCtx> Request;
    TActorId SchemeCache;
    TActorId LeaderPipeCache;
    TDuration Timeout;
    TActorId TimeoutTimerActorId;
    bool WaitingResolveReply;
    bool Finished;
    Ydb::ClickhouseInternal::ScanResult Result;

    TAutoPtr<TKeyDesc> KeyRange;
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> ResolveNamesResult;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;

    // Positions of key and value fields in the request proto struct
    struct TFieldDescription {
        ui32 ColId;
        ui32 PositionInStruct;
        NScheme::TTypeInfo Type;
    };
    TVector<TFieldDescription> KeyColumnPositions;
    TVector<TFieldDescription> ValueColumnPositions;

    TSerializedCellVec MinKey;
    bool MinKeyInclusive;
    TSerializedCellVec MaxKey;
    bool MaxKeyInclusive;

    ui32 ShardRequestCount;
    ui32 ShardReplyCount;

    TActorId SysViewScanActor;
    std::unique_ptr<IBlockBuilder> BlockBuilder;
    TVector<NScheme::TTypeInfo> ValueColumnTypes;
    ui64 SysViewMaxRows;
    ui64 SysViewMaxBytes;
    ui64 SysViewRowsReceived;

    TKikhouseSnapshotId SnapshotId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TReadColumnsRPC(std::unique_ptr<IRequestOpCtx>&& request)
        : TBase()
        , Request(std::move(request))
        , SchemeCache(MakeSchemeCacheID())
        , LeaderPipeCache(MakePipePerNodeCacheID(false))
        , Timeout(TDuration::Seconds(DEFAULT_TIMEOUT_SEC))
        , WaitingResolveReply(false)
        , Finished(false)
        , MinKeyInclusive(0)
        , MaxKeyInclusive(0)
        , ShardRequestCount(0)
        , ShardReplyCount(0)
        , SysViewMaxRows(100000)
        , SysViewMaxBytes(10*1024*1024)
        , SysViewRowsReceived(0)
    {}

    const Ydb::ClickhouseInternal::ScanRequest* GetProtoRequest() {
        return TEvReadColumnsRequest::GetProtoRequest(Request);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        auto proto = GetProtoRequest();
        if (const auto& snapshotId = proto->snapshot_id()) {
            if (!SnapshotId.Parse(snapshotId)) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "Invalid snapshot id specified", ctx);
            }
        }

        ResolveTable(proto->Gettable(), ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        Y_ABORT_UNLESS(Finished);
        Y_ABORT_UNLESS(!WaitingResolveReply);
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        if (SysViewScanActor) {
            ctx.Send(SysViewScanActor, new TEvents::TEvPoisonPill());
        }
        TBase::Die(ctx);
    }

private:
    STFUNC(StateWaitResolveTable) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvLocalSchemeTxResponse, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void ResolveTable(const TString& table, const NActors::TActorContext& ctx) {
        // TODO: check all params;
        // Cerr << *Request->GetProtoRequest() << Endl;

        auto path = ::NKikimr::SplitPath(table);
        TMaybe<ui64> tabletId = TryParseLocalDbPath(path);
        if (tabletId) {
            if (Request->GetSerializedToken().empty() || !IsSuperUser(NACLib::TUserToken(Request->GetSerializedToken()), *AppData(ctx))) {
                return ReplyWithError(Ydb::StatusIds::NOT_FOUND, "Invalid table path specified", ctx);
            }

            std::unique_ptr<TEvTablet::TEvLocalSchemeTx> ev(new TEvTablet::TEvLocalSchemeTx());
            ctx.Send(MakePipePerNodeCacheID(true), new TEvPipeCache::TEvForward(ev.release(), *tabletId, true), IEventHandle::FlagTrackDelivery);

            TBase::Become(&TThis::StateWaitResolveTable);
            WaitingResolveReply = true;
        } else {
            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = std::move(path);
            if (entry.Path.empty()) {
                return ReplyWithError(Ydb::StatusIds::NOT_FOUND, "Invalid table path specified", ctx);
            }
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            request->ResultSet.emplace_back(entry);

            ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

            TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

            TBase::Become(&TThis::StateWaitResolveTable);
            WaitingResolveReply = true;
        }
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, "Request timed out", ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        ResolveNamesResult = ev->Get()->Request;

        return ProceedWithSchema(ctx);
    }

    void Handle(TEvTablet::TEvLocalSchemeTxResponse::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        ResolveNamesResult = new NSchemeCache::TSchemeCacheNavigate();
        auto &record = ev->Get()->Record;

        const TString& table = GetProtoRequest()->table();
        auto path = ::NKikimr::SplitPath(table);
        FillLocalDbTableSchema(*ResolveNamesResult, record.GetFullScheme(), path.back());
        ResolveNamesResult->ResultSet.back().Path = path;

        return ProceedWithSchema(ctx);
    }

    void ProceedWithSchema(const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveNamesResult->ResultSet.size() == 1);
        const auto& entry = ResolveNamesResult->ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, ToString(entry.Status), ctx);
        }

        TString errorMessage;
        if (!CheckAccess(errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, errorMessage, ctx);
        }

        if (!BuildSchema(ctx)) {
            return;
        }

        if (!ExtractAllKeys(errorMessage)) {
             return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errorMessage, ctx);
        }

        if (GetProtoRequest()->columns().empty()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                                  TStringBuilder() << "Empty column list",
                                  ctx);
        }

        if (ResolveNamesResult->ResultSet.front().TableId.IsSystemView()) {
            return ScanSystemView(ctx);
        } if (TryParseLocalDbPath(ResolveNamesResult->ResultSet.front().Path)) {
            return ScanLocalDbTable(ctx);
        } else {
            return ResolveShards(ctx);
        }
    }

    void ScanSystemView(const NActors::TActorContext& ctx) {
        if (SnapshotId) {
            Request->RaiseIssue(
                MakeIssue(
                    NKikimrIssues::TIssuesIds::WARNING,
                    "Snapshots are ignored when scanning system views"));
        }

        const auto proto = GetProtoRequest();

        if (auto maxRows = proto->max_rows(); maxRows && maxRows <= 100)
            SysViewMaxRows = maxRows;

        if (auto maxBytes = proto->max_bytes(); maxBytes && maxBytes <= 10000)
            SysViewMaxBytes = maxBytes;

        // List of columns requested by user
        TVector<std::pair<TString, NScheme::TTypeInfo>> valueColumnNamesAndTypes;

        // This list of columns will be requested from sys view scan actor
        // It starts with all key columns followed by all the columns requested by user possibly including key columns again
        TSmallVec<NMiniKQL::TKqpComputeContextBase::TColumn> columns;

        {
            auto& entry = ResolveNamesResult->ResultSet.front();
            THashMap<TString, ui32> columnsByName;
            for (const auto& ci : entry.Columns) {
                columnsByName[ci.second.Name] = ci.second.Id;
                if (ci.second.KeyOrder != -1) {
                    KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), ci.second.KeyOrder + 1));
                    KeyColumnTypes[ci.second.KeyOrder] = ci.second.PType;

                    columns.resize(Max<size_t>(columns.size(), ci.second.KeyOrder + 1));
                    columns[ci.second.KeyOrder] = {ci.second.Id, ci.second.PType, ci.second.PTypeMod};
                }
            }

            for (TString col : proto->columns()) {
                auto id = columnsByName.find(col);
                if (id == columnsByName.end()) {
                    return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                                   TStringBuilder() << "Unknown column: " << col,
                                   ctx);
                }

                auto ci = entry.Columns.find(id->second);
                columns.push_back({ci->second.Id, ci->second.PType, ci->second.PTypeMod});

                valueColumnNamesAndTypes.push_back({ci->second.Name, ci->second.PType});
                ValueColumnTypes.push_back(ci->second.PType);
            }
        }

        Y_DEBUG_ABORT_UNLESS(columns.size() == KeyColumnTypes.size() + ValueColumnTypes.size());

        {
            TString format = "clickhouse_native";
            BlockBuilder = AppData()->FormatFactory->CreateBlockBuilder(format);
            if (!BlockBuilder) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                                      TStringBuilder() << "Unsupported block format: " << format.data(),
                                      ctx);
            }

            ui64 rowsPerBlock = 64000;
            ui64 bytesPerBlock = 64000;

            TString err;
            if (!BlockBuilder->Start(valueColumnNamesAndTypes, rowsPerBlock, bytesPerBlock, err)) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                                      TStringBuilder() << "Block format error: " << err.data(),
                                      ctx);
            }
        }

        {
            TTableRange range(MinKey.GetCells(), MinKeyInclusive, MaxKey.GetCells(), MaxKeyInclusive);
            auto tableScanActor = NSysView::CreateSystemViewScan(ctx.SelfID, 0,
                ResolveNamesResult->ResultSet.front().TableId,
                range,
                columns);

            if (!tableScanActor) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                                      TStringBuilder() << "Failed to create system view scan, table id: " << ResolveNamesResult->ResultSet.front().TableId,
                                      ctx);
            }

            SysViewScanActor = ctx.Register(tableScanActor.Release());

            auto ackEv = MakeHolder<NKqp::TEvKqpCompute::TEvScanDataAck>(0);
            ctx.Send(SysViewScanActor, ackEv.Release());
        }

        TBase::Become(&TThis::StateSysViewScan);
    }

    void ScanLocalDbTable(const NActors::TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveNamesResult);

        ui64 tabletId = -1;
        TString tabletIdStr = ResolveNamesResult->ResultSet.front().Path[2];
        try {
            tabletId = FromString<ui64>(tabletIdStr);
        } catch (...) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                                  TStringBuilder() << "Invalid tabeltId: " << tabletIdStr,
                                  ctx);
        }

        TString tableName = ResolveNamesResult->ResultSet.front().Path.back();

        // Send request to the first shard
        std::unique_ptr<TEvTablet::TEvLocalReadColumns> ev =
                std::make_unique<TEvTablet::TEvLocalReadColumns>();
        ev->Record.SetTableName(tableName);
        auto proto = GetProtoRequest();
        for (TString col : proto->columns()) {
            ev->Record.AddColumns(col);
        }
        ev->Record.SetFromKey(MinKey.GetBuffer());
        ev->Record.SetFromKeyInclusive(MinKeyInclusive);
        ev->Record.SetMaxRows(proto->max_rows());
        ev->Record.SetMaxBytes(proto->max_bytes());

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Sending request to tablet " << tabletId);

        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.release(), tabletId, true), IEventHandle::FlagTrackDelivery);

        ++ShardRequestCount;

        TBase::Become(&TThis::StateWaitResults);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr &ev, const TActorContext &ctx) {
        auto scanId = ev->Get()->ScanId;
        Y_UNUSED(scanId);

        size_t keyColumnCount = KeyColumnTypes.size();

        TString lastKey;
        size_t rowsExtracted = 0;
        bool skippedBeforeMinKey = false;

        if (ev->Get()->GetDataFormat() == NKikimrDataEvents::FORMAT_ARROW) {
            return ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, "Arrow format not supported yet", ctx);
        }

        for (auto&& row : ev->Get()->Rows) {
            ++rowsExtracted;
            if (row.size() != keyColumnCount + ValueColumnTypes.size()) {
                return ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR,
                                      "System view row format doesn't match the schema",
                                      ctx);
            }

            TDbTupleRef rowKey(KeyColumnTypes.data(), row.data(), keyColumnCount);

            if (!skippedBeforeMinKey) {
                int cmp = CompareTypedCellVectors(MinKey.GetCells().data(), rowKey.Cells().data(),
                                                  KeyColumnTypes.data(),
                                                  MinKey.GetCells().size(), rowKey.Cells().size());

                // Skip rows before MinKey just in case (because currently sys view scan ignores key range)
                if (cmp > 0 || (cmp == 0 && !MinKeyInclusive)) {
                    LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Skipped rows by sys view scan");
                    continue;
                } else {
                    skippedBeforeMinKey = true;
                }
            }

            TDbTupleRef rowValues(ValueColumnTypes.data(), row.data() + keyColumnCount, row.size() - keyColumnCount);
            BlockBuilder->AddRow(rowKey, rowValues);
            ++SysViewRowsReceived;

            if (SysViewRowsReceived >= SysViewMaxRows || BlockBuilder->Bytes() >= SysViewMaxBytes) {
                lastKey = TSerializedCellVec::Serialize(rowKey.Cells());
                break;
            }
        }

        auto ackEv = MakeHolder<NKqp::TEvKqpCompute::TEvScanDataAck>(0);
        ctx.Send(ev->Sender, ackEv.Release());

        bool done =
                ev->Get()->Finished ||
                SysViewRowsReceived >= SysViewMaxRows ||
                BlockBuilder->Bytes() >= SysViewMaxBytes;

        if (done) {
            TString buffer = BlockBuilder->Finish();
            buffer.resize(BlockBuilder->Bytes());

            Result.add_blocks(buffer);
            Result.set_last_key(lastKey);
            Result.set_last_key_inclusive(true);
            Result.set_eos(ev->Get()->Finished && rowsExtracted == ev->Get()->Rows.size());
            return ReplySuccess(ctx);
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev, const TActorContext& ctx) {
        NYql::TIssues issues;
        Ydb::StatusIds::StatusCode status = ev->Get()->Record.GetStatus();
        NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);

        ReplyWithError(status, issues, ctx);
    }

    STFUNC(StateSysViewScan) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqpCompute::TEvScanData, Handle);
            HFunc(NKqp::TEvKqpCompute::TEvScanError, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    bool CheckAccess(TString& errorMessage) {
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());

        const ui32 access = NACLib::EAccessRights::SelectRow;
        for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : ResolveNamesResult->ResultSet) {
            if (access != 0 && entry.SecurityObject != nullptr &&
                    !entry.SecurityObject->CheckAccess(access, userToken))
            {
                TStringStream explanation;
                explanation << "Access denied for " << userToken.GetUserSID()
                            << " with access " << NACLib::AccessRightsToString(access)
                            << " to table [" << GetProtoRequest()->Gettable() << "]";

                errorMessage = explanation.Str();
                return false;
            }
        }
        return true;
    }

    bool BuildSchema(const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto& entry = ResolveNamesResult->ResultSet.front();

        TVector<ui32> keyColumnIds;
        THashMap<TString, ui32> columnByName;
        for (const auto& ci : entry.Columns) {
            columnByName[ci.second.Name] = ci.second.Id;
            i32 keyOrder = ci.second.KeyOrder;
            if (keyOrder != -1) {
                Y_ABORT_UNLESS(keyOrder >= 0);
                KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), keyOrder + 1));
                KeyColumnTypes[keyOrder] = ci.second.PType;
                keyColumnIds.resize(Max<size_t>(keyColumnIds.size(), keyOrder + 1));
                keyColumnIds[keyOrder] = ci.second.Id;
            }
        }

        KeyColumnPositions.resize(KeyColumnTypes.size());

        return true;
    }

    bool CheckCellSizes(const TConstArrayRef<TCell>& cells, const TConstArrayRef<NScheme::TTypeInfo>& types) {
        if (cells.size() > types.size())
            return false;

        for (size_t i = 0; i < cells.size(); ++i) {
            if (!cells[i].IsNull() &&
                NScheme::GetFixedSize(types[i]) != 0 &&
                NScheme::GetFixedSize(types[i]) != cells[i].Size())
            {
                return false;
            }
        }

        return true;
    }

    bool ExtractAllKeys(TString& errorMessage) {
        auto proto = GetProtoRequest();
        if (!proto->from_key().empty()) {
            if (!TSerializedCellVec::TryParse(proto->from_key(), MinKey) ||
                !CheckCellSizes(MinKey.GetCells(), KeyColumnTypes))
            {
                errorMessage = "Invalid from key";
                return false;
            }
            MinKeyInclusive = proto->from_key_inclusive();
        } else {
            TVector<TCell> allNulls(KeyColumnTypes.size());
            MinKey = TSerializedCellVec(allNulls);
            MinKeyInclusive = true;
        }

        if (!proto->to_key().empty()) {
            if (!TSerializedCellVec::TryParse(proto->to_key(), MaxKey) ||
                !CheckCellSizes(MaxKey.GetCells(), KeyColumnTypes))
            {
                errorMessage = "Invalid to key";
                return false;
            }
            MaxKeyInclusive = proto->to_key_inclusive();
        } else {
            TVector<TCell> infinity;
            MaxKey = TSerializedCellVec(infinity);
            MaxKeyInclusive = false;
        }

        return true;
    }

    void ResolveShards(const NActors::TActorContext& ctx) {
        auto& entry = ResolveNamesResult->ResultSet.front();

        // We are going to set all columns
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& ci : entry.Columns) {
            TKeyDesc::TColumnOp op = { ci.second.Id, TKeyDesc::EColumnOperation::Set, ci.second.PType, 0, 0 };
            columns.push_back(op);
        }

        // Set MaxKey = MinKey to touch only 1 shard in request
        TTableRange range(MinKey.GetCells(), true, MinKey.GetCells(), true, false);
        KeyRange.Reset(new TKeyDesc(entry.TableId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, columns));

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Resolving range: "
                    << " fromKey: " << PrintKey(MinKey.GetBuffer(), *AppData(ctx)->TypeRegistry)
                    << " fromInclusive: " << true);

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());

        request->ResultSet.emplace_back(std::move(KeyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ctx.Send(SchemeCache, resolveReq.Release());

        TBase::Become(&TThis::StateWaitResolveShards);
        WaitingResolveReply = true;
    }

    STFUNC(StateWaitResolveShards) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Request->ResultSet.size() == 1);
        KeyRange = std::move(msg->Request->ResultSet[0].KeyDescription);

        if (msg->Request->ErrorCount > 0) {
            return ReplyWithError(Ydb::StatusIds::NOT_FOUND, Sprintf("Unknown table '%s'", GetProtoRequest()->Gettable().data()), ctx);
        }

        auto getShardsString = [] (const TVector<TKeyDesc::TPartitionInfo>& partitions) {
            TVector<ui64> shards;
            shards.reserve(partitions.size());
            for (auto& partition : partitions) {
                shards.push_back(partition.ShardId);
            }

            return JoinVectorIntoString(shards, ", ");
        };

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Range shards: "
            << getShardsString(KeyRange->GetPartitions()));

        MakeShardRequests(ctx);
    }

    void MakeShardRequests(const NActors::TActorContext& ctx) {
        Y_ABORT_UNLESS(!KeyRange->GetPartitions().empty());
        auto proto = GetProtoRequest();

        // Send request to the first shard
        std::unique_ptr<TEvDataShard::TEvReadColumnsRequest> ev =
                std::make_unique<TEvDataShard::TEvReadColumnsRequest>();
        ev->Record.SetTableId(KeyRange->TableId.PathId.LocalPathId);
        for (const TString& col : proto->columns()) {
            ev->Record.AddColumns(col);
        }
        ev->Record.SetFromKey(MinKey.GetBuffer());
        ev->Record.SetFromKeyInclusive(MinKeyInclusive);
        ev->Record.SetMaxRows(proto->max_rows());
        ev->Record.SetMaxBytes(proto->max_bytes());
        if (SnapshotId) {
            ev->Record.SetSnapshotStep(SnapshotId.Step);
            ev->Record.SetSnapshotTxId(SnapshotId.TxId);
        }

        ui64 shardId = KeyRange->GetPartitions()[0].ShardId;

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Sending request to shards " << shardId);

        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.release(), shardId, true), IEventHandle::FlagTrackDelivery);

        ++ShardRequestCount;

        TBase::Become(&TThis::StateWaitResults);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, "Internal error: pipe cache is not available, the cluster might not be configured properly", ctx);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
        ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Failed to connect to shard %lu", ev->Get()->TabletId), ctx);
    }

    STFUNC(StateWaitResults) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDataShard::TEvReadColumnsResponse, Handle);
            HFunc(TEvTablet::TEvLocalReadColumnsResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }


    TString PrintKey(const TString& serialized, const NScheme::TTypeRegistry& typeRegistry) {
        TSerializedCellVec lastKeyCells(serialized);
        TDbTupleRef lastKeyTuple(KeyColumnTypes.data(), lastKeyCells.GetCells().data(), std::min(KeyColumnTypes.size(), lastKeyCells.GetCells().size()));
        return DbgPrintTuple(lastKeyTuple, typeRegistry);
    }

    void Handle(TEvDataShard::TEvReadColumnsResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& shardResponse = ev->Get()->Record;

        // Notify the cache that we are done with the pipe
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shardResponse.GetTabletID()));

        if (shardResponse.GetStatus() != NKikimrTxDataShard::TError::OK) {
            StatusIds::StatusCode status = Ydb::StatusIds::GENERIC_ERROR;
            switch (shardResponse.GetStatus()) {
            case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                status = Ydb::StatusIds::UNAVAILABLE;
                break;
            case NKikimrTxDataShard::TError::BAD_ARGUMENT:
                status = Ydb::StatusIds::BAD_REQUEST;
                break;
            case NKikimrTxDataShard::TError::SCHEME_ERROR:
                status = Ydb::StatusIds::SCHEME_ERROR;
                break;
            case NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST:
                status = Ydb::StatusIds::NOT_FOUND;
                break;
            }

            ReplyWithError(status, shardResponse.GetErrorDescription(), ctx);
            return;
        }

        ++ShardReplyCount;

        Result.add_blocks(shardResponse.GetBlocks());
        Result.set_last_key(shardResponse.GetLastKey());
        Result.set_last_key_inclusive(shardResponse.GetLastKeyInclusive());
        Result.set_eos(shardResponse.GetLastKey().empty()); // TODO: ??

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Got reply from shard: " << shardResponse.GetTabletID()
                    << " lastKey: " << PrintKey(shardResponse.GetLastKey(), *AppData(ctx)->TypeRegistry)
                    << " inclusive: " << shardResponse.GetLastKeyInclusive());

        if (ShardReplyCount == ShardRequestCount)
            ReplySuccess(ctx);
    }

    void Handle(TEvTablet::TEvLocalReadColumnsResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& shardResponse = ev->Get()->Record;

        // Notify the cache that we are done with the pipe
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shardResponse.GetTabletID()));

        if (shardResponse.GetStatus() != Ydb::StatusIds::SUCCESS) {
            ReplyWithError((StatusIds::StatusCode)shardResponse.GetStatus(), shardResponse.GetErrorDescription(), ctx);
            return;
        }

        ++ShardReplyCount;

        Result.add_blocks(shardResponse.GetBlocks());
        Result.set_last_key(shardResponse.GetLastKey());
        Result.set_last_key_inclusive(shardResponse.GetLastKeyInclusive());
        Result.set_eos(shardResponse.GetLastKey().empty()); // TODO: ??

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Got reply from shard: " << shardResponse.GetTabletID()
                    << " lastKey: " << PrintKey(shardResponse.GetLastKey(), *AppData(ctx)->TypeRegistry)
                    << " inclusive: " << shardResponse.GetLastKeyInclusive());

        if (ShardReplyCount == ShardRequestCount)
            ReplySuccess(ctx);
    }

    void ReplySuccess(const NActors::TActorContext& ctx) {
        Finished = true;
        ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
    }

    void ReplyWithError(StatusIds::StatusCode status, const TString& message, const TActorContext& ctx) {
        Finished = true;
        Request->RaiseIssue(NYql::TIssue(message));
        Request->ReplyWithYdbStatus(status);

        // We cannot Die() while scheme cache request is in flight because that request has pointer to
        // KeyRange member so we must not destroy it before we get the response
        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }

    void ReplyWithError(StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        Finished = true;
        Request->RaiseIssues(issues);
        Request->ReplyWithYdbStatus(status);

        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }

    void ReplyWithResult(StatusIds::StatusCode status,
                         const Ydb::ClickhouseInternal::ScanResult& result,
                         const TActorContext& ctx) {
        Request->SendResult(result, status);
        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }
};

void DoReadColumnsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TReadColumnsRPC(std::move(p)));
}

} // namespace NKikimr
} // namespace NGRpcService
