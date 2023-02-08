#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/actorsystem.h>

#include <util/generic/intrlist.h>

namespace {

static constexpr ui64 MAX_SHARD_RETRIES = 5;
static constexpr ui64 MAX_SHARD_RESOLVES = 3;

bool IsDebugLogEnabled(const NActors::TActorSystem* actorSystem, NActors::NLog::EComponent component) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, component);
}

struct TDefaultRangeEvReadSettings {
    NKikimrTxDataShard::TEvRead Data;

    TDefaultRangeEvReadSettings() {
        Data.SetMaxRows(32767);
        Data.SetMaxBytes(200_MB);
    }

} DefaultRangeEvReadSettings;

THolder<NKikimr::TEvDataShard::TEvRead> DefaultReadSettings() {
    auto result = MakeHolder<NKikimr::TEvDataShard::TEvRead>();
    result->Record.MergeFrom(DefaultRangeEvReadSettings.Data);
    return result;
}

struct TDefaultRangeEvReadAckSettings {
    NKikimrTxDataShard::TEvReadAck Data;

    TDefaultRangeEvReadAckSettings() {
        Data.SetMaxRows(32767);
        Data.SetMaxBytes(200_MB);
    }

} DefaultRangeEvReadAckSettings;

THolder<NKikimr::TEvDataShard::TEvReadAck> DefaultAckSettings() {
    auto result = MakeHolder<NKikimr::TEvDataShard::TEvReadAck>();
    result->Record.MergeFrom(DefaultRangeEvReadAckSettings.Data);
    return result;
}

}


namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NDataShard;

class TKqpReadActor : public TActorBootstrapped<TKqpReadActor>, public NYql::NDq::IDqComputeActorAsyncInput {
    using TBase = TActorBootstrapped<TKqpReadActor>;
public:
    struct TResult {
        ui64 ShardId;
        THolder<TEventHandle<TEvDataShard::TEvReadResult>> ReadResult;
        TMaybe<NKikimr::NMiniKQL::TUnboxedValueVector> Batch;
        size_t ProcessedRows = 0;

        TResult(ui64 shardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>> readResult)
            : ShardId(shardId)
            , ReadResult(std::move(readResult))
        {
        }
    };

    struct TShardState : public TIntrusiveListItem<TShardState> {
        TSmallVec<TSerializedTableRange> Ranges;
        TSmallVec<TSerializedCellVec> Points;

        TOwnedCellVec LastKey;
        TMaybe<ui32> FirstUnprocessedRequest;
        TMaybe<ui32> ReadId;
        ui64 TabletId;

        size_t ResolveAttempt = 0;
        size_t RetryAttempt = 0;

        bool NeedResolve = false;

        void AssignContinuationToken(TShardState* state) {
            if (state->LastKey.DataSize() != 0) {
                LastKey = std::move(state->LastKey);
            }
            FirstUnprocessedRequest = state->FirstUnprocessedRequest;
        }

        TShardState(ui64 tabletId)
            : TabletId(tabletId)
        {
        }

        TTableRange GetBounds() {
            if (Ranges.empty()) {
                YQL_ENSURE(!Points.empty());
                return TTableRange(
                    Points.front().GetCells(), true,
                    Points.back().GetCells(), true);
            } else {
                return TTableRange(
                    Ranges.front().From.GetCells(), Ranges.front().FromInclusive,
                    Ranges.back().To.GetCells(), Ranges.back().ToInclusive);
            }
        }

        static void MakePrefixRange(TSerializedTableRange& range, size_t keyColumns) {
            if (keyColumns == 0) {
                return;
            }
            bool fromInclusive = range.FromInclusive;
            TConstArrayRef<TCell> from = range.From.GetCells();

            bool toInclusive = range.ToInclusive;
            TConstArrayRef<TCell> to = range.To.GetCells();

            bool noop = true;
            // Recognize and remove padding made here https://a.yandex-team.ru/arcadia/ydb/core/kqp/executer/kqp_partition_helper.cpp?rev=r10109549#L284

            // Absent cells mean infinity. So in prefix notation `From` should be exclusive.
            // For example x >= (Key1, Key2, +infinity) is equivalent to x > (Key1, Key2) where x is arbitrary tuple
            if (from.size() < keyColumns) {
                noop = range.FromInclusive;
                fromInclusive = false;
            } else if (fromInclusive) {
                // Nulls are minimum values so we should remove null padding.
                // x >= (Key1, Key2, null) is equivalent to x >= (Key1, Key2)
                ssize_t i = from.size();
                while (i > 0 && from[i - 1].IsNull()) {
                    --i;
                    noop = false;
                }
                from = from.subspan(0, i);
            }

            // Absent cells mean infinity. So in prefix notation `To` should be inclusive.
            // For example x < (Key1, Key2, +infinity) is equivalent to x <= (Key1, Key2) where x is arbitrary tuple
            if (to.size() < keyColumns) {
                toInclusive = true;
                noop = noop && range.ToInclusive;
            } else if (!range.ToInclusive) {
                // Nulls are minimum values so we should remove null padding.
                // For example x < (Key1, Key2, null) is equivalent to x < (Key1, Key2)
                ssize_t i = to.size();
                while (i > 0 && to[i - 1].IsNull()) {
                    --i;
                    noop = false;
                }
                to = to.subspan(0, i);
            }

            if (noop) {
                return;
            }

            range = TSerializedTableRange(from, fromInclusive, to, toInclusive);
        }

        void FillUnprocessedRanges(
            TVector<TSerializedTableRange>& result,
            TConstArrayRef<NScheme::TTypeInfo> keyTypes,
            bool reverse) const
        {
            // Form new vector. Skip ranges already read.
            bool lastKeyEmpty = LastKey.DataSize() == 0;

            if (!lastKeyEmpty) {
                YQL_ENSURE(keyTypes.size() == LastKey.size(), "Key columns size != last key");
            }

            if (reverse) {
                auto rangeIt = Ranges.begin() + FirstUnprocessedRequest.GetOrElse(Ranges.size());

                if (!lastKeyEmpty) {
                    // It is range, where read was interrupted. Restart operation from last read key.
                    result.emplace_back(std::move(TSerializedTableRange(
                        rangeIt->From.GetBuffer(), TSerializedCellVec::Serialize(LastKey), rangeIt->ToInclusive, false
                        )));
                }
                result.insert(result.begin(), Ranges.begin(), rangeIt);
            } else {
                auto rangeIt = Ranges.begin() + FirstUnprocessedRequest.GetOrElse(0);

                if (!lastKeyEmpty) {
                    // It is range, where read was interrupted. Restart operation from last read key.
                    result.emplace_back(std::move(TSerializedTableRange(
                        TSerializedCellVec::Serialize(LastKey), rangeIt->To.GetBuffer(), false, rangeIt->ToInclusive
                        )));
                    ++rangeIt;
                }

                // And push all others
                result.insert(result.end(), rangeIt, Ranges.end());
            }
            for (auto& range : result) {
                MakePrefixRange(range, keyTypes.size());
            }
        }

        void FillUnprocessedPoints(TVector<TSerializedCellVec>& result, bool reverse) const {
            if (reverse) {
                auto it = FirstUnprocessedRequest ? Points.begin() + *FirstUnprocessedRequest + 1 : Points.end();
                result.insert(result.begin(), Points.begin(), it);
            } else {
                auto it = FirstUnprocessedRequest ? Points.begin() + *FirstUnprocessedRequest : Points.begin();
                result.insert(result.begin(), it, Points.end());
            }
        }

        void FillEvRead(TEvDataShard::TEvRead& ev, TConstArrayRef<NScheme::TTypeInfo> keyTypes, bool reversed) {
            if (Ranges.empty()) {
                FillUnprocessedPoints(ev.Keys, reversed);
            } else {
                FillUnprocessedRanges(ev.Ranges, keyTypes, reversed);
            }
        }

        TString ToString(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
            TStringBuilder sb;
            sb << "TShardState{ TabletId: " << TabletId << ", Last Key " << PrintLastKey(keyTypes)
                << ", Ranges: [";
            for (size_t i = 0; i < Ranges.size(); ++i) {
                sb << "#" << i << ": " << DebugPrintRange(keyTypes, Ranges[i].ToTableRange(), *AppData()->TypeRegistry);
                if (i + 1 != Ranges.size()) {
                    sb << ", ";
                }
            }
            sb << "], "
                << ", RetryAttempt: " << RetryAttempt << ", ResolveAttempt: " << ResolveAttempt << " }";
            return sb;
        }

        TString PrintLastKey(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
            if (LastKey.empty()) {
                return "<none>";
            }
            return DebugPrintPoint(keyTypes, LastKey, *AppData()->TypeRegistry);
        }
    };

    using TShardQueue = TIntrusiveListWithAutoDelete<TShardState, TDelete>;

    struct TReadState {
        TShardState* Shard = nullptr;
        bool Finished = false;
        ui64 LastSeqNo;
        TMaybe<TString> SerializedContinuationToken;

        void RegisterMessage(const TEvDataShard::TEvReadResult& result) {
            LastSeqNo = result.Record.GetSeqNo();
            Finished = result.Record.GetFinished();
        }

        bool IsLastMessage(const TEvDataShard::TEvReadResult& result) {
            return result.Record.GetFinished() || (Finished && result.Record.GetSeqNo() == LastSeqNo);
        }

        operator bool () {
            return Shard;
        }

        void Reset() {
            Shard = nullptr;
        }
    };

public:
    TKqpReadActor(
        NKikimrTxDataShard::TKqpReadRangesSourceSettings&& settings,
        const NYql::NDq::TDqAsyncIoFactory::TSourceArguments& args)
        : Settings(std::move(settings))
        , LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ", CA Id " << args.ComputeActorId << ". ")
        , ComputeActorId(args.ComputeActorId)
        , InputIndex(args.InputIndex)
        , TypeEnv(args.TypeEnv)
        , HolderFactory(args.HolderFactory)
        , Alloc(args.Alloc)
    {
        TableId = TTableId(
            Settings.GetTable().GetTableId().GetOwnerId(),
            Settings.GetTable().GetTableId().GetTableId(),
            Settings.GetTable().GetSysViewInfo(),
            Settings.GetTable().GetTableId().GetSchemaVersion()
        );

        KeyColumnTypes.reserve(Settings.GetKeyColumnTypes().size());
        for (size_t i = 0; i < Settings.KeyColumnTypesSize(); ++i) {
            auto typeId = Settings.GetKeyColumnTypes(i);
            KeyColumnTypes.push_back(
                NScheme::TTypeInfo(
                    (NScheme::TTypeId)typeId,
                    (typeId == NScheme::NTypeIds::Pg) ?
                        NPg::TypeDescFromPgTypeId(
                            Settings.GetKeyColumnTypeInfos(i).GetPgTypeId()
                        ) : nullptr));
        }
    }

    virtual ~TKqpReadActor() {
        if (!Results.empty() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Results.clear();
        }
    }

    STFUNC(ReadyState) {
        Y_UNUSED(ctx);
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDataShard::TEvReadResult, HandleRead);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        THolder<TShardState> stateHolder = MakeHolder<TShardState>(Settings.GetShardIdHint());
        PendingShards.PushBack(stateHolder.Get());
        auto& state = *stateHolder.Release();

        if (Settings.HasFullRange()) {
            state.Ranges.push_back(TSerializedTableRange(Settings.GetFullRange()));
        } else {
            YQL_ENSURE(Settings.HasRanges());
            if (Settings.GetRanges().KeyRangesSize() > 0) {
                YQL_ENSURE(Settings.GetRanges().KeyPointsSize() == 0);
                for (const auto& range : Settings.GetRanges().GetKeyRanges()) {
                    state.Ranges.push_back(TSerializedTableRange(range));
                }
            } else {
                for (const auto& point : Settings.GetRanges().GetKeyPoints()) {
                    state.Points.push_back(TSerializedCellVec(point));
                }
            }
        }

        if (!Settings.HasShardIdHint()) {
            state.NeedResolve = true;
            ResolveShard(&state);
        } else {
            StartTableScan();
        }
        Become(&TKqpReadActor::ReadyState);
    }

    bool StartTableScan() {
        const ui32 maxAllowedInFlight = Settings.GetSorted() ? 1 : MaxInFlight;
        bool isFirst = true;
        while (!PendingShards.Empty() && RunningReads() + 1 <= maxAllowedInFlight) {
            if (isFirst) {
                CA_LOG_D("BEFORE: " << PendingShards.Size() << "." << RunningReads());
                isFirst = false;
            }
            if (Settings.GetReverse()) {
                auto state = THolder<TShardState>(PendingShards.PopBack());
                InFlightShards.PushBack(state.Get());
                StartRead(state.Release());
            } else {
                auto state = THolder<TShardState>(PendingShards.PopFront());
                InFlightShards.PushFront(state.Get());
                StartRead(state.Release());
            }
        }
        if (!isFirst) {
            CA_LOG_D("AFTER: " << PendingShards.Size() << "." << RunningReads());
        }

        CA_LOG_D("Scheduled table scans, in flight: " << RunningReads() << " shards. "
            << "pending shards to read: " << PendingShards.Size() << ", ");

        return RunningReads() > 0 || !PendingShards.Empty();
    }

    void ResolveShard(TShardState* state) {
        if (state->ResolveAttempt >= MAX_SHARD_RESOLVES) {
            RuntimeError(TStringBuilder() << "Table '" << Settings.GetTable().GetTablePath() << "' resolve limit exceeded",
                NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        state->ResolveAttempt++;

        auto range = state->GetBounds();
        TVector<TKeyDesc::TColumnOp> columns;
        columns.reserve(Settings.GetColumns().size());
        for (const auto& column : Settings.GetColumns()) {
            TKeyDesc::TColumnOp op;
            op.Column = column.GetId();
            op.Operation = TKeyDesc::EColumnOperation::Read;
            op.ExpectedType = MakeTypeInfo(column);
            columns.emplace_back(std::move(op));
        }

        auto keyDesc = MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Read,
            KeyColumnTypes, columns);

        CA_LOG_D("Sending TEvResolveKeySet update for table '" << Settings.GetTable().GetTablePath() << "'"
            << ", range: " << DebugPrintRange(KeyColumnTypes, range, *AppData()->TypeRegistry)
            << ", attempt #" << state->ResolveAttempt);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(std::move(keyDesc));

        request->ResultSet.front().UserData = ResolveShardId;
        ResolveShards[ResolveShardId] = state;
        ResolveShardId += 1;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        CA_LOG_D("Received TEvResolveKeySetResult update for table '" << Settings.GetTable().GetTablePath() << "'");

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            CA_LOG_E("Resolve request failed for table '" << Settings.GetTable().GetTablePath() << "', ErrorCount# " << request->ErrorCount);

            auto statusCode = NDqProto::StatusIds::UNAVAILABLE;
            TString error;

            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32)NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    // invalidate table
                    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));

                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            error = TStringBuilder() << "Table '" << Settings.GetTable().GetTablePath() << "' not exists.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            error = TStringBuilder() << "Table '" << Settings.GetTable().GetTablePath() << "' scheme changed.";
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::LookupError:
                            statusCode = NDqProto::StatusIds::UNAVAILABLE;
                            error = TStringBuilder() << "Failed to resolve table '" << Settings.GetTable().GetTablePath() << "'.";
                            break;
                        default:
                            statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                            error = TStringBuilder() << "Unresolved table '" << Settings.GetTable().GetTablePath() << "'. Status: " << x.Status;
                            break;
                    }
                }
            }

            return RuntimeError(error, statusCode);
        }

        auto keyDesc = std::move(request->ResultSet[0].KeyDescription);
        THolder<TShardState> state;
        if (auto ptr = ResolveShards[request->ResultSet[0].UserData]) {
            state = THolder<TShardState>(ptr);
            ResolveShards.erase(request->ResultSet[0].UserData);
        } else {
            return;
        }

        if (keyDesc->GetPartitions().size() == 1 && !state->NeedResolve) {
            // we re-resolved the same shard
            RuntimeError(TStringBuilder() << "too many retries for shard " << state->TabletId, NDqProto::StatusIds::StatusIds::INTERNAL_ERROR);
            PendingShards.PushBack(state.Release());
            return;
        }

        if (keyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << Settings.GetTable().GetTablePath() << "'";
            CA_LOG_E(error);
            return RuntimeError(error, NDqProto::StatusIds::SCHEME_ERROR);
        }

        const auto& tr = *AppData()->TypeRegistry;

        TVector<THolder<TShardState>> newShards;
        newShards.reserve(keyDesc->GetPartitions().size());

        for (ui64 idx = 0, i = 0; idx < keyDesc->GetPartitions().size(); ++idx) {
            const auto& partition = keyDesc->GetPartitions()[idx];

            TTableRange partitionRange{
                idx == 0 ? state->Ranges.front().From.GetCells() : keyDesc->GetPartitions()[idx - 1].Range->EndKeyPrefix.GetCells(),
                idx == 0 ? state->Ranges.front().FromInclusive : !keyDesc->GetPartitions()[idx - 1].Range->IsInclusive,
                keyDesc->GetPartitions()[idx].Range->EndKeyPrefix.GetCells(),
                keyDesc->GetPartitions()[idx].Range->IsInclusive
            };

            CA_LOG_D("Processing resolved ShardId# " << partition.ShardId
                << ", partition range: " << DebugPrintRange(KeyColumnTypes, partitionRange, tr)
                << ", i: " << i << ", state ranges: " << state->Ranges.size());

            auto newShard = MakeHolder<TShardState>(partition.ShardId);

            if (((!Settings.GetReverse() && idx == 0) || (Settings.GetReverse() && idx + 1 == keyDesc->GetPartitions().size())) && state) {
                newShard->AssignContinuationToken(state.Get());
            }

            for (ui64 j = i; j < state->Ranges.size(); ++j) {
                CA_LOG_D("Intersect state range #" << j << " " << DebugPrintRange(KeyColumnTypes, state->Ranges[j].ToTableRange(), tr)
                    << " with partition range " << DebugPrintRange(KeyColumnTypes, partitionRange, tr));

                auto intersection = Intersect(KeyColumnTypes, partitionRange, state->Ranges[j].ToTableRange());

                if (!intersection.IsEmptyRange(KeyColumnTypes)) {
                    CA_LOG_D("Add range to new shardId: " << partition.ShardId
                        << ", range: " << DebugPrintRange(KeyColumnTypes, intersection, tr));

                    newShard->Ranges.emplace_back(TSerializedTableRange(intersection));
                } else {
                    CA_LOG_D("empty intersection");
                    if (j > i) {
                        i = j - 1;
                    }
                    break;
                }
            }

            if (!newShard->Ranges.empty()) {
                newShards.push_back(std::move(newShard));
            }
        }

        YQL_ENSURE(!newShards.empty());
        if (Settings.GetReverse()) {
            for (size_t i = 0; i < newShards.size(); ++i) {
                PendingShards.PushBack(newShards[i].Release());
            }
        } else {
            for (int i = newShards.ysize() - 1; i >= 0; --i) {
                PendingShards.PushFront(newShards[i].Release());
            }
        }

        if (IsDebugLogEnabled(TlsActivationContext->ActorSystem(), NKikimrServices::KQP_COMPUTE)
            && PendingShards.Size() + RunningReads() > 0)
        {
            TStringBuilder sb;
            if (!PendingShards.Empty()) {
                sb << "Pending shards States: ";
                for (auto& st : PendingShards) {
                    sb << st.ToString(KeyColumnTypes) << "; ";
                }
            }

            if (!InFlightShards.Empty()) {
                sb << "In Flight shards States: ";
                for (auto& st : InFlightShards) {
                    sb << st.ToString(KeyColumnTypes) << "; ";
                }
            }
            CA_LOG_D(sb);
        }
        StartTableScan();
    }

    void RetryRead(ui64 id) {
        if (!Reads[id]) {
            return;
        }

        auto state = Reads[id].Shard;
        Reads[id].Finished = true;

        state->RetryAttempt += 1;
        if (state->RetryAttempt >= MAX_SHARD_RETRIES) {
            return ResolveShard(state);
        }
        CA_LOG_D("Retrying read #" << id);

        auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
        cancel->Record.SetReadId(id);
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(cancel.Release(), state->TabletId), IEventHandle::FlagTrackDelivery);

        if (Reads[id].SerializedContinuationToken) {
            NKikimrTxDataShard::TReadContinuationToken token;
            Y_VERIFY(token.ParseFromString(*(Reads[id].SerializedContinuationToken)), "Failed to parse continuation token");
            state->FirstUnprocessedRequest = token.GetFirstUnprocessedQuery();

            if (token.GetLastProcessedKey()) {
                TSerializedCellVec vec(token.GetLastProcessedKey());
                state->LastKey = TOwnedCellVec(vec.GetCells());
            }
        }

        StartRead(state);
    }

    void StartRead(TShardState* state) {
        TMaybe<ui64> limit;
        if (Settings.GetItemsLimit()) {
            limit = Settings.GetItemsLimit() - Min(Settings.GetItemsLimit(), RecievedRowCount);

            if (*limit == 0) {
                return;
            }
        }

        auto ev = ::DefaultReadSettings();
        auto& record = ev->Record;

        state->FillEvRead(*ev, KeyColumnTypes, Settings.GetReverse());
        for (const auto& column : Settings.GetColumns()) {
            if (!IsSystemColumn(column.GetId())) {
                record.AddColumns(column.GetId());
            }
        }

        if (Settings.HasSnapshot()) {
            record.MutableSnapshot()->SetTxId(Settings.GetSnapshot().GetTxId());
            record.MutableSnapshot()->SetStep(Settings.GetSnapshot().GetStep());
        }

        //if (RuntimeSettings.Timeout) {
        //    ev->Record.SetTimeoutMs(RuntimeSettings.Timeout.Get()->MilliSeconds());
        //}
        //ev->Record.SetStatsMode(RuntimeSettings.StatsMode);
        //ev->Record.SetTxId(std::get<ui64>(TxId));

        auto id = ReadId++;
        Reads.resize(ReadId);
        Reads[id].Shard = state;
        state->ReadId = id;

        record.SetReadId(id);

        record.MutableTableId()->SetOwnerId(Settings.GetTable().GetTableId().GetOwnerId());
        record.MutableTableId()->SetTableId(Settings.GetTable().GetTableId().GetTableId());
        record.MutableTableId()->SetSchemaVersion(Settings.GetTable().GetSchemaVersion());

        record.SetReverse(Settings.GetReverse());
        if (limit) {
            record.SetMaxRows(*limit);
        }

        record.SetResultFormat(Settings.GetDataFormat());

        if (Settings.HasLockTxId() && BrokenLocks.empty()) {
            record.SetLockTxId(Settings.GetLockTxId());
        }

        if (Settings.HasLockNodeId()) {
            record.SetLockNodeId(Settings.GetLockNodeId());
        }

        CA_LOG_D(TStringBuilder() << "Send EvRead to shardId: " << state->TabletId << ", tablePath: " << Settings.GetTable().GetTablePath()
            << ", ranges: " << DebugPrintRanges(KeyColumnTypes, ev->Ranges, *AppData()->TypeRegistry)
            << ", limit: " << limit
            << ", readId = " << id
            << " snapshot = (txid=" << Settings.GetSnapshot().GetTxId() << ",step=" << Settings.GetSnapshot().GetStep() << ")"
            << " lockTxId = " << Settings.GetLockTxId());

        ReadIdByTabletId[state->TabletId].push_back(id);
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), state->TabletId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr ev) {
        const auto& record = ev->Get()->Record;
        auto id = record.GetReadId();
        if (!Reads[id] || Reads[id].Finished) {
            // dropped read
            return;
        }

        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            for (auto& issue : record.GetStatus().GetIssues()) {
                CA_LOG_D("read id #" << id << " got issue " << issue.Getmessage());
            }
            return RetryRead(id);
        }
        for (auto& lock : record.GetTxLocks()) {
            Locks.push_back(lock);
        }

        for (auto& lock : record.GetBrokenTxLocks()) {
            BrokenLocks.push_back(lock);
        }

        CA_LOG_D("Taken " << Locks.size() << " locks");
        Reads[id].SerializedContinuationToken = record.GetContinuationToken();

        Reads[id].RegisterMessage(*ev->Get());


        RecievedRowCount += ev->Get()->GetRowsCount();
        Results.push({Reads[id].Shard->TabletId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())});
        CA_LOG_D(TStringBuilder() << "new data for read #" << id << " pushed");
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto& msg = *ev->Get();

        TVector<ui32> reads;
        reads.swap(ReadIdByTabletId[msg.TabletId]);
        for (auto read : reads) {
            CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered);
            RetryRead(read);
        }
    }

    size_t RunningReads() const {
        return Reads.size() - ResetReads;
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    NMiniKQL::TBytesStatistics GetRowSize(const NUdf::TUnboxedValue* row) {
        NMiniKQL::TBytesStatistics rowStats{0, 0};
        size_t columnIndex = 0;
        for (size_t resultColumnIndex = 0; resultColumnIndex < Settings.ColumnsSize(); ++resultColumnIndex) {
            if (IsSystemColumn(Settings.GetColumns(resultColumnIndex).GetId())) {
                rowStats.AllocatedBytes += sizeof(NUdf::TUnboxedValue);
            } else {
                rowStats.AddStatistics(NMiniKQL::GetUnboxedValueSize(row[columnIndex], MakeTypeInfo(Settings.GetColumns(resultColumnIndex))));
                columnIndex += 1;
            }
        }
        if (Settings.ColumnsSize() == 0) {
            rowStats.AddStatistics({sizeof(ui64), sizeof(ui64)});
        }
        return rowStats;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    NMiniKQL::TBytesStatistics PackArrow(TResult& handle) {
        auto& [shardId, result, batch, _] = handle;
        NMiniKQL::TBytesStatistics stats;
        bool hasResultColumns = false;
        if (result->Get()->GetRowsCount() == 0) {
            return {};
        }
        if (Settings.ColumnsSize() == 0) {
            batch->resize(result->Get()->GetRowsCount(), HolderFactory.GetEmptyContainer());
        } else {
            TVector<NUdf::TUnboxedValue*> editAccessors(result->Get()->GetRowsCount());
            batch->reserve(result->Get()->GetRowsCount());

            for (ui64 rowIndex = 0; rowIndex < result->Get()->GetRowsCount(); ++rowIndex) {
                batch->emplace_back(HolderFactory.CreateDirectArrayHolder(
                    Settings.columns_size(),
                    editAccessors[rowIndex])
                );
            }

            size_t columnIndex = 0;
            for (size_t resultColumnIndex = 0; resultColumnIndex < Settings.ColumnsSize(); ++resultColumnIndex) {
                auto tag = Settings.GetColumns(resultColumnIndex).GetId();
                auto type = NScheme::TTypeInfo((NScheme::TTypeId)Settings.GetColumns(resultColumnIndex).GetType());
                if (IsSystemColumn(tag)) {
                    for (ui64 rowIndex = 0; rowIndex < result->Get()->GetRowsCount(); ++rowIndex) {
                        NMiniKQL::FillSystemColumn(editAccessors[rowIndex][resultColumnIndex], shardId, tag, type);
                        stats.AllocatedBytes += sizeof(NUdf::TUnboxedValue);
                    }
                } else {
                    hasResultColumns = true;
                    stats.AddStatistics(
                        NMiniKQL::WriteColumnValuesFromArrow(editAccessors, *result->Get()->GetArrowBatch(), columnIndex, resultColumnIndex, type)
                    );
                    columnIndex += 1;
                }
            }
        }

        if (!hasResultColumns) {
            auto rowsCnt = result->Get()->GetRowsCount();
            stats.AddStatistics({sizeof(ui64) * rowsCnt, sizeof(ui64) * rowsCnt});
        }
        return stats;
    }

    NMiniKQL::TBytesStatistics PackCells(TResult& handle) {
        auto& [shardId, result, batch, _] = handle;
        NMiniKQL::TBytesStatistics stats;
        batch->reserve(batch->size());
        for (size_t rowIndex = 0; rowIndex < result->Get()->GetRowsCount(); ++rowIndex) {
            const auto& row = result->Get()->GetCells(rowIndex);
            NUdf::TUnboxedValue* rowItems = nullptr;
            batch->emplace_back(HolderFactory.CreateDirectArrayHolder(Settings.ColumnsSize(), rowItems));
            size_t rowSize = 0;
            size_t columnIndex = 0;
            for (size_t resultColumnIndex = 0; resultColumnIndex < Settings.ColumnsSize(); ++resultColumnIndex) {
                auto tag = Settings.GetColumns(resultColumnIndex).GetId();
                auto type = MakeTypeInfo(Settings.GetColumns(resultColumnIndex));
                if (IsSystemColumn(tag)) {
                    NMiniKQL::FillSystemColumn(rowItems[resultColumnIndex], shardId, tag, type);
                } else {
                    rowItems[resultColumnIndex] = NMiniKQL::GetCellValue(row[columnIndex], type);
                    rowSize += row[columnIndex].Size(); 
                    columnIndex += 1;
                }
            }
            stats.DataBytes += std::max(rowSize, (size_t)8);
            stats.AllocatedBytes += GetRowSize(rowItems).AllocatedBytes;
        }
        return stats;
    }

    i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueVector& resultVector,
        TMaybe<TInstant>&,
        bool& finished,
        i64 freeSpace) override
    {
        ui64 bytes = 0;
        while (!Results.empty()) {
            auto& result = Results.front();
            auto& batch = result.Batch;
            auto& msg = *result.ReadResult->Get();
            if (!batch.Defined()) {
                batch.ConstructInPlace();
                switch (msg.Record.GetResultFormat()) {
                    case NKikimrTxDataShard::EScanDataFormat::ARROW:
                        BytesStats.AddStatistics(PackArrow(result));
                        break;
                    case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED:
                    case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
                        BytesStats.AddStatistics(PackCells(result));
                }
            }

            auto id = result.ReadResult->Get()->Record.GetReadId();
            if (!Reads[id]) {
                Results.pop();
                continue;
            }
            auto* state = Reads[id].Shard;

            for (; result.ProcessedRows < batch->size(); ++result.ProcessedRows) {
                NMiniKQL::TBytesStatistics rowSize = GetRowSize((*batch)[result.ProcessedRows].GetElements());
                if (static_cast<ui64>(freeSpace) < bytes + rowSize.AllocatedBytes) {
                    break;
                }
                resultVector.push_back(std::move((*batch)[result.ProcessedRows]));
                ProcessedRowCount += 1;
                bytes += rowSize.AllocatedBytes;
                if (ProcessedRowCount == Settings.GetItemsLimit()) {
                    finished = true;
                    return bytes;
                }
            }
            CA_LOG_D(TStringBuilder() << "returned " << resultVector.size() << " rows");

            if (batch->size() == result.ProcessedRows) {
                auto& record = msg.Record;
                if (Reads[id].IsLastMessage(msg)) {
                    Reads[id].Reset();
                    ResetReads++;
                } else if (!Reads[id].Finished) {
                    TMaybe<ui64> limit;
                    if (Settings.GetItemsLimit()) {
                        limit = Settings.GetItemsLimit() - Min(Settings.GetItemsLimit(), RecievedRowCount);
                    }

                    if (!limit || *limit > 0) {
                        auto request = ::DefaultAckSettings();
                        request->Record.SetReadId(record.GetReadId());
                        request->Record.SetSeqNo(record.GetSeqNo());
                        if (limit) {
                            request->Record.SetMaxRows(*limit);
                        }
                        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), state->TabletId, true),
                            IEventHandle::FlagTrackDelivery);
                    } else {
                        auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
                        cancel->Record.SetReadId(id);
                        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(cancel.Release(), state->TabletId), IEventHandle::FlagTrackDelivery);
                        Reads[id].Reset();
                        ResetReads++;
                    }
                }

                StartTableScan();

                Results.pop();
                CA_LOG_D("dropping batch");

                if (RunningReads() == 0 || (Settings.GetItemsLimit() && ProcessedRowCount >= Settings.GetItemsLimit())) {
                    finished = true;
                    break;
                }
            } else {
                break;
            }
        }

        return bytes;
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last) override {
        if (last) {
            NDqProto::TDqTableStats* tableStats = nullptr;
            for (size_t i = 0; i < stats->TablesSize(); ++i) {
                auto* table = stats->MutableTables(i);
                if (table->GetTablePath() == Settings.GetTable().GetTablePath()) {
                    tableStats = table;
                }
            }
            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(Settings.GetTable().GetTablePath());

            }

            //FIXME: use evread statistics after KIKIMR-16924
            tableStats->SetReadRows(tableStats->GetReadRows() + RecievedRowCount);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + BytesStats.DataBytes);
            tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + InFlightShards.Size());
        }
    }


    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDqProto::TSourceState&) override {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) override {}
    void LoadState(const NYql::NDqProto::TSourceState&) override {}

    void PassAway() override {
        {
            auto guard = BindAllocator();
            Results.clear();
        }
        TBase::PassAway();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
        for (auto& lock : Locks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        for (auto& lock : BrokenLocks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        result.PackFrom(resultInfo);
        return result;
    }


    NScheme::TTypeInfo MakeTypeInfo(const NKikimrTxDataShard::TKqpTransaction_TColumnMeta& info) {
        auto typeId = info.GetType();
        return NScheme::TTypeInfo(
            (NScheme::TTypeId)typeId,
            (typeId == NScheme::NTypeIds::Pg) ?
                NPg::TypeDescFromPgTypeId(
                    info.GetTypeInfo().GetPgTypeId()
                ) : nullptr);
    }

private:
    NKikimrTxDataShard::TKqpReadRangesSourceSettings Settings;

    TVector<NScheme::TTypeInfo> KeyColumnTypes;

    NMiniKQL::TBytesStatistics BytesStats;
    ui64 RecievedRowCount = 0;
    ui64 ProcessedRowCount = 0;
    ui64 ResetReads = 0;
    ui64 ReadId = 0;
    TVector<TReadState> Reads;
    THashMap<ui64, TVector<ui32>> ReadIdByTabletId;

    THashMap<ui64, TShardState*> ResolveShards;
    ui64 ResolveShardId = 0;

    TShardQueue InFlightShards;
    TShardQueue PendingShards;

    TQueue<TResult> Results;

    TVector<NKikimrTxDataShard::TLock> Locks;
    TVector<NKikimrTxDataShard::TLock> BrokenLocks;

    ui32 MaxInFlight = 1024;
    TString LogPrefix;
    TTableId TableId;

    const TActorId ComputeActorId;
    const ui64 InputIndex;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};


void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory& factory) {
    factory.RegisterSource<NKikimrTxDataShard::TKqpReadRangesSourceSettings>(
        TString(NYql::KqpReadRangesSourceName),
        [] (NKikimrTxDataShard::TKqpReadRangesSourceSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSourceArguments&& args) {
            auto* actor = new TKqpReadActor(std::move(settings), args);
            return std::make_pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(actor, actor);
        });
}

void InjectRangeEvReadSettings(const NKikimrTxDataShard::TEvRead& read) {
    ::DefaultRangeEvReadSettings.Data.MergeFrom(read);
}

void InjectRangeEvReadAckSettings(const NKikimrTxDataShard::TEvReadAck& ack) {
    ::DefaultRangeEvReadAckSettings.Data.MergeFrom(ack);
}

} // namespace NKqp
} // namespace NKikimr
