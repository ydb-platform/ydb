#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/actorlib_impl/long_timer.h>

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
        Data.SetMaxBytes(5_MB);
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
        Data.SetMaxBytes(5_MB);
    }

} DefaultRangeEvReadAckSettings;

THolder<NKikimr::TEvDataShard::TEvReadAck> DefaultAckSettings() {
    auto result = MakeHolder<NKikimr::TEvDataShard::TEvReadAck>();
    result->Record.MergeFrom(DefaultRangeEvReadAckSettings.Data);
    return result;
}

NActors::TActorId PipeCacheId = NKikimr::MakePipePeNodeCacheID(false);

TDuration StartRetryDelay = TDuration::MilliSeconds(250);

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
        size_t PackedRows = 0;

        TResult(ui64 shardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>> readResult)
            : ShardId(shardId)
            , ReadResult(std::move(readResult))
        {
        }
    };

    struct TShardState : public TIntrusiveListItem<TShardState> {

        TOwnedCellVec LastKey;
        TMaybe<ui32> FirstUnprocessedRequest;
        TMaybe<ui32> ReadId;
        ui64 TabletId;

        TVector<Ydb::Issue::IssueMessage> Issues;

        size_t ResolveAttempt = 0;
        size_t RetryAttempt = 0;

        TShardState(ui64 tabletId)
            : TabletId(tabletId)
        {
        }

        TTableRange GetBounds(bool reverse) {
            if (Ranges.empty()) {
                YQL_ENSURE(!Points.empty());
                if (reverse) {
                    return TTableRange(
                        Points.front().GetCells(), true,
                        Points[FirstUnprocessedRequest.GetOrElse(Points.size() - 1)].GetCells(), true);
                } else {
                    return TTableRange(
                        Points[FirstUnprocessedRequest.GetOrElse(0)].GetCells(), true,
                        Points.back().GetCells(), true);
                }
            } else {
                if (reverse) {
                    if (LastKey.empty()) {
                        return TTableRange(
                            Ranges.front().From.GetCells(), Ranges.front().FromInclusive,
                            Ranges[FirstUnprocessedRequest.GetOrElse(Ranges.size() - 1)].To.GetCells(), Ranges.back().ToInclusive);
                    } else {
                        return TTableRange(
                            Ranges.front().From.GetCells(), Ranges.front().FromInclusive,
                            LastKey, false);
                    }
                } else {
                    if (LastKey.empty()) {
                        return TTableRange(
                            Ranges[FirstUnprocessedRequest.GetOrElse(0)].From.GetCells(), Ranges.front().FromInclusive,
                            Ranges.back().To.GetCells(), Ranges.back().ToInclusive);
                    } else {
                        return TTableRange(
                            LastKey, false,
                            Ranges.back().To.GetCells(), Ranges.back().ToInclusive);
                    }
                }
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
                auto rangeIt = Ranges.begin() + FirstUnprocessedRequest.GetOrElse(Ranges.size() - 1);

                if (!lastKeyEmpty) {
                    // It is range, where read was interrupted. Restart operation from last read key.
                    result.emplace_back(std::move(TSerializedTableRange(
                        rangeIt->From.GetBuffer(), TSerializedCellVec::Serialize(LastKey), rangeIt->ToInclusive, false
                        )));
                } else {
                    ++rangeIt;
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
                for (auto& range : ev.Ranges) {
                    MakePrefixRange(range, keyTypes.size());
                }
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

        bool HasRanges() {
            return !Ranges.empty();
        }

        bool HasPoints() {
            return !Points.empty();
        }

        void AddRange(TSerializedTableRange&& range) {
            Ranges.push_back(std::move(range));
        }

        void AddPoint(TSerializedCellVec&& point) {
            Points.push_back(std::move(point));
        }

    private:
        TSmallVec<TSerializedTableRange> Ranges;
        TSmallVec<TSerializedCellVec> Points;
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
            Finished = true;
        }
    };

    enum EEv {
        EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    };

    struct TEvRetryShard: public TEventLocal<TEvRetryShard, EvRetryShard> {
    public:
        explicit TEvRetryShard(const ui64 readId, const ui64 maxSeqNo)
            : ReadId(readId)
            , MaxSeqNo(maxSeqNo)
        {
        }
    public:
        ui64 ReadId = 0;
        ui64 MaxSeqNo = 0;
    };

public:
    TKqpReadActor(
        NKikimrTxDataShard::TKqpReadRangesSourceSettings&& settings,
        const NYql::NDq::TDqAsyncIoFactory::TSourceArguments& args,
        TIntrusivePtr<TKqpCounters> counters)
        : Settings(std::move(settings))
        , LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ", CA Id " << args.ComputeActorId << ". ")
        , ComputeActorId(args.ComputeActorId)
        , InputIndex(args.InputIndex)
        , TypeEnv(args.TypeEnv)
        , HolderFactory(args.HolderFactory)
        , Alloc(args.Alloc)
        , Counters(counters)
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
        Counters->ReadActorsCount->Inc();
        Snapshot = IKqpGateway::TKqpSnapshot(Settings.GetSnapshot().GetStep(), Settings.GetSnapshot().GetTxId());
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SOURCE_READ_ACTOR;
    }

    virtual ~TKqpReadActor() {
        if (!Results.empty() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Results.clear();
        }
    }

    STFUNC(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDataShard::TEvReadResult, HandleRead);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
                hFunc(TEvRetryShard, HandleRetry);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
    }

    void StartTableScan() {
        ScanStarted = true;
        THolder<TShardState> stateHolder = MakeHolder<TShardState>(Settings.GetShardIdHint());
        PendingShards.PushBack(stateHolder.Get());
        auto& state = *stateHolder.Release();

        if (Settings.HasFullRange()) {
            state.AddRange(TSerializedTableRange(Settings.GetFullRange()));
        } else {
            YQL_ENSURE(Settings.HasRanges());
            if (Settings.GetRanges().KeyRangesSize() > 0) {
                YQL_ENSURE(Settings.GetRanges().KeyPointsSize() == 0);
                for (const auto& range : Settings.GetRanges().GetKeyRanges()) {
                    state.AddRange(TSerializedTableRange(range));
                }
            } else {
                for (const auto& point : Settings.GetRanges().GetKeyPoints()) {
                    state.AddPoint(TSerializedCellVec(point));
                }
            }
        }

        if (!Settings.HasShardIdHint()) {
            ResolveShard(&state);
        } else {
            StartShards();
        }
        Become(&TKqpReadActor::ReadyState);
    }

    bool StartShards() {
        const ui32 maxAllowedInFlight = Settings.GetSorted() ? 1 : MaxInFlight;
        CA_LOG_D("effective maxinflight " << maxAllowedInFlight << " sorted " << Settings.GetSorted());
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

        Counters->IteratorsShardResolve->Inc();
        state->ResolveAttempt++;

        auto range = state->GetBounds(Settings.GetReverse());
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

        if (keyDesc->GetPartitions().size() == 1) {
            auto& partition = keyDesc->GetPartitions()[0];
            if (partition.ShardId == state->TabletId) {
                // we re-resolved the same shard
                NYql::TIssues issues;
                for (auto& issue : state->Issues) {
                    issues.AddIssue(issue.message());
                }
                RuntimeError(TStringBuilder() << "Too many retries for shard " << state->TabletId, NDqProto::StatusIds::StatusIds::INTERNAL_ERROR, issues);
                PendingShards.PushBack(state.Release());
                return;
            }
        } else if (!Snapshot.IsValid()) {
            return RuntimeError("inconsistent reads after shards split", NDqProto::StatusIds::INTERNAL_ERROR);
        }

        if (keyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << Settings.GetTable().GetTablePath() << "'";
            CA_LOG_E(error);
            return RuntimeError(error, NDqProto::StatusIds::SCHEME_ERROR);
        }

        const auto& tr = *AppData()->TypeRegistry;

        TVector<THolder<TShardState>> newShards;
        newShards.reserve(keyDesc->GetPartitions().size());

        auto bounds = state->GetBounds(Settings.GetReverse());
        size_t pointIndex = 0;
        size_t rangeIndex = 0;
        TVector<TSerializedTableRange> ranges;
        if (state->HasRanges()) {
            state->FillUnprocessedRanges(ranges, KeyColumnTypes, Settings.GetReverse());
        }

        TVector<TSerializedCellVec> points;
        if (state->HasPoints()) {
            state->FillUnprocessedPoints(points, Settings.GetReverse());
        }

        for (ui64 idx = 0; idx < keyDesc->GetPartitions().size(); ++idx) {
            const auto& partition = keyDesc->GetPartitions()[idx];

            TTableRange partitionRange{
                idx == 0 ? bounds.From : keyDesc->GetPartitions()[idx - 1].Range->EndKeyPrefix.GetCells(),
                idx == 0 ? bounds.InclusiveFrom : !keyDesc->GetPartitions()[idx - 1].Range->IsInclusive,
                keyDesc->GetPartitions()[idx].Range->EndKeyPrefix.GetCells(),
                keyDesc->GetPartitions()[idx].Range->IsInclusive
            };

            CA_LOG_D("Processing resolved ShardId# " << partition.ShardId
                << ", partition range: " << DebugPrintRange(KeyColumnTypes, partitionRange, tr)
                << ", i: " << rangeIndex << ", state ranges: " << ranges.size()
                << ", points: " << points.size());

            auto newShard = MakeHolder<TShardState>(partition.ShardId);

            if (state->HasRanges()) {
                for (ui64 j = rangeIndex; j < ranges.size(); ++j) {
                    CA_LOG_D("Intersect state range #" << j << " " << DebugPrintRange(KeyColumnTypes, ranges[j].ToTableRange(), tr)
                        << " with partition range " << DebugPrintRange(KeyColumnTypes, partitionRange, tr));

                    auto intersection = Intersect(KeyColumnTypes, partitionRange, ranges[j].ToTableRange());

                    if (!intersection.IsEmptyRange(KeyColumnTypes)) {
                        CA_LOG_D("Add range to new shardId: " << partition.ShardId
                            << ", range: " << DebugPrintRange(KeyColumnTypes, intersection, tr));

                        newShard->AddRange(TSerializedTableRange(intersection));
                    } else {
                        CA_LOG_D("empty intersection");
                        if (j > rangeIndex) {
                            rangeIndex = j - 1;
                        }
                        break;
                    }
                }

                if (newShard->HasRanges()) {
                    newShards.push_back(std::move(newShard));
                }
            }
            if (state->HasPoints()) {
                while (pointIndex < points.size()) {
                    int intersection = ComparePointAndRange(
                        points[pointIndex].GetCells(),
                        partitionRange,
                        KeyColumnTypes,
                        KeyColumnTypes);

                    if (intersection == 0) {
                        newShard->AddPoint(std::move(points[pointIndex]));
                        CA_LOG_D("Add point to new shardId: " << partition.ShardId);
                    }
                    if (intersection < 0) {
                        break;
                    }
                    pointIndex += 1;
                }
                if (newShard->HasPoints()) {
                    newShards.push_back(std::move(newShard));
                }
            }
        }

        YQL_ENSURE(!newShards.empty());
        Counters->IteratorsReadSplits->Add(newShards.size() - 1);
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
        StartShards();
    }

    void HandleRetry(TEvRetryShard::TPtr& ev) {
        auto& read = Reads[ev->Get()->ReadId];
        if (read.LastSeqNo <= ev->Get()->MaxSeqNo) {
            DoRetryRead(ev->Get()->ReadId);
        }
    }

    void RetryRead(ui64 id, bool allowInstantRetry = true) {
        if (!Reads[id]) {
            return;
        }

        auto state = Reads[id].Shard;
        if (state->RetryAttempt == 0 && allowInstantRetry) { // instant retry
            return DoRetryRead(id);
        }
        auto delay = ::StartRetryDelay;
        for (size_t i = 0; i < state->RetryAttempt; ++i) {
            delay *= 2;
        }

        CA_LOG_D("schedule retry #" << id << " after " << delay);
        TlsActivationContext->Schedule(delay, new IEventHandle(SelfId(), SelfId(), new TEvRetryShard(id, Reads[id].LastSeqNo)));
    }

    void DoRetryRead(ui64 id) {
        if (!Reads[id]) {
            return;
        }

        auto state = Reads[id].Shard;

        state->RetryAttempt += 1;
        if (state->RetryAttempt >= MAX_SHARD_RETRIES) {
            ResetRead(id);
            return ResolveShard(state);
        }
        CA_LOG_D("Retrying read #" << id);

        ResetRead(id);

        if (Reads[id].SerializedContinuationToken) {
            NKikimrTxDataShard::TReadContinuationToken token;
            Y_VERIFY(token.ParseFromString(*(Reads[id].SerializedContinuationToken)), "Failed to parse continuation token");
            state->FirstUnprocessedRequest = token.GetFirstUnprocessedQuery();

            if (token.GetLastProcessedKey()) {
                TSerializedCellVec vec(token.GetLastProcessedKey());
                state->LastKey = TOwnedCellVec(vec.GetCells());
            }
        }

        Counters->ReadActorRetries->Inc();
        StartRead(state);
    }

    void StartRead(TShardState* state) {
        TMaybe<ui64> limit;
        if (Settings.GetItemsLimit()) {
            limit = Settings.GetItemsLimit() - Min(Settings.GetItemsLimit(), ReceivedRowCount);

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

        if (Snapshot.IsValid()) {
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
            record.MutableSnapshot()->SetStep(Snapshot.Step);
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
        record.SetMaxBytes(Min<ui64>(record.GetMaxBytes(), BufSize));

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
            << ", reverse = " << record.GetReverse()
            << " snapshot = (txid=" << Settings.GetSnapshot().GetTxId() << ",step=" << Settings.GetSnapshot().GetStep() << ")"
            << " lockTxId = " << Settings.GetLockTxId());

        Counters->CreatedIterators->Inc();
        ReadIdByTabletId[state->TabletId].push_back(id);
        Send(::PipeCacheId, new TEvPipeCache::TEvForward(ev.Release(), state->TabletId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void NotifyCA() {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    TString DebugPrintContionuationToken(TString s) {
        NKikimrTxDataShard::TReadContinuationToken token;
        Y_VERIFY(token.ParseFromString(s));
        TString lastKey = "(empty)";
        if (!token.GetLastProcessedKey().empty()) {
            TStringBuilder builder;
            TVector<NScheme::TTypeInfo> types;
            for (auto& column : Settings.GetColumns()) {
                types.push_back(NScheme::TTypeInfo((NScheme::TTypeId)column.GetType()));
            }

            TSerializedCellVec row(token.GetLastProcessedKey());

            lastKey = DebugPrintPoint(types, row.GetCells(), *AppData()->TypeRegistry);
        }
        return TStringBuilder() << "first request = " << token.GetFirstUnprocessedQuery() << " lastkey = " << lastKey;
    }

    void HandleRead(TEvDataShard::TEvReadResult::TPtr ev) {
        const auto& record = ev->Get()->Record;
        auto id = record.GetReadId();
        if (!Reads[id] || Reads[id].Finished) {
            // dropped read
            return;
        }

        Counters->DataShardIteratorMessages->Inc();
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Counters->DataShardIteratorFails->Inc();
        }

        for (auto& issue : record.GetStatus().GetIssues()) {
            CA_LOG_D("read id #" << id << " got issue " << issue.Getmessage());
            Reads[id].Shard->Issues.push_back(issue);
        }
        switch (record.GetStatus().GetCode()) {
            case Ydb::StatusIds::SUCCESS:
                break;
            case Ydb::StatusIds::OVERLOADED: {
                return RetryRead(id, false);
            }
            case Ydb::StatusIds::INTERNAL_ERROR: {
                return RetryRead(id);
            }
            case Ydb::StatusIds::NOT_FOUND: {
                auto shard = Reads[id].Shard;
                ResetRead(id);
                return ResolveShard(shard);
            }
            default: {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(record.GetStatus().GetIssues(), issues);
                return RuntimeError("Read request aborted", NYql::NDqProto::StatusIds::ABORTED, issues);
            }
        }

        for (auto& lock : record.GetTxLocks()) {
            Locks.push_back(lock);
        }

        if (!Snapshot.IsValid()) {
            Snapshot = IKqpGateway::TKqpSnapshot(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
        }

        for (auto& lock : record.GetBrokenTxLocks()) {
            BrokenLocks.push_back(lock);
        }

        CA_LOG_D("Taken " << Locks.size() << " locks");
        Reads[id].SerializedContinuationToken = record.GetContinuationToken();

        Reads[id].RegisterMessage(*ev->Get());


        ReceivedRowCount += ev->Get()->GetRowsCount();
        CA_LOG_D(TStringBuilder() << "new data for read #" << id
            << " seqno = " << ev->Get()->Record.GetSeqNo()
            << " finished = " << ev->Get()->Record.GetFinished());
        CA_LOG_T(TStringBuilder() << "read #" << id << " pushed " << DebugPrintCells(ev->Get()) << " continuation token " << DebugPrintContionuationToken(record.GetContinuationToken()));
        Results.push({Reads[id].Shard->TabletId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())});
        NotifyCA();
    }

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto& msg = *ev->Get();

        TVector<ui32> reads;
        reads = ReadIdByTabletId[msg.TabletId];
        CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered);
        for (auto read : reads) {
            if (Reads[read]) {
                Counters->IteratorDeliveryProblems->Inc();
            }
            RetryRead(read);
        }
    }

    size_t RunningReads() const {
        return Reads.size() - ResetReads;
    }

    void ResetRead(size_t id) {
        if (Reads[id]) {
            Counters->SentIteratorCancels->Inc();
            auto* state = Reads[id].Shard;
            auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
            cancel->Record.SetReadId(id);
            Send(::PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), state->TabletId, false));

            Reads[id].Reset();
            ResetReads++;
        }
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

    NMiniKQL::TBytesStatistics PackArrow(TResult& handle, i64& freeSpace) {
        auto& [shardId, result, batch, _, packed] = handle;
        NMiniKQL::TBytesStatistics stats;
        bool hasResultColumns = false;
        if (result->Get()->GetRowsCount() == 0) {
            return {};
        }
        YQL_ENSURE(packed == 0);
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
        freeSpace -= static_cast<i64>(stats.AllocatedBytes);
        packed = result->Get()->GetRowsCount();
        return stats;
    }

    TString DebugPrintCells(const TEvDataShard::TEvReadResult* result) {
        if (result->Record.GetResultFormat() == NKikimrTxDataShard::EScanDataFormat::ARROW) {
            return "{ARROW}";
        }
        TStringBuilder builder;
        TVector<NScheme::TTypeInfo> types;
        for (auto& column : Settings.GetColumns()) {
            types.push_back(NScheme::TTypeInfo((NScheme::TTypeId)column.GetType()));
        }

        for (size_t rowIndex = 0; rowIndex < result->GetRowsCount(); ++rowIndex) {
            const auto& row = result->GetCells(rowIndex);
            builder << "|" << DebugPrintPoint(types, row, *AppData()->TypeRegistry);
        }
        return builder;
    }

    NMiniKQL::TBytesStatistics PackCells(TResult& handle, i64& freeSpace) {
        auto& [shardId, result, batch, _, packed] = handle;
        NMiniKQL::TBytesStatistics stats;
        batch->reserve(batch->size());
        for (size_t rowIndex = packed; rowIndex < result->Get()->GetRowsCount(); ++rowIndex) {
            const auto& row = result->Get()->GetCells(rowIndex);
            NUdf::TUnboxedValue* rowItems = nullptr;
            batch->emplace_back(HolderFactory.CreateDirectArrayHolder(Settings.ColumnsSize(), rowItems));

            i64 rowSize = 0;
            size_t columnIndex = 0;
            for (size_t resultColumnIndex = 0; resultColumnIndex < Settings.ColumnsSize(); ++resultColumnIndex) {
                auto tag = Settings.GetColumns(resultColumnIndex).GetId();
                if (!IsSystemColumn(tag)) {
                    rowSize += row[columnIndex].Size();
                    columnIndex += 1;
                }
            }
            // min row size according to datashard
            rowSize = std::max(rowSize, (i64)8);

            columnIndex = 0;
            for (size_t resultColumnIndex = 0; resultColumnIndex < Settings.ColumnsSize(); ++resultColumnIndex) {
                auto tag = Settings.GetColumns(resultColumnIndex).GetId();
                auto type = MakeTypeInfo(Settings.GetColumns(resultColumnIndex));
                if (IsSystemColumn(tag)) {
                    NMiniKQL::FillSystemColumn(rowItems[resultColumnIndex], shardId, tag, type);
                } else {
                    rowItems[resultColumnIndex] = NMiniKQL::GetCellValue(row[columnIndex], type);
                    columnIndex += 1;
                }
            }

            stats.DataBytes += rowSize;
            stats.AllocatedBytes += GetRowSize(rowItems).AllocatedBytes;
            freeSpace -= rowSize;
            packed = rowIndex + 1;

            if (freeSpace <= 0) {
                break;
            }
        }
        return stats;
    }

    bool LimitReached() const {
        return Settings.GetItemsLimit() && ProcessedRowCount >= Settings.GetItemsLimit();
    }

    i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueVector& resultVector,
        TMaybe<TInstant>&,
        bool& finished,
        i64 freeSpace) override
    {
        if (!ScanStarted) {
            BufSize = freeSpace;
            StartTableScan();
            return 0;
        }

        CA_LOG_D(TStringBuilder() << " enter getasyncinputdata results size " << Results.size());
        ui64 bytes = 0;
        while (!Results.empty()) {
            auto& result = Results.front();

            auto& batch = result.Batch;
            auto& msg = *result.ReadResult->Get();
            if (!batch.Defined()) {
                batch.ConstructInPlace();
                switch (msg.Record.GetResultFormat()) {
                    case NKikimrTxDataShard::EScanDataFormat::ARROW:
                        BytesStats.AddStatistics(PackArrow(result, freeSpace));
                        break;
                    case NKikimrTxDataShard::EScanDataFormat::UNSPECIFIED:
                    case NKikimrTxDataShard::EScanDataFormat::CELLVEC:
                        BytesStats.AddStatistics(PackCells(result, freeSpace));
                }
            }

            auto id = result.ReadResult->Get()->Record.GetReadId();

            for (; result.ProcessedRows < result.PackedRows; ++result.ProcessedRows) {
                NMiniKQL::TBytesStatistics rowSize = GetRowSize((*batch)[result.ProcessedRows].GetElements());
                resultVector.push_back(std::move((*batch)[result.ProcessedRows]));
                ProcessedRowCount += 1;
                bytes += rowSize.AllocatedBytes;
                if (ProcessedRowCount == Settings.GetItemsLimit()) {
                    finished = true;
                    CA_LOG_D(TStringBuilder() << " returned async data because limit reached");
                    return bytes;
                }
            }
            CA_LOG_D(TStringBuilder() << "returned " << resultVector.size() << " rows; processed " << ProcessedRowCount << " rows");

            size_t rowCount = result.ReadResult.Get()->Get()->GetRowsCount();
            if (rowCount == result.ProcessedRows) {
                auto& record = msg.Record;
                if (!Reads[id].Finished) {
                    TMaybe<ui64> limit;
                    if (Settings.GetItemsLimit()) {
                        limit = Settings.GetItemsLimit() - Min(Settings.GetItemsLimit(), ReceivedRowCount);
                    }

                    if (!limit || *limit > 0) {
                        auto request = ::DefaultAckSettings();
                        request->Record.SetReadId(record.GetReadId());
                        request->Record.SetSeqNo(record.GetSeqNo());
                        request->Record.SetMaxBytes(Min<ui64>(request->Record.GetMaxBytes(), BufSize));
                        if (limit) {
                            request->Record.SetMaxRows(*limit);
                        }
                        Counters->SentIteratorAcks->Inc();
                        CA_LOG_D("sending ack for read #" << id << " limit " << limit << " seqno = " << record.GetSeqNo());
                        Send(::PipeCacheId, new TEvPipeCache::TEvForward(request.Release(), Reads[id].Shard->TabletId, true),
                            IEventHandle::FlagTrackDelivery);
                    } else {
                        Reads[id].Finished = true;
                    }
                }

                if (Reads[id].IsLastMessage(msg)) {
                    ResetRead(id);
                }

                StartShards();

                Results.pop();
                CA_LOG_D("dropping batch for read #" << id);

                if (LimitReached()) {
                    finished = true;
                    break;
                }
            } else {
                break;
            }
        }

        if (RunningReads() == 0 && PendingShards.Empty() && ScanStarted) {
            finished = true;
        }

        CA_LOG_D(TStringBuilder() << "returned async data"
            << " processed rows " << ProcessedRowCount
            << " received rows " << ReceivedRowCount
            << " running reads " << RunningReads()
            << " pending shards " << PendingShards.Size()
            << " finished = " << finished
            << " has limit " << (Settings.GetItemsLimit() != 0)
            << " limit reached " << LimitReached());

        return bytes;
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats* mstats) override {
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

            auto consumedRows = mstats ? mstats->Inputs[InputIndex]->RowsConsumed : ReceivedRowCount;

            //FIXME: use real rows count
            tableStats->SetReadRows(tableStats->GetReadRows() + consumedRows);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + (mstats ? mstats->Inputs[InputIndex]->BytesConsumed : BytesStats.DataBytes));
            tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + InFlightShards.Size());

            //FIXME: use evread statistics after KIKIMR-16924
            //tableStats->SetReadRows(tableStats->GetReadRows() + ReceivedRowCount);
            //tableStats->SetReadBytes(tableStats->GetReadBytes() + BytesStats.DataBytes);
            //tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + InFlightShards.Size());
        }
    }


    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDqProto::TSourceState&) override {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) override {}
    void LoadState(const NYql::NDqProto::TSourceState&) override {}

    void PassAway() override {
        Counters->ReadActorsCount->Dec();
        {
            auto guard = BindAllocator();
            Results.clear();
            for (size_t i = 0; i < Reads.size(); ++i) {
                ResetRead(i);
            }
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
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
    ui64 ReceivedRowCount = 0;
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

    IKqpGateway::TKqpSnapshot Snapshot;

    ui32 MaxInFlight = 1024;
    TString LogPrefix;
    TTableId TableId;

    bool ScanStarted = false;
    size_t BufSize = 0;

    const TActorId ComputeActorId;
    const ui64 InputIndex;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    TIntrusivePtr<TKqpCounters> Counters;
};


void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSource<NKikimrTxDataShard::TKqpReadRangesSourceSettings>(
        TString(NYql::KqpReadRangesSourceName),
        [counters] (NKikimrTxDataShard::TKqpReadRangesSourceSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSourceArguments&& args) {
            auto* actor = new TKqpReadActor(std::move(settings), args, counters);
            return std::make_pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(actor, actor);
        });
}

void InjectRangeEvReadSettings(const NKikimrTxDataShard::TEvRead& read) {
    ::DefaultRangeEvReadSettings.Data.MergeFrom(read);
}

void InjectRangeEvReadAckSettings(const NKikimrTxDataShard::TEvReadAck& ack) {
    ::DefaultRangeEvReadAckSettings.Data.MergeFrom(ack);
}

void InterceptReadActorPipeCache(NActors::TActorId id) {
    ::PipeCacheId = id;
}

} // namespace NKqp
} // namespace NKikimr
