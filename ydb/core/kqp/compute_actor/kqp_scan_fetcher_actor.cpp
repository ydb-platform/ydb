#include "kqp_scan_fetcher_actor.h"
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

namespace NKikimr::NKqp::NScanPrivate {

namespace {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr::NKqp::NComputeActor;

static constexpr ui64 MAX_SHARD_RETRIES = 5; // retry after: 0, 250, 500, 1000, 2000
static constexpr ui64 MAX_TOTAL_SHARD_RETRIES = 20;
static constexpr ui64 MAX_SHARD_RESOLVES = 3;

} // anonymous namespace


TKqpScanFetcherActor::TKqpScanFetcherActor(const NKikimrKqp::TKqpSnapshot& snapshot,
    const TComputeRuntimeSettings& settings, std::vector<NActors::TActorId>&& computeActors, const ui64 txId,
    const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta, const TShardsScanningPolicy& shardsScanningPolicy,
    TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId)
    : Meta(meta)
    , ScanDataMeta(Meta)
    , RuntimeSettings(settings)
    , TxId(txId)
    , ComputeActorIds(std::move(computeActors))
    , Snapshot(snapshot)
    , ShardsScanningPolicy(shardsScanningPolicy)
    , Counters(counters)
    , InFlightShards(ScanId, *this)
    , InFlightComputes(ComputeActorIds)
{
    Y_UNUSED(traceId);
    AFL_ENSURE(!Meta.GetReads().empty());
    AFL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "META:" << meta.DebugString();
    KeyColumnTypes.reserve(Meta.GetKeyColumnTypes().size());
    for (size_t i = 0; i < Meta.KeyColumnTypesSize(); i++) {
        auto typeId = Meta.GetKeyColumnTypes().at(i);
        KeyColumnTypes.push_back(NScheme::TTypeInfo(
            (NScheme::TTypeId)typeId,
            (typeId == NScheme::NTypeIds::Pg) ?
            NPg::TypeDescFromPgTypeId(
                Meta.GetKeyColumnTypeInfos().at(i).GetPgTypeId()
            ) : nullptr
        ));
    }
}

TVector<NKikimr::TSerializedTableRange> TKqpScanFetcherActor::BuildSerializedTableRanges(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& readData) {
    TVector<TSerializedTableRange> resultLocal;
    resultLocal.reserve(readData.GetKeyRanges().size());
    for (const auto& range : readData.GetKeyRanges()) {
        auto& sr = resultLocal.emplace_back(TSerializedTableRange(range));
        if (!range.HasTo()) {
            sr.To = sr.From;
            sr.FromInclusive = sr.ToInclusive = true;
        }
    }
    Y_DEBUG_ABORT_UNLESS(!resultLocal.empty());
    return resultLocal;
}

void TKqpScanFetcherActor::Bootstrap() {
    LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ". ";

    ShardsScanningPolicy.FillRequestScanFeatures(Meta, MaxInFlight, IsAggregationRequest);
    for (const auto& read : Meta.GetReads()) {
        auto& state = PendingShards.emplace_back(TShardState(read.GetShardId()));
        state.Ranges = BuildSerializedTableRanges(read);
    }
    for (auto&& c : ComputeActorIds) {
        Sender<TEvScanExchange::TEvRegisterFetcher>().SendTo(c);
    }
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "bootstrap")("compute", ComputeActorIds.size())("shards", PendingShards.size());
    StartTableScan();
    Become(&TKqpScanFetcherActor::StateFunc);
}

void TKqpScanFetcherActor::HandleExecute(TEvScanExchange::TEvAckData::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->GetFreeSpace());
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "AckDataFromCompute")("self_id", SelfId())("scan_id", ScanId)
        ("packs_to_send", InFlightComputes.GetPacksToSendCount())
        ("from", ev->Sender)("shards remain", PendingShards.size())
        ("in flight scans", InFlightShards.GetScansCount())
        ("in flight shards", InFlightShards.GetShardsCount());
    InFlightComputes.OnComputeAck(ev->Sender, ev->Get()->GetFreeSpace());
    CheckFinish();
}

void TKqpScanFetcherActor::HandleExecute(TEvScanExchange::TEvTerminateFromCompute::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "TEvTerminateFromCompute")("sender", ev->Sender)("info", ev->Get()->GetIssues().ToOneLineString());
    TStringBuilder sb;
    sb << "Send abort execution from compute actor, message: " << ev->Get()->GetIssues().ToOneLineString();

    InFlightShards.AbortAllScanners(sb);
    PassAway();
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = ev->Get()->Record;
    auto scanActorId = ActorIdFromProto(msg.GetScanActorId());
    InFlightShards.RegisterScannerActor(msg.GetTabletId(), msg.GetGeneration(), scanActorId);
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto state = InFlightShards.GetShardStateByActorId(ev->Sender);
    if (!state || state->Generation != ev->Get()->Generation) {
        return;
    }
    AFL_ENSURE(state->State == EShardState::Running)("state", state->State)("actor_id", state->ActorId)("ev_sender", ev->Sender);

    TInstant startTime = TActivationContext::Now();
    if (ev->Get()->Finished) {
        state->State = EShardState::PostRunning;
    }
    PendingScanData.emplace_back(std::make_pair(ev, startTime));

    ProcessScanData();
    CheckFinish();
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = ev->Get()->Record;

    Ydb::StatusIds::StatusCode status = msg.GetStatus();
    TIssues issues;
    IssuesFromMessage(msg.GetIssues(), issues);

    CA_LOG_W("Got EvScanError scan state: "
        << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
        << ", reason: " << issues.ToString()
        << ", tablet id: " << msg.GetTabletId() << ", actor_id: " << ev->Sender);

    auto state = InFlightShards.GetShardStateByActorId(ev->Sender);
    if (!state) {
        state = InFlightShards.GetShardState(msg.GetTabletId());
        if (!state) {
            AFL_WARN(NKikimrServices::KQP_COMPUTE)("event", "incorrect_error_source")("actor_id", ev->Sender)("tablet_id", msg.GetTabletId());
            return;
        }
    }
    if (state->Generation != ev->Get()->Record.GetGeneration()) {
        return;
    }

    if (state->State == EShardState::Starting) {
        ++TotalRetries;
        if (TotalRetries >= MAX_TOTAL_SHARD_RETRIES) {
            CA_LOG_E("TKqpScanFetcherActor: broken tablet for this request " << state->TabletId
                << ", retries limit exceeded (" << state->TotalRetries << "/" << TotalRetries << ")");
            SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, YdbStatusToDqStatus(status), issues);
            return PassAway();
        }

        if (FindSchemeErrorInIssues(status, issues)) {
            return EnqueueResolveShard(state);
        }
        SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, YdbStatusToDqStatus(status), issues);
        return PassAway();
    }

    if (state->State == EShardState::PostRunning || state->State == EShardState::Running) {
        CA_LOG_E("TKqpScanFetcherActor: broken tablet for this request " << state->TabletId
            << ", retries limit exceeded (" << state->TotalRetries << "/" << TotalRetries << ")");
        SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, YdbStatusToDqStatus(status), issues);
        return PassAway();
    }
}

void TKqpScanFetcherActor::HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = *ev->Get();

    auto state = InFlightShards.GetShardState(msg.TabletId);
    if (!state) {
        return;
    }

    const auto shardState = state->State;
    CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered << ", " << shardState);
    if (state->State == EShardState::Starting || state->State == EShardState::Running) {
        return RetryDeliveryProblem(state);
    }
}

void TKqpScanFetcherActor::HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto state = InFlightShards.GetShardStateVerified(ev->Get()->TabletId);
    InFlightShards.StartScanner(*state);
}

void TKqpScanFetcherActor::HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    AFL_ENSURE(!PendingResolveShards.empty());
    auto state = std::move(PendingResolveShards.front());
    PendingResolveShards.pop_front();
    ResolveNextShard();

    Y_ABORT_UNLESS(!InFlightShards.GetShardScanner(state.TabletId));

    AFL_ENSURE(state.State == EShardState::Resolving);
    CA_LOG_D("Received TEvResolveKeySetResult update for table '" << ScanDataMeta.TablePath << "'");

    auto* request = ev->Get()->Request.Get();
    if (request->ErrorCount > 0) {
        CA_LOG_E("Resolve request failed for table '" << ScanDataMeta.TablePath << "', ErrorCount# " << request->ErrorCount);

        auto statusCode = NDqProto::StatusIds::UNAVAILABLE;
        auto issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
        TString error;

        for (const auto& x : request->ResultSet) {
            if ((ui32)x.Status < (ui32)NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                // invalidate table
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanDataMeta.TableId, {}));

                switch (x.Status) {
                    case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                        statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                        issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                        error = TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' not exists.";
                        break;
                    case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                        statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                        issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                        error = TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' scheme changed.";
                        break;
                    case NSchemeCache::TSchemeCacheRequest::EStatus::LookupError:
                        statusCode = NDqProto::StatusIds::UNAVAILABLE;
                        issueCode = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
                        error = TStringBuilder() << "Failed to resolve table '" << ScanDataMeta.TablePath << "'.";
                        break;
                    default:
                        statusCode = NDqProto::StatusIds::SCHEME_ERROR;
                        issueCode = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                        error = TStringBuilder() << "Unresolved table '" << ScanDataMeta.TablePath << "'. Status: " << x.Status;
                        break;
                }
            }
        }

        SendGlobalFail(statusCode, issueCode, error);
        return;
    }

    auto keyDesc = std::move(request->ResultSet[0].KeyDescription);

    if (keyDesc->GetPartitions().empty()) {
        TString error = TStringBuilder() << "No partitions to read from '" << ScanDataMeta.TablePath << "'";
        CA_LOG_E(error);
        SendGlobalFail(NDqProto::StatusIds::SCHEME_ERROR, TIssuesIds::KIKIMR_SCHEME_MISMATCH, error);
        return;
    }

    const auto& tr = *AppData()->TypeRegistry;

    TVector<TShardState> newShards;
    newShards.reserve(keyDesc->GetPartitions().size());

    for (ui64 idx = 0, i = 0; idx < keyDesc->GetPartitions().size(); ++idx) {
        const auto& partition = keyDesc->GetPartitions()[idx];

        TTableRange partitionRange{
            idx == 0 ? state.Ranges.front().From.GetCells() : keyDesc->GetPartitions()[idx - 1].Range->EndKeyPrefix.GetCells(),
            idx == 0 ? state.Ranges.front().FromInclusive : !keyDesc->GetPartitions()[idx - 1].Range->IsInclusive,
            keyDesc->GetPartitions()[idx].Range->EndKeyPrefix.GetCells(),
            keyDesc->GetPartitions()[idx].Range->IsInclusive
        };

        CA_LOG_D("Processing resolved ShardId# " << partition.ShardId
            << ", partition range: " << DebugPrintRange(KeyColumnTypes, partitionRange, tr)
            << ", i: " << i << ", state ranges: " << state.Ranges.size());

        auto newShard = TShardState(partition.ShardId);

        for (ui64 j = i; j < state.Ranges.size(); ++j) {
            auto comparison = CompareRanges(partitionRange, state.Ranges[j].ToTableRange(), KeyColumnTypes);
            CA_LOG_D("Compare range #" << j << " " << DebugPrintRange(KeyColumnTypes, state.Ranges[j].ToTableRange(), tr)
                << " with partition range " << DebugPrintRange(KeyColumnTypes, partitionRange, tr)
                << " : " << comparison);

            if (comparison > 0) {
                continue;
            } else if (comparison == 0) {
                auto intersection = Intersect(KeyColumnTypes, partitionRange, state.Ranges[j].ToTableRange());
                CA_LOG_D("Add range to new shardId: " << partition.ShardId
                    << ", range: " << DebugPrintRange(KeyColumnTypes, intersection, tr));

                newShard.Ranges.emplace_back(TSerializedTableRange(intersection));
            } else {
                break;
            }
            i = j;
        }

        if (!newShard.Ranges.empty()) {
            newShards.emplace_back(std::move(newShard));
        }
    }

    AFL_ENSURE(!newShards.empty());

    for (int i = newShards.ysize() - 1; i >= 0; --i) {
        PendingShards.emplace_front(std::move(newShards[i]));
    }

    if (!state.LastKey.empty()) {
        PendingShards.front().LastKey = std::move(state.LastKey);
        while(!PendingShards.empty() && PendingShards.front().GetScanRanges(KeyColumnTypes).empty()) {
            CA_LOG_D("Nothing to read " << PendingShards.front().ToString(KeyColumnTypes));
            auto readShard = std::move(PendingShards.front());
            PendingShards.pop_front();
            PendingShards.front().LastKey = std::move(readShard.LastKey);
        }

        AFL_ENSURE(!PendingShards.empty());
    }
    StartTableScan();
}

void TKqpScanFetcherActor::HandleExecute(TEvents::TEvUndelivered::TPtr& ev) {
    switch (ev->Get()->SourceType) {
        case TEvDataShard::TEvKqpScan::EventType:
            // Handled by TEvPipeCache::TEvDeliveryProblem event.
            return;
        case TEvKqpCompute::TEvScanDataAck::EventType:
            if (!!InFlightShards.GetShardScanner(ev->Cookie)) {
                SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR, "Delivery problem: EvScanDataAck lost.");
            }
            return;
    }
    Y_ABORT("UNEXPECTED EVENT TYPE");
}

void TKqpScanFetcherActor::HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    auto nodeId = ev->Get()->NodeId;
    CA_LOG_N("Disconnected node " << nodeId);

    TrackingNodes.erase(nodeId);
    SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
        TStringBuilder() << "Connection with node " << nodeId << " lost.");
}

bool TKqpScanFetcherActor::SendGlobalFail(const NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssuesIds::EIssueCode issueCode, const TString& message) const {
    for (auto&& i : ComputeActorIds) {
        Send(i, new TEvScanExchange::TEvTerminateFromFetcher(statusCode, issueCode, message));
    }
    return true;
}

bool TKqpScanFetcherActor::SendGlobalFail(const NDqProto::EComputeState state, NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues) const {
    for (auto&& i : ComputeActorIds) {
        Send(i, new TEvScanExchange::TEvTerminateFromFetcher(state, statusCode, issues));
    }
    return true;
}

bool TKqpScanFetcherActor::SendScanFinished() {
    for (auto&& i : ComputeActorIds) {
        Sender<TEvScanExchange::TEvFetcherFinished>().SendTo(i);
    }
    return true;
}

std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> TKqpScanFetcherActor::BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const {
    auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
    ev->Record.SetLocalPathId(ScanDataMeta.TableId.PathId.LocalPathId);
    for (auto& column : ScanDataMeta.GetColumns()) {
        ev->Record.AddColumnTags(column.Tag);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
        ev->Record.AddColumnTypes(columnType.TypeId);
        if (columnType.TypeInfo) {
            *ev->Record.AddColumnTypeInfos() = *columnType.TypeInfo;
        } else {
            *ev->Record.AddColumnTypeInfos() = NKikimrProto::TTypeInfo();
        }
    }
    ev->Record.MutableSkipNullKeys()->CopyFrom(Meta.GetSkipNullKeys());

    auto protoRanges = ev->Record.MutableRanges();
    protoRanges->Reserve(ranges.size());

    for (auto& range : ranges) {
        auto newRange = protoRanges->Add();
        range.Serialize(*newRange);
    }

    ev->Record.MutableSnapshot()->CopyFrom(Snapshot);
    if (RuntimeSettings.Timeout) {
        ev->Record.SetTimeoutMs(RuntimeSettings.Timeout.Get()->MilliSeconds());
    }
    ev->Record.SetStatsMode(RuntimeSettings.StatsMode);
    ev->Record.SetScanId(scanId);
    ev->Record.SetTxId(std::get<ui64>(TxId));
    ev->Record.SetTablePath(ScanDataMeta.TablePath);
    ev->Record.SetSchemaVersion(ScanDataMeta.TableId.SchemaVersion);

    ev->Record.SetGeneration(gen);

    ev->Record.SetReverse(Meta.GetReverse());
    ev->Record.SetItemsLimit(Meta.GetItemsLimit());

    if (Meta.GroupByColumnNamesSize()) {
        ev->Record.MutableComputeShardingPolicy()->SetShardsCount(ComputeActorIds.size());
        for (auto&& i : Meta.GetGroupByColumnNames()) {
            ev->Record.MutableComputeShardingPolicy()->AddColumnNames(i);
        }
    }

    if (Meta.HasOlapProgram()) {
        TString programBytes;
        TStringOutput stream(programBytes);
        Meta.GetOlapProgram().SerializeToArcadiaStream(&stream);
        ev->Record.SetOlapProgram(programBytes);
        ev->Record.SetOlapProgramType(
            NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS
        );
    }

    ev->Record.SetDataFormat(Meta.GetDataFormat());
    return ev;
}

void TKqpScanFetcherActor::ProcessPendingScanDataItem(TEvKqpCompute::TEvScanData::TPtr& ev, const TInstant& enqueuedAt) noexcept {
    auto& msg = *ev->Get();

    auto state = InFlightShards.GetShardStateByActorId(ev->Sender);
    if (!state) {
        return;
    }

    TDuration latency;
    if (enqueuedAt != TInstant::Zero()) {
        latency = TActivationContext::Now() - enqueuedAt;
        Counters->ScanQueryRateLimitLatency->Collect(latency.MilliSeconds());
    }

    AFL_ENSURE(state->ActorId == ev->Sender)("expected", state->ActorId)("got", ev->Sender);

    state->LastKey = std::move(msg.LastKey);
    const ui64 rowsCount = msg.GetRowsCount();
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("action","got EvScanData")("rows", rowsCount)("finished", msg.Finished)("exceeded", msg.RequestedBytesLimitReached)
        ("scan", ScanId)("packs_to_send", InFlightComputes.GetPacksToSendCount())
        ("from", ev->Sender)("shards remain", PendingShards.size())
        ("in flight scans", InFlightShards.GetScansCount())
        ("in flight shards", InFlightShards.GetShardsCount())
        ("delayed_for_seconds_by_ratelimiter", latency.SecondsFloat())
        ("tablet_id", state->TabletId);
    auto shardScanner = InFlightShards.GetShardScannerVerified(state->TabletId);
    auto tasksForCompute = shardScanner->OnReceiveData(msg, shardScanner);
    AFL_ENSURE(tasksForCompute.size() == 1 || tasksForCompute.size() == 0 || tasksForCompute.size() == ComputeActorIds.size())("size", tasksForCompute.size())("compute_size", ComputeActorIds.size());
    for (auto&& i : tasksForCompute) {
        const std::optional<ui32> computeShardId = i->GetComputeShardId();
        InFlightComputes.OnReceiveData(computeShardId, std::move(i));
    }

    state->AvailablePacks = msg.AvailablePacks;
    InFlightShards.MutableStatistics(state->TabletId).AddPack(rowsCount, 0);
    Stats.AddReadStat(state->TabletId, rowsCount, 0);

    CA_LOG_D("EVLOGKQP:" << IsAggregationRequest << "/" << Meta.GetItemsLimit() << "/" << InFlightShards.GetTotalRowsCount() << "/" << rowsCount);
    if (msg.Finished) {
        Stats.CompleteShard(state);
        InFlightShards.StopScanner(state->TabletId);
        StartTableScan();
    }
}

void TKqpScanFetcherActor::ProcessScanData() {
    AFL_ENSURE(!PendingScanData.empty());

    auto ev = std::move(PendingScanData.front().first);
    auto enqueuedAt = std::move(PendingScanData.front().second);
    PendingScanData.pop_front();

    auto state = InFlightShards.GetShardStateByActorId(ev->Sender);
    if (!state)
        return;

    AFL_ENSURE(state->State == EShardState::Running || state->State == EShardState::PostRunning)("state", state->State);
    ProcessPendingScanDataItem(ev, enqueuedAt);
}

void TKqpScanFetcherActor::StartTableScan() {
    const ui32 maxAllowedInFlight = MaxInFlight;
    bool isFirst = true;
    while (!PendingShards.empty() && GetShardsInProgressCount() + 1 <= maxAllowedInFlight) {
        if (isFirst) {
            CA_LOG_D("BEFORE: " << PendingShards.size() << " + " << InFlightShards.GetScansCount() << " + " << PendingResolveShards.size());
            isFirst = false;
        }
        auto state = InFlightShards.Put(std::move(PendingShards.front()));
        PendingShards.pop_front();
        InFlightShards.StartScanner(*state);
    }
    if (!isFirst) {
        CA_LOG_D("AFTER: " << PendingShards.size() << "." << InFlightShards.GetScansCount() << "." << PendingResolveShards.size());
    }

    CA_LOG_D("Scheduled table scans, in flight: " << InFlightShards.GetScansCount() << " shards. "
        << "pending shards to read: " << PendingShards.size() << ", "
        << "pending resolve shards: " << PendingResolveShards.size() << ", "
        << "average read rows: " << Stats.AverageReadRows() << ", "
        << "average read bytes: " << Stats.AverageReadBytes() << ", ");

}

void TKqpScanFetcherActor::RetryDeliveryProblem(TShardState::TPtr state) {
    InFlightShards.StopScanner(state->TabletId, false);
    Counters->ScanQueryShardDisconnect->Inc();

    if (state->TotalRetries >= MAX_TOTAL_SHARD_RETRIES) {
        CA_LOG_E("TKqpScanFetcherActor: broken pipe with tablet " << state->TabletId
            << ", retries limit exceeded (" << state->TotalRetries << ")");
        SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            TStringBuilder() << "Retries limit with shard " << state->TabletId << " exceeded.");
        return;
    }

    // note: it might be possible that shard is already removed after successful split/merge operation and cannot be found
    // in this case the next TEvKqpScan request will receive the delivery problem response.
    // so after several consecutive delivery problem responses retry logic should
    // resolve shard details again.
    if (state->RetryAttempt >= MAX_SHARD_RETRIES) {
        state->ResetRetry();
        Send(state->ActorId, new NActors::TEvents::TEvPoisonPill());
        return EnqueueResolveShard(state);
    }

    ++TotalRetries;
    auto retryDelay = state->CalcRetryDelay();
    CA_LOG_W("TKqpScanFetcherActor: broken pipe with tablet " << state->TabletId
        << ", restarting scan from last received key " << state->PrintLastKey(KeyColumnTypes)
        << ", attempt #" << state->RetryAttempt << " (total " << state->TotalRetries << ")"
        << " schedule after " << retryDelay);

    state->RetryTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), retryDelay,
        new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryShard(state->TabletId, state->Generation)));
}

void TKqpScanFetcherActor::ResolveShard(TShardState& state) {
    if (state.ResolveAttempt >= MAX_SHARD_RESOLVES) {
        SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            TStringBuilder() << "Table '" << ScanDataMeta.TablePath << "' resolve limit exceeded");
        return;
    }

    Counters->ScanQueryShardResolve->Inc();

    state.State = EShardState::Resolving;
    state.ResolveAttempt++;
    state.SubscribedOnTablet = false;

    auto range = TTableRange(state.Ranges.front().From.GetCells(), state.Ranges.front().FromInclusive,
        state.Ranges.back().To.GetCells(), state.Ranges.back().ToInclusive);

    TVector<TKeyDesc::TColumnOp> columns;
    columns.reserve(ScanDataMeta.GetColumns().size());
    for (const auto& column : ScanDataMeta.GetColumns()) {
        TKeyDesc::TColumnOp op;
        op.Column = column.Tag;
        op.Operation = TKeyDesc::EColumnOperation::Read;
        op.ExpectedType = column.Type;
        columns.emplace_back(std::move(op));
    }

    auto keyDesc = MakeHolder<TKeyDesc>(ScanDataMeta.TableId, range, TKeyDesc::ERowOperation::Read,
        KeyColumnTypes, columns);

    CA_LOG_D("Sending TEvResolveKeySet update for table '" << ScanDataMeta.TablePath << "'"
        << ", range: " << DebugPrintRange(KeyColumnTypes, range, *AppData()->TypeRegistry)
        << ", attempt #" << state.ResolveAttempt);

    auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
    request->ResultSet.emplace_back(std::move(keyDesc));
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(ScanDataMeta.TableId, {}));
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
}

void TKqpScanFetcherActor::EnqueueResolveShard(const std::shared_ptr<TShardState>& state) {
    CA_LOG_D("Enqueue for resolve " << state->TabletId);
    InFlightShards.StopScanner(state->TabletId);
    PendingResolveShards.emplace_back(*state);
    if (PendingResolveShards.size() == 1) {
        ResolveNextShard();
    }
}

void TKqpScanFetcherActor::StopOnError(const TString& errorMessage) const {
    CA_LOG_E("unexpected problem: " << errorMessage);
    TIssue issue(errorMessage);
    TIssues issues;
    issues.AddIssue(std::move(issue));
    SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, NYql::NDqProto::StatusIds::INTERNAL_ERROR, issues);
}

ui32 TKqpScanFetcherActor::GetShardsInProgressCount() const {
    return InFlightShards.GetShardsCount() + PendingResolveShards.size();
}

void TKqpScanFetcherActor::CheckFinish() {
    if (GetShardsInProgressCount() == 0 && InFlightComputes.GetPacksToSendCount() == 0) {
        SendScanFinished();
        InFlightShards.Stop();
        CA_LOG_D("EVLOGKQP(max_in_flight:" << MaxInFlight << ")"
            << Endl << InFlightShards.GetDurationStats()
            << Endl << InFlightShards.StatisticsToString()
        );
        PassAway();
    }
}

}
