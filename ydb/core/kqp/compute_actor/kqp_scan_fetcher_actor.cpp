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
    , KqpComputeActorSpan(TWilsonKqp::ComputeActor, std::move(traceId), "KqpScanActor")
    , InFlightShards(ShardsScanningPolicy, KqpComputeActorSpan)
{
    KqpComputeActorSpan.SetEnabled(IS_DEBUG_LOG_ENABLED(NKikimrServices::KQP_COMPUTE) || KqpComputeActorSpan.GetTraceId());
    YQL_ENSURE(!Meta.GetReads().empty());
    YQL_ENSURE(Meta.GetTable().GetTableKind() != (ui32)ETableKind::SysView);
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
    Y_VERIFY_DEBUG(!resultLocal.empty());
    return resultLocal;
}

void TKqpScanFetcherActor::Bootstrap() {
    auto gTime = KqpComputeActorSpan.StartStackTimeGuard("bootstrap");
    LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ". ";
    CA_LOG_D("EVLOGKQP START");

    ShardsScanningPolicy.FillRequestScanFeatures(Meta, MaxInFlight, IsAggregationRequest);
    for (const auto& read : Meta.GetReads()) {
        auto& state = PendingShards.emplace_back(TShardState(read.GetShardId(), ++ScansCounter));
        state.Ranges = BuildSerializedTableRanges(read);
    }
    for (auto&& c : ComputeActorIds) {
        Sender<TEvScanExchange::TEvRegisterFetcher>().SendTo(c);
    }
    StartTableScan();
    Become(&TKqpScanFetcherActor::StateFunc);
}

void TKqpScanFetcherActor::HandleExecute(TEvScanExchange::TEvAckData::TPtr& ev) {
    Y_VERIFY(ev->Get()->GetFreeSpace());
    ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): " << ev->Sender;
    if (!InFlightComputes.OnComputeAck(ev->Sender, ev->Get()->GetFreeSpace())) {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): " << ev->Sender << " IGNORED";
        return;
    }
    DoAckAvailableWaiting();
}

void TKqpScanFetcherActor::HandleExecute(TEvScanExchange::TEvTerminateFromCompute::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "TEvTerminateFromCompute")("sender", ev->Sender);
    for (auto&& itTablet : InFlightShards) {
        for (auto&& it : itTablet.second) {
            auto state = it.second;
            TStringBuilder sb;
            if (state->ActorId) {
                sb << "Send abort execution event to scan over tablet: " << state->TabletId <<
                    ", scan actor: " << state->ActorId << ", message: " << ev->Get()->GetIssues().ToOneLineString();
                Send(state->ActorId, new TEvKqp::TEvAbortExecution(
                    ev->Get()->IsSuccess() ? NYql::NDqProto::StatusIds::SUCCESS : NYql::NDqProto::StatusIds::ABORTED, ev->Get()->GetIssues()));
            } else {
                sb << "Tablet: " << state->TabletId << ", scan has not been started yet";
            }
            if (ev->Get()->IsSuccess()) {
                CA_LOG_D(sb);
            } else {
                CA_LOG_W(sb);
            }
        }
    }
    PassAway();
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = ev->Get()->Record;
    auto scanActorId = ActorIdFromProto(msg.GetScanActorId());
    auto state = GetShardState(msg, scanActorId);
    if (!state)
        return;

    CA_LOG_D("Got EvScanInitActor from " << scanActorId << ", gen: " << msg.GetGeneration()
        << ", state: " << state->State << ", stateGen: " << state->Generation
        << ", tabletId: " << state->TabletId);

    YQL_ENSURE(state->Generation == msg.GetGeneration());

    if (state->State == EShardState::Starting) {
        state->State = EShardState::Running;
        state->ActorId = scanActorId;
        state->ResetRetry();

        InFlightShards.NeedAck(state);
        SendScanDataAck(state);
    } else {
        TerminateExpiredScan(scanActorId, "Got unexpected/expired EvScanInitActor, terminate it");
    }
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = *ev->Get();
    auto state = GetShardState(msg, ev->Sender);
    if (!state) {
        return;
    }
    YQL_ENSURE(state->Generation == msg.Generation);
    if (state->State != EShardState::Running) {
        return TerminateExpiredScan(ev->Sender, "Cancel expired scan");
    }

    YQL_ENSURE(state->ActorId == ev->Sender, "expected: " << state->ActorId << ", got: " << ev->Sender);
    TInstant startTime = TActivationContext::Now();
    if (ev->Get()->Finished) {
        state->State = EShardState::PostRunning;
    }
    PendingScanData.emplace_back(std::make_pair(ev, startTime));

    ProcessScanData();
}

void TKqpScanFetcherActor::HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = ev->Get()->Record;

    Ydb::StatusIds::StatusCode status = msg.GetStatus();
    TIssues issues;
    IssuesFromMessage(msg.GetIssues(), issues);

    auto state = GetShardState(msg, TActorId());
    if (!state)
        return;
    InFlightComputes.OnScanError(state->TabletId);

    CA_LOG_W("Got EvScanError scan state: " << state->State
        << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
        << ", reason: " << issues.ToString()
        << ", tablet id: " << state->TabletId);

    YQL_ENSURE(state->Generation == msg.GetGeneration());


    if (state->State == EShardState::Starting) {
        if (FindSchemeErrorInIssues(status, issues)) {
            return EnqueueResolveShard(state);
        }
        SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, YdbStatusToDqStatus(status), issues);
        return PassAway();
    }

    if (state->State == EShardState::PostRunning || state->State == EShardState::Running) {
        state->State = EShardState::Initial;
        state->ActorId = {};
        InFlightShards.ClearAckState(state);
        state->ResetRetry();
        ++TotalRetries;
        return StartReadShard(state);
    }
}

void TKqpScanFetcherActor::HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    auto& msg = *ev->Get();

    auto* states = InFlightShards.MutableByTabletId(msg.TabletId);
    if (!states) {
        CA_LOG_E("Broken pipe with unknown tablet " << msg.TabletId);
        return;
    }
    InFlightComputes.OnScanError(msg.TabletId);

    for (auto& [_, state] : *states) {
        const auto shardState = state->State;
        CA_LOG_W("Got EvDeliveryProblem, TabletId: " << msg.TabletId << ", NotDelivered: " << msg.NotDelivered << ", " << shardState);
        if (state->State == EShardState::Starting || state->State == EShardState::Running) {
            return RetryDeliveryProblem(state);
        }
    }
}

void TKqpScanFetcherActor::HandleExecute(TEvPrivate::TEvRetryShard::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    const ui32 scannerIdx = InFlightShards.GetIndexByGeneration(ev->Get()->Generation);
    auto state = InFlightShards.GetStateByIndex(scannerIdx);
    if (!state) {
        CA_LOG_E("Received retry shard for unexpected tablet " << ev->Get()->TabletId << " / " << ev->Get()->Generation);
        return;
    }

    SendStartScanRequest(state, state->Generation);
}

void TKqpScanFetcherActor::HandleExecute(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    if (!InFlightShards.IsActive()) {
        return;
    }
    YQL_ENSURE(!PendingResolveShards.empty());
    auto state = std::move(PendingResolveShards.front());
    PendingResolveShards.pop_front();
    ResolveNextShard();

    Y_VERIFY(!InFlightShards.GetStateByIndex(state.ScannerIdx));

    YQL_ENSURE(state.State == EShardState::Resolving);
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

        auto newShard = TShardState(partition.ShardId, ++ScansCounter);

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

    YQL_ENSURE(!newShards.empty());

    for (int i = newShards.ysize() - 1; i >= 0; --i) {
        PendingShards.emplace_front(std::move(newShards[i]));
    }

    if (!state.LastKey.empty()) {
        PendingShards.front().LastKey = std::move(state.LastKey);
    }

    if (IsDebugLogEnabled(TlsActivationContext->ActorSystem(), NKikimrServices::KQP_COMPUTE)
        && PendingShards.size() + InFlightShards.GetScansCount() > 0) {
        TStringBuilder sb;
        if (!PendingShards.empty()) {
            sb << "Pending shards States: ";
            for (auto& st : PendingShards) {
                sb << st.ToString(KeyColumnTypes) << "; ";
            }
        }

        if (!InFlightShards.empty()) {
            sb << "In Flight shards States: ";
            for (auto& [_, st] : InFlightShards) {
                for (auto&& [_, i] : st) {
                    sb << i->ToString(KeyColumnTypes) << "; ";
                }
            }
        }
        CA_LOG_D(sb);
    }
    StartTableScan();
}

void TKqpScanFetcherActor::HandleExecute(TEvents::TEvUndelivered::TPtr& ev) {
    switch (ev->Get()->SourceType) {
        case TEvDataShard::TEvKqpScan::EventType:
            // Handled by TEvPipeCache::TEvDeliveryProblem event.
            // CostData request is KqpScan request too.
            return;
        case TEvKqpCompute::TEvScanDataAck::EventType:
            ui64 tabletId = ev->Cookie;
            const auto& shards = InFlightShards.GetByTabletId(tabletId);
            if (shards.empty()) {
                CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", " << tabletId);
                return;
            }

            for (auto& [_, state] : shards) {
                const auto actorId = state->ActorId;
                if (state->State == EShardState::Running && ev->Sender == actorId) {
                    CA_LOG_E("TEvScanDataAck lost while running scan, terminate execution. DataShard actor: " << actorId);
                    SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                        "Delivery problem: EvScanDataAck lost.");
                } else {
                    CA_LOG_D("Skip lost TEvScanDataAck to " << ev->Sender << ", active scan actor: " << actorId);
                }
            }
            return;
    }
    Y_FAIL("UNEXPECTED EVENT TYPE");
}

void TKqpScanFetcherActor::HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    auto nodeId = ev->Get()->NodeId;
    CA_LOG_N("Disconnected node " << nodeId);

    TrackingNodes.erase(nodeId);
    for (auto& [tabletId, states] : InFlightShards) {
        for (auto&& [_, state] : states) {
            if (state->ActorId && state->ActorId.NodeId() == nodeId) {
                SendGlobalFail(NDqProto::StatusIds::UNAVAILABLE, TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Connection with node " << nodeId << " lost.");
                break;
            }
        }
    }
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

bool TKqpScanFetcherActor::ProvideDataToCompute(const NActors::TActorId& scannerId, TEvKqpCompute::TEvScanData& msg, TShardState::TPtr state) noexcept {
    if (msg.IsEmpty()) {
        InFlightComputes.OnEmptyDataReceived(state->TabletId, msg.RequestedBytesLimitReached || msg.Finished);
    } else {
        auto computeActorInfo = InFlightComputes.OnDataReceived(state->TabletId, msg.RequestedBytesLimitReached || msg.Finished);
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << SelfId() << " PROVIDING (FROM " << scannerId << " to " << computeActorInfo.GetActorId() <<
            "): used free compute " << InFlightComputes.DebugString();
        Send(computeActorInfo.GetActorId(), new TEvScanExchange::TEvSendData(msg, state->TabletId));
    }
    return true;
}

bool TKqpScanFetcherActor::SendScanFinished() {
    for (auto&& i : ComputeActorIds) {
        Sender<TEvScanExchange::TEvFetcherFinished>().SendTo(i);
    }
    return true;
}

THolder<NKikimr::TEvDataShard::TEvKqpScan> TKqpScanFetcherActor::BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const {
    auto ev = MakeHolder<TEvDataShard::TEvKqpScan>();
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

    auto state = GetShardState(msg, ev->Sender);
    if (!state) {
        return;
    }

    TDuration latency;
    if (enqueuedAt != TInstant::Zero()) {
        latency = TActivationContext::Now() - enqueuedAt;
        Counters->ScanQueryRateLimitLatency->Collect(latency.MilliSeconds());
    }

    YQL_ENSURE(state->ActorId == ev->Sender, "expected: " << state->ActorId << ", got: " << ev->Sender);

    state->LastKey = std::move(msg.LastKey);
    const ui64 rowsCount = msg.GetRowsCount();
    CA_LOG_D("action=got EvScanData;rows=" << rowsCount << ";finished=" << msg.Finished << ";exceeded=" << msg.RequestedBytesLimitReached
        << ";from=" << ev->Sender << ";shards remain=" << PendingShards.size()
        << ";in flight scans=" << InFlightShards.GetScansCount()
        << ";in flight shards=" << InFlightShards.GetShardsCount()
        << ";delayed_for=" << latency.SecondsFloat() << " seconds by ratelimiter"
        << ";tablet_id=" << state->TabletId);
    ProvideDataToCompute(ev->Sender, msg, state);
    state->AvailablePacks = msg.AvailablePacks;
    InFlightShards.MutableStatistics(state->TabletId).AddPack(rowsCount, 0);
    Stats.AddReadStat(state->ScannerIdx, rowsCount, 0);

    bool stopFinally = false;
    CA_LOG_D("EVLOGKQP:" << IsAggregationRequest << "/" << Meta.GetItemsLimit() << "/" << InFlightShards.GetTotalRowsCount() << "/" << rowsCount);
    if (!msg.Finished) {
        if (msg.RequestedBytesLimitReached) {
            InFlightShards.NeedAck(state);
            if (!state->AvailablePacks || *state->AvailablePacks == 0) {
                ReturnShardInPool(state);
                DoAckAvailableWaiting();
            } else {
                SendScanDataAck(state);
            }
        }
    } else {
        CA_LOG_D("Chunk " << state->TabletId << "/" << state->ScannerIdx << " scan finished");
        Stats.CompleteShard(state);
        StopReadChunk(*state);
        CA_LOG_T("TRACE:" << InFlightShards.TraceToString());
        stopFinally = !StartTableScan();
    }
    if (stopFinally) {
        SendScanFinished();
        InFlightShards.Stop();
        CA_LOG_D("EVLOGKQP(scans_count:" << ScansCounter << ";max_in_flight:" << MaxInFlight << ")"
            << Endl << InFlightShards.GetDurationStats()
            << Endl << InFlightShards.StatisticsToString()
            << KqpComputeActorSpan.ProfileToString()
        );
        PassAway();
    }

    CA_LOG_T("TRACE:" << InFlightShards.TraceToString() << ":" << rowsCount);
}

void TKqpScanFetcherActor::ProcessScanData() {
    YQL_ENSURE(!PendingScanData.empty());

    auto ev = std::move(PendingScanData.front().first);
    auto enqueuedAt = std::move(PendingScanData.front().second);
    PendingScanData.pop_front();

    auto& msg = *ev->Get();
    auto state = GetShardState(msg, ev->Sender);
    if (!state)
        return;

    if (state->State == EShardState::Running || state->State == EShardState::PostRunning) {
        ProcessPendingScanDataItem(ev, enqueuedAt);
    } else {
        TerminateExpiredScan(ev->Sender, "Cancel expired scan");
    }
}

bool TKqpScanFetcherActor::StartTableScan() {
    const ui32 maxAllowedInFlight = MaxInFlight;
    bool isFirst = true;
    while (!PendingShards.empty() && InFlightShards.GetScansCount() + PendingResolveShards.size() + 1 <= maxAllowedInFlight) {
        if (isFirst) {
            CA_LOG_D("BEFORE: " << PendingShards.size() << "." << InFlightShards.GetScansCount() << "." << PendingResolveShards.size());
            isFirst = false;
        }
        auto state = InFlightShards.Put(std::move(PendingShards.front()));
        PendingShards.pop_front();
        StartReadShard(state);
    }
    if (!isFirst) {
        CA_LOG_D("AFTER: " << PendingShards.size() << "." << InFlightShards.GetScansCount() << "." << PendingResolveShards.size());
    }

    CA_LOG_D("Scheduled table scans, in flight: " << InFlightShards.GetScansCount() << " shards. "
        << "pending shards to read: " << PendingShards.size() << ", "
        << "pending resolve shards: " << PendingResolveShards.size() << ", "
        << "average read rows: " << Stats.AverageReadRows() << ", "
        << "average read bytes: " << Stats.AverageReadBytes() << ", ");

    return InFlightShards.GetScansCount() + PendingShards.size() + PendingResolveShards.size() > 0;
}

void TKqpScanFetcherActor::StartReadShard(TShardState::TPtr state) {
    YQL_ENSURE(state->State == EShardState::Initial);
    state->State = EShardState::Starting;
    state->Generation = InFlightShards.AllocateGeneration(state);
    state->ActorId = {};
    SendStartScanRequest(state, state->Generation);
}

bool TKqpScanFetcherActor::ReturnShardInPool(TShardState::TPtr state) {
    return InFlightComputes.ReturnShardInPool(state);
}

bool TKqpScanFetcherActor::SendScanDataAck(TShardState::TPtr state) {
    ui64 freeSpace;
    if (!InFlightComputes.PrepareShardAck(state, freeSpace)) {
        CA_LOG_D("Send EvScanDataAck denied: no free actors: " << InFlightComputes.DebugString());
        return false;
    } else {
        CA_LOG_D("Send EvScanDataAck allow: has free actors: " << InFlightComputes.DebugString());
    }
    CA_LOG_D("Send EvScanDataAck to " << state->ActorId << ", gen: " << state->Generation);
    ui32 flags = IEventHandle::FlagTrackDelivery;
    if (TrackingNodes.insert(state->ActorId.NodeId()).second) {
        flags |= IEventHandle::FlagSubscribeOnSession;
    }
    Send(state->ActorId, new TEvKqpCompute::TEvScanDataAck(freeSpace, state->Generation, 1), flags, state->TabletId);
    InFlightShards.AckSent(state);
    return true;
}

void TKqpScanFetcherActor::SendStartScanRequest(TShardState::TPtr state, ui32 gen) {
    YQL_ENSURE(state->State == EShardState::Starting);

    auto ranges = state->GetScanRanges(KeyColumnTypes);
    CA_LOG_D("Start scan request, " << state->ToString(KeyColumnTypes));
    THolder<TEvDataShard::TEvKqpScan> ev = BuildEvKqpScan(0, gen, ranges);

    bool subscribed = std::exchange(state->SubscribedOnTablet, true);

    CA_LOG_D("Send EvKqpScan to shardId: " << state->TabletId << ", tablePath: " << ScanDataMeta.TablePath
        << ", gen: " << gen << ", subscribe: " << (!subscribed)
        << ", range: " << DebugPrintRanges(KeyColumnTypes, ranges, *AppData()->TypeRegistry));

    Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), state->TabletId, !subscribed),
        IEventHandle::FlagTrackDelivery);
}

void TKqpScanFetcherActor::RetryDeliveryProblem(TShardState::TPtr state) {
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
        Send(state->ActorId, new NActors::TEvents::TEvPoisonPill());
        return ResolveShard(*state);
    }

    ++TotalRetries;

    InFlightShards.ClearAckState(state);
    state->RetryAttempt++;
    state->TotalRetries++;
    state->Generation = InFlightShards.AllocateGeneration(state);
    state->ActorId = {};
    state->State = EShardState::Starting;
    state->SubscribedOnTablet = false;
    auto retryDelay = state->CalcRetryDelay();
    CA_LOG_W("TKqpScanFetcherActor: broken pipe with tablet " << state->TabletId
        << ", restarting scan from last received key " << state->PrintLastKey(KeyColumnTypes)
        << ", attempt #" << state->RetryAttempt << " (total " << state->TotalRetries << ")"
        << " schedule after " << retryDelay);

    state->RetryTimer = CreateLongTimer(TlsActivationContext->AsActorContext(), retryDelay,
        new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryShard(state->TabletId, state->Generation)));
}

bool TKqpScanFetcherActor::StopReadChunk(const TShardState& state) {
    CA_LOG_D("Unlink from tablet " << state.TabletId << " chunk " << state.ScannerIdx << " and stop reading from it.");
    const ui64 tabletId = state.TabletId;
    const ui32 scannerIdx = state.ScannerIdx;

    if (InFlightComputes.StopReadShard(state.TabletId)) {
        DoAckAvailableWaiting();
    }

    Y_VERIFY(InFlightShards.RemoveIfExists(scannerIdx));

    const size_t remainChunksCount = InFlightShards.GetByTabletId(tabletId).size();
    if (remainChunksCount == 0) {
        CA_LOG_D("Unlink fully for tablet " << state.TabletId);
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(tabletId));
    } else {
        CA_LOG_D("Tablet " << state.TabletId << " not ready for unlink. Ramained chunks count: " << remainChunksCount);
    }
    return true;
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

TShardState::TPtr TKqpScanFetcherActor::GetShardStateByGeneration(const ui32 generation, const TActorId& scanActorId) {
    if (!InFlightShards.IsActive()) {
        return nullptr;
    }

    const ui32 scannerIdx = InFlightShards.GetIndexByGeneration(generation);
    YQL_ENSURE(scannerIdx, "Received message from unknown scan or request. Generation: " << generation);

    TShardState::TPtr statePtr = InFlightShards.GetStateByIndex(scannerIdx);
    if (!statePtr) {
        TString error = TStringBuilder() << "Received message from scan shard which is not currently in flight, scannerIdx " << scannerIdx;
        CA_LOG_W(error);
        if (scanActorId) {
            TerminateExpiredScan(scanActorId, error);
        }

        return nullptr;
    }

    auto& state = *statePtr;
    if (state.Generation != generation) {
        TString error = TStringBuilder() << "Received message from expired scan, generation mistmatch, "
            << "expected: " << state.Generation << ", received: " << generation;
        CA_LOG_W(error);
        if (scanActorId) {
            TerminateExpiredScan(scanActorId, error);
        }

        return nullptr;
    }

    return statePtr;
}

void TKqpScanFetcherActor::TerminateExpiredScan(const TActorId& actorId, TStringBuf msg) {
    CA_LOG_W(msg);

    auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Cancel unexpected/expired scan");
    Send(actorId, abortEv.Release());
}

void TKqpScanFetcherActor::EnqueueResolveShard(TShardState::TPtr state) {
    CA_LOG_D("Enqueue for resolve " << state->TabletId << " chunk " << state->ScannerIdx);
    YQL_ENSURE(StopReadChunk(*state));
    PendingResolveShards.emplace_back(*state);
    if (PendingResolveShards.size() == 1) {
        ResolveNextShard();
    }
}

void TKqpScanFetcherActor::DoAckAvailableWaiting() {
    std::optional<TInFlightComputes::TWaitingShard> ev;
    if (InFlightComputes.ExtractWaitingForProvide(ev)) {
        if (!ev) {
            ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): no waiting events";
        } else {
            Y_VERIFY(SendScanDataAck(ev->GetShardState()));
        }
    } else {
        ALS_DEBUG(NKikimrServices::KQP_COMPUTE) << "EvAckData (" << SelfId() << "): no available computes for waiting events usage";
    }
}

void TKqpScanFetcherActor::StopOnError(const TString& errorMessage) const {
    CA_LOG_E("unexpected problem: " << errorMessage);
    TIssue issue(errorMessage);
    TIssues issues;
    issues.AddIssue(std::move(issue));
    SendGlobalFail(NDqProto::COMPUTE_STATE_FAILURE, NYql::NDqProto::StatusIds::INTERNAL_ERROR, issues);
}

}
