#include "actor.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blobs_reader/read_coordinator.h>
#include <ydb/core/tx/columnshard/resource_subscriber/actor.h>

#include <yql/essentials/core/issue/yql_issue.h>

namespace NKikimr::NOlap::NReader {
constexpr TDuration SCAN_HARD_TIMEOUT = TDuration::Minutes(10);
constexpr TDuration SCAN_HARD_TIMEOUT_GAP = TDuration::Seconds(5);

void TColumnShardScan::PassAway() {
    Send(ResourceSubscribeActorId, new TEvents::TEvPoisonPill);
    Send(ReadCoordinatorActorId, new TEvents::TEvPoisonPill);
    IActor::PassAway();
}

TColumnShardScan::TColumnShardScan(const TActorId& columnShardActorId, const TActorId& scanComputeActorId,
    const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const TComputeShardingPolicy& computeShardingPolicy, ui32 scanId, ui64 txId, ui32 scanGen, ui64 requestCookie, ui64 tabletId,
    TDuration timeout, const TReadMetadataBase::TConstPtr& readMetadataRange, NKikimrDataEvents::EDataFormat dataFormat,
    const NColumnShard::TScanCounters& scanCountersPool)
    : StoragesManager(storagesManager)
    , DataAccessorsManager(dataAccessorsManager)
    , ColumnShardActorId(columnShardActorId)
    , ScanComputeActorId(scanComputeActorId)
    , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    , ScanId(scanId)
    , TxId(txId)
    , ScanGen(scanGen)
    , RequestCookie(requestCookie)
    , DataFormat(dataFormat)
    , TabletId(tabletId)
    , ReadMetadataRange(readMetadataRange)
    , Timeout(timeout ? timeout + SCAN_HARD_TIMEOUT_GAP : SCAN_HARD_TIMEOUT)
    , ScanCountersPool(scanCountersPool, TValidator::CheckNotNull(ReadMetadataRange)->GetProgram().GetGraphOptional())
    , Stats(NTracing::TTraceClient::GetLocalClient("SHARD", ::ToString(TabletId) /*, "SCAN_TXID:" + ::ToString(TxId)*/))
    , ComputeShardingPolicy(computeShardingPolicy) {
    AFL_VERIFY(ReadMetadataRange);
    KeyYqlSchema = ReadMetadataRange->GetKeyYqlSchema();
}

void TColumnShardScan::Bootstrap(const TActorContext& ctx) {
    //    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN) ("SelfId", SelfId())(
    //        "TabletId", TabletId)("ScanId", ScanId)("TxId", TxId)("ScanGen", ScanGen));
    auto g = Stats->MakeGuard("bootstrap");
    ScanActorId = ctx.SelfID;

    Y_ABORT_UNLESS(!ScanIterator);
    ResourceSubscribeActorId = ctx.Register(new NResourceBroker::NSubscribe::TActor(TabletId, SelfId()));
    ReadCoordinatorActorId = ctx.Register(new NBlobOperations::NRead::TReadCoordinatorActor(TabletId, SelfId()));

    std::shared_ptr<TReadContext> context = std::make_shared<TReadContext>(StoragesManager, DataAccessorsManager, ScanCountersPool,
        ReadMetadataRange, SelfId(), ResourceSubscribeActorId, ReadCoordinatorActorId, ComputeShardingPolicy, ScanId);
    ScanIterator = ReadMetadataRange->StartScan(context);
    auto startResult = ScanIterator->Start();
    StartInstant = TMonotonic::Now();
    if (!startResult) {
        ACFL_ERROR("event", "BootstrapError")("error", startResult.GetErrorMessage());
        SendScanError("scanner_start_error:" + startResult.GetErrorMessage());
        Finish(NColumnShard::TScanCounters::EStatusFinish::ProblemOnStart);
    } else {
        ScheduleWakeup(GetDeadline());

        // propagate self actor id // TODO: FlagSubscribeOnSession ?
        Send(ScanComputeActorId, new NKqp::TEvKqpCompute::TEvScanInitActor(ScanId, ctx.SelfID, ScanGen, TabletId),
            IEventHandle::FlagTrackDelivery);

        Become(&TColumnShardScan::StateScan);
        ContinueProcessing();
    }
}

void TColumnShardScan::HandleScan(NColumnShard::TEvPrivate::TEvTaskProcessedResult::TPtr& ev) {
    auto g = Stats->MakeGuard("task_result");
    auto result = ev->Get()->ExtractResult();
    if (result.IsFail()) {
        ACFL_ERROR("event", "TEvTaskProcessedResult")("error", result.GetErrorMessage());
        SendScanError("task_error:" + result.GetErrorMessage());
        Finish(NColumnShard::TScanCounters::EStatusFinish::ConveyorInternalError);
    } else {
        ACFL_DEBUG("event", "TEvTaskProcessedResult");
        auto t = static_pointer_cast<IApplyAction>(result.GetResult());
        Y_DEBUG_ABORT_UNLESS(dynamic_pointer_cast<IDataTasksProcessor::ITask>(result.GetResult()));
        if (!ScanIterator->Finished()) {
            ScanIterator->Apply(t);
        }
    }
    ContinueProcessing();
}

void TColumnShardScan::HandleScan(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
    auto g = Stats->MakeGuard("ack");
    AFL_VERIFY(!AckReceivedInstant);
    AckReceivedInstant = TMonotonic::Now();

    AFL_VERIFY(ev->Get()->Generation == ScanGen)("ev_gen", ev->Get()->Generation)("scan_gen", ScanGen);

    ChunksLimiter = TChunksLimiter(ev->Get()->FreeSpace, ev->Get()->MaxChunksCount);
    AFL_VERIFY(ev->Get()->MaxChunksCount == 1);
    ACFL_DEBUG("event", "TEvScanDataAck")("info", ChunksLimiter.DebugString());
    if (ScanIterator) {
        if (!!ScanIterator->GetAvailableResultsCount() && !*ScanIterator->GetAvailableResultsCount()) {
            ScanCountersPool.OnEmptyAck();
        } else {
            ScanCountersPool.OnNotEmptyAck();
        }
    }
    ContinueProcessing();
}

void TColumnShardScan::HandleScan(NActors::TEvents::TEvPoison::TPtr& /*ev*/) noexcept {
    PassAway();
}

void TColumnShardScan::HandleScan(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) noexcept {
    auto& msg = ev->Get()->Record;
    const TString reason = ev->Get()->GetIssues().ToOneLineString();

    auto prio = msg.GetStatusCode() == NYql::NDqProto::StatusIds::SUCCESS ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
    LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " got AbortExecution"
                << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
                << " code: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode()) << " reason: " << reason);

    AbortReason = std::move(reason);
    Finish(NColumnShard::TScanCounters::EStatusFinish::ExternalAbort);
}

void TColumnShardScan::HandleScan(TEvents::TEvUndelivered::TPtr& ev) {
    ui32 eventType = ev->Get()->SourceType;
    switch (eventType) {
        case NKqp::TEvKqpCompute::TEvScanInitActor::EventType:
            AbortReason = "init failed";
            break;
        case NKqp::TEvKqpCompute::TEvScanData::EventType:
            AbortReason = "failed to send data batch";
            break;
    }

    LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " undelivered event: " << eventType << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen
                << " tablet: " << TabletId << " reason: " << ev->Get()->Reason << " description: " << AbortReason);

    Finish(NColumnShard::TScanCounters::EStatusFinish::UndeliveredEvent);
}

void TColumnShardScan::HandleScan(TEvents::TEvWakeup::TPtr& /*ev*/) {
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " guard execution timeout"
                << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId);

    if (TMonotonic::Now() >= GetDeadline()) {
        SendScanError("ColumnShard scanner timeout: HAS_ACK=" + ::ToString(!!AckReceivedInstant));
        Finish(NColumnShard::TScanCounters::EStatusFinish::Deadline);
    } else {
        ScheduleWakeup(GetDeadline());
    }
}

bool TColumnShardScan::ProduceResults() noexcept {
    auto g = Stats->MakeGuard("ProduceResults");
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build()("method", "produce result"));

    ACFL_DEBUG("stage", "start")("iterator", ScanIterator->DebugString());
    Y_ABORT_UNLESS(!Finished);
    Y_ABORT_UNLESS(ScanIterator);

    if (ScanIterator->Finished()) {
        ACFL_DEBUG("stage", "scan iterator is finished")("iterator", ScanIterator->DebugString());
        return false;
    }

    if (!ChunksLimiter.HasMore()) {
        ScanIterator->PrepareResults();
        ACFL_DEBUG("stage", "limit exhausted")("limit", ChunksLimiter.DebugString());
        return false;
    }

    auto resultConclusion = ScanIterator->GetBatch();
    if (resultConclusion.IsFail()) {
        ACFL_ERROR("stage", "got error")("iterator", ScanIterator->DebugString())("message", resultConclusion.GetErrorMessage());
        SendScanError(resultConclusion.GetErrorMessage());

        ScanIterator.reset();
        Finish(NColumnShard::TScanCounters::EStatusFinish::IteratorInternalErrorResult);
        return false;
    }

    std::shared_ptr<TPartialReadResult> resultOpt = resultConclusion.DetachResult();
    if (!resultOpt) {
        ACFL_DEBUG("stage", "no data is ready yet")("iterator", ScanIterator->DebugString());
        return false;
    }

    auto& result = *resultOpt;

    if (!result.GetRecordsCount()) {
        ACFL_DEBUG("stage", "got empty batch")("iterator", ScanIterator->DebugString());
        return true;
    }

    auto& shardedBatch = result.GetShardedBatch();
    auto batch = shardedBatch.GetRecordBatch();
    int numRows = batch->num_rows();
    int numColumns = batch->num_columns();
    ACFL_DEBUG("stage", "ready result")("iterator", ScanIterator->DebugString())("columns", numColumns)("rows", result.GetRecordsCount());

    AFL_VERIFY(DataFormat == NKikimrDataEvents::FORMAT_ARROW);
    {
        MakeResult(0);
        if (shardedBatch.IsSharded()) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "compute_sharding_success")(
                "count", shardedBatch.GetSplittedByShards().size())("info", ComputeShardingPolicy.DebugString());
            Result->SplittedBatches = shardedBatch.GetSplittedByShards();
        } else {
            if (ComputeShardingPolicy.IsEnabled()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "compute_sharding_problems")(
                    "info", ComputeShardingPolicy.DebugString());
            }
        }
        TMemoryProfileGuard mGuard("SCAN_PROFILE::RESULT::TO_KQP", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        Result->ArrowBatch = shardedBatch.GetRecordBatch();
        Rows += batch->num_rows();
        Bytes += NArrow::GetTableDataSize(Result->ArrowBatch);

        ACFL_DEBUG("stage", "data_format")("batch_size", NArrow::GetTableDataSize(Result->ArrowBatch))("num_rows", numRows)(
            "batch_columns", JoinSeq(",", batch->schema()->field_names()));
    }
    if (CurrentLastReadKey) {
        auto pNew = NArrow::TReplaceKey::FromBatch(result.GetScanCursor()->GetPKCursor(), 0);
        auto pOld = NArrow::TReplaceKey::FromBatch(CurrentLastReadKey->GetPKCursor(), 0);
        if (!ReadMetadataRange->IsDescSorted()) {
            AFL_VERIFY(!(pNew < pOld))("old", pOld.DebugJson().GetStringRobust())("new", pNew.DebugJson().GetStringRobust());
        } else {
            AFL_VERIFY(!(pOld < pNew))("old", pOld.DebugJson().GetStringRobust())("new", pNew.DebugJson().GetStringRobust());
        }
    }
    CurrentLastReadKey = result.GetScanCursor();

    Result->LastKey = ConvertLastKey(CurrentLastReadKey->GetPKCursor());
    Result->LastCursorProto = CurrentLastReadKey->SerializeToProto();
    SendResult(false, false);
    ScanIterator->OnSentDataFromInterval(result.GetNotFinishedIntervalIdx());
    ACFL_DEBUG("stage", "finished")("iterator", ScanIterator->DebugString());
    return true;
}

void TColumnShardScan::ContinueProcessing() {
    if (!ScanIterator) {
        ACFL_DEBUG("event", "ContinueProcessing")("stage", "iterator is not initialized");
        return;
    }
    // Send new results if there is available capacity
    while (ScanIterator && ProduceResults()) {
    }
    if (!!AckReceivedInstant) {
        LastResultInstant = TMonotonic::Now();
    }

    if (ScanIterator) {
        // Switch to the next range if the current one is finished
        if (ScanIterator->Finished()) {
            if (ChunksLimiter.HasMore()) {
                auto g = Stats->MakeGuard("Finish");
                MakeResult();
                Finish(NColumnShard::TScanCounters::EStatusFinish::Success);
                SendResult(false, true);
                ScanIterator.reset();
            }
        } else {
            while (true) {
                TConclusion<bool> hasMoreData = ScanIterator->ReadNextInterval();
                if (hasMoreData.IsFail()) {
                    ACFL_ERROR("event", "ContinueProcessing")("error", hasMoreData.GetErrorMessage());
                    ScanIterator.reset();
                    SendScanError("iterator_error:" + hasMoreData.GetErrorMessage());
                    return Finish(NColumnShard::TScanCounters::EStatusFinish::IteratorInternalErrorScan);
                } else if (!*hasMoreData) {
                    break;
                }
            }
        }
    }
    AFL_VERIFY(!!FinishInstant || !ScanIterator || !ChunksLimiter.HasMore() || ScanCountersPool.InWaiting())("scan_actor_id", ScanActorId)("tx_id", TxId)("scan_id", ScanId)(
                                             "gen", ScanGen)("tablet", TabletId)("debug", ScanIterator->DebugString())(
                                             "counters", ScanCountersPool.DebugString());
}

void TColumnShardScan::MakeResult(size_t reserveRows /*= 0*/) {
    if (!Finished && !Result) {
        Result = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId, ScanGen);
        if (reserveRows) {
            Y_ABORT_UNLESS(DataFormat != NKikimrDataEvents::FORMAT_ARROW);
            Result->Rows.reserve(reserveRows);
        }
    }
}

void TColumnShardScan::AddRow(const TConstArrayRef<TCell>& row) {
    Result->Rows.emplace_back(TOwnedCellVec::Make(row));
    ++Rows;

    // NOTE: Some per-row overhead to deal with the case when no columns were requested
    Bytes += std::max((ui64)8, (ui64)Result->Rows.back().DataSize());
}

NKikimr::TOwnedCellVec TColumnShardScan::ConvertLastKey(const std::shared_ptr<arrow::RecordBatch>& lastReadKey) {
    Y_ABORT_UNLESS(lastReadKey, "last key must be passed");

    struct TSingeRowWriter: public IRowWriter {
        TOwnedCellVec Row;
        bool Done = false;
        void AddRow(const TConstArrayRef<TCell>& row) override {
            Y_ABORT_UNLESS(!Done);
            Row = TOwnedCellVec::Make(row);
            Done = true;
        }
    } singleRowWriter;
    NArrow::TArrowToYdbConverter converter(KeyYqlSchema, singleRowWriter);
    TString errStr;
    bool ok = converter.Process(*lastReadKey, errStr);
    Y_ABORT_UNLESS(ok, "%s", errStr.c_str());

    Y_ABORT_UNLESS(singleRowWriter.Done);
    return singleRowWriter.Row;
}

bool TColumnShardScan::SendResult(bool pageFault, bool lastBatch) {
    if (Finished) {
        return true;
    }

    Result->PageFault = pageFault;
    Result->PageFaults = PageFaults;
    Result->Finished = lastBatch;
    if (ScanIterator) {
        Result->AvailablePacks = ScanIterator->GetAvailableResultsCount();
    }
    TDuration totalElapsedTime = TDuration::Seconds(GetElapsedTicksAsSeconds());
    // Result->TotalTime = totalElapsedTime - LastReportedElapsedTime;
    // TODO: Result->CpuTime = ...
    LastReportedElapsedTime = totalElapsedTime;

    PageFaults = 0;

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "send_data")("compute_actor_id", ScanComputeActorId)("bytes", Bytes)("rows", Rows)(
        "faults", Result->PageFaults)("finished", Result->Finished)("fault", Result->PageFault)(
        "schema", (Result->ArrowBatch ? Result->ArrowBatch->schema()->ToString() : ""));
    Finished = Result->Finished;
    if (Finished) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scan_finished")("compute_actor_id", ScanComputeActorId)("packs_sum", PacksSum)(
            "bytes", Bytes)("bytes_sum", BytesSum)("rows", Rows)("rows_sum", RowsSum)("faults", Result->PageFaults)(
            "finished", Result->Finished)("fault", Result->PageFault)("stats", Stats->ToJson())(
            "iterator", (ScanIterator ? ScanIterator->DebugString(false) : "NO"));
        Result->StatsOnFinished = std::make_shared<TScanStatsOwner>(ScanIterator->GetStats());
    } else {
        AFL_VERIFY(ChunksLimiter.Take(Bytes));
        Result->RequestedBytesLimitReached = !ChunksLimiter.HasMore();
        AFL_VERIFY(AckReceivedInstant);
        ScanCountersPool.AckWaitingInfo(TMonotonic::Now() - *AckReceivedInstant);
    }
    ReadMetadataRange->OnReplyConstruction(TabletId, *Result);
    AckReceivedInstant.reset();
    LastResultInstant = TMonotonic::Now();

    Result->CpuTime = ScanCountersPool.GetExecutionDuration();

    Send(ScanComputeActorId, Result.Release(), IEventHandle::FlagTrackDelivery);   // TODO: FlagSubscribeOnSession ?

    ReportStats();

    return true;
}

void TColumnShardScan::SendScanError(const TString& reason) {
    AFL_VERIFY(reason);
    const TString msg = TStringBuilder() << "Scan failed at tablet " << TabletId << ", reason: " + reason;

    auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(ScanGen, TabletId);
    ev->Record.SetStatus(Ydb::StatusIds::GENERIC_ERROR);
    auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_RESULT_UNAVAILABLE, msg);
    NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scan_finish")("compute_actor_id", ScanComputeActorId)("stats", Stats->ToJson())(
        "iterator", (ScanIterator ? ScanIterator->DebugString(false) : "NO"));

    Send(ScanComputeActorId, ev.Release());
}

void TColumnShardScan::Finish(const NColumnShard::TScanCounters::EStatusFinish status) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN, "Scan " << ScanActorId << " finished for tablet " << TabletId);
    Send(ColumnShardActorId, new NColumnShard::TEvPrivate::TEvReadFinished(RequestCookie, TxId));
    AFL_VERIFY(StartInstant);
    FinishInstant = TMonotonic::Now();
    ScanCountersPool.OnScanFinished(status, *FinishInstant - *StartInstant);
    ReportStats();
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scan_finish")("compute_actor_id", ScanComputeActorId)("stats", Stats->ToJson())(
        "iterator", (ScanIterator ? ScanIterator->DebugString(false) : "NO"));
    PassAway();
}

void TColumnShardScan::ReportStats() {
    Send(ColumnShardActorId, new NColumnShard::TEvPrivate::TEvScanStats(Rows, Bytes));
    Rows = 0;
    Bytes = 0;
}

void TColumnShardScan::ScheduleWakeup(const TMonotonic deadline) {
    if (deadline != TMonotonic::Max()) {
        Schedule(deadline, new TEvents::TEvWakeup);
    }
}

TMonotonic TColumnShardScan::GetDeadline() const {
    AFL_VERIFY(StartInstant);
    if (LastResultInstant) {
        return *LastResultInstant + Timeout;
    }
    return *StartInstant + Timeout;
}
}   // namespace NKikimr::NOlap::NReader
