#include "actor.h"
#include <ydb/core/tx/columnshard/blobs_reader/read_coordinator.h>
#include <ydb/core/tx/columnshard/resource_subscriber/actor.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap::NReader {
constexpr i64 DEFAULT_READ_AHEAD_BYTES = (i64)2 * 1024 * 1024 * 1024;
constexpr TDuration SCAN_HARD_TIMEOUT = TDuration::Minutes(10);
constexpr TDuration SCAN_HARD_TIMEOUT_GAP = TDuration::Seconds(5);

namespace {
class TInFlightGuard: NNonCopyable::TNonCopyable {
private:
    static inline TAtomicCounter InFlightGlobal = 0;
    i64 InFlightGuarded = 0;
public:
    ~TInFlightGuard() {
        Return(InFlightGuarded);
    }

    bool CanTake() {
        return InFlightGlobal.Val() < DEFAULT_READ_AHEAD_BYTES || !InFlightGuarded;
    }

    void Take(const ui64 bytes) {
        InFlightGlobal.Add(bytes);
        InFlightGuarded += bytes;
    }

    void Return(const ui64 bytes) {
        Y_ABORT_UNLESS(InFlightGlobal.Sub(bytes) >= 0);
        InFlightGuarded -= bytes;
        Y_ABORT_UNLESS(InFlightGuarded >= 0);
    }
};

}

void TColumnShardScan::PassAway() {
    Send(ResourceSubscribeActorId, new TEvents::TEvPoisonPill);
    Send(ReadCoordinatorActorId, new TEvents::TEvPoisonPill);
    IActor::PassAway();
}

TColumnShardScan::TColumnShardScan(const TActorId& columnShardActorId, const TActorId& scanComputeActorId, const std::shared_ptr<IStoragesManager>& storagesManager, 
    const TComputeShardingPolicy& computeShardingPolicy, ui32 scanId, ui64 txId, ui32 scanGen, ui64 requestCookie, 
    ui64 tabletId, TDuration timeout, const TReadMetadataBase::TConstPtr& readMetadataRange,
    NKikimrDataEvents::EDataFormat dataFormat, const NColumnShard::TScanCounters& scanCountersPool)
    : StoragesManager(storagesManager)
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
    , Deadline(TInstant::Now() + (timeout ? timeout + SCAN_HARD_TIMEOUT_GAP : SCAN_HARD_TIMEOUT))
    , ScanCountersPool(scanCountersPool)
    , Stats(NTracing::TTraceClient::GetLocalClient("SHARD", ::ToString(TabletId)/*, "SCAN_TXID:" + ::ToString(TxId)*/))
    , ComputeShardingPolicy(computeShardingPolicy)
{
    AFL_VERIFY(ReadMetadataRange);
    KeyYqlSchema = ReadMetadataRange->GetKeyYqlSchema();
}

void TColumnShardScan::Bootstrap(const TActorContext& ctx) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("SelfId", SelfId())("TabletId", TabletId)("ScanId", ScanId)("TxId", TxId)("ScanGen", ScanGen)
    );
    auto g = Stats->MakeGuard("bootstrap");
    ScanActorId = ctx.SelfID;
    Schedule(Deadline, new TEvents::TEvWakeup);

    Y_ABORT_UNLESS(!ScanIterator);
    ResourceSubscribeActorId = ctx.Register(new NResourceBroker::NSubscribe::TActor(TabletId, SelfId()));
    ReadCoordinatorActorId = ctx.Register(new NBlobOperations::NRead::TReadCoordinatorActor(TabletId, SelfId()));

    std::shared_ptr<TReadContext> context = std::make_shared<TReadContext>(StoragesManager, ScanCountersPool,
        ReadMetadataRange, SelfId(), ResourceSubscribeActorId, ReadCoordinatorActorId, ComputeShardingPolicy);
    ScanIterator = ReadMetadataRange->StartScan(context);
    auto startResult = ScanIterator->Start();
    StartInstant = TMonotonic::Now();
    if (!startResult) {
        ACFL_ERROR("event", "BootstrapError")("error", startResult.GetErrorMessage());
        SendScanError("scanner_start_error:" + startResult.GetErrorMessage());
        Finish(NColumnShard::TScanCounters::EStatusFinish::ProblemOnStart);
    } else {

        // propagate self actor id // TODO: FlagSubscribeOnSession ?
        Send(ScanComputeActorId, new NKqp::TEvKqpCompute::TEvScanInitActor(ScanId, ctx.SelfID, ScanGen, TabletId), IEventHandle::FlagTrackDelivery);

        Become(&TColumnShardScan::StateScan);
        ContinueProcessing();
    }
}

void TColumnShardScan::HandleScan(NColumnShard::TEvPrivate::TEvTaskProcessedResult::TPtr& ev) {
    --InFlightReads;
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
    Y_ABORT_UNLESS(!AckReceivedInstant);
    AckReceivedInstant = TMonotonic::Now();

    AFL_VERIFY(ev->Get()->Generation == ScanGen)("ev_gen", ev->Get()->Generation)("scan_gen", ScanGen);

    ChunksLimiter = TChunksLimiter(ev->Get()->FreeSpace, ev->Get()->MaxChunksCount);
    Y_ABORT_UNLESS(ev->Get()->MaxChunksCount == 1);
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

void TColumnShardScan::HandleScan(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) noexcept {
    auto& msg = ev->Get()->Record;
    const TString reason = ev->Get()->GetIssues().ToOneLineString();

    auto prio = msg.GetStatusCode() == NYql::NDqProto::StatusIds::SUCCESS ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
    LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " got AbortExecution"
        << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
        << " code: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
        << " reason: " << reason);

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
        "Scan " << ScanActorId << " undelivered event: " << eventType
        << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
        << " reason: " << ev->Get()->Reason
        << " description: " << AbortReason);

    Finish(NColumnShard::TScanCounters::EStatusFinish::UndeliveredEvent);
}

void TColumnShardScan::HandleScan(TEvents::TEvWakeup::TPtr& /*ev*/) {
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " guard execution timeout"
        << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId);

    Finish(NColumnShard::TScanCounters::EStatusFinish::Deadline);
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

    std::optional<TPartialReadResult> resultOpt = resultConclusion.DetachResult();
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
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "compute_sharding_success")("count", shardedBatch.GetSplittedByShards().size())("info", ComputeShardingPolicy.DebugString());
            Result->SplittedBatches = shardedBatch.GetSplittedByShards();
        } else {
            if (ComputeShardingPolicy.IsEnabled()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "compute_sharding_problems")("info", ComputeShardingPolicy.DebugString());
            }
        }
        TMemoryProfileGuard mGuard("SCAN_PROFILE::RESULT::TO_KQP", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        Result->ArrowBatch = shardedBatch.GetRecordBatch();
        Rows += batch->num_rows();
        Bytes += NArrow::GetTableDataSize(Result->ArrowBatch);
        ACFL_DEBUG("stage", "data_format")("batch_size", NArrow::GetTableDataSize(Result->ArrowBatch))("num_rows", numRows)("batch_columns", JoinSeq(",", batch->schema()->field_names()));
    }
    if (CurrentLastReadKey) {
        NArrow::NMerger::TSortableBatchPosition pNew(result.GetLastReadKey(), 0, result.GetLastReadKey()->schema()->field_names(), {}, ReadMetadataRange->IsDescSorted());
        NArrow::NMerger::TSortableBatchPosition pOld(CurrentLastReadKey, 0, CurrentLastReadKey->schema()->field_names(), {}, ReadMetadataRange->IsDescSorted());
        AFL_VERIFY(pOld < pNew)("old", pOld.DebugJson().GetStringRobust())("new", pNew.DebugJson().GetStringRobust());
    }
    CurrentLastReadKey = result.GetLastReadKey();

    Result->LastKey = ConvertLastKey(result.GetLastReadKey());
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

    if (ScanIterator) {
        // Switch to the next range if the current one is finished
        if (ScanIterator->Finished()) {
            if (ChunksLimiter.HasMore()) {
                auto g = Stats->MakeGuard("Finish");
                MakeResult();
                SendResult(false, true);
                ScanIterator.reset();
                Finish(NColumnShard::TScanCounters::EStatusFinish::Success);
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
    AFL_VERIFY(!ScanIterator || !ChunksLimiter.HasMore() || InFlightReads || ScanCountersPool.InWaiting())("scan_actor_id", ScanActorId)("tx_id", TxId)("scan_id", ScanId)("gen", ScanGen)("tablet", TabletId)
        ("debug", ScanIterator->DebugString());
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

    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " send ScanData to " << ScanComputeActorId
        << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
        << " bytes: " << Bytes << " rows: " << Rows << " page faults: " << Result->PageFaults
        << " finished: " << Result->Finished << " pageFault: " << Result->PageFault
        << " arrow schema:\n" << (Result->ArrowBatch ? Result->ArrowBatch->schema()->ToString() : ""));

    Finished = Result->Finished;
    if (Finished) {
        ALS_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN) <<
            "Scanner finished " << ScanActorId << " and sent to " << ScanComputeActorId
            << " packs: " << PacksSum << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
            << " bytes: " << Bytes << "/" << BytesSum << " rows: " << Rows << "/" << RowsSum << " page faults: " << Result->PageFaults
            << " finished: " << Result->Finished << " pageFault: " << Result->PageFault
            << " stats:" << Stats->ToJson() << ";iterator:" << (ScanIterator ? ScanIterator->DebugString(false) : "NO");
        Result->StatsOnFinished = std::make_shared<TScanStatsOwner>(ScanIterator->GetStats());
    } else {
        Y_ABORT_UNLESS(ChunksLimiter.Take(Bytes));
        Result->RequestedBytesLimitReached = !ChunksLimiter.HasMore();
        Y_ABORT_UNLESS(AckReceivedInstant);
        ScanCountersPool.AckWaitingInfo(TMonotonic::Now() - *AckReceivedInstant);
    }
    AckReceivedInstant.reset();

    Send(ScanComputeActorId, Result.Release(), IEventHandle::FlagTrackDelivery); // TODO: FlagSubscribeOnSession ?

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

    Send(ScanComputeActorId, ev.Release());
}

void TColumnShardScan::Finish(const NColumnShard::TScanCounters::EStatusFinish status) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
        "Scan " << ScanActorId << " finished for tablet " << TabletId);

    bool success = (status == NColumnShard::TScanCounters::EStatusFinish::Success);
    Send(ColumnShardActorId, new NColumnShard::TEvPrivate::TEvReadFinished(RequestCookie, TxId, success));
    AFL_VERIFY(StartInstant);
    ScanCountersPool.OnScanDuration(status, TMonotonic::Now() - *StartInstant);
    ReportStats();
    PassAway();
}

void TColumnShardScan::ReportStats() {
    Send(ColumnShardActorId, new NColumnShard::TEvPrivate::TEvScanStats(Rows, Bytes));
    Rows = 0;
    Bytes = 0;
}

}