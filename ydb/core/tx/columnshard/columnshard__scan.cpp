#include "blobs_reader/actor.h"
#include "blobs_reader/events.h"
#include "blobs_reader/read_coordinator.h"
#include "engines/reader/read_context.h"
#include "resource_subscriber/actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard__read_base.h>
#include <ydb/core/tx/columnshard/columnshard__scan.h>
#include <ydb/core/tx/columnshard/columnshard__stats_scan.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/tracing/usage/tracing.h>

#include <util/generic/noncopyable.h>
#include <ydb/library/chunks_limiter/chunks_limiter.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/services/metadata/request/common.h>

#include <tuple>

namespace NKikimr::NColumnShard {

using namespace NKqp;
using NBlobCache::TBlobRange;

class TTxScan: public TTxReadBase {
public:
    using TReadMetadataPtr = NOlap::TReadMetadataBase::TConstPtr;

    TTxScan(TColumnShard* self, TEvColumnShard::TEvScan::TPtr& ev)
        : TTxReadBase(self)
        , Ev(ev) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_START_SCAN; }

private:
    std::shared_ptr<NOlap::TReadMetadataBase> CreateReadMetadata(NOlap::TReadDescription& read,
        bool isIndexStats, bool isReverse, ui64 limit);

private:
    TEvColumnShard::TEvScan::TPtr Ev;
    std::vector<TReadMetadataPtr> ReadMetadataRanges;
};


constexpr i64 DEFAULT_READ_AHEAD_BYTES = (i64)2 * 1024 * 1024 * 1024;
constexpr TDuration SCAN_HARD_TIMEOUT = TDuration::Minutes(10);
constexpr TDuration SCAN_HARD_TIMEOUT_GAP = TDuration::Seconds(5);

class TColumnShardScan : public TActorBootstrapped<TColumnShardScan>, NArrow::IRowWriter {
private:
    std::shared_ptr<NOlap::TActorBasedMemoryAccesor> MemoryAccessor;
    TActorId ResourceSubscribeActorId;
    TActorId ReadCoordinatorActorId;
    const std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_OLAP_SCAN;
    }

public:
    virtual void PassAway() override {
        Send(ResourceSubscribeActorId, new TEvents::TEvPoisonPill);
        Send(ReadCoordinatorActorId, new TEvents::TEvPoisonPill);
        IActor::PassAway();
    }

    TColumnShardScan(const TActorId& columnShardActorId, const TActorId& scanComputeActorId,
        const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const NOlap::TComputeShardingPolicy& computeShardingPolicy,
        ui32 scanId, ui64 txId, ui32 scanGen, ui64 requestCookie,
        ui64 tabletId, TDuration timeout, std::vector<TTxScan::TReadMetadataPtr>&& readMetadataList,
        NKikimrDataEvents::EDataFormat dataFormat, const TScanCounters& scanCountersPool)
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
        , ReadMetadataRanges(std::move(readMetadataList))
        , ReadMetadataIndex(0)
        , Deadline(TInstant::Now() + (timeout ? timeout + SCAN_HARD_TIMEOUT_GAP : SCAN_HARD_TIMEOUT))
        , ScanCountersPool(scanCountersPool)
        , Stats(NTracing::TTraceClient::GetLocalClient("SHARD", ::ToString(TabletId)/*, "SCAN_TXID:" + ::ToString(TxId)*/))
        , ComputeShardingPolicy(computeShardingPolicy)
    {
        AFL_VERIFY(ReadMetadataRanges.size() == 1);
        KeyYqlSchema = ReadMetadataRanges[ReadMetadataIndex]->GetKeyYqlSchema();
    }

    void Bootstrap(const TActorContext& ctx) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("SelfId", SelfId())("TabletId", TabletId)("ScanId", ScanId)("TxId", TxId)("ScanGen", ScanGen)
        );
        auto g = Stats->MakeGuard("bootstrap");
        ScanActorId = ctx.SelfID;
        Schedule(Deadline, new TEvents::TEvWakeup);

        Y_ABORT_UNLESS(!ScanIterator);
        MemoryAccessor = std::make_shared<NOlap::TActorBasedMemoryAccesor>(SelfId(), "CSScan/Result");
        ResourceSubscribeActorId = ctx.Register(new NOlap::NResourceBroker::NSubscribe::TActor(TabletId, SelfId()));
        ReadCoordinatorActorId = ctx.Register(new NOlap::NBlobOperations::NRead::TReadCoordinatorActor(TabletId, SelfId()));

        std::shared_ptr<NOlap::TReadContext> context = std::make_shared<NOlap::TReadContext>(StoragesManager, ScanCountersPool,
            ReadMetadataRanges[ReadMetadataIndex], SelfId(), ResourceSubscribeActorId, ReadCoordinatorActorId, ComputeShardingPolicy);
        ScanIterator = ReadMetadataRanges[ReadMetadataIndex]->StartScan(context);

        // propagate self actor id // TODO: FlagSubscribeOnSession ?
        Send(ScanComputeActorId, new TEvKqpCompute::TEvScanInitActor(ScanId, ctx.SelfID, ScanGen, TabletId), IEventHandle::FlagTrackDelivery);

        Become(&TColumnShardScan::StateScan);
        ContinueProcessing();
    }

private:
    STATEFN(StateScan) {
        auto g = Stats->MakeGuard("processing");
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("SelfId", SelfId())("TabletId", TabletId)("ScanId", ScanId)("TxId", TxId)("ScanGen", ScanGen)
        );
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpCompute::TEvScanDataAck, HandleScan);
            hFunc(TEvKqp::TEvAbortExecution, HandleScan);
            hFunc(TEvents::TEvUndelivered, HandleScan);
            hFunc(TEvents::TEvWakeup, HandleScan);
            hFunc(NConveyor::TEvExecution::TEvTaskProcessedResult, HandleScan);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    bool ReadNextBlob() {
        while (ScanIterator->ReadNextInterval()) {
        }
        return true;
    }

    void HandleScan(NConveyor::TEvExecution::TEvTaskProcessedResult::TPtr& ev) {
        --InFlightReads;
        auto g = Stats->MakeGuard("task_result");
        if (ev->Get()->GetErrorMessage()) {
            ACFL_ERROR("event", "TEvTaskProcessedResult")("error", ev->Get()->GetErrorMessage());
            SendScanError("task_error:" + ev->Get()->GetErrorMessage());
            Finish();
        } else {
            ACFL_DEBUG("event", "TEvTaskProcessedResult");
            auto t = static_pointer_cast<IDataTasksProcessor::ITask>(ev->Get()->GetResult());
            Y_DEBUG_ABORT_UNLESS(dynamic_pointer_cast<IDataTasksProcessor::ITask>(ev->Get()->GetResult()));
            if (!ScanIterator->Finished()) {
                ScanIterator->Apply(t);
            }
        }
        ContinueProcessing();
    }

    void HandleScan(TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
        auto g = Stats->MakeGuard("ack");
        Y_ABORT_UNLESS(!AckReceivedInstant);
        AckReceivedInstant = TMonotonic::Now();

        Y_ABORT_UNLESS(ev->Get()->Generation == ScanGen);

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

    // Returns true if it was able to produce new batch
    bool ProduceResults() noexcept {
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
            ACFL_DEBUG("stage", "bytes limit exhausted")("limit", ChunksLimiter.DebugString());
            return false;
        }

        auto resultOpt = ScanIterator->GetBatch();
        if (!resultOpt) {
            ACFL_DEBUG("stage", "no data is ready yet")("iterator", ScanIterator->DebugString());
            return false;
        }
        auto& result = *resultOpt;
        if (!result.ErrorString.empty()) {
            ACFL_ERROR("stage", "got error")("iterator", ScanIterator->DebugString())("message", result.ErrorString);
            SendAbortExecution(TString(result.ErrorString.data(), result.ErrorString.size()));

            ScanIterator.reset();
            Finish();
            return false;
        }

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
                Result->ArrowBatch = shardedBatch.GetRecordBatch();
            } else {
                if (ComputeShardingPolicy.IsEnabled()) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "compute_sharding_problems")("info", ComputeShardingPolicy.DebugString());
                }
                Result->ArrowBatch = shardedBatch.GetRecordBatch();
            }
            Rows += batch->num_rows();
            Bytes += NArrow::GetBatchDataSize(batch);
            ACFL_DEBUG("stage", "data_format")("batch_size", NArrow::GetBatchDataSize(batch))("num_rows", numRows)("batch_columns", JoinSeq(",", batch->schema()->field_names()));
        }
        if (CurrentLastReadKey) {
            NOlap::NIndexedReader::TSortableBatchPosition pNew(result.GetLastReadKey(), 0, result.GetLastReadKey()->schema()->field_names(), {}, false);
            NOlap::NIndexedReader::TSortableBatchPosition pOld(CurrentLastReadKey, 0, CurrentLastReadKey->schema()->field_names(), {}, false);
            AFL_VERIFY(pOld < pNew)("old", pOld.DebugJson().GetStringRobust())("new", pNew.DebugJson().GetStringRobust());
        }
        CurrentLastReadKey = result.GetLastReadKey();
        
        Result->LastKey = ConvertLastKey(result.GetLastReadKey());
        SendResult(false, false);
        ACFL_DEBUG("stage", "finished")("iterator", ScanIterator->DebugString());
        return true;
    }

    void ContinueProcessingStep() {
        if (!ScanIterator) {
            ACFL_DEBUG("event", "ContinueProcessingStep")("stage", "iterator is not initialized");
            return;
        }
        const bool hasAck = !!AckReceivedInstant;
        // Send new results if there is available capacity
        while (ScanIterator && ProduceResults()) {
        }

        // Switch to the next range if the current one is finished
        if (ScanIterator && ScanIterator->Finished() && hasAck) {
            NextReadMetadata();
        }

        if (ScanIterator) {
            // Make read-ahead requests for the subsequent blobs
            ReadNextBlob();
        }
    }

    void ContinueProcessing() {
        const i64 maxSteps = ReadMetadataRanges.size();
        for (i64 step = 0; step <= maxSteps; ++step) {
            ContinueProcessingStep();
            if  (!ScanIterator || !ChunksLimiter.HasMore() || InFlightReads || MemoryAccessor->InWaiting() || ScanCountersPool.InWaiting()) {
                return;
            }
        }
        ScanCountersPool.Hanging->Add(1);
        // The loop has finished without any progress!
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
            "Scan " << ScanActorId << " is hanging"
            << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId << " debug: " << ScanIterator->DebugString());
        Y_DEBUG_ABORT_UNLESS(false);
    }

    void HandleScan(TEvKqp::TEvAbortExecution::TPtr& ev) noexcept {
        auto& msg = ev->Get()->Record;
        const TString reason = ev->Get()->GetIssues().ToOneLineString();

        auto prio = msg.GetStatusCode() == NYql::NDqProto::StatusIds::SUCCESS ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
        LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::TX_COLUMNSHARD_SCAN,
            "Scan " << ScanActorId << " got AbortExecution"
            << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
            << " code: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << " reason: " << reason);

        AbortReason = std::move(reason);
        Finish();
    }

    void HandleScan(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 eventType = ev->Get()->SourceType;
        switch (eventType) {
            case TEvKqpCompute::TEvScanInitActor::EventType:
                AbortReason = "init failed";
                break;
            case TEvKqpCompute::TEvScanData::EventType:
                AbortReason = "failed to send data batch";
                break;
        }

        LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
            "Scan " << ScanActorId << " undelivered event: " << eventType
            << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId
            << " reason: " << ev->Get()->Reason
            << " description: " << AbortReason);

        Finish();
    }

    void HandleScan(TEvents::TEvWakeup::TPtr& /*ev*/) {
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
            "Scan " << ScanActorId << " guard execution timeout"
            << " txId: " << TxId << " scanId: " << ScanId << " gen: " << ScanGen << " tablet: " << TabletId);

        Finish();
    }

private:
    void MakeResult(size_t reserveRows = 0) {
        if (!Finished && !Result) {
            Result = MakeHolder<TEvKqpCompute::TEvScanData>(ScanId, ScanGen);
            if (reserveRows) {
                Y_ABORT_UNLESS(DataFormat != NKikimrDataEvents::FORMAT_ARROW);
                Result->Rows.reserve(reserveRows);
            }
        }
    }

    void NextReadMetadata() {
        auto g = Stats->MakeGuard("NextReadMetadata");
        if (++ReadMetadataIndex == ReadMetadataRanges.size()) {
            // Send empty batch with "finished" flag
            MakeResult();
            SendResult(false, true);
            ScanIterator.reset();
            return Finish();
        }

        auto context = std::make_shared<NOlap::TReadContext>(StoragesManager, ScanCountersPool, ReadMetadataRanges[ReadMetadataIndex], SelfId(),
            ResourceSubscribeActorId, ReadCoordinatorActorId, ComputeShardingPolicy);
        ScanIterator = ReadMetadataRanges[ReadMetadataIndex]->StartScan(context);
    }

    void AddRow(const TConstArrayRef<TCell>& row) override {
        Result->Rows.emplace_back(TOwnedCellVec::Make(row));
        ++Rows;

        // NOTE: Some per-row overhead to deal with the case when no columns were requested
        Bytes += std::max((ui64)8, (ui64)Result->Rows.back().DataSize());
    }

    TOwnedCellVec ConvertLastKey(const std::shared_ptr<arrow::RecordBatch>& lastReadKey) {
        Y_ABORT_UNLESS(lastReadKey, "last key must be passed");

        struct TSingeRowWriter : public IRowWriter {
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

    class TScanStatsOwner: public NKqp::TEvKqpCompute::IShardScanStats {
    private:
        YDB_READONLY_DEF(NOlap::TReadStats, Stats);
    public:
        TScanStatsOwner(const NOlap::TReadStats& stats)
            : Stats(stats) {

        }

        virtual THashMap<TString, ui64> GetMetrics() const override {
            THashMap<TString, ui64> result;
            result["compacted_bytes"] = Stats.CompactedPortionsBytes;
            result["inserted_bytes"] = Stats.InsertedPortionsBytes;
            result["committed_bytes"] = Stats.CommittedPortionsBytes;
            return result;
        }
    };

    bool SendResult(bool pageFault, bool lastBatch) {
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

    void SendScanError(TString reason = {}) {
        TString msg = TStringBuilder() << "Scan failed at tablet " << TabletId;
        if (!reason.empty()) {
            msg += ", reason: " + reason;
        }

        auto ev = MakeHolder<TEvKqpCompute::TEvScanError>(ScanGen, TabletId);
        ev->Record.SetStatus(Ydb::StatusIds::GENERIC_ERROR);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_RESULT_UNAVAILABLE, msg);
        NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

        Send(ScanComputeActorId, ev.Release());
    }

    void SendAbortExecution(TString reason = {}) {
        auto status = NYql::NDqProto::StatusIds::PRECONDITION_FAILED;
        TString msg = TStringBuilder() << "Scan failed at tablet " << TabletId;
        if (!reason.empty()) {
            msg += ", reason: " + reason;
        }

        Send(ScanComputeActorId, new TEvKqp::TEvAbortExecution(status, msg));
    }

    void Finish() {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COLUMNSHARD_SCAN,
            "Scan " << ScanActorId << " finished for tablet " << TabletId);

        Send(ColumnShardActorId, new TEvPrivate::TEvReadFinished(RequestCookie, TxId));
        ReportStats();
        PassAway();
    }

    void ReportStats() {
        Send(ColumnShardActorId, new TEvPrivate::TEvScanStats(Rows, Bytes));
        Rows = 0;
        Bytes = 0;
    }

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

private:
    const TActorId ColumnShardActorId;
    const TActorId ReadBlobsActorId;
    const TActorId ScanComputeActorId;
    std::optional<TMonotonic> AckReceivedInstant;
    TActorId ScanActorId;
    TActorId BlobCacheActorId;
    const ui32 ScanId;
    const ui64 TxId;
    const ui32 ScanGen;
    const ui64 RequestCookie;
    const NKikimrDataEvents::EDataFormat DataFormat;
    const ui64 TabletId;

    std::vector<NOlap::TReadMetadataBase::TConstPtr> ReadMetadataRanges;
    ui32 ReadMetadataIndex;
    std::unique_ptr<TScanIteratorBase> ScanIterator;

    std::vector<std::pair<TString, NScheme::TTypeInfo>> KeyYqlSchema;
    const TSerializedTableRange TableRange;
    const TSmallVec<bool> SkipNullKeys;
    const TInstant Deadline;
    TConcreteScanCounters ScanCountersPool;

    TMaybe<TString> AbortReason;

    TChunksLimiter ChunksLimiter;
    THolder<TEvKqpCompute::TEvScanData> Result;
    std::shared_ptr<arrow::RecordBatch> CurrentLastReadKey;
    i64 InFlightReads = 0;
    bool Finished = false;

    class TBlobStats {
    private:
        ui64 PartsCount = 0;
        ui64 Bytes = 0;
        TDuration ReadingDurationSum;
        TDuration ReadingDurationMax;
        NMonitoring::THistogramPtr BlobDurationsCounter;
        NMonitoring::THistogramPtr ByteDurationsCounter;
    public:
        TBlobStats(const NMonitoring::THistogramPtr blobDurationsCounter, const NMonitoring::THistogramPtr byteDurationsCounter)
            : BlobDurationsCounter(blobDurationsCounter)
            , ByteDurationsCounter(byteDurationsCounter)
        {

        }
        void Received(const NBlobCache::TBlobRange& br, const TDuration d) {
            ReadingDurationSum += d;
            ReadingDurationMax = Max(ReadingDurationMax, d);
            ++PartsCount;
            Bytes += br.Size;
            BlobDurationsCounter->Collect(d.MilliSeconds());
            ByteDurationsCounter->Collect((i64)d.MilliSeconds(), br.Size);
        }
        TString DebugString() const {
            TStringBuilder sb;
            if (PartsCount) {
                sb << "p_count=" << PartsCount << ";";
                sb << "bytes=" << Bytes << ";";
                sb << "d_avg=" << ReadingDurationSum / PartsCount << ";";
                sb << "d_max=" << ReadingDurationMax << ";";
            } else {
                sb << "NO_BLOBS;";
            }
            return sb;
        }
    };

    NTracing::TTraceClientGuard Stats;
    const NOlap::TComputeShardingPolicy ComputeShardingPolicy;
    ui64 Rows = 0;
    ui64 BytesSum = 0;
    ui64 RowsSum = 0;
    ui64 PacksSum = 0;
    ui64 Bytes = 0;
    ui32 PageFaults = 0;
    TDuration LastReportedElapsedTime;
};

static bool FillPredicatesFromRange(NOlap::TReadDescription& read, const ::NKikimrTx::TKeyRange& keyRange,
                                    const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbPk, ui64 tabletId, const NOlap::TIndexInfo* indexInfo, TString& error) {
    TSerializedTableRange range(keyRange);
    auto fromPredicate = std::make_shared<NOlap::TPredicate>();
    auto toPredicate = std::make_shared<NOlap::TPredicate>();
    std::tie(*fromPredicate, *toPredicate) = RangePredicates(range, ydbPk);

    LOG_S_DEBUG("TTxScan range predicate. From key size: " << range.From.GetCells().size()
        << " To key size: " << range.To.GetCells().size()
        << " greater predicate over columns: " << fromPredicate->ToString()
        << " less predicate over columns: " << toPredicate->ToString()
        << " at tablet " << tabletId);

    if (!read.PKRangesFilter.Add(fromPredicate, toPredicate, indexInfo)) {
        error = "Error building filter";
        return false;
    }
    return true;
}

std::shared_ptr<NOlap::TReadStatsMetadata>
PrepareStatsReadMetadata(ui64 tabletId, const NOlap::TReadDescription& read, const std::unique_ptr<NOlap::IColumnEngine>& index, TString& error, const bool isReverse) {
    THashSet<ui32> readColumnIds(read.ColumnIds.begin(), read.ColumnIds.end());
    for (auto& [id, name] : read.GetProgram().GetSourceColumns()) {
        readColumnIds.insert(id);
    }

    for (ui32 colId : readColumnIds) {
        if (!PrimaryIndexStatsSchema.Columns.contains(colId)) {
            error = Sprintf("Columnd id %" PRIu32 " not found", colId);
            return {};
        }
    }

    auto out = std::make_shared<NOlap::TReadStatsMetadata>(tabletId,
                isReverse ? NOlap::TReadStatsMetadata::ESorting::DESC : NOlap::TReadStatsMetadata::ESorting::ASC,
                read.GetProgram(), index ? index->GetVersionedIndex().GetSchema(read.GetSnapshot()) : nullptr, read.GetSnapshot());

    out->SetPKRangesFilter(read.PKRangesFilter);
    out->ReadColumnIds.assign(readColumnIds.begin(), readColumnIds.end());
    out->ResultColumnIds = read.ColumnIds;

    const NOlap::TColumnEngineForLogs* logsIndex = dynamic_cast<const NOlap::TColumnEngineForLogs*>(index.get());
    if (!index || !logsIndex) {
        return out;
    }
    THashMap<ui64, THashSet<ui64>> portionsInUse;
    const auto predStatSchema = [](const std::shared_ptr<NOlap::TPortionInfo>& l, const std::shared_ptr<NOlap::TPortionInfo>& r) {
        return std::tuple(l->GetPathId(), l->GetPortionId()) < std::tuple(r->GetPathId(), r->GetPortionId());
    };
    for (auto&& filter : read.PKRangesFilter) {
        const ui64 fromPathId = *filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1);
        const ui64 toPathId = *filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>());
        if (read.TableName.EndsWith(NOlap::TIndexInfo::TABLE_INDEX_STATS_TABLE)) {
            if (fromPathId <= read.PathId && toPathId >= read.PathId) {
                auto pathInfo = logsIndex->GetGranuleOptional(read.PathId);
                if (!pathInfo) {
                    continue;
                }
                for (auto&& p : pathInfo->GetPortions()) {
                    if (portionsInUse[read.PathId].emplace(p.first).second) {
                        out->IndexPortions.emplace_back(p.second);
                    }
                }
            }
            std::sort(out->IndexPortions.begin(), out->IndexPortions.end(), predStatSchema);
        } else if (read.TableName.EndsWith(NOlap::TIndexInfo::STORE_INDEX_STATS_TABLE)) {
            auto pathInfos = logsIndex->GetTables(fromPathId, toPathId);
            for (auto&& pathInfo: pathInfos) {
                for (auto&& p: pathInfo->GetPortions()) {
                    if (portionsInUse[p.second->GetPathId()].emplace(p.first).second) {
                        out->IndexPortions.emplace_back(p.second);
                    }
                }
            }
            std::sort(out->IndexPortions.begin(), out->IndexPortions.end(), predStatSchema);
        }
    }

    return out;
}

std::shared_ptr<NOlap::TReadMetadataBase> TTxScan::CreateReadMetadata(NOlap::TReadDescription& read,
    bool indexStats, bool isReverse, ui64 itemsLimit)
{
    std::shared_ptr<NOlap::TReadMetadataBase> metadata;
    if (indexStats) {
        metadata = PrepareStatsReadMetadata(Self->TabletID(), read, Self->TablesManager.GetPrimaryIndex(), ErrorDescription, isReverse);
    } else {
        metadata = PrepareReadMetadata(read, Self->InsertTable, Self->TablesManager.GetPrimaryIndex(),
                                       ErrorDescription, isReverse);
    }

    if (!metadata) {
        return nullptr;
    }

    if (itemsLimit) {
        metadata->Limit = itemsLimit;
    }

    return metadata;
}


bool TTxScan::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Y_UNUSED(txc);

    auto& record = Ev->Get()->Record;
    const auto& snapshot = record.GetSnapshot();
    const auto scanId = record.GetScanId();
    const ui64 txId = record.GetTxId();

    LOG_S_DEBUG("TTxScan prepare txId: " << txId << " scanId: " << scanId << " at tablet " << Self->TabletID());

    ui64 itemsLimit = record.HasItemsLimit() ? record.GetItemsLimit() : 0;

    NOlap::TReadDescription read(NOlap::TSnapshot(snapshot.GetStep(), snapshot.GetTxId()), record.GetReverse());
    read.PathId = record.GetLocalPathId();
    read.ReadNothing = !(Self->TablesManager.HasTable(read.PathId));
    read.TableName = record.GetTablePath();
    bool isIndexStats = read.TableName.EndsWith(NOlap::TIndexInfo::STORE_INDEX_STATS_TABLE) ||
        read.TableName.EndsWith(NOlap::TIndexInfo::TABLE_INDEX_STATS_TABLE);
    read.ColumnIds.assign(record.GetColumnTags().begin(), record.GetColumnTags().end());
    read.StatsMode = record.GetStatsMode();

    const NOlap::TIndexInfo* indexInfo = nullptr;
    if (!isIndexStats) {
        indexInfo = &(Self->TablesManager.GetIndexInfo(NOlap::TSnapshot(snapshot.GetStep(), snapshot.GetTxId())));
    }

    bool parseResult;

    if (!isIndexStats) {
        TIndexColumnResolver columnResolver(*indexInfo);
        parseResult = ParseProgram(record.GetOlapProgramType(), record.GetOlapProgram(), read, columnResolver);
    } else {
        TStatsColumnResolver columnResolver;
        parseResult = ParseProgram(record.GetOlapProgramType(), record.GetOlapProgram(), read, columnResolver);
    }

    if (!parseResult) {
        return true;
    }

    if (!record.RangesSize()) {
        auto range = CreateReadMetadata(read, isIndexStats, record.GetReverse(), itemsLimit);
        if (range) {
            ReadMetadataRanges = {range};
        }
        return true;
    }

    ReadMetadataRanges.reserve(record.RangesSize());

    auto ydbKey = isIndexStats ?
        NOlap::GetColumns(PrimaryIndexStatsSchema, PrimaryIndexStatsSchema.KeyColumns) :
        indexInfo->GetPrimaryKeyColumns();

    for (auto& range: record.GetRanges()) {
        if (!FillPredicatesFromRange(read, range, ydbKey, Self->TabletID(), isIndexStats ? nullptr : indexInfo, ErrorDescription)) {
            ReadMetadataRanges.clear();
            return true;
        }
    }
    {
        auto newRange = CreateReadMetadata(read, isIndexStats, record.GetReverse(), itemsLimit);
        if (!newRange) {
            ReadMetadataRanges.clear();
            return true;
        }
        ReadMetadataRanges.emplace_back(newRange);
    }
    Y_ABORT_UNLESS(ReadMetadataRanges.size() == 1);

    return true;
}

template <typename T>
struct TContainerPrinter {
    const T& Ref;

    TContainerPrinter(const T& ref)
        : Ref(ref)
    {}

    friend IOutputStream& operator << (IOutputStream& out, const TContainerPrinter& cont) {
        for (auto& ptr : cont.Ref) {
            out << *ptr << " ";
        }
        return out;
    }
};

void TTxScan::Complete(const TActorContext& ctx) {
    auto& request = Ev->Get()->Record;
    auto scanComputeActor = Ev->Sender;
    const auto& snapshot = request.GetSnapshot();
    const auto scanId = request.GetScanId();
    const ui64 txId = request.GetTxId();
    const ui32 scanGen = request.GetGeneration();
    TString table = request.GetTablePath();
    auto dataFormat = request.GetDataFormat();
    const TDuration timeout = TDuration::MilliSeconds(request.GetTimeoutMs());
    if (scanGen > 1) {
        Self->IncCounter(COUNTER_SCAN_RESTARTED);
    }

    TStringStream detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << TContainerPrinter(ReadMetadataRanges) << ")" << " req: " << request;
    }
    if (ReadMetadataRanges.empty()) {
        LOG_S_DEBUG("TTxScan failed "
                << " txId: " << txId
                << " scanId: " << scanId
                << " gen: " << scanGen
                << " table: " << table
                << " snapshot: " << snapshot
                << " timeout: " << timeout
                << detailedInfo.Str()
                << " at tablet " << Self->TabletID());

        auto ev = MakeHolder<TEvKqpCompute::TEvScanError>(scanGen, Self->TabletID());

        ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
            << "Table " << table << " (shard " << Self->TabletID() << ") scan failed, reason: " << ErrorDescription ? ErrorDescription : "unknown error");
        NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

        ctx.Send(scanComputeActor, ev.Release());
        return;
    }

    const NOlap::TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex();
    }
    ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(ReadMetadataRanges, index);
    auto statsDelta = Self->InFlightReadsTracker.GetSelectStatsDelta();

    Self->IncCounter(COUNTER_READ_INDEX_PORTIONS, statsDelta.Portions);
    Self->IncCounter(COUNTER_READ_INDEX_BLOBS, statsDelta.Blobs);
    Self->IncCounter(COUNTER_READ_INDEX_ROWS, statsDelta.Rows);
    Self->IncCounter(COUNTER_READ_INDEX_BYTES, statsDelta.Bytes);

    NOlap::TComputeShardingPolicy shardingPolicy;
    AFL_VERIFY(shardingPolicy.DeserializeFromProto(request.GetComputeShardingPolicy()));

    auto scanActor = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(),
        shardingPolicy, scanId, txId, scanGen, requestCookie, Self->TabletID(), timeout, std::move(ReadMetadataRanges), dataFormat, Self->ScanCounters));

    LOG_S_DEBUG("TTxScan starting " << scanActor
                << " txId: " << txId
                << " scanId: " << scanId
                << " gen: " << scanGen
                << " table: " << table
                << " snapshot: " << snapshot
                << " timeout: " << timeout
                << detailedInfo.Str()
                << " at tablet " << Self->TabletID());
}


void TColumnShard::Handle(TEvColumnShard::TEvScan::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();
    const auto& scanId = record.GetScanId();
    const auto& snapshot = record.GetSnapshot();

    NOlap::TSnapshot readVersion(snapshot.GetStep(), snapshot.GetTxId());
    NOlap::TSnapshot maxReadVersion = GetMaxReadVersion();

    LOG_S_DEBUG("EvScan txId: " << txId
        << " scanId: " << scanId
        << " version: " << readVersion
        << " readable: " << maxReadVersion
        << " at tablet " << TabletID());

    if (maxReadVersion < readVersion) {
        WaitingScans.emplace(readVersion, std::move(ev));
        WaitPlanStep(readVersion.GetPlanStep());
        return;
    }

    LastAccessTime = TAppData::TimeProvider->Now();
    ScanTxInFlight.insert({txId, LastAccessTime});
    SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    Execute(new TTxScan(this, ev), ctx);
}

const NKikimr::NOlap::TReadStats& TScanIteratorBase::GetStats() const {
    return Default<NOlap::TReadStats>();
}

}

namespace NKikimr::NOlap {

class TCurrentBatch {
private:
    std::vector<TPartialReadResult> Results;
    ui64 RecordsCount = 0;
public:
    ui64 GetRecordsCount() const {
        return RecordsCount;
    }

    void AddChunk(TPartialReadResult&& res) {
        RecordsCount += res.GetRecordsCount();
        Results.emplace_back(std::move(res));
    }

    void FillResult(std::vector<TPartialReadResult>& result) const {
        if (Results.empty()) {
            return;
        }
        for (auto&& i : Results) {
            result.emplace_back(std::move(i));
        }
    }
};

std::vector<NKikimr::NOlap::TPartialReadResult> TPartialReadResult::SplitResults(std::vector<TPartialReadResult>&& resultsExt, const ui32 maxRecordsInResult) {
    std::vector<TCurrentBatch> resultBatches;
    TCurrentBatch currentBatch;
    for (auto&& i : resultsExt) {
        AFL_VERIFY(i.GetRecordsCount());
        currentBatch.AddChunk(std::move(i));
        if (currentBatch.GetRecordsCount() >= maxRecordsInResult) {
            resultBatches.emplace_back(std::move(currentBatch));
            currentBatch = TCurrentBatch();
        }
    }
    if (currentBatch.GetRecordsCount()) {
        resultBatches.emplace_back(std::move(currentBatch));
    }

    std::vector<TPartialReadResult> result;
    for (auto&& i : resultBatches) {
        i.FillResult(result);
    }
    return result;
}

}
