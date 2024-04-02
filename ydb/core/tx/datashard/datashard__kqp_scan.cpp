#include "datashard_impl.h"
#include "range_ops.h"
#include <util/string/vector.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tablet_flat/flat_row_celled.h>

#include <ydb/library/chunks_limiter/chunks_limiter.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

namespace NKikimr {
namespace NDataShard {

// using namespace NTabletFlatExecutor;
using namespace NKqp;
using namespace NMiniKQL;

constexpr ui64 MAX_BATCH_ROWS = 10'000;
constexpr ui64 INIT_BATCH_ROWS = 1'000;
constexpr ui64 MIN_BATCH_ROWS_ON_PAGEFAULT = 1'000;
constexpr ui64 MIN_BATCH_SIZE_ON_PAGEFAULT = 256_KB;
constexpr ui64 READAHEAD_LO = 32_KB;
constexpr ui64 READAHEAD_HI = 512_KB;
constexpr TDuration SCAN_HARD_TIMEOUT = TDuration::Minutes(10);
constexpr TDuration SCAN_HARD_TIMEOUT_GAP = TDuration::Seconds(5);

class TKqpScanResult : public IDestructable {};

class TKqpScan : public TActor<TKqpScan>, public NTable::IScan {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TABLE_SCAN;
    }

public:
    TKqpScan(const TActorId& computeActorId, const TActorId& datashardActorId, ui32 scanId,
        NDataShard::TUserTable::TCPtr tableInfo, const TSmallVec<TSerializedTableRange>&& tableRanges,
        const TSmallVec<NTable::TTag>&& columnTags, const TSmallVec<bool>&& skipNullKeys,
        const NYql::NDqProto::EDqStatsMode& statsMode, ui64 timeoutMs, ui32 generation,
        NKikimrDataEvents::EDataFormat dataFormat, const ui64 tabletId)
        : TActor(&TKqpScan::StateScan)
        , ComputeActorId(computeActorId)
        , DatashardActorId(datashardActorId)
        , ScanId(scanId)
        , TableInfo(tableInfo)
        , TablePath(TableInfo->Path)
        , TableRanges(std::move(tableRanges))
        , CurrentRange(0)
        , Tags(std::move(columnTags))
        , SkipNullKeys(std::move(skipNullKeys))
        , StatsMode(statsMode)
        , Deadline(TInstant::Now() + (timeoutMs ? TDuration::MilliSeconds(timeoutMs) + SCAN_HARD_TIMEOUT_GAP : SCAN_HARD_TIMEOUT))
        , Generation(generation)
        , DataFormat(dataFormat)
        , Sleep(true)
        , IsLocal(computeActorId.NodeId() == datashardActorId.NodeId())
        , TabletId(tabletId)
    {
        if (DataFormat == NKikimrDataEvents::FORMAT_ARROW) {
            BatchBuilder = MakeHolder<NArrow::TArrowBatchBuilder>();
            TVector<std::pair<TString, NScheme::TTypeInfo>> schema;
            if (!Tags.empty()) {
                Types.reserve(Tags.size());
                schema.reserve(Tags.size());
                for (const auto tag: Tags) {
                    const auto& column = TableInfo->Columns.at(tag);
                    Types.emplace_back(column.Type);
                    schema.emplace_back(column.Name, column.Type);
                }
                BatchBuilder->Reserve(INIT_BATCH_ROWS);
                auto started = BatchBuilder->Start(schema);
                YQL_ENSURE(started.ok(), "Failed to start BatchBuilder: " + started.ToString());
            }
        }

        for (auto& range : TableRanges) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "--> Scan range: "
                << DebugPrintRange(TableInfo->KeyColumnTypes, range.ToTableRange(), *AppData()->TypeRegistry));
        }
    }

private:
    STATEFN(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpCompute::TEvScanDataAck, HandleScan);
            hFunc(TEvKqpCompute::TEvKillScanTablet, HandleScan);
            hFunc(TEvKqp::TEvAbortExecution, HandleScan);
            hFunc(TEvents::TEvUndelivered, HandleScan);
            hFunc(TEvents::TEvWakeup, HandleScan);
            default:
                Y_ABORT("TKqpScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    void HandleScan(TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
        if (!Driver) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Got ScanDataAck while driver not set");
            PassAway();
            return;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Got ScanDataAck"
            << ", at: " << ScanActorId << ", scanId: " << ScanId << ", table: " << TablePath
            << ", gen: " << ev->Get()->Generation << ", tablet: " << DatashardActorId
            << ", freeSpace: " << ev->Get()->FreeSpace << ";" << ChunksLimiter.DebugString());

        YQL_ENSURE(ev->Get()->Generation == Generation, "expected: " << Generation << ", got: " << ev->Get()->Generation);

        if (!ComputeActorId) {
            ComputeActorId = ev->Sender;
        }

        ChunksLimiter = TChunksLimiter(ev->Get()->FreeSpace, ev->Get()->MaxChunksCount);
        if (ChunksLimiter.HasMore()) {
            if (Y_UNLIKELY(IsProfile())) {
                StartWaitTime = TInstant::Now();
            }
            if (Sleep) {
                Sleep = false;
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Wakeup driver at: " << ScanActorId);
                Driver->Touch(EScan::Feed);
            }
        }
    }

    void HandleScan(TEvKqpCompute::TEvKillScanTablet::TPtr&) {
        LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Kill self tablet " << DatashardActorId);
        Send(DatashardActorId, new TEvents::TEvPoison);
    }

    void HandleScan(TEvKqp::TEvAbortExecution::TPtr& ev) {
        if (!Driver) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Got AbortExecution while driver not set");
            PassAway();
            return;
        }

        auto& msg = ev->Get()->Record;

        auto prio = msg.GetStatusCode() == NYql::NDqProto::StatusIds::SUCCESS ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN;
        LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::TX_DATASHARD, "Got AbortExecution"
            << ", at: " << ScanActorId << ", tablet: " << DatashardActorId
            << ", scanId: " << ScanId << ", table: " << TablePath
            << ", code: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", reason: " << ev->Get()->GetIssues().ToOneLineString());

        AbortEvent = ev->Release();
        Driver->Touch(EScan::Final);
    }

    void HandleScan(TEvents::TEvUndelivered::TPtr& ev) {
        if (!Driver) {
            PassAway();
            return;
        }

        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Undelivered event: " << ev->GetTypeRewrite()
            << ", at: " << ScanActorId << ", tablet: " << DatashardActorId
            << ", scanId: " << ScanId << ", table: " << TablePath);

        switch (ev->GetTypeRewrite()) {
            case TEvKqpCompute::TEvScanInitActor::EventType:
            case TEvKqpCompute::TEvScanData::EventType:
                Driver->Touch(EScan::Final);
        }
    }

    void HandleScan(TEvents::TEvWakeup::TPtr&) {
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Guard execution timeout at: " << ScanActorId
            << ", scanId: " << ScanId << ", table: " << TablePath);

        TimeoutActorId = {};

        if (Driver) {
            Driver->Touch(EScan::Final);
        } else {
            PassAway();
        }
    }

private:
    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) noexcept final {
        Y_ABORT_UNLESS(scheme);
        Y_ABORT_UNLESS(driver);

        Driver = driver;
        ScanActorId = TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        // propagate self actor id
        Send(ComputeActorId, new TEvKqpCompute::TEvScanInitActor(ScanId, ScanActorId, Generation, TabletId),
             IEventHandle::FlagTrackDelivery);

        Sleep = true;

        TInitialState startConfig;
        startConfig.Scan = EScan::Sleep;
        startConfig.Conf.ReadAheadLo = READAHEAD_LO;
        startConfig.Conf.ReadAheadHi = READAHEAD_HI;

        TimeoutActorId = CreateLongTimer(TlsActivationContext->AsActorContext(), Deadline - TInstant::Now(),
            new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup));

        if (Y_UNLIKELY(IsProfile())) {
            StartWaitTime = TInstant::Now();
        }

        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Start scan"
            << ", at: " << ScanActorId << ", tablet: " << DatashardActorId
            << ", scanId: " << ScanId << ", table: " << TablePath << ", gen: " << Generation
            << ", deadline: " << Deadline);

        return startConfig;
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        YQL_ENSURE(seq == CurrentRange);

        if (CurrentRange == TableRanges.size()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "TableRanges is over"
                << ", at: " << ScanActorId << ", scanId: " << ScanId
                << ", table: " << TablePath);
            return EScan::Final;
        }

        auto& range = TableRanges[CurrentRange];

        int cmpFrom;
        int cmpTo;
        cmpFrom = CompareBorders<false, false>(
            range.From.GetCells(),
            TableInfo->Range.From.GetCells(),
            range.FromInclusive,
            TableInfo->Range.FromInclusive,
            TableInfo->KeyColumnTypes);

        cmpTo = CompareBorders<true, true>(
            range.To.GetCells(),
            TableInfo->Range.To.GetCells(),
            range.ToInclusive,
            TableInfo->Range.ToInclusive,
            TableInfo->KeyColumnTypes);

        if (cmpFrom > 0) {
            auto seek = range.FromInclusive ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(Tags, range.From.GetCells(), seek);
        } else {
            lead.To(Tags, {}, NTable::ESeek::Lower);
        }

        if (cmpTo < 0) {
            lead.Until(range.To.GetCells(), range.ToInclusive);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final {
        LastKey = TOwnedCellVec(key);

        Y_ABORT_UNLESS(SkipNullKeys.size() <= key.size());
        for (ui32 i = 0; i < SkipNullKeys.size(); ++i) {
            if (SkipNullKeys[i] && key[i].IsNull()) {
                return EScan::Feed;
            }
        }

        MakeResult();

        if (Y_UNLIKELY(IsProfile())) {
            Result->WaitTime += TInstant::Now() - StartWaitTime;
        }

        Y_DEFER {
            if (Y_UNLIKELY(IsProfile())) {
                StartWaitTime = TInstant::Now();
            }
        };

        AddRow(row);

        auto sent = SendResult(/* pageFault */ false);

        if (!sent) {
            // There is free space in memory and results are not sent to caller
            return EScan::Feed;
        }

        if (!ChunksLimiter.HasMore()) {
            Sleep = true;
            return EScan::Sleep;
        }

        return EScan::Feed; // sent by rows limit, can send one more batch
    }

    EScan Exhausted() noexcept override {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
            "Range " << CurrentRange << " of " << TableRanges.size() << " exhausted: try next one."
            << " table: " << TablePath
            << " range: " << DebugPrintRange(
                TableInfo->KeyColumnTypes, TableRanges[CurrentRange].ToTableRange(), *AppData()->TypeRegistry
                )
            << " next range: " << ((CurrentRange + 1) >= TableRanges.size() ? "<none>" : DebugPrintRange(
                TableInfo->KeyColumnTypes, TableRanges[CurrentRange + 1].ToTableRange(), *AppData()->TypeRegistry
                ))
        );

        ++CurrentRange;
        return EScan::Reset;
    }

    EScan PageFault() noexcept override final {
        ++PageFaults;
        if (Result && !Result->Rows.empty()) {
            bool sent = SendResult(/* pageFault */ true);

            if (sent && !ChunksLimiter.HasMore()) {
                Sleep = true;
                return EScan::Sleep;
            }

            if (sent && ChunksLimiter.HasMore() && Y_UNLIKELY(IsProfile())) {
                StartWaitTime = TInstant::Now();
            }
        }
        return EScan::Feed;
    }

private:
    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        auto prio = abort == EAbort::None ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR;
        LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::TX_DATASHARD, "Finish scan"
            << ", at: " << ScanActorId << ", scanId: " << ScanId
            << ", table: " << TablePath << ", reason: " << (int) abort
            << ", abortEvent: " << (AbortEvent ? AbortEvent->Record.ShortDebugString() : TString("<none>")));

        if (abort != EAbort::None || AbortEvent) {
            auto ev = MakeHolder<TEvKqpCompute::TEvScanError>(Generation, TabletId);

            if (AbortEvent) {
                ev->Record.SetStatus(NYql::NDq::DqStatusToYdbStatus(AbortEvent->Record.GetStatusCode()));
                auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED, TStringBuilder()
                    << "Table " << TablePath << " scan failed");
                for (const NYql::TIssue& i : AbortEvent->GetIssues()) {
                    issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
                }
                IssueToMessage(issue, ev->Record.MutableIssues()->Add());
            } else {
                ev->Record.SetStatus(Ydb::StatusIds::ABORTED);
                auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED, TStringBuilder()
                    << "Table " << TablePath << " scan failed, reason: " << ToString((int) abort));
                IssueToMessage(issue, ev->Record.MutableIssues()->Add());
            }

            Send(ComputeActorId, ev.Release());
        } else {
            if (Result) {
                // TODO:
                // Result->CpuTime = CpuTime;
            } else {
                Result = MakeHolder<TEvKqpCompute::TEvScanData>(ScanId, Generation);
            }
            auto send = SendResult(Result->PageFault, true);
            Y_DEBUG_ABORT_UNLESS(send);
        }

        Driver = nullptr;
        if (TimeoutActorId) {
            Send(TimeoutActorId, new TEvents::TEvPoison);
        }
        PassAway();

        return new TKqpScanResult();
    }

    void Describe(IOutputStream& out) const noexcept final {
        out << "TExecuteKqpScanTxUnit, TKqpScan";
    }

    void MakeResult() {
        if (!Result) {
            Result = MakeHolder<TEvKqpCompute::TEvScanData>(ScanId, Generation);
            switch (DataFormat) {
                case NKikimrDataEvents::FORMAT_UNSPECIFIED:
                case NKikimrDataEvents::FORMAT_CELLVEC: {
                    Result->Rows.reserve(INIT_BATCH_ROWS);
                    break;
                }
                case NKikimrDataEvents::FORMAT_ARROW: {
                }
            }
        }
    }

    void AddRow(const TRow& row) {
        ++Rows;
        // NOTE: Some per-row overhead to deal with the case when no columns were requested
        if (Tags.empty()) {
            CellvecBytes += 8;
        }
        for (auto& cell: *row) {
            CellvecBytes += std::max((ui64)8, (ui64)cell.Size());
        }
        switch (DataFormat) {
            case NKikimrDataEvents::FORMAT_UNSPECIFIED:
            case NKikimrDataEvents::FORMAT_CELLVEC: {
                Result->Rows.emplace_back(TOwnedCellVec::Make(*row));
                break;
            }
            case NKikimrDataEvents::FORMAT_ARROW: {
                NKikimr::TDbTupleRef key;
                Y_DEBUG_ABORT_UNLESS((*row).size() == Types.size());
                NKikimr::TDbTupleRef value = NKikimr::TDbTupleRef(Types.data(), (*row).data(), Types.size());
                BatchBuilder->AddRow(key, value);
                break;
            }
        }
    }

    bool SendResult(bool pageFault, bool finish = false) noexcept {
        if (Rows >= MAX_BATCH_ROWS || CellvecBytes >= ChunksLimiter.GetRemainedBytes() ||
            (pageFault && (Rows >= MIN_BATCH_ROWS_ON_PAGEFAULT || CellvecBytes >= MIN_BATCH_SIZE_ON_PAGEFAULT)) || finish)
        {
            Result->PageFault = pageFault;
            Result->PageFaults = PageFaults;
            if (finish) {
                Result->Finished = true;
            } else {
                Result->LastKey = LastKey;
            }
            auto sendBytes = CellvecBytes;

            if (DataFormat == NKikimrDataEvents::FORMAT_ARROW) {
                FlushBatchToResult();
                sendBytes = NArrow::GetTableDataSize(Result->ArrowBatch);
                // Batch is stored inside BatchBuilder until we flush it into Result. So we verify number of rows here.
                YQL_ENSURE(Rows == 0 && Result->ArrowBatch == nullptr || Result->ArrowBatch->num_rows() == (i64) Rows);
            } else {
                YQL_ENSURE(Result->Rows.size() == Rows);
            }

            PageFaults = 0;

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Send ScanData"
                << ", from: " << ScanActorId << ", to: " << ComputeActorId
                << ", scanId: " << ScanId << ", table: " << TablePath
                << ", bytes: " << sendBytes << ", rows: " << Rows << ", page faults: " << Result->PageFaults
                << ", finished: " << Result->Finished << ", pageFault: " << Result->PageFault);

            if (sendBytes >= 48_MB) {
                LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Query size limit exceeded.");
                if (finish) {
                    bool sent = Send(ComputeActorId, new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                        "Query size limit exceeded."));
                    Y_ABORT_UNLESS(sent);

                    ReportDatashardStats();
                    return true;
                } else {
                    bool sent = Send(SelfId(), new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                        "Query size limit exceeded."));
                    Y_ABORT_UNLESS(sent);

                    ReportDatashardStats();
                    return false;
                }
            }

            if (!finish) {
                Y_ABORT_UNLESS(ChunksLimiter.Take(sendBytes));
                Result->RequestedBytesLimitReached = !ChunksLimiter.HasMore();
            }

            Send(ComputeActorId, Result.Release(), IEventHandle::FlagTrackDelivery);
            ReportDatashardStats();

            return true;
        }
        return false;
    }

    // Call only after MakeResult method.
    void FlushBatchToResult() {
        // FlushBatch reset Batch pointer in BatchBuilder only if some rows were added after. So we if we have already
        // send a batch and try to send an empty batch again without adding rows, then a copy of the batch will be send
        // instead. So we check Rows here.
        if (Rows != 0) {
            Result->ArrowBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({Tags.empty() ? NArrow::CreateNoColumnsBatch(Rows) : BatchBuilder->FlushBatch(true)}));
        }
    }

    void ReportDatashardStats() {
        Send(DatashardActorId, new TDataShard::TEvPrivate::TEvScanStats(Rows, CellvecBytes));
        Rows = 0;
        CellvecBytes = 0;
    }

    bool IsProfile() const {
        return StatsMode >= NYql::NDqProto::DQ_STATS_MODE_PROFILE;
    }

private:
    TActorId ComputeActorId;
    const TActorId DatashardActorId;
    const ui32 ScanId;
    const NDataShard::TUserTable::TCPtr TableInfo;
    const TString TablePath;
    const TSmallVec<TSerializedTableRange> TableRanges;
    ui32 CurrentRange;
    const TSmallVec<NTable::TTag> Tags;
    TSmallVec<NScheme::TTypeInfo> Types;
    const TSmallVec<bool> SkipNullKeys;
    const NYql::NDqProto::EDqStatsMode StatsMode;
    const TInstant Deadline;
    const ui32 Generation;
    const NKikimrDataEvents::EDataFormat DataFormat;
    TChunksLimiter ChunksLimiter;
    bool Sleep;
    const bool IsLocal;
    const ui64 TabletId;

    IDriver* Driver = nullptr;
    TActorId ScanActorId;
    TActorId TimeoutActorId;
    TAutoPtr<TEvKqp::TEvAbortExecution> AbortEvent;

    THolder<NArrow::TArrowBatchBuilder> BatchBuilder;
    THolder<TEvKqpCompute::TEvScanData> Result;
    ui64 Rows = 0;
    ui64 CellvecBytes = 0;
    ui32 PageFaults = 0;
    TInstant StartWaitTime;

    // Key corresponding to the latest row added to RowsBuffer. Used to avoid row duplication on
    // scan restarts.
    TOwnedCellVec LastKey;
};

class TDataShard::TTxHandleSafeKqpScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeKqpScan(TDataShard* self, TEvDataShard::TEvKqpScan::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvKqpScan::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeKqpScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext&) {
    auto& request = ev->Get()->Record;
    auto scanComputeActor = ev->Sender;
    auto generation = request.GetGeneration();

    if (VolatileTxManager.HasVolatileTxsAtSnapshot(TRowVersion(request.GetSnapshot().GetStep(), request.GetSnapshot().GetTxId()))) {
        VolatileTxManager.AttachWaitingSnapshotEvent(
            TRowVersion(request.GetSnapshot().GetStep(), request.GetSnapshot().GetTxId()),
            std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto infoIt = TableInfos.find(request.GetLocalPathId());

    auto reportError = [this, scanComputeActor, generation] (const TString& table, const TString& detailedReason) {
        auto ev = MakeHolder<TEvKqpCompute::TEvScanError>(generation, TabletID());
        ev->Record.SetStatus(Ydb::StatusIds::ABORTED);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder() <<
            "Table '" << table << "' scheme changed.");
        IssueToMessage(issue, ev->Record.MutableIssues()->Add());
        Send(scanComputeActor, ev.Release(), IEventHandle::FlagTrackDelivery);
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, detailedReason);
    };

    if (infoIt == TableInfos.end()) {
        reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
            << " Can not find table '" << request.GetTablePath() << "'"
            << " by LocalPathId " << request.GetLocalPathId() << " at " << TabletID());
        return;
    }

    auto tableInfo = infoIt->second; // copy table info ptr here
    auto& tableColumns = tableInfo->Columns;
    Y_ABORT_UNLESS(request.GetColumnTags().size() == request.GetColumnTypes().size());

    if (tableInfo->GetTableSchemaVersion() != 0 &&
        request.GetSchemaVersion() != tableInfo->GetTableSchemaVersion())
    {
        reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
            << " Table '" << request.GetTablePath() << "'"
            << " schema version changed at " << TabletID());
        return;
    }

    for (int i = 0; i < request.GetColumnTags().size(); ++i) {
        auto* column = tableColumns.FindPtr(request.GetColumnTags(i));
        if (!column) {
            reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
                << " Cant find table '" << request.GetTablePath() << "'"
                << " column " << request.GetColumnTags(i)  << " at " << TabletID());
            return;
        }

        // TODO: support pg types
        if (column->Type.GetTypeId() != request.GetColumnTypes(i)) {
            reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
                << " Table '" << request.GetTablePath() << "'"
                << " column " << request.GetColumnTags(i)  << " type mismatch at " << TabletID());
            return;
        }
    }

    if (request.HasOlapProgram()) {
        auto msg = TStringBuilder() << "TxId: " << request.GetTxId() << "."
            << " Unexpected process program in datashard scan at " << TabletID();
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, msg);

        auto ev = MakeHolder<TEvKqpCompute::TEvScanError>(generation, TabletID());
        ev->Record.SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::DEFAULT_ERROR, msg);
        IssueToMessage(issue, ev->Record.MutableIssues()->Add());
        Send(scanComputeActor, ev.Release(), IEventHandle::FlagTrackDelivery);
        return;
    }

    auto& snapshot = request.GetSnapshot();

    auto snapshotKey = TSnapshotKey(PathOwnerId, request.GetLocalPathId(), snapshot.GetStep(), snapshot.GetTxId());
    if (!SnapshotManager.FindAvailable(snapshotKey)) {
        reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
            << " Snapshot is not valid, tabletId: " << TabletID() << ", step: " << snapshot.GetStep());
        return;
    }

    if (!IsStateActive()) {
        reportError(request.GetTablePath(), TStringBuilder() << "TxId: " << request.GetTxId() << "."
            << " Shard " << TabletID() << " is not ready to process requests.");
        return;
    }

    Pipeline.StartStreamingTx(snapshot.GetTxId(), 1);

    TSmallVec<TSerializedTableRange> ranges;
    ranges.reserve(request.RangesSize());

    for (auto range: request.GetRanges()) {
        ranges.emplace_back(std::move(TSerializedTableRange(range)));
    }

    auto* tableScan = new TKqpScan(
        scanComputeActor,
        SelfId(),
        request.GetScanId(),
        tableInfo,
        std::move(ranges),
        std::move(TSmallVec<NTable::TTag>(request.GetColumnTags().begin(), request.GetColumnTags().end())),
        std::move(TSmallVec<bool>(request.GetSkipNullKeys().begin(), request.GetSkipNullKeys().end())),
        request.GetStatsMode(),
        request.GetTimeoutMs(),
        generation,
        request.GetDataFormat(),
        TabletID()
    );

    auto scanOptions = TScanOptions()
        .DisableResourceBroker()
        .SetReadPrio(TScanOptions::EReadPrio::Low)
        .SetReadAhead(READAHEAD_LO, READAHEAD_HI)
        .SetSnapshotRowVersion(TRowVersion(snapshot.GetStep(), snapshot.GetTxId()));

    Executor()->QueueScan(tableInfo->LocalTid, tableScan, snapshot.GetTxId(), scanOptions);
}

}
}
