#include "service.h"
#include "topic_reader_stats.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_replication.pb.h>
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/replication/common/schema_change.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/deque.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_SERVICE

namespace NKikimr::NReplication::NService {

TEvWorker::TEvPoll::TEvPoll(bool skipCommit)
    : SkipCommit(skipCommit)
{
}

TString TEvWorker::TEvPoll::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " SkipCommit: " << SkipCommit
    << " }";
}

TEvWorker::TEvCommit::TEvCommit(size_t offset)
    : Offset(offset)
{
}

TString TEvWorker::TEvCommit::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Offset: " << Offset
    << " }";
}

TEvWorker::TEvCommitResult::TEvCommitResult(size_t offset)
    : Offset(offset)
{
}

TString TEvWorker::TEvCommitResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Offset: " << Offset
    << " }";
}

TEvWorker::TEvSchemaChange::TEvSchemaChange(const NKikimrReplication::TSchemaChange& schema, size_t offset)
    : Schema(schema)
    , Offset(offset)
{
}

TString TEvWorker::TEvSchemaChange::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Offset: " << Offset
        << " Schema: " << Schema.ShortDebugString()
    << " }";
}

TEvWorker::TEvSchemaChangeApplied::TEvSchemaChangeApplied(const NKikimrReplication::TSchemaChange& schema)
    : Schema(schema)
{
}

TString TEvWorker::TEvSchemaChangeApplied::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Schema: " << Schema.ShortDebugString()
    << " }";
}

TEvWorker::TEvReaderStarted::TEvReaderStarted(ui64 committedOffset)
    : CommittedOffset(committedOffset)
{
}

TString TEvWorker::TEvReaderStarted::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " CommittedOffset: " << CommittedOffset
    << " }";
}

TEvWorker::TEvData::TEvData(ui32 partitionId, const TString& source, const TVector<TTopicMessage>& records)
    : PartitionId(partitionId)
    , Source(source)
    , Records(records)
{
}

TEvWorker::TEvData::TEvData(ui32 partitionId, const TString& source, TVector<TTopicMessage>&& records)
    : PartitionId(partitionId)
    , Source(source)
    , Records(std::move(records))
{
}

TString TEvWorker::TEvData::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Source: " << Source
        << " Records [" << JoinSeq(",", Records) << "]"
    << " }";
}

TEvWorker::TEvGone::TEvGone(EStatus status, const TString& errorDescription)
    : Status(status)
    , ErrorDescription(errorDescription)
{
}

TString TEvWorker::TEvGone::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Status: " << Status
        << " ErrorDescription: " << ErrorDescription
    << " }";
}

TEvWorker::TEvStatus::TEvStatus(TDuration lag)
    : Lag(lag)
{
}

TEvWorker::TEvStatus::TEvStatus(std::unique_ptr<TWorkerDetailedStats>&& detailedStats)
    : Lag(TDuration::Zero())
    , DetailedStats(std::move(detailedStats))
{
}

TEvWorker::TEvStatus* TEvWorker::TEvStatus::FromOperation(EWorkerOperation operation) {
    auto detailedStats = std::make_unique<TWorkerDetailedStats>();
    detailedStats->CurrentOperation = operation;
    return new TEvStatus(std::move(detailedStats));
}

TString TEvWorker::TEvStatus::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Lag: " << Lag
        << " HasStats: " << (DetailedStats != nullptr)
    << " }";
}

TEvWorker::TEvDataEnd::TEvDataEnd(ui64 partitionId, TVector<ui64>&& adjacentPartitionsIds, TVector<ui64>&& childPartitionsIds)
    : PartitionId(partitionId)
    , AdjacentPartitionsIds(std::move(adjacentPartitionsIds))
    , ChildPartitionsIds(std::move(childPartitionsIds))
{
}

TString TEvWorker::TEvDataEnd::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " PartitionId: " << PartitionId
        << " AdjacentPartitionsIds: " << JoinSeq(", ", AdjacentPartitionsIds)
        << " ChildPartitionsIds: " << JoinSeq(", ", ChildPartitionsIds)
    << " }";
}

TEvWorker::TEvTerminateWriter::TEvTerminateWriter(ui64 partitionId)
    : PartitionId(partitionId)
{
}

TString TEvWorker::TEvTerminateWriter::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " PartitionId: " << PartitionId
    << " }";
}

TEvWorker::TEvStatsWakeup::TEvStatsWakeup(ui64 sessionToAdd, ui64 sessionToRemove)
    : SessionToAdd(sessionToAdd)
    , SessionToRemove(sessionToRemove)
{
}

class TWorker: public TActorBootstrapped<TWorker> {
    class TActorInfo {
        std::function<IActor*(void)> CreateFn;
        TActorId ActorId;
        bool InitDone;
        ui32 CreateAttempt;

    public:
        explicit TActorInfo(std::function<IActor*(void)>&& createFn)
            : CreateFn(std::move(createFn))
            , InitDone(false)
            , CreateAttempt(0)
        {
        }

        operator TActorId() const {
            return ActorId;
        }

        explicit operator bool() const {
            return InitDone;
        }

        void Register(IActorOps* ops) {
            ActorId = ops->RegisterWithSameMailbox(CreateFn());
            ops->Send(ActorId, new TEvWorker::TEvHandshake());
            InitDone = false;
            ++CreateAttempt;
        }

        void Registered() {
            InitDone = true;
        }

        void ResetCreateAttempt() {
            CreateAttempt = 0;
        }

        ui32 GetCreateAttempt() const {
            return CreateAttempt;
        }
    };

    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "Worker"},
            {"selfId", SelfId()});
    }

    THolder<NKikimrReplication::TEvSourceOffsetReport> MakeSourceOffsetReport() {
        auto request = MakeHolder<NKikimrReplication::TEvSourceOffsetReport>();
        request->SetWorkerInstance(ToString(SelfId()));
        request->SetRequestId(++SourceOffsetReportId);
        return request;
    }

    void SendSourceOffsetReport(const NKikimrReplication::TEvSourceOffsetReport& request) {
        auto ev = MakeHolder<TEvService::TEvSourceOffsetReport>();
        ev->Record.CopyFrom(request);
        Send(Parent, std::move(ev));
    }

    void ProcessSourceActions() {
        if (Draining) {
            SourceActions.clear();
            return MaybeDrained();
        }
        if (TrackSourceOffsets && (!Reader || !ReaderValidated || SourceOffsetReport || !SourceOffsetMismatch.empty())) {
            return;
        }
        while (!SourceActions.empty()) {
            auto& action = SourceActions.front();
            if (TrackSourceOffsets && action.Offset && (!AuthorizedSourceOffset || *AuthorizedSourceOffset < *action.Offset)) {
                SourceOffsetReport = MakeSourceOffsetReport();
                SourceOffsetReport->SetNextOffset(*action.Offset);
                SendSourceOffsetReport(*SourceOffsetReport);
                return;
            }
            if (action.Event->Type() == TEvWorker::EvPoll) {
                ReadRequested = true;
            }
            Send(Reader, action.Event.Release());
            SourceActions.pop_front();
        }
    }

    void AuthorizeSource(TMaybe<ui64> offset, THolder<IEventBase> event) {
        if (offset && (!ProcessedSourceOffset || *offset > *ProcessedSourceOffset)) {
            ProcessedSourceOffset = offset;
        }
        SourceActions.push_back({offset, std::move(event)});
        ProcessSourceActions();
    }

    void PollSource(bool skipCommit = false) {
        AuthorizeSource(skipCommit ? Nothing() : BatchEnd, MakeHolder<TEvWorker::TEvPoll>(skipCommit));
    }

    void CommitSource(ui64 offset) {
        AuthorizeSource(offset, MakeHolder<TEvWorker::TEvCommit>(offset));
    }

    void StartValidatedReader() {
        if (!Reader || !ReaderValidated || Draining) {
            return;
        }
        if (RecoveredCompletionSchema && ReaderCommittedOffset
            && *ReaderCommittedOffset > RecoveredCompletionOffset)
        {
            ReportRecoveredCompletion();
        }
        if (PendingSchemaChange && !SchemaReportCommitted) {
            CommitSource(PendingSchemaChange->Offset);
        } else if (PendingSchemaChange && SchemaAdvanceInFlight) {
            CommitSource(PendingSchemaChange->Offset + 1);
        } else if (PendingWriterCheckpoint) {
            CommitSource(*PendingWriterCheckpoint);
        } else if (!InFlightData && !TerminateWriter && !ReadRequested && SourceActions.empty()) {
            PollSource();
        } else {
            ProcessSourceActions();
        }
    }

    void Handle(TEvService::TEvSourceOffsetResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetWorkerInstance() != ToString(SelfId())) {
            return;
        }
        if (!SourceOffsetReport || SourceOffsetReport->GetRequestId() != record.GetRequestId()) {
            return;
        }
        const bool drained = SourceOffsetReport->GetDrained();
        SourceOffsetReport.Reset();
        if (record.HasAuthorizedNextOffset()) {
            AuthorizedSourceOffset = record.GetAuthorizedNextOffset();
        }
        if (drained) {
            return PassAway();
        }
        if (record.GetStop()) {
            BeginDrain();
        }
        ProcessSourceActions();
    }

    void BeginDrain() {
        if (!Draining) {
            Draining = true;
            SourceActions.clear();
            Send(Reader, new TEvents::TEvPoison());
            if (PendingSchemaChange) {
                // The writer is parked after draining the old-schema prefix.
                InFlightData.Reset();
                PendingSchemaChange.Reset();
                Send(Writer, new TEvents::TEvPoison());
            }
        }
        MaybeDrained();
    }

    void Handle(TEvWorker::TEvDrain::TPtr&) {
        BeginDrain();
    }

    void MaybeDrained() {
        if (!Draining || InFlightData || PendingBuildProgress || SourceOffsetReport) {
            return;
        }
        SourceOffsetReport = MakeSourceOffsetReport();
        SourceOffsetReport->SetDrained(true);
        if (ProcessedSourceOffset) {
            SourceOffsetReport->SetNextOffset(*ProcessedSourceOffset);
        }
        if (LastHeartbeat) {
            LastHeartbeat->ToProto(SourceOffsetReport->MutableHeartbeat());
        }
        SendSourceOffsetReport(*SourceOffsetReport);
    }

    void Handle(TEvService::TEvHeartbeat::TPtr& ev) {
        if (ev->Sender != Writer) {
            return;
        }
        LastHeartbeat = TRowVersion::FromProto(ev->Get()->Record.GetVersion());
        Forward(ev);
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender == Reader) {
            YDB_LOG_INFO("Handshake with reader",
                {"sender", ev->Sender});

            // This handshake only confirms creation of the SDK session wrapper.
            // Keep the retry budget until the reader actually delivers data.
            Reader.Registered();
            if (TrackSourceOffsets) {
                return StartValidatedReader();
            }
            if (ReaderSessionStarted && PendingSchemaChange && !SchemaReportCommitted) {
                // The reader may have been recreated after receiving the
                // explicit checkpoint request but before its completion
                // notification. Reissue the idempotent offset commit only
                // after its partition session is ready.
                CommitSource(PendingSchemaChange->Offset);
            } else if (ReaderSessionStarted && PendingSchemaChange && SchemaAdvanceInFlight) {
                // The post-ack checkpoint may have been interrupted while
                // recreating the reader. It is idempotent and must complete
                // before the controller may retire this barrier.
                CommitSource(PendingSchemaChange->Offset + 1);
            } else if (ReaderSessionStarted && PendingWriterCheckpoint) {
                CommitSource(*PendingWriterCheckpoint);
            } else if (!InFlightData && !TerminateWriter) {
                PollSource();
            }
        } else if (ev->Sender == Writer) {
            YDB_LOG_INFO("Handshake with writer",
                {"sender", ev->Sender});

            Writer.Registered();
            Writer.ResetCreateAttempt();
            if (LastBuildProgress) {
                auto progress = MakeHolder<TEvService::TEvIndexBuildProgressResult>();
                progress->Record.MutableProgress()->CopyFrom(*LastBuildProgress);
                Send(Writer, std::move(progress));
            }

            if (Draining && !InFlightData) {
                return MaybeDrained();
            }
            if (InFlightData) {
                Send(Writer, new TEvWorker::TEvData(InFlightData->PartitionId, InFlightData->Source, InFlightData->Records));
            } else if (TerminateWriter) {
                Send(Writer, new TEvWorker::TEvTerminateWriter(TerminateWriter->PartitionId));
            }
        } else {
            YDB_LOG_WARN("Handshake from unknown actor",
                {"sender", ev->Sender});
            return;
        }
    }

    void Handle(TEvService::TEvIndexBuildProgress::TPtr& ev) {
        if (ev->Sender != Writer || !InFlightData) {
            return;
        }

        PendingBuildProgress = MakeHolder<NKikimrReplication::TIndexBuildProgress>(ev->Get()->Record.GetProgress());
        Forward(ev);
    }

    void UpdateLag() {
        if (InFlightData) {
            const auto& records = InFlightData->Records;
            auto it = MinElementBy(records, [](const auto& record) {
                return record.GetCreateTime();
            });

            if (it != records.end()) {
                Lag = TlsActivationContext->Now() - it->GetCreateTime();
            }
        }
    }

    void Handle(TEvService::TEvIndexBuildProgressResult::TPtr& ev) {
        if (!PendingBuildProgress || !InFlightData
            || ev->Get()->Record.GetProgress().SerializeAsString() != PendingBuildProgress->SerializeAsString())
        {
            return;
        }

        LastBuildProgress = std::move(PendingBuildProgress);
        Send(ev->Forward(Writer));
        const auto offset = LastBuildProgress->GetOffset();
        if (Draining) {
            ProcessedSourceOffset = Max(ProcessedSourceOffset.GetOrElse(0), offset);
            InFlightData.Reset();
            Send(Writer, new TEvents::TEvPoison());
            return MaybeDrained();
        }
        UpdateLag();
        auto& records = InFlightData->Records;
        const auto firstNew = FindIf(records, [offset](const auto& record) { return record.GetOffset() >= offset; });
        records.erase(records.begin(), firstNew);
        if (!records.empty()) {
            Send(Writer, new TEvWorker::TEvData(InFlightData->PartitionId, InFlightData->Source, records));
        } else {
            InFlightData.Reset();
            TerminateWriter.Reset();
            if (Reader) {
                PollSource();
            }
        }
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Writer) {
            YDB_LOG_WARN("Poll from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        UpdateLag();

        // A schema barrier owns the raw batch until the controller has
        // applied the schema and the writer has refreshed. A normal poll from
        // the writer must never drop that retained suffix.
        Y_ABORT_UNLESS(!PendingSchemaChange);

        InFlightData.Reset();
        TerminateWriter.Reset();
        if (Reader || Draining) {
            PollSource(ev->Get()->SkipCommit);
        }
    }

    void Handle(TEvWorker::TEvCommit::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Writer) {
            YDB_LOG_WARN("Commit from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        const auto offset = ev->Get()->Offset;
        if (!PendingWriterCheckpoint || offset > *PendingWriterCheckpoint) {
            PendingWriterCheckpoint = offset;
        }

        if (Reader || Draining) {
            CommitSource(*PendingWriterCheckpoint);
        }
    }

    void Handle(TEvWorker::TEvSchemaChange::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Writer || !InFlightData) {
            YDB_LOG_WARN("Unexpected schema change",
                {"sender", ev->Sender});
            return;
        }

        const auto offset = ev->Get()->Offset;
        if (Draining) {
            // Drain only the prefix before the schema record.
            InFlightData.Reset();
            PendingSchemaChange.Reset();
            ProcessedSourceOffset = Max(ProcessedSourceOffset.GetOrElse(0), offset);
            Send(Writer, new TEvents::TEvPoison());
            return MaybeDrained();
        }
        if (PendingSchemaChange) {
            if (PendingSchemaChange->Offset != offset || !IsSameSchemaChange(PendingSchemaChange->Schema, ev->Get()->Schema)) {
                YDB_LOG_WARN("Conflicting schema change from writer",
                    {"offset", offset});
                return;
            }
            // A replacement writer has rebuilt its local barrier from the
            // retained batch. Re-deliver an already received controller
            // release only after that barrier exists in the writer.
            WriterHasSchemaBarrier = true;
            if (SchemaReleaseReceived) {
                auto result = MakeHolder<TEvService::TEvSchemaChangeResult>();
                result->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
                Send(Writer, result.Release());
            }
            return;
        }

        const auto& records = InFlightData->Records;
        const auto it = FindIf(records, [offset](const auto& record) {
            return record.GetOffset() == offset;
        });
        if (it == records.end()) {
            YDB_LOG_ERROR("Schema barrier offset is not in the in-flight batch",
                {"offset", offset});
            Send(Parent, new TEvWorker::TEvGone(TEvWorker::TEvGone::SCHEME_ERROR,
                "Schema barrier offset is not in the in-flight batch"));
            return PassAway();
        }

        PendingSchemaChange = MakeHolder<TEvWorker::TEvSchemaChange>(ev->Get()->Schema, offset);
        WriterHasSchemaBarrier = true;
        CommitSource(offset);
    }

    void Handle(TEvWorker::TEvCommitResult::TPtr& ev) {
        if (Draining) {
            return;
        }
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Reader) {
            YDB_LOG_WARN("Unexpected commit result",
                {"sender", ev->Sender});
            return;
        }

        if (!PendingSchemaChange) {
            if (PendingWriterCheckpoint && ev->Get()->Offset >= *PendingWriterCheckpoint) {
                PendingWriterCheckpoint.Clear();
            } else {
                YDB_LOG_WARN("Unexpected writer checkpoint result",
                    {"offset", ev->Get()->Offset});
            }
            return;
        }

        if (SchemaAdvanceInFlight && ev->Get()->Offset == PendingSchemaChange->Offset + 1) {
            SchemaAdvanceInFlight = false;
            SchemaAdvanceCommitted = true;
            if (!SchemaApplied) {
                return;
            }

            auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
            report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
            report->Record.SetOffset(PendingSchemaChange->Offset);
            report->Record.SetCompleted(true);
            Send(Parent, report.Release());
            return;
        }

        if (ev->Get()->Offset != PendingSchemaChange->Offset) {
            YDB_LOG_WARN("Unexpected schema checkpoint result",
                {"offset", ev->Get()->Offset});
            return;
        }

        auto& records = InFlightData->Records;
        const auto firstUncommitted = FindIf(records, [offset = ev->Get()->Offset](const auto& record) {
            return record.GetOffset() == offset;
        });
        Y_ABORT_UNLESS(firstUncommitted != records.end());
        records.erase(records.begin(), firstUncommitted);

        auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
        report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
        report->Record.SetOffset(PendingSchemaChange->Offset);
        Send(Parent, report.Release());
        SchemaReportCommitted = true;
    }

    void Handle(TEvService::TEvSchemaChangeResult::TPtr& ev) {
        if (Draining) {
            return;
        }
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (!ev->Get()->Record.HasSchema()) {
            YDB_LOG_WARN("Unexpected schema change result",
                {"sender", ev->Sender});
            return;
        }

        const bool matchesPendingSchemaChange = PendingSchemaChange
            && PendingSchemaChange->Offset == ev->Get()->Record.GetOffset()
            && IsSameSchemaChange(PendingSchemaChange->Schema, ev->Get()->Record.GetSchema());

        if (ev->Get()->Record.GetCompleted()
            && RecoveredCompletionSchema
            && RecoveredCompletionOffset == ev->Get()->Record.GetOffset()
            && IsSameSchemaChange(*RecoveredCompletionSchema, ev->Get()->Record.GetSchema()))
        {
            RecoveredCompletionSchema.Reset();
            RecoveredCompletionReported = false;
            return;
        }

        // A controller can replay the applied result for a barrier that this
        // worker crossed before it restarted.  The replacement may already
        // be parked at a later schema barrier when that replay arrives, so
        // recognize it from the durable consumer position before comparing
        // it with the current local barrier.
        if (ev->Get()->Record.GetApplied()
            && ReaderCommittedOffset
            && *ReaderCommittedOffset > ev->Get()->Record.GetOffset()
            && !matchesPendingSchemaChange)
        {
            if (RecoveredCompletionSchema
                && (RecoveredCompletionOffset != ev->Get()->Record.GetOffset()
                    || !IsSameSchemaChange(*RecoveredCompletionSchema, ev->Get()->Record.GetSchema())))
            {
                YDB_LOG_WARN("Conflicting recovered schema change result",
                    {"sender", ev->Sender});
                return;
            }

            RecoveredCompletionSchema = MakeHolder<NKikimrReplication::TSchemaChange>(ev->Get()->Record.GetSchema());
            RecoveredCompletionOffset = ev->Get()->Record.GetOffset();
            RecoveredCompletionReported = false;
            ReportRecoveredCompletion();
            return;
        }

        if (!PendingSchemaChange) {
            // AppliedWorkers is durable in the controller before it permits
            // the post-schema consumer checkpoint. Remember its replay across
            // a whole worker restart and compare it with the consumer's
            // durable session-start offset.
            if (ev->Get()->Record.GetApplied()) {
                RecoveredCompletionSchema = MakeHolder<NKikimrReplication::TSchemaChange>(ev->Get()->Record.GetSchema());
                RecoveredCompletionOffset = ev->Get()->Record.GetOffset();
                RecoveredCompletionReported = false;
                if (ReaderCommittedOffset && *ReaderCommittedOffset > RecoveredCompletionOffset) {
                    ReportRecoveredCompletion();
                }
            } else {
                YDB_LOG_WARN("Unexpected schema release without local barrier",
                    {"sender", ev->Sender});
            }
            return;
        }

        if (!matchesPendingSchemaChange) {
            YDB_LOG_WARN("Unexpected schema change result",
                {"sender", ev->Sender});
            return;
        }

        if (ev->Get()->Record.GetApplied()) {
            // The acknowledgement may race with a writer restart. Remember
            // it until the replacement writer has applied the same barrier.
            SchemaApplyAcknowledged = true;
        }

        if (SchemaAdvanceCommitted) {
            if (!ev->Get()->Record.GetCompleted()) {
                YDB_LOG_WARN("Unexpected schema completion acknowledgement",
                    {"sender", ev->Sender});
                return;
            }
            SchemaCompletionReceived = true;
            if (SchemaApplied) {
                return FinishSchemaChange();
            }
            return;
        }

        if (SchemaApplied) {
            if (!ev->Get()->Record.GetApplied()) {
                YDB_LOG_WARN("Unexpected schema release after local apply",
                    {"sender", ev->Sender});
                return;
            }
            if (SchemaAdvanceInFlight) {
                return;
            }
            SchemaAdvanceInFlight = true;
            CommitSource(PendingSchemaChange->Offset + 1);
            return;
        }

        if (ev->Get()->Record.GetApplied()) {
            // The acknowledgement may belong to a writer that was replaced
            // after reporting applied.  The new writer must consume the
            // release and refresh before the offset can advance.
            SchemaReleaseReceived = true;
            if (WriterHasSchemaBarrier) {
                Send(ev->Forward(Writer));
            }
            return;
        }

        SchemaReleaseReceived = true;
        if (WriterHasSchemaBarrier) {
            Send(ev->Forward(Writer));
        }
    }

    void Handle(TEvWorker::TEvSchemaChangeApplied::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Writer || !PendingSchemaChange
            || !IsSameSchemaChange(ev->Get()->Schema, PendingSchemaChange->Schema))
        {
            YDB_LOG_WARN("Unexpected schema change applied",
                {"sender", ev->Sender});
            return;
        }

        // Keep the offset at the schema record until the controller has
        // durably acknowledged this worker's completion. Otherwise a later
        // DDL could overtake a disconnected partition.
        if (SchemaApplied) {
            return;
        }

        SchemaApplied = true;
        if (SchemaAdvanceCommitted) {
            if (SchemaCompletionReceived) {
                return FinishSchemaChange();
            }

            auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
            report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
            report->Record.SetOffset(PendingSchemaChange->Offset);
            report->Record.SetCompleted(true);
            Send(Parent, report.Release());
            return;
        }

        if (SchemaApplyAcknowledged) {
            if (!SchemaAdvanceInFlight) {
                SchemaAdvanceInFlight = true;
                CommitSource(PendingSchemaChange->Offset + 1);
            }
            return;
        }

        auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
        report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
        report->Record.SetOffset(PendingSchemaChange->Offset);
        report->Record.SetApplied(true);
        Send(Parent, report.Release());
    }

    void FinishSchemaChange() {
        auto& records = InFlightData->Records;
        Y_ABORT_UNLESS(!records.empty() && records.front().GetOffset() == PendingSchemaChange->Offset);
        records.erase(records.begin()); // The persisted schema barrier is now applied.
        PendingSchemaChange.Reset();
        SchemaReportCommitted = false;
        SchemaReleaseReceived = false;
        WriterHasSchemaBarrier = false;
        SchemaApplied = false;
        SchemaApplyAcknowledged = false;
        SchemaAdvanceInFlight = false;
        SchemaAdvanceCommitted = false;
        SchemaCompletionReceived = false;

        if (records.empty()) {
            InFlightData.Reset();
            PollSource();
        } else {
            Send(Writer, new TEvWorker::TEvData(InFlightData->PartitionId, InFlightData->Source, InFlightData->Records));
        }
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Reader) {
            YDB_LOG_WARN("Data from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        ReadRequested = false;
        if (Draining) {
            return;
        }
        if (TrackSourceOffsets && !ReaderValidated) {
            return;
        }
        Y_ABORT_UNLESS(!InFlightData);
        if (!ev->Get()->Records.empty()) {
            const auto& last = ev->Get()->Records.back();
            BatchEnd = last.GetOffset() + last.GetLogicalMessageCount();
        }

        if (ev->Get()->Stats) {
            Send(Parent, MakeEvStatusFromReaderStats(std::move(ev->Get()->Stats)));
        }

        if (ReaderReplayBoundary) {
            auto& records = ev->Get()->Records;
            const auto firstNew = FindIf(records, [boundary = *ReaderReplayBoundary](const auto& record) {
                return record.GetOffset() >= boundary;
            });
            records.erase(records.begin(), firstNew);
            if (records.empty()) {
                PollSource();
                return;
            }
            ReaderReplayBoundary.Clear();
        }

        Reader.ResetCreateAttempt();
        InFlightData = MakeHolder<TEvWorker::TEvData>(ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records);

        if (Writer) {
            Send(ev->Forward(Writer));
        }
    }

    void Handle(TEvWorker::TEvReaderStarted::TPtr& ev) {
        if (Draining) {
            return;
        }
        if (ev->Sender != Reader) {
            YDB_LOG_WARN("Reader start from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        // The SDK's start event reports the consumer's durable next offset.
        // Only a position strictly after this worker's persisted schema
        // record proves that its post-schema checkpoint completed.
        ReaderSessionStarted = true;
        ReaderCommittedOffset = ev->Get()->CommittedOffset;
        if (TrackSourceOffsets) {
            ReaderValidated = false;
            if (AuthorizedSourceOffset && *ReaderCommittedOffset > *AuthorizedSourceOffset) {
                SourceOffsetMismatch = TStringBuilder()
                    << "Source consumer committed next offset " << *ReaderCommittedOffset
                    << " is ahead of destination processed source offset " << *AuthorizedSourceOffset
                    << ". Destination history is inconsistent; replication stopped.";
                ReportSourceOffsetMismatch();
                return;
            }
            ReaderValidated = true;
            return StartValidatedReader();
        }
        if (RecoveredCompletionSchema && *ReaderCommittedOffset > RecoveredCompletionOffset) {
            ReportRecoveredCompletion();
        }

        if (PendingSchemaChange && !SchemaReportCommitted) {
            CommitSource(PendingSchemaChange->Offset);
        } else if (PendingSchemaChange && SchemaAdvanceInFlight) {
            CommitSource(PendingSchemaChange->Offset + 1);
        } else if (PendingWriterCheckpoint) {
            CommitSource(*PendingWriterCheckpoint);
        }
    }

    void Handle(TEvWorker::TEvTerminateWriter::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Reader) {
            YDB_LOG_WARN("Terminate writer from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        Y_ABORT_UNLESS(!TerminateWriter);
        TerminateWriter = MakeHolder<TEvWorker::TEvTerminateWriter>(ev->Get()->PartitionId);

        if (Writer) {
            Send(ev->Forward(Writer));
        }
    }

    void Handle(TEvWorker::TEvGone::TPtr& ev) {
        if (ev->Sender == Reader) {
            YDB_LOG_INFO("Reader has gone",
                {"sender", ev->Sender},
                {"ev", ev->Get()->ToString()});
            if (InFlightData && !InFlightData->Records.empty()) {
                const auto& last = InFlightData->Records.back();
                const auto boundary = last.GetOffset() + last.GetLogicalMessageCount();
                if (!ReaderReplayBoundary || boundary > *ReaderReplayBoundary) {
                    ReaderReplayBoundary = boundary;
                }
            }
            ReaderSessionStarted = false;
            ReaderValidated = false;
            ReadRequested = false;
            if (Draining) {
                return MaybeDrained();
            }
            MaybeRecreateActor(ev, Reader);
        } else if (ev->Sender == Writer) {
            YDB_LOG_INFO("Writer has gone",
                {"sender", ev->Sender},
                {"ev", ev->Get()->ToString()});
            if (PendingSchemaChange) {
                // These flags describe the dead writer. Keep the durable
                // controller release, but wait until the replacement has
                // rebuilt and applied its own schema barrier.
                SchemaApplied = false;
                WriterHasSchemaBarrier = false;
            }
            if (Draining) {
                // Keep the failed writer pending until the drain deadline.
                return;
            }
            MaybeRecreateActor(ev, Writer);
        } else {
            YDB_LOG_WARN("Unknown actor has gone",
                {"sender", ev->Sender});
        }
    }

    void Handle(TEvWorker::TEvStatus::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});
        if (!ev->Get()->DetailedStats) {
            YDB_LOG_WARN("Unexpected TEvWorker::TEvStatus with no stats, ignored",
                {"sender", ev->Sender});
            return;
        }
        Forward(ev);
    }

    std::unique_ptr<TEvWorker::TEvStatus> MakeEvStatusFromReaderStats(std::unique_ptr<TWorkerDetailedStats>&& stats) const {
        Y_ENSURE(stats->ReaderStats);
        std::unique_ptr<TEvWorker::TEvStatus> ev{TEvWorker::TEvStatus::FromOperation(EWorkerOperation::NONE)};
        ev->DetailedStats->ReaderStats = std::move(stats->ReaderStats);
        return std::move(ev);
    }

    void MaybeRecreateActor(TEvWorker::TEvGone::TPtr& ev, TActorInfo& info) {
        switch (ev->Get()->Status) {
        case TEvWorker::TEvGone::UNAVAILABLE:
            if (info.GetCreateAttempt() < MaxAttempts) {
                return info.Register(this);
            }
            [[fallthrough]];
        default:
            return Leave(ev);
        }
    }

    void Leave(TEvWorker::TEvGone::TPtr& ev) {
        YDB_LOG_INFO("Leave",
            {"status", ev->Get()->Status},
            {"error", ev->Get()->ErrorDescription});

        ev->Sender = SelfId();
        Send(ev->Forward(Parent));

        PassAway();
    }

    void Handle(TEvService::TEvTxIdResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});
        Send(ev->Forward(Writer));
    }

    template <typename TEventPtr>
    void Forward(TEventPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        ev->Sender = SelfId();
        Send(ev->Forward(Parent));
    }

    void ScheduleLagReport() {
        const auto random = TDuration::MicroSeconds(TAppData::RandomProvider->GenRand64() % LagReportInterval.MicroSeconds());
        Schedule(LagReportInterval + random, new TEvents::TEvWakeup());
    }

    void ReportSourceOffsetMismatch() {
        auto status = MakeHolder<TEvWorker::TEvStatus>(TDuration::Zero());
        status->SourceOffsetMismatch = SourceOffsetMismatch;
        Send(Parent, std::move(status));
    }

    void ReportLag() {
        if (!SourceOffsetMismatch.empty() && !Draining) {
            ReportSourceOffsetMismatch();
        }
        if (SourceOffsetReport) {
            SendSourceOffsetReport(*SourceOffsetReport);
        }
        if (PendingBuildProgress) {
            auto progress = MakeHolder<TEvService::TEvIndexBuildProgress>();
            progress->Record.MutableProgress()->CopyFrom(*PendingBuildProgress);
            Send(Parent, std::move(progress));
        }

        ScheduleLagReport();

        if (!Reader || !Writer) {
            return;
        }

        Send(Parent, new TEvWorker::TEvStatus(Lag));
        Lag = TDuration::Zero();

        // The controller treats reports as idempotent.  Re-send while parked
        // after the topic offset checkpoint, so a lost service/controller
        // message cannot leave this partition at the barrier forever.
        if (SchemaReportCommitted && PendingSchemaChange) {
            auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
            report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
            report->Record.SetOffset(PendingSchemaChange->Offset);
            report->Record.SetApplied(SchemaApplied && !SchemaAdvanceCommitted);
            report->Record.SetCompleted(SchemaApplied && SchemaAdvanceCommitted);
            Send(Parent, report.Release());
        }

        if (RecoveredCompletionSchema && RecoveredCompletionReported) {
            ReportRecoveredCompletion();
        }
    }

    void ReportRecoveredCompletion() {
        Y_ABORT_UNLESS(RecoveredCompletionSchema);
        auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
        report->Record.MutableSchema()->CopyFrom(*RecoveredCompletionSchema);
        report->Record.SetOffset(RecoveredCompletionOffset);
        report->Record.SetCompleted(true);
        Send(Parent, report.Release());
        RecoveredCompletionReported = true;
    }

    void PassAway() override {
        for (auto* actor : {&Reader, &Writer}) {
            Send(*actor, new TEvents::TEvPoison());
        }

        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_WORKER;
    }

    explicit TWorker(
            const TActorId& parent,
            std::function<IActor*(void)>&& createReaderFn,
            std::function<IActor*(void)>&& createWriterFn,
            bool trackSourceOffsets, TMaybe<ui64> processedSourceOffset)
        : Parent(parent)
        , Reader(std::move(createReaderFn))
        , Writer(std::move(createWriterFn))
        , TrackSourceOffsets(trackSourceOffsets)
        , AuthorizedSourceOffset(processedSourceOffset)
        , Lag(TDuration::Zero())
    {
    }

    void Bootstrap() {
        for (auto* actor : {&Reader, &Writer}) {
            actor->Register(this);
        }

        Become(&TThis::StateWork);
        ScheduleLagReport();
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvDrain, Handle);
            hFunc(TEvService::TEvSourceOffsetResult, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvWorker::TEvCommit, Handle);
            hFunc(TEvWorker::TEvCommitResult, Handle);
            hFunc(TEvWorker::TEvReaderStarted, Handle);
            hFunc(TEvWorker::TEvSchemaChange, Handle);
            hFunc(TEvWorker::TEvSchemaChangeApplied, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvWorker::TEvDataEnd, Forward);
            hFunc(TEvWorker::TEvGone, Handle);
            hFunc(TEvWorker::TEvTerminateWriter, Handle);
            hFunc(TEvWorker::TEvStatus, Handle);
            hFunc(TEvService::TEvGetTxId, Forward);
            hFunc(TEvService::TEvTxIdResult, Handle);
            hFunc(TEvService::TEvHeartbeat, Handle);
            hFunc(TEvService::TEvIndexBuildProgress, Handle);
            hFunc(TEvService::TEvIndexBuildProgressResult, Handle);
            hFunc(TEvService::TEvSchemaChangeResult, Handle);
            sFunc(TEvents::TEvWakeup, ReportLag);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    static constexpr ui32 MaxAttempts = 3;
    static constexpr TDuration LagReportInterval = TDuration::Seconds(7);

    struct TSourceAction {
        TMaybe<ui64> Offset;
        THolder<IEventBase> Event;
    };
    const TActorId Parent;
    TActorInfo Reader;
    TActorInfo Writer;
    THolder<TEvWorker::TEvData> InFlightData;
    THolder<TEvWorker::TEvTerminateWriter> TerminateWriter;
    THolder<TEvWorker::TEvSchemaChange> PendingSchemaChange;
    THolder<NKikimrReplication::TIndexBuildProgress> PendingBuildProgress;
    THolder<NKikimrReplication::TIndexBuildProgress> LastBuildProgress;
    bool SchemaReportCommitted = false;
    bool SchemaReleaseReceived = false;
    bool WriterHasSchemaBarrier = false;
    bool SchemaApplied = false;
    bool SchemaApplyAcknowledged = false;
    bool SchemaAdvanceInFlight = false;
    bool SchemaAdvanceCommitted = false;
    bool SchemaCompletionReceived = false;
    // Set only from a durable AppliedWorkers replay by the controller. This
    // survives neither worker lifetime nor controller routing, so it is used
    // solely to reconstruct completion from the topic's durable offset.
    THolder<NKikimrReplication::TSchemaChange> RecoveredCompletionSchema;
    ui64 RecoveredCompletionOffset = 0;
    bool RecoveredCompletionReported = false;
    TMaybe<ui64> ReaderCommittedOffset;
    TMaybe<ui64> ReaderReplayBoundary;
    TMaybe<ui64> PendingWriterCheckpoint;
    bool ReaderSessionStarted = false;
    const bool TrackSourceOffsets;
    bool ReaderValidated = false;
    bool ReadRequested = false;
    bool Draining = false;
    ui64 SourceOffsetReportId = 0;
    TMaybe<ui64> AuthorizedSourceOffset;
    TMaybe<ui64> BatchEnd;
    TMaybe<ui64> ProcessedSourceOffset;
    TMaybe<TRowVersion> LastHeartbeat;
    TDeque<TSourceAction> SourceActions;
    TString SourceOffsetMismatch;
    THolder<NKikimrReplication::TEvSourceOffsetReport> SourceOffsetReport;
    TDuration Lag;
    TInstant StartTime = TInstant::Zero();
};

IActor* CreateWorker(
        const TActorId& parent,
        std::function<IActor*(void)>&& createReaderFn,
        std::function<IActor*(void)>&& createWriterFn,
        bool trackSourceOffsets, TMaybe<ui64> processedSourceOffset)
{
    return new TWorker(parent, std::move(createReaderFn), std::move(createWriterFn), trackSourceOffsets, processedSourceOffset);
}

}
