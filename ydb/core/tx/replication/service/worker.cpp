#include "service.h"
#include "topic_reader_stats.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_replication.pb.h>
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
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

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender == Reader) {
            YDB_LOG_INFO("Handshake with reader",
                {"sender", ev->Sender});

            Reader.Registered();
            if (ReaderSessionStarted && PendingSchemaChange && !SchemaReportCommitted) {
                // The reader may have been recreated after receiving the
                // explicit checkpoint request but before its completion
                // notification. Reissue the idempotent offset commit only
                // after its partition session is ready.
                Send(Reader, new TEvWorker::TEvCommit(PendingSchemaChange->Offset));
            } else if (ReaderSessionStarted && PendingSchemaChange && SchemaAdvanceInFlight) {
                // The post-ack checkpoint may have been interrupted while
                // recreating the reader. It is idempotent and must complete
                // before the controller may retire this barrier.
                Send(Reader, new TEvWorker::TEvCommit(PendingSchemaChange->Offset + 1));
            } else if (!InFlightData && !TerminateWriter) {
                Send(Reader, new TEvWorker::TEvPoll());
            }
        } else if (ev->Sender == Writer) {
            YDB_LOG_INFO("Handshake with writer",
                {"sender", ev->Sender});

            const bool recreated = WriterInitialized;
            Writer.Registered();
            WriterInitialized = true;
            if (recreated && PendingSchemaChange) {
                // SchemaApplied belongs to a particular writer instance.  A
                // replacement must refresh the released schema itself before
                // the worker can advance past the retained schema record.
                SchemaApplied = false;
                WriterHasSchemaBarrier = false;
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

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Writer) {
            YDB_LOG_WARN("Poll from unknown actor",
                {"sender", ev->Sender});
            return;
        }

        if (InFlightData) {
            const auto& records = InFlightData->Records;
            auto it = MinElementBy(records, [](const auto& record) {
                return record.GetCreateTime();
            });

            if (it != records.end()) {
                Lag = TlsActivationContext->Now() - it->GetCreateTime();
            }
        }

        // A schema barrier owns the raw batch until the controller has
        // applied the schema and the writer has refreshed. A normal poll from
        // the writer must never drop that retained suffix.
        Y_ABORT_UNLESS(!PendingSchemaChange);

        InFlightData.Reset();
        TerminateWriter.Reset();
        if (Reader) {
            Send(ev->Forward(Reader));
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

        if (Reader) {
            Send(ev->Forward(Reader));
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
        if (PendingSchemaChange) {
            if (PendingSchemaChange->Offset != offset
                || PendingSchemaChange->Schema.SerializeAsString() != ev->Get()->Schema.SerializeAsString()) {
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
        Send(Reader, new TEvWorker::TEvCommit(offset));
    }

    void Handle(TEvWorker::TEvCommitResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Sender != Reader || !PendingSchemaChange) {
            YDB_LOG_WARN("Unexpected commit result",
                {"sender", ev->Sender});
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
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        if (!ev->Get()->Record.HasSchema()) {
            YDB_LOG_WARN("Unexpected schema change result",
                {"sender", ev->Sender});
            return;
        }

        const bool matchesPendingSchemaChange = PendingSchemaChange
            && PendingSchemaChange->Offset == ev->Get()->Record.GetOffset()
            && PendingSchemaChange->Schema.SerializeAsString()
                == ev->Get()->Record.GetSchema().SerializeAsString();

        if (ev->Get()->Record.GetCompleted()
            && RecoveredCompletionSchema
            && RecoveredCompletionOffset == ev->Get()->Record.GetOffset()
            && RecoveredCompletionSchema->SerializeAsString()
                == ev->Get()->Record.GetSchema().SerializeAsString()) {
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
            && !matchesPendingSchemaChange) {
            if (RecoveredCompletionSchema
                && (RecoveredCompletionOffset != ev->Get()->Record.GetOffset()
                    || RecoveredCompletionSchema->SerializeAsString()
                        != ev->Get()->Record.GetSchema().SerializeAsString())) {
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

        if (ev->Get()->Record.GetSchema().SerializeAsString() != PendingSchemaChange->Schema.SerializeAsString()) {
            YDB_LOG_WARN("Unexpected schema change result",
                {"sender", ev->Sender});
            return;
        }

        if (SchemaAdvanceCommitted) {
            if (!ev->Get()->Record.GetCompleted()) {
                YDB_LOG_WARN("Unexpected schema completion acknowledgement",
                    {"sender", ev->Sender});
                return;
            }
            return FinishSchemaChange();
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
            Send(Reader, new TEvWorker::TEvCommit(PendingSchemaChange->Offset + 1));
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
            || ev->Get()->Schema.SerializeAsString() != PendingSchemaChange->Schema.SerializeAsString()) {
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
            auto report = MakeHolder<TEvService::TEvSchemaChangeReport>();
            report->Record.MutableSchema()->CopyFrom(PendingSchemaChange->Schema);
            report->Record.SetOffset(PendingSchemaChange->Offset);
            report->Record.SetCompleted(true);
            Send(Parent, report.Release());
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
        SchemaAdvanceInFlight = false;
        SchemaAdvanceCommitted = false;

        if (records.empty()) {
            InFlightData.Reset();
            Send(Reader, new TEvWorker::TEvPoll());
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

        Y_ABORT_UNLESS(!InFlightData);
        InFlightData = MakeHolder<TEvWorker::TEvData>(ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records);

        if (ev->Get()->Stats) {
            Send(Parent, MakeEvStatusFromReaderStats(std::move(ev->Get()->Stats)));
        }

        if (Writer) {
            Send(ev->Forward(Writer));
        }
    }

    void Handle(TEvWorker::TEvReaderStarted::TPtr& ev) {
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
        if (RecoveredCompletionSchema && *ReaderCommittedOffset > RecoveredCompletionOffset) {
            ReportRecoveredCompletion();
        }

        if (PendingSchemaChange && !SchemaReportCommitted) {
            Send(Reader, new TEvWorker::TEvCommit(PendingSchemaChange->Offset));
        } else if (PendingSchemaChange && SchemaAdvanceInFlight) {
            Send(Reader, new TEvWorker::TEvCommit(PendingSchemaChange->Offset + 1));
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
            ReaderSessionStarted = false;
            MaybeRecreateActor(ev, Reader);
        } else if (ev->Sender == Writer) {
            YDB_LOG_INFO("Writer has gone",
                {"sender", ev->Sender},
                {"ev", ev->Get()->ToString()});
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

    void ReportLag() {
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
            std::function<IActor*(void)>&& createWriterFn)
        : Parent(parent)
        , Reader(std::move(createReaderFn))
        , Writer(std::move(createWriterFn))
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
            hFunc(TEvService::TEvHeartbeat, Forward);
            hFunc(TEvService::TEvSchemaChangeResult, Handle);
            sFunc(TEvents::TEvWakeup, ReportLag);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    static constexpr ui32 MaxAttempts = 3;
    static constexpr TDuration LagReportInterval = TDuration::Seconds(7);

    const TActorId Parent;
    TActorInfo Reader;
    TActorInfo Writer;
    THolder<TEvWorker::TEvData> InFlightData;
    THolder<TEvWorker::TEvTerminateWriter> TerminateWriter;
    THolder<TEvWorker::TEvSchemaChange> PendingSchemaChange;
    bool SchemaReportCommitted = false;
    bool SchemaReleaseReceived = false;
    bool WriterHasSchemaBarrier = false;
    bool SchemaApplied = false;
    bool SchemaAdvanceInFlight = false;
    bool SchemaAdvanceCommitted = false;
    // Set only from a durable AppliedWorkers replay by the controller. This
    // survives neither worker lifetime nor controller routing, so it is used
    // solely to reconstruct completion from the topic's durable offset.
    THolder<NKikimrReplication::TSchemaChange> RecoveredCompletionSchema;
    ui64 RecoveredCompletionOffset = 0;
    bool RecoveredCompletionReported = false;
    TMaybe<ui64> ReaderCommittedOffset;
    bool ReaderSessionStarted = false;
    bool WriterInitialized = false;
    TDuration Lag;
    TInstant StartTime = TInstant::Zero();
};

IActor* CreateWorker(
        const TActorId& parent,
        std::function<IActor*(void)>&& createReaderFn,
        std::function<IActor*(void)>&& createWriterFn)
{
    return new TWorker(parent, std::move(createReaderFn), std::move(createWriterFn));
}

}
