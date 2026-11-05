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

    void SendSourceOffsetReport(const NKikimrReplication::TEvSourceOffsetReport& request) {
        auto ev = MakeHolder<TEvService::TEvSourceOffsetReport>();
        ev->Record.CopyFrom(request);
        Send(Parent, std::move(ev), 0, SourceOffsetReportId);
    }

    bool IsReaderValidated() const {
        return ReaderSessionStarted && SourceOffsetMismatch.empty();
    }

    void ProcessSourceCommits() {
        if (!Reader || !IsReaderValidated() || SourceOffsetReport) {
            return;
        }
        while (!PendingSourceCommits.empty()) {
            const auto offset = PendingSourceCommits.front();
            if (!PersistedSourceOffset || *PersistedSourceOffset < offset) {
                SourceOffsetReport = MakeHolder<NKikimrReplication::TEvSourceOffsetReport>();
                ++SourceOffsetReportId;
                SourceOffsetReport->SetNextOffset(offset);
                SendSourceOffsetReport(*SourceOffsetReport);
                return;
            }
            Send(Reader, new TEvWorker::TEvCommit(offset));
            PendingSourceCommits.pop_front();
        }
    }

    void PollSource(bool skipCommit = false) {
        if (TrackSourceOffsets) {
            if (!Reader || !IsReaderValidated() || ReadRequested || InFlightData || TerminateWriter) {
                return;
            }
            if (PendingWriterCheckpoint && TlsActivationContext->Now() - SourceOffsetCommitStarted >= SourceOffsetCommitInterval) {
                return;
            }
            skipCommit = true;
        }
        ReadRequested = true;
        Send(Reader, new TEvWorker::TEvPoll(skipCommit));
    }

    void AdvanceCompletedSourceOffset(ui64 offset) {
        if (TrackSourceOffsets) {
            CompletedSourceOffset = Max(CompletedSourceOffset.GetOrElse(0), offset);
        }
    }

    void CommitSource(ui64 offset) {
        if (!TrackSourceOffsets) {
            Send(Reader, new TEvWorker::TEvCommit(offset));
            return;
        }
        AdvanceCompletedSourceOffset(offset);
        PendingSourceCommits.push_back(offset);
        ProcessSourceCommits();
    }

    bool CommitPendingSourceOffset() {
        if (!ReaderSessionStarted) {
            return false;
        }
        if (PendingWriterCheckpoint) {
            CommitSource(*PendingWriterCheckpoint);
        } else {
            return false;
        }
        return true;
    }

    void StartValidatedReader() {
        if (!Reader || !IsReaderValidated()) {
            return;
        }
        CommitPendingSourceOffset();
        ProcessSourceCommits();
        PollSource();
    }

    void Handle(TEvService::TEvSourceOffsetResult::TPtr& ev) {
        if (!SourceOffsetReport || ev->Cookie != SourceOffsetReportId) {
            return;
        }
        PersistedSourceOffset = SourceOffsetReport->GetNextOffset();
        SourceOffsetReport.Reset();
        ProcessSourceCommits();
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
            if (!InFlightData && !TerminateWriter) {
                PollSource();
            }
        } else if (ev->Sender == Writer) {
            YDB_LOG_INFO("Handshake with writer",
                {"sender", ev->Sender});

            Writer.Registered();
            Writer.ResetCreateAttempt();
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

        if (BatchEnd && !ev->Get()->SkipCommit) {
            AdvanceCompletedSourceOffset(*BatchEnd);
        }
        InFlightData.Reset();
        TerminateWriter.Reset();
        if (Reader) {
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

        if (!TrackSourceOffsets) {
            if (Reader) {
                Send(ev->Forward(Reader));
            }
            return;
        }
        const auto offset = ev->Get()->Offset;
        if (!PendingWriterCheckpoint || offset > *PendingWriterCheckpoint) {
            PendingWriterCheckpoint = offset;
            SourceOffsetCommitStarted = TlsActivationContext->Now();
        }
        if (Reader) {
            CommitSource(*PendingWriterCheckpoint);
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
        if (TrackSourceOffsets && !IsReaderValidated()) {
            return;
        }
        if (!ev->Get()->Records.empty()) {
            const auto& last = ev->Get()->Records.back();
            BatchEnd = last.GetOffset() + last.GetLogicalMessageCount();
        }
        if (TrackSourceOffsets && ReaderReplayBoundary) {
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
        Y_ABORT_UNLESS(!InFlightData);
        InFlightData = MakeHolder<TEvWorker::TEvData>(ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records);

        if (ev->Get()->Stats) {
            Send(Parent, MakeEvStatusFromReaderStats(std::move(ev->Get()->Stats)));
        }

        Reader.ResetCreateAttempt();
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

        ReaderSessionStarted = true;
        ReaderCommittedOffset = ev->Get()->CommittedOffset;
        if (TrackSourceOffsets) {
            if (PersistedSourceOffset && *ReaderCommittedOffset > *PersistedSourceOffset) {
                SourceOffsetMismatch = TStringBuilder()
                    << "Source consumer committed next offset " << *ReaderCommittedOffset
                    << " is ahead of destination processed source offset " << *PersistedSourceOffset
                    << ". Destination history is inconsistent; replication stopped.";
                ReportSourceOffsetMismatch();
                return;
            }
            return StartValidatedReader();
        }
    }

    void Handle(TEvWorker::TEvCommitResult::TPtr& ev) {
        if (ev->Sender != Reader || !TrackSourceOffsets) {
            return;
        }
        const auto offset = ev->Get()->Offset;
        if (PendingWriterCheckpoint && offset >= *PendingWriterCheckpoint) {
            PendingWriterCheckpoint.Clear();
        }
        ReaderCommittedOffset = Max(ReaderCommittedOffset.GetOrElse(0), offset);
        PollSource();
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
            if (TrackSourceOffsets && InFlightData && !InFlightData->Records.empty()) {
                const auto& last = InFlightData->Records.back();
                const auto boundary = last.GetOffset() + last.GetLogicalMessageCount();
                if (!ReaderReplayBoundary || boundary > *ReaderReplayBoundary) {
                    ReaderReplayBoundary = boundary;
                }
            }
            ReaderSessionStarted = false;
            ReadRequested = false;
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
        Schedule(LagReportInterval + random, new TEvents::TEvWakeup(LagReportWakeupTag));
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == LagReportWakeupTag) {
            return ReportLag();
        }
        AFL_VERIFY(ev->Get()->Tag == SourceOffsetCommitWakeupTag);
        Schedule(SourceOffsetCommitInterval, new TEvents::TEvWakeup(SourceOffsetCommitWakeupTag));
        if (!IsReaderValidated() || PendingWriterCheckpoint
            || !CompletedSourceOffset || *CompletedSourceOffset <= ReaderCommittedOffset.GetOrElse(0))
        {
            return;
        }
        PendingWriterCheckpoint = CompletedSourceOffset;
        SourceOffsetCommitStarted = TlsActivationContext->Now();
        CommitSource(*PendingWriterCheckpoint);
    }

    void ReportSourceOffsetMismatch() {
        auto status = MakeHolder<TEvWorker::TEvStatus>(TDuration::Zero());
        status->SourceOffsetMismatch = SourceOffsetMismatch;
        status->SourceCommittedOffset = ReaderCommittedOffset;
        Send(Parent, std::move(status));
    }

    void ReportLag() {
        if (!SourceOffsetMismatch.empty()) {
            ReportSourceOffsetMismatch();
        }
        if (SourceOffsetReport) {
            SendSourceOffsetReport(*SourceOffsetReport);
        }
        ScheduleLagReport();

        if (!Reader || !Writer) {
            return;
        }

        Send(Parent, new TEvWorker::TEvStatus(Lag));
        Lag = TDuration::Zero();
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
        , PersistedSourceOffset(processedSourceOffset)
        , Lag(TDuration::Zero())
    {
    }

    void Bootstrap() {
        for (auto* actor : {&Reader, &Writer}) {
            actor->Register(this);
        }

        Become(&TThis::StateWork);
        ScheduleLagReport();
        if (TrackSourceOffsets) {
            auto intervalMs = AppData()->ReplicationConfig.GetSourceOffsetCommitIntervalMilliSeconds();
            if (!intervalMs) {
                intervalMs = NKikimrReplication::TReplicationDefaults::default_instance().GetSourceOffsetCommitIntervalMilliSeconds();
            }
            SourceOffsetCommitInterval = TDuration::MilliSeconds(intervalMs);
            Schedule(SourceOffsetCommitInterval, new TEvents::TEvWakeup(SourceOffsetCommitWakeupTag));
        }
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvService::TEvSourceOffsetResult, Handle);
            hFunc(TEvWorker::TEvReaderStarted, Handle);
            hFunc(TEvWorker::TEvCommitResult, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvWorker::TEvCommit, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvWorker::TEvDataEnd, Forward);
            hFunc(TEvWorker::TEvGone, Handle);
            hFunc(TEvWorker::TEvTerminateWriter, Handle);
            hFunc(TEvWorker::TEvStatus, Handle);
            hFunc(TEvService::TEvGetTxId, Forward);
            hFunc(TEvService::TEvTxIdResult, Handle);
            hFunc(TEvService::TEvHeartbeat, Forward);
            hFunc(TEvents::TEvWakeup, HandleWakeup);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    static constexpr ui32 MaxAttempts = 3;
    static constexpr TDuration LagReportInterval = TDuration::Seconds(7);
    static constexpr ui64 LagReportWakeupTag = 0;
    static constexpr ui64 SourceOffsetCommitWakeupTag = 1;

    const TActorId Parent;
    TActorInfo Reader;
    TActorInfo Writer;
    THolder<TEvWorker::TEvData> InFlightData;
    THolder<TEvWorker::TEvTerminateWriter> TerminateWriter;
    TMaybe<ui64> ReaderCommittedOffset;
    TMaybe<ui64> ReaderReplayBoundary;
    TMaybe<ui64> PendingWriterCheckpoint;
    bool ReaderSessionStarted = false;
    const bool TrackSourceOffsets;
    bool ReadRequested = false;
    ui64 SourceOffsetReportId = 0;
    TMaybe<ui64> PersistedSourceOffset;
    TMaybe<ui64> BatchEnd;
    TMaybe<ui64> CompletedSourceOffset;
    TDuration SourceOffsetCommitInterval;
    TInstant SourceOffsetCommitStarted;
    TDeque<ui64> PendingSourceCommits;
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
