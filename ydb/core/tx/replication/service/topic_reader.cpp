#include "logging.h"
#include "topic_reader.h"
#include "transfer_reader_stats.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NKikimr::NReplication::NService {

class TRemoteTopicReader: public TActor<TRemoteTopicReader> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[RemoteTopicReader]"
                << "[" << Settings.GetBase().Topics_[0].Path_ << "]"
                << "[" << Settings.GetBase().Topics_[0].PartitionIds_[0] << "]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        CreatingReadSessionInProgress = true;
        LOG_D("Handshake"
            << ": worker# " << Worker);

        Y_ABORT_UNLESS(!ReadSession);
        Send(YdbProxy, new TEvYdbProxy::TEvCreateTopicReaderRequest(Settings));
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
        ReadSession = ev->Get()->Result;
        CreatingReadSessionInProgress = false;
        LOG_D("Create read session"
            << ": session# " << ReadSession);

        if (StoppingInProgress) {
            return PassAway();
        }

        Y_ABORT_UNLESS(Worker);
        Send(Worker, new TEvWorker::TEvHandshake());
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(ReadSession);
        auto settings = TEvYdbProxy::TReadTopicSettings()
            .SkipCommit(ev->Get()->SkipCommit);

        Send(ReadSession, new TEvYdbProxy::TEvReadTopicRequest(settings));
        if (Settings.ReportStats_) {
            SendOperationChange(EWorkerOperation::READ);
        }
        ReadQueue.emplace_back(Now());
    }

    void Handle(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(!ReadQueue.empty());
        TResponseDataTrancker req{GetElapsedTicksAsSeconds(), Now() - ReadQueue.front().ReadStartTime, ev};
        if (AppData()->FeatureFlags.GetTransferInternalDataDecompression()) {
            DecompressQueue.emplace_back(std::move(req));
            Schedule(TDuration::Zero(), new TEvents::TEvWakeup(DecompressWakeupTag));
        } else {
            ResponseQueue.emplace_back(std::move(req));
            ProcessData();
        }

        ReadQueue.pop_front();
    }

    void DecompressData() {
        Y_ABORT_UNLESS(!DecompressQueue.empty());
        auto& requestData = DecompressQueue.front();
        auto& readResult = requestData.DataEv->Get()->Result;
        for (auto& msg : readResult.Messages) {
            bool compressed = (msg.GetCodec() != NYdb::NTopic::ECodec::RAW);
            if (compressed && AppData()->FeatureFlags.GetTransferInternalDataDecompression()) {
                if (!requestData.DecompressionDone && Settings.ReportStats_) {
                    SendOperationChange(EWorkerOperation::DECOMPRESS);
                }
                requestData.DecompressionDone = true;
                DecompressMessage(msg);
            }
        }
        ResponseQueue.emplace_back(std::move(requestData));
        DecompressQueue.pop_front();
        Schedule(TDuration::Zero(), new TEvents::TEvWakeup(DecompressionDoneWakeupTag));
    }

    void DecompressMessage(TTopicMessage& message) {
        const NYdb::NTopic::ICodec* codecImpl = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(message.GetCodec()));
        auto decompressed = codecImpl->Decompress(message.GetData());
        message.GetData() = std::move(decompressed);
    }

    void ProcessData() {
        Y_ABORT_UNLESS(!ResponseQueue.empty());
        ui64 totalSize = 0;

        auto& response = ResponseQueue.front();
        auto& result = response.DataEv->Get()->Result;
        TVector<TTopicMessage> records(::Reserve(result.Messages.size()));
        auto msgCount = result.Messages.size();
        ui64 maxOffset = result.Messages.back().GetOffset();
        for (auto& msg : result.Messages) {
            totalSize += msg.GetData().size();
        }

        auto* event = new TEvWorker::TEvData(result.PartitionId, ToString(result.PartitionId), std::move(result.Messages));

        if (Settings.ReportStats_) {
            event->Stats = std::make_unique<TWorkerDetailedStats>(EWorkerOperation::NONE, std::make_unique<TTransferReadStats>(),
                    nullptr);
            event->Stats->ReaderStats->ReadTime = response.ReadDuration;
            event->Stats->ReaderStats->DecompressCpu = response.DecompressionDone
                        ? TDuration::Seconds(GetElapsedTicksAsSeconds() - response.StartCpuUsageSec)
                        : TDuration::Zero();
            event->Stats->ReaderStats->Partition = result.PartitionId;
            event->Stats->ReaderStats->Offset = maxOffset;
            event->Stats->ReaderStats->Messages = msgCount;
            event->Stats->ReaderStats->Bytes = totalSize;
        }
        ResponseQueue.pop_front();
        Send(Worker, event);
    }

    void Handle(TEvYdbProxy::TEvEndTopicPartition::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto& result = ev->Get()->Result;
        Send(Worker, new TEvWorker::TEvDataEnd(result.PartitionId, std::move(result.AdjacentPartitionsIds), std::move(result.ChildPartitionsIds)));
    }

    void Handle(TEvYdbProxy::TEvStartTopicReadingSession::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        ReadSessionId = ev->Get()->Result.ReadSessionId;
    }

    void Handle(TEvWorker::TEvCommit::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (!YdbProxy || !ReadSessionId) {
            return Leave(TEvWorker::TEvGone::UNAVAILABLE);
        }

        CommittedOffset = ev->Get()->Offset;
        Send(YdbProxy, CreateCommitOffsetRequest().release());
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetResponse::TPtr& ev) {
        if (!ev->Get()->Result.IsSuccess()) {
            LOG_W("Handle " << ev->Get()->ToString());
            return Leave(TEvWorker::TEvGone::UNAVAILABLE);
        } else {
            LOG_D("Handle " << CommittedOffset << " " << ev->Get()->ToString());
            if (CommittedOffset) {
                Send(ReadSession, CreateCommitOffsetRequest().release());
            }
        }
    }

    std::unique_ptr<TEvYdbProxy::TEvCommitOffsetRequest> CreateCommitOffsetRequest() {
        auto settings = NYdb::NTopic::TCommitOffsetSettings()
            .ReadSessionId(ReadSessionId);

        const auto& topicName = Settings.GetBase().Topics_.at(0).Path_;
        const auto partitionId = Settings.GetBase().Topics_.at(0).PartitionIds_.at(0);
        const auto& consumerName = Settings.GetBase().ConsumerName_;

        return std::make_unique<TEvYdbProxy::TEvCommitOffsetRequest>(topicName, partitionId, consumerName, CommittedOffset, std::move(settings));
    }

    void Handle(TEvYdbProxy::TEvTopicReaderGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        if (Settings.ReportStats_) {
            SendError();
        }

        switch (ev->Get()->Result.GetStatus()) {
        case NYdb::EStatus::SCHEME_ERROR:
        case NYdb::EStatus::BAD_REQUEST:
            return Leave(TEvWorker::TEvGone::SCHEME_ERROR, ev->Get()->Result.GetIssues().ToOneLineString());
        default:
            return Leave(TEvWorker::TEvGone::UNAVAILABLE, ev->Get()->Result.GetIssues().ToOneLineString());
        }
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case DecompressWakeupTag:
                DecompressData();
                break;
            case DecompressionDoneWakeupTag:
                ProcessData();
                break;
            default:
                LOG_W("Handle Wakeup with unexpected tag " << ev->Get()->Tag);
        }
    }

    template <typename... Args>
    void Leave(Args&&... args) {
        LOG_I("Leave");

        Send(Worker, new TEvWorker::TEvGone(std::forward<Args>(args)...));
        PassAway();
    }

    void PassAway() override {
        if (CreatingReadSessionInProgress) {
            StoppingInProgress = true;
            return;
        }

        if (const auto& actorId = std::exchange(ReadSession, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }

        TActor::PassAway();
    }

    void SendOperationChange(EWorkerOperation currentOperation) {
        Send(Worker, TEvWorker::TEvStatus::FromOperation(currentOperation));
    }

    void SendError() {
        auto* ev = TEvWorker::TEvStatus::FromOperation(EWorkerOperation::NONE);
        ev->DetailedStats->ReaderStats = std::make_unique<TTransferReadStats>();
        ev->DetailedStats->ReaderStats->Errors = 1;
        Send(Worker, ev);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_REMOTE_TOPIC_READER;
    }

    explicit TRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts)
        : TActor(&TThis::StateWork)
        , YdbProxy(ydbProxy)
        , Settings(opts)
    {
        const auto& base = Settings.GetBase();
        Y_ABORT_UNLESS(base.Topics_.size() == 1);
        Y_ABORT_UNLESS(base.Topics_.at(0).PartitionIds_.size() == 1);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvWorker::TEvCommit, Handle);
            hFunc(TEvYdbProxy::TEvCreateTopicReaderResponse, Handle);
            hFunc(TEvYdbProxy::TEvReadTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetResponse, Handle);
            hFunc(TEvYdbProxy::TEvStartTopicReadingSession, Handle);
            hFunc(TEvYdbProxy::TEvEndTopicPartition, Handle);
            hFunc(TEvYdbProxy::TEvTopicReaderGone, Handle);
            hFunc(TEvents::TEvWakeup, HandleWakeup);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    struct TReadRequestDataTracker {
        TInstant ReadStartTime;
    };

    struct TResponseDataTrancker {
        double StartCpuUsageSec;
        TDuration ReadDuration;
        TEvYdbProxy::TEvReadTopicResponse::TPtr DataEv;
        bool DecompressionDone = false;
    };


    const TActorId YdbProxy;
    TEvYdbProxy::TTopicReaderSettings Settings;
    mutable TMaybe<TString> LogPrefix;

    TActorId Worker;
    TActorId ReadSession;
    TString ReadSessionId;
    ui64 CommittedOffset = 0;

    bool CreatingReadSessionInProgress = false;
    bool StoppingInProgress = false;
    TDeque<TReadRequestDataTracker> ReadQueue;
    TDeque<TResponseDataTrancker> DecompressQueue;
    TDeque<TResponseDataTrancker> ResponseQueue;

    constexpr const static ui64 DecompressWakeupTag = 1;
    constexpr const static ui64 DecompressionDoneWakeupTag = 2;
}; // TRemoteTopicReader

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts) {
    return new TRemoteTopicReader(ydbProxy, opts);
}

} //NKikimr::NReplication::NService
