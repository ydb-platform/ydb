#include "logging.h"
#include "topic_reader.h"
#include "worker.h"

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
        LOG_D("Handshake"
            << ": worker# " << Worker);

        Y_ABORT_UNLESS(!ReadSession);
        Send(YdbProxy, new TEvYdbProxy::TEvCreateTopicReaderRequest(Settings));
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
        ReadSession = ev->Get()->Result;
        LOG_D("Create read session"
            << ": session# " << ReadSession);

        Y_ABORT_UNLESS(Worker);
        Send(Worker, new TEvWorker::TEvHandshake());
    }

    void Handle(TEvWorker::TEvPoll::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(ReadSession);
        auto settings = TEvYdbProxy::TReadTopicSettings()
            .SkipCommit(ev->Get()->SkipCommit);

        Send(ReadSession, new TEvYdbProxy::TEvReadTopicRequest(settings));
    }

    void Handle(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto& result = ev->Get()->Result;
        TVector<TTopicMessage> records(::Reserve(result.Messages.size()));

        for (auto& msg : result.Messages) {
            Y_ABORT_UNLESS(msg.GetCodec() == NYdb::NTopic::ECodec::RAW);
            records.push_back(std::move(msg));
        }

        Send(Worker, new TEvWorker::TEvData(result.PartitionId, ToString(result.PartitionId), std::move(records)));
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

        switch (ev->Get()->Result.GetStatus()) {
        case NYdb::EStatus::SCHEME_ERROR:
            return Leave(TEvWorker::TEvGone::SCHEME_ERROR, ev->Get()->Result.GetIssues().ToOneLineString());
        default:
            return Leave(TEvWorker::TEvGone::UNAVAILABLE);
        }
    }

    template <typename... Args>
    void Leave(Args&&... args) {
        LOG_I("Leave");

        Send(Worker, new TEvWorker::TEvGone(std::forward<Args>(args)...));
        PassAway();
    }

    void PassAway() override {
        if (const auto& actorId = std::exchange(ReadSession, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }

        TActor::PassAway();
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
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId YdbProxy;
    const TEvYdbProxy::TTopicReaderSettings Settings;
    mutable TMaybe<TString> LogPrefix;

    TActorId Worker;
    TActorId ReadSession;
    TString ReadSessionId;
    ui64 CommittedOffset = 0;

}; // TRemoteTopicReader

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts) {
    return new TRemoteTopicReader(ydbProxy, opts);
}

}
