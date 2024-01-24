#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NService {

class TRemoteTopicReader: public TActor<TRemoteTopicReader> {
    using TReadSessionSettings = NYdb::NTopic::TReadSessionSettings;

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;

        Y_ABORT_UNLESS(!ReadSession);
        Send(YdbProxy, new TEvYdbProxy::TEvCreateTopicReaderRequest(Settings));
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
        ReadSession = ev->Get()->Result;

        Y_ABORT_UNLESS(Worker);
        Send(Worker, new TEvWorker::TEvHandshake());
    }

    void Handle(TEvWorker::TEvPoll::TPtr&) {
        Y_ABORT_UNLESS(ReadSession);
        Send(ReadSession, new TEvYdbProxy::TEvReadTopicRequest());

        if (CommitOffset) {
            Send(YdbProxy, new TEvYdbProxy::TEvCommitOffsetRequest(
                Settings.Topics_[0].Path_,
                Settings.Topics_[0].PartitionIds_[0],
                Settings.ConsumerName_,
                CommitOffset, {}
            ));
        }
    }

    void Handle(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        auto& result = ev->Get()->Result;
        TVector<TEvWorker::TEvData::TRecord> records(Reserve(result.Messages.size()));

        for (auto& msg : result.Messages) {
            Y_ABORT_UNLESS(msg.GetCodec() == NYdb::NTopic::ECodec::RAW);
            Y_DEBUG_ABORT_UNLESS(msg.GetOffset() + 1 > CommitOffset);
            CommitOffset = Max(CommitOffset, msg.GetOffset() + 1);
            records.emplace_back(msg.GetOffset(), std::move(msg.GetData()));
        }

        Send(Worker, new TEvWorker::TEvData(std::move(records)));
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetResponse::TPtr& ev) {
        if (!ev->Get()->Result.IsSuccess()) {
            Leave();
        }
    }

    void Leave() {
        Send(Worker, new TEvents::TEvGone());
        PassAway();
    }

    void PassAway() override {
        if (const auto& actorId = std::exchange(ReadSession, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }

        TActor::PassAway();
    }

public:
    explicit TRemoteTopicReader(const TActorId& ydbProxy, const TReadSessionSettings& opts)
        : TActor(&TThis::StateWork)
        , YdbProxy(ydbProxy)
        , Settings(opts)
    {
        Y_ABORT_UNLESS(Settings.Topics_.size() == 1);
        Y_ABORT_UNLESS(Settings.Topics_.at(0).PartitionIds_.size() == 1);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvPoll, Handle);
            hFunc(TEvYdbProxy::TEvCreateTopicReaderResponse, Handle);
            hFunc(TEvYdbProxy::TEvReadTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetResponse, Handle);
            sFunc(TEvents::TEvGone, Leave);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId YdbProxy;
    const TReadSessionSettings Settings;

    TActorId Worker;
    TActorId ReadSession;
    ui64 CommitOffset = 0;

}; // TRemoteTopicReader

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const NYdb::NTopic::TReadSessionSettings& opts) {
    return new TRemoteTopicReader(ydbProxy, opts);
}

}
