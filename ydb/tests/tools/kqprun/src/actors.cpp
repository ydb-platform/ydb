#include "actors.h"

#include <ydb/core/kqp/common/simple/services.h>


namespace NKqpRun {

namespace {

class TRunScriptActorMock : public NActors::TActorBootstrapped<TRunScriptActorMock> {
public:
    TRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request, NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise, ui64 resultSizeLimit)
        : Request_(std::move(request))
        , Promise_(promise)
        , ResultSizeLimit_(std::numeric_limits<i64>::max())
    {
        if (resultSizeLimit && resultSizeLimit < std::numeric_limits<i64>::max()) {
            ResultSizeLimit_ = resultSizeLimit;
        }
    }

    void Bootstrap() {
        NActors::ActorIdToProto(SelfId(), Request_->Record.MutableRequestActorId());
        Send(NKikimr::NKqp::MakeKqpProxyID(SelfId().NodeId()), std::move(Request_));

        Become(&TRunScriptActorMock::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NKikimr::NKqp::TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(NKikimr::NKqp::TEvKqp::TEvQueryResponse, Handle);
    )
    
    void Handle(NKikimr::NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        auto response = MakeHolder<NKikimr::NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        response->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        response->Record.SetFreeSpace(ResultSizeLimit_);

        Send(ev->Sender, response.Release());
    }
    
    void Handle(NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Promise_.SetValue(std::move(ev));
        PassAway();
    }

private:
    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Request_;
    NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> Promise_;
    i64 ResultSizeLimit_;
};

}  // anonymous namespace

NActors::IActor* CreateRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request, NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise, ui64 resultSizeLimit) {
    return new TRunScriptActorMock(std::move(request), promise, resultSizeLimit);
}

}  // namespace NKqpRun
