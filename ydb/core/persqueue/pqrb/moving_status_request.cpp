#include "moving_status_request.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NPQ {
namespace {

class TMovingStatusRequest : public TActorBootstrapped<TMovingStatusRequest> {
    const TActorId Parent;
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> Proposal;

public:
    TMovingStatusRequest(const TActorId& parent,
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposal)
        : Parent(parent)
        , Proposal(std::move(proposal))
    {}

    void Bootstrap() {
        Send(MakeTxProxyID(), Proposal.release());
        Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

private:
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        Send(Parent, ev->Release().Release());
        PassAway();
    }

    void HandleTimeout() {
        Send(Parent, new TEvTxUserProxy::TEvProposeTransactionStatus(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout));
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    )
};

} // namespace

IActor* CreateMovingStatusRequest(const TActorId& parent,
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposal)
{
    return new TMovingStatusRequest(parent, std::move(proposal));
}

} // namespace NKikimr::NPQ
