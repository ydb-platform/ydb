#include "interconnect_proxy_wrapper.h"
#include "interconnect_tcp_proxy.h"
#include <ydb/library/actors/interconnect/mock/ic_mock.h>

namespace NActors {

    class TInterconnectProxyWrapper : public IActorCallback {
        TIntrusivePtr<TInterconnectProxyCommon> Common;
        const ui32 NodeId;
        TInterconnectMock *Mock;
        IActor *Proxy = nullptr;

    public:
        TInterconnectProxyWrapper(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 nodeId, TInterconnectMock *mock)
            : IActorCallback(static_cast<TReceiveFunc>(&TInterconnectProxyWrapper::StateFunc), EActivityType::INTERCONNECT_PROXY_WRAPPER)
            , Common(std::move(common))
            , NodeId(nodeId)
            , Mock(mock)
        {}

        STFUNC(StateFunc) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Poison && !Proxy) {
                PassAway();
            } else {
                if (!Proxy) {
                    IActor *actor = Mock
                        ? Mock->CreateProxyMock(TActivationContext::ActorSystem()->NodeId, NodeId, Common)
                        : new TInterconnectProxyTCP(NodeId, Common, &Proxy);
                    RegisterWithSameMailbox(actor);
                    if (Mock) {
                        Proxy = actor;
                    }
                    Y_ABORT_UNLESS(Proxy);
                }
                InvokeOtherActor(*Proxy, &IActor::Receive, ev);
            }
        }
    };

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId,
            TInterconnectMock *mock) {
        return [=](TActorSystem *as, ui32 nodeId) -> TActorId {
            return as->Register(new TInterconnectProxyWrapper(common, nodeId, mock), TMailboxType::HTSwap, poolId);
        };
    }

} // NActors
