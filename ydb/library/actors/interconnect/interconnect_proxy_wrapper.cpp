#include "interconnect_proxy_wrapper.h"
#include "interconnect_tcp_proxy.h"

namespace NActors {

    static IActor* CreateIcProxy(const ui32 nodeId, TInterconnectProxyCommon::TPtr common, IActor** self) {
        return new TInterconnectProxyTCP(nodeId, std::move(common), self);
    }

    class TInterconnectProxyWrapper : public IActorCallback {
        TIntrusivePtr<TInterconnectProxyCommon> Common;
        const ui32 NodeId;
        TIcProxyCreator Creator;
        IActor *Proxy = nullptr;

    public:
        TInterconnectProxyWrapper(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 nodeId, TIcProxyCreator creator)
            : IActorCallback(static_cast<TReceiveFunc>(&TInterconnectProxyWrapper::StateFunc), EActivityType::INTERCONNECT_PROXY_WRAPPER)
            , Common(std::move(common))
            , NodeId(nodeId)
            , Creator(std::move(creator))
        {}

        STFUNC(StateFunc) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Poison && !Proxy) {
                PassAway();
            } else {
                if (!Proxy) {
                    IActor *actor = Creator(NodeId, Common, &Proxy);
                    RegisterWithSameMailbox(actor);
                    Y_ABORT_UNLESS(Proxy);
                }
                InvokeOtherActor(*Proxy, &IActor::Receive, ev);
            }
        }
    };

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common,
            TInterconnectSessionPoolMapping poolMapping) {
        return [common = std::move(common), poolMapping = std::move(poolMapping)]
                (TActorSystem *as, ui32 nodeId) -> TActorId {
            return as->Register(new TInterconnectProxyWrapper(common, nodeId, &CreateIcProxy), TMailboxType::HTSwap,
                poolMapping.GetPoolId(nodeId));
        };
    }

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId) {
        return CreateProxyWrapperFactory(std::move(common), TInterconnectSessionPoolMapping({poolId}), &CreateIcProxy);
    }

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common,
            TInterconnectSessionPoolMapping poolMapping, TIcProxyCreator proxyCreator) {
        return [common = std::move(common), poolMapping = std::move(poolMapping), proxyCreator = std::move(proxyCreator)]
                (TActorSystem *as, ui32 nodeId) -> TActorId {
            return as->Register(new TInterconnectProxyWrapper(common, nodeId, proxyCreator), TMailboxType::HTSwap,
                poolMapping.GetPoolId(nodeId));
        };
    }

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId, TIcProxyCreator proxyCreator) {
        return CreateProxyWrapperFactory(std::move(common), TInterconnectSessionPoolMapping({poolId}), proxyCreator);
    }
} // NActors
