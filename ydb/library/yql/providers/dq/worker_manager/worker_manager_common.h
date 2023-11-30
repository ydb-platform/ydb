#pragma once

#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

#include <ydb/library/actors/helpers/future_callback.h>

namespace NYql::NDqs {

template<typename TDerived>
class TWorkerManagerCommon: public TRichActor<TDerived> {
public:
    TWorkerManagerCommon(void (TDerived::*func)(TAutoPtr<NActors::IEventHandle>& ev))
        : TRichActor<TDerived>(func)
    { }

    void OnRoutesRequest(TEvRoutesRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        auto localRequest = MakeHolder<NActors::TEvInterconnect::TEvListNodes>();

        auto replyTo = ev->Sender;
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;

        auto callback = MakeHolder<NActors::TActorFutureCallback<NActors::TEvInterconnect::TEvNodesInfo>>(
            [replyTo, actorSystem] (TAutoPtr<NActors::TEventHandle<NActors::TEvInterconnect::TEvNodesInfo>>& event) {
                auto response = MakeHolder<TEvRoutesResponse>();
                for (const auto& node: event->Get()->Nodes) {
                    auto* n = response->Record.MutableResponse()->AddNodes();
                    n->SetPort(node.Port);
                    n->SetAddress(node.Address);
                    n->SetNodeId(node.NodeId);
                }
                if (actorSystem) {
                    actorSystem->Send(replyTo, response.Release());
                }
            });

        auto callbackId = ctx.Register(callback.Release());

        ctx.Send(new NActors::IEventHandle(NActors::GetNameserviceActorId(), callbackId, localRequest.Release()));
    }
};

}
