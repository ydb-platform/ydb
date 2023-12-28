#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <unordered_map>

namespace NKikimr::NTabletPipe {

class TPipeHelper {
public:
    static IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config = TClientConfig()) {
        return NKikimr::NTabletPipe::CreateClient(owner, tabletId, config);
    }

    static void SendData(const TActorContext& ctx, const TActorId& clientId, IEventBase* payload, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        return NKikimr::NTabletPipe::SendData(ctx, clientId, payload, cookie, std::move(traceId));
    }
};

namespace NTest {

    class TPipeMock {
    private:
        class TPipeActorMock: public TActorBootstrapped<TPipeActorMock> {
        public:
            TPipeActorMock(const TActorId& clientId, ui64 tabletId, const TActorId& forwardTo)
                : ClientId(clientId)
                , TabletId(tabletId)
                , ForwardTo(forwardTo) {}
        
            void Bootstrap(const TActorContext&  ctx) {
                if (ForwardTo) {
                    ctx.Send(ForwardTo, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::EReplyStatus::OK, ClientId, ClientId, true, false, 1));
                    Become(&TPipeActorMock::StateForward);
                } else {
                    ctx.Send(ForwardTo, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::EReplyStatus::ERROR, ClientId, ClientId, true, false, 0));
                    Die(ctx);
                }
            }

        private:
            STFUNC(StateForward) {
                auto ctx = TActivationContext::ActorContextFor(TActor<TPipeActorMock>::SelfId());
                ctx.Forward(ev, ForwardTo);
            }

        private:
            TActorId ClientId;
            ui64 TabletId;
            TActorId ForwardTo;
        };

    public:
        static IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config = TClientConfig()) {
            Y_UNUSED(config);

            auto it = Tablets.find(tabletId);
            auto actorId = it == Tablets.end() ? TActorId() : it->second;
            return new TPipeActorMock(owner, tabletId, actorId);
        }

        static void Clear() {
            Tablets.clear();
        }

        static void Register(ui64 tabletId, const TActorId& actorId) {
            Tablets[tabletId] = actorId;
        }
    private:
        static std::unordered_map<ui64, TActorId> Tablets;
    };

} // namespace NTest

} // namespace NKikimr::NTabletPipe
