#include "cq_actor.h"
#include "rdma/events.h"
#include "rdma/rdma.h"
#include "logging.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/poller_actor.h>

using namespace NActors;

namespace NInterconnect::NRdma {

using TCqFactory = std::function<ICq::TPtr(const TRdmaCtx*)>;

class TCqActor: public TInterconnectLoggingBase,
                public TActorBootstrapped<TCqActor> {
    class TAsyncEventDesctiptor : public TSharedDescriptor {
    public:
        TAsyncEventDesctiptor(const TRdmaCtx* ctx)
            : Ctx(ctx)
        {}
        int GetDescriptor() override {
            return Ctx->GetContext()->async_fd;
        }
        const TRdmaCtx* GetContext() const {
            return Ctx;
        }
    private:
        const TRdmaCtx* Ctx;
    };
public:
    TCqActor(TCqFactory cqFactory)
        : TInterconnectLoggingBase("TCqActor")
        , CqFactory(std::move(cqFactory))
    {}
    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::EActivityType::INTERCONNECT_RDMA_CQ;
    }

    void Bootstrap() {
        Become(&TCqActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetCqHandle, Handle);
        hFunc(NActors::TEvPollerRegisterResult, Handle);
        hFunc(NActors::TEvPollerReady, Handle);
    )

    bool RegisterForAsyncEvent(const TRdmaCtx* ctx) {
        int flags = fcntl(ctx->GetContext()->async_fd, F_GETFL);
        int ret = fcntl(ctx->GetContext()->async_fd, F_SETFL, flags | O_NONBLOCK);
        if (ret < 0) {
	        return false;
        }
        auto rv = Send(MakePollerActorId(),
            new NActors::TEvPollerRegister(new TAsyncEventDesctiptor(ctx), SelfId(), SelfId()));
        Y_ABORT_UNLESS(rv, "actor system poller service must present");
        return true;
    }



    void Handle(TEvGetCqHandle::TPtr& ev) {
        auto rdmaCtx = ev->Get()->Ctx;
        auto it = CqMap.find(rdmaCtx);
        ICq::TPtr cqPtr = nullptr;
        if (it == CqMap.end()) {
            if (RegisterForAsyncEvent(rdmaCtx)) {
                CqMap.emplace(rdmaCtx, TWaitPollerReg(ev));
                return;
            }
            LOG_ERROR_IC("ICRDMA", "Unable to register async_fd: %d",
                rdmaCtx->GetContext()->async_fd);
        } else if (it->second.index() == 0) {
            cqPtr = std::get<0>(it->second).Cq;
        } else {
            Y_ABORT_UNLESS(it->second.index() == 1);
            std::get<1>(it->second).Waiters.emplace_back(ev);
            return;
        }

        // nullptr in case of err
        ev->Get()->CqPtr = cqPtr;

        TlsActivationContext->Send(ev->Sender, std::unique_ptr<TEvGetCqHandle>(ev->Release().Release()));
        Y_UNUSED(it);
    }

    void Handle(NActors::TEvPollerRegisterResult::TPtr& ev) {
        auto rdmaCtx = static_cast<TAsyncEventDesctiptor*>(ev->Get()->Socket.Get())->GetContext();
        auto cqPtr = CqFactory(rdmaCtx);
        auto it = CqMap.find(rdmaCtx);
        Y_ABORT_UNLESS(it != CqMap.end());
        Y_ABORT_UNLESS(it->second.index() == 1);
        auto waiters = std::get<1>(it->second).Waiters;
        if (cqPtr) {
            it->second = TCtxData {
                .Cq = cqPtr,
                .AsyncEventToken = ev->Get()->PollerToken,
            };
            for (auto p : waiters) {
                p->Get()->CqPtr = cqPtr;
                TlsActivationContext->Send(p->Sender, std::unique_ptr<TEvGetCqHandle>(p->Release().Release()));
            }
            std::get<0>(it->second).AsyncEventToken->Request(true, false);
        } else {
            for (auto p : waiters) {
                TlsActivationContext->Send(p->Sender, std::unique_ptr<TEvGetCqHandle>(p->Release().Release()));
            }
            CqMap.erase(it);
        }
    }

    void Handle(NActors::TEvPollerReady::TPtr& ev) {
        auto res = ProcessIbvAsyncEvents(static_cast<TAsyncEventDesctiptor*>(ev->Get()->Socket.Get()));
        Y_ABORT_UNLESS(res, "unable to get async event while poller told us is's ready");
    }
private:

    static TString GetAsyncEventDbg(const TRdmaCtx* ctx, const ibv_async_event& event) {
        switch (event.event_type) {
            /* QP events */
            case IBV_EVENT_QP_FATAL:
                return Sprintf("QP fatal event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_REQ_ERR:
                return Sprintf("QP Requestor error for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_ACCESS_ERR:
                return Sprintf("QP access error event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_COMM_EST:
                return Sprintf("QP communication established event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_SQ_DRAINED:
                return Sprintf("QP Send Queue drained event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_PATH_MIG:
                return Sprintf("QP Path migration loaded event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_PATH_MIG_ERR:
                return Sprintf("QP Path migration error event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_LAST_WQE_REACHED:
                return Sprintf("QP last WQE reached event for QP with handle %p\n", event.element.qp);

            /* CQ events */
            case IBV_EVENT_CQ_ERR:
                return Sprintf("CQ error for CQ with handle %p\n", event.element.cq);

            /* SRQ events */
            case IBV_EVENT_SRQ_ERR:
                return Sprintf("SRQ error for SRQ with handle %p\n", event.element.srq);
            case IBV_EVENT_SRQ_LIMIT_REACHED:
                return Sprintf("SRQ limit reached event for SRQ with handle %p\n", event.element.srq);

            /* Port events */
            case IBV_EVENT_PORT_ACTIVE:
                return Sprintf("Port active event for port number %d\n", event.element.port_num);
            case IBV_EVENT_PORT_ERR:
                return Sprintf("Port error event for port number %d\n", event.element.port_num);
            case IBV_EVENT_LID_CHANGE:
                return Sprintf("LID change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_PKEY_CHANGE:
                return Sprintf("P_Key table change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_GID_CHANGE:
                return Sprintf("GID table change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_SM_CHANGE:
                return Sprintf("SM change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_CLIENT_REREGISTER:
                return Sprintf("Client reregister event for port number %d\n", event.element.port_num);

            /* RDMA device events */
            case IBV_EVENT_DEVICE_FATAL:
                return Sprintf("Fatal error event for device %s\n", ctx->GetDeviceName());

            default:
                return Sprintf("Unknown event (%d)\n", event.event_type);
        }
    }
    void ProcessCqErr(auto it) {
        TCtxData& c = std::get<0>(it->second);
        LOG_ERROR_IC("ICRDMA", "Cq error issued on ctx %s, notify all pending cq callbacks",
            it->first->ToString().data());
        c.Cq->NotifyErr();
    }
    bool ProcessIbvAsyncEvents(const TAsyncEventDesctiptor* desc) {
        auto rdmaCtx = desc->GetContext();
        ibv_async_event async_event;
        if (ibv_get_async_event(rdmaCtx->GetContext(), &async_event)) {
            return false;
        }
        auto it = CqMap.find(rdmaCtx);
        Y_ABORT_UNLESS(it != CqMap.end());

        LOG_ERROR_IC("ICRDMA", "RDMA async event issued on ctx %s, %s",
            rdmaCtx->ToString().data(), GetAsyncEventDbg(rdmaCtx, async_event).data());

        switch (async_event.event_type) {
            case IBV_EVENT_CQ_ERR:
                ProcessCqErr(it);
                CqMap.erase(it);
            break;
            default:
                std::get<0>(it->second).AsyncEventToken->Request(true, false);
            break;
        }
        ibv_ack_async_event(&async_event);
        return true;
    }

    struct TCtxData {
        ICq::TPtr Cq;
        NActors::TPollerToken::TPtr AsyncEventToken;
    };
    struct TWaitPollerReg {
        TWaitPollerReg(TEvGetCqHandle::TPtr& ev)
        {
            Waiters.emplace_back(ev);
        }
        std::vector<TEvGetCqHandle::TPtr> Waiters;
    };
    std::map<const TRdmaCtx*, std::variant<TCtxData, TWaitPollerReg>> CqMap;
    TCqFactory CqFactory;
};

NActors::IActor* CreateCqActor(int max_cqe) {
    return new TCqActor([max_cqe](const TRdmaCtx* ctx) {
        return CreateSimpleCq(ctx, TlsActivationContext->AsActorContext().ActorSystem(), max_cqe);
    });
}

NActors::IActor* CreateCqMockActor(int max_cqe) {
    return new TCqActor([max_cqe](const TRdmaCtx* ctx) {
        return CreateSimpleCqMock(ctx, TlsActivationContext->AsActorContext().ActorSystem(), max_cqe);
    });
}

NActors::TActorId MakeCqActorId() {
    char x[12] = {'I', 'C', 'R', 'D', 'M', 'A', 'C', 'Q', '\xDE', '\xAD', '\xBE', '\xEF'};
    return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
}

}
