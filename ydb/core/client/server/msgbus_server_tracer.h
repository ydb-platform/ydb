#pragma once
#include <util/stream/file.h>
#include <util/thread/pool.h>
#include <util/generic/buffer.h>
#include <library/cpp/messagebus/handler.h>
#include <library/cpp/messagebus/message.h>
#include <library/cpp/messagebus/defs.h>
#include <ydb/public/lib/base/msgbus.h>
#include <library/cpp/actors/core/hfunc.h>
#include "msgbus_server.h"
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr {
namespace NMessageBusTracer {


class TMessageBusTracingServer : public NKikimr::NMsgBusProxy::TMessageBusServer, public NMsgBusProxy::IMessageWatcher {
public:
    using TBusKey = NBus::TBusKey;
    TMessageBusTracingServer(
        const NBus::TBusServerSessionConfig &sessionConfig,
        NBus::TBusMessageQueue *busQueue,
        const TString &tracePath,
        ui32 bindPort
    );
    IActor* CreateMessageBusTraceService() override;
protected:
    TActorId MessageBusTracerActorID;
    bool TraceActive;
    TString TracePath;
    void OnMessage(NBus::TOnMessageContext &msg) override;
    void OnMessageDied(NBus::TBusKey id) override;
    void OnMessageReplied(NBus::TBusKey id, NBus::TBusMessage *response) override;
    void SaveRequest(NBus::TBusMessage *msg, NBus::TBusKey replyId = YBUS_KEYINVALID);
};

struct TEvMessageBusTracer {
    enum EEv {
        EvTraceEvent = EventSpaceBegin(TKikimrEvents::ES_MSGBUS_TRACER),
        EvStartTrace,
        EvStopTrace,
        EvTraceStatus,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_MSGBUS_TRACER), "expect End < EventSpaceEnd(TKikimrEvents::ES_MSGBUS_TRACER)");

    class TEvTraceEvent : public TEventLocal<TEvTraceEvent, TEvMessageBusTracer::EvTraceEvent> {
    public:
        using TBusKey = NBus::TBusKey;
        TBusKey ReplyId;
        NBus::TBusHeader Header;
        TBuffer Content;

        TEvTraceEvent(NBus::TBusMessage *msg, TBuffer &&content, TBusKey replyId = YBUS_KEYINVALID);
    };

    class TEvStartTrace : public TEventLocal<TEvStartTrace, TEvMessageBusTracer::EvStartTrace> {
    public:
        TString Path;

        TEvStartTrace(const TString &path);
    };

    class TEvStopTrace : public TEventLocal<TEvStopTrace, TEvMessageBusTracer::EvStopTrace> {
    public:
        TEvStopTrace() {}
    };

    class TEvTraceStatus : public TEventLocal<TEvTraceStatus, TEvMessageBusTracer::EvTraceStatus> {
    public:
        bool TraceActive;
        TString Path;

        TEvTraceStatus(bool traceActive, const TString &path);
    };

};

class TMessageBusTracerService : public TActor<TMessageBusTracerService> {
public:
    using TProtocol = NKikimr::NMsgBusProxy::TProtocol;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_TRACER_ACTOR;
    }

    TMessageBusTracerService();
    void StateFunc(TAutoPtr<IEventHandle> &ev);

private:
    void HandleStartTrace(TEvMessageBusTracer::TEvStartTrace::TPtr &ev, const TActorContext &ctx);
    void HandleStopTrace(TEvMessageBusTracer::TEvStopTrace::TPtr &ev, const TActorContext &ctx);
    void HandleTraceEvent(TEvMessageBusTracer::TEvTraceEvent::TPtr &ev, const TActorContext &);

    TAutoPtr<IOutputStream> Stream;
    TString Path;
};

inline TActorId MakeMessageBusTraceServiceID(ui32 node = 0) {
    char x[12] = {'m','s','g','b','u','s','t','r','a','c','e','r'};
    return TActorId(node, TStringBuf(x, 12));
}

IActor* CreateMessageBusTracerStartTrace(NMsgBusProxy::TBusMessageContext &msg, const TString &path);
IActor* CreateMessageBusTracerStopTrace(NMsgBusProxy::TBusMessageContext &msg);

}
}
