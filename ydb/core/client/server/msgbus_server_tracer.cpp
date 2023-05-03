#include "msgbus_server_tracer.h"
#include "msgbus_servicereq.h"
#include <util/folder/path.h>

namespace NKikimr {
namespace NMessageBusTracer {


TMessageBusTracingServer::TMessageBusTracingServer(
    const NBus::TBusServerSessionConfig &sessionConfig,
    NBus::TBusMessageQueue *busQueue,
    const TString& tracePath,
    ui32 bindPort
)
    : NKikimr::NMsgBusProxy::TMessageBusServer(sessionConfig, busQueue, bindPort)
    , MessageBusTracerActorID(MakeMessageBusTraceServiceID())
    , TraceActive(false)
    , TracePath(tracePath)
{
}

void TMessageBusTracingServer::OnMessage(NBus::TOnMessageContext &msg) {
    NMsgBusProxy::TBusMessageContext msgCtx(msg, TraceActive ? this : nullptr);
    if (msgCtx.GetMessage()->GetHeader()->Type == NMsgBusProxy::MTYPE_CLIENT_MESSAGE_BUS_TRACE) {
        const auto &record = static_cast<NMsgBusProxy::TBusMessageBusTraceRequest*>(msgCtx.GetMessage())->Record;
        if (record.HasCommand()) {
            const NKikimrClient::TMessageBusTraceRequest::ECommand command = record.GetCommand();
            switch (command) {
            case NKikimrClient::TMessageBusTraceRequest::START:
                if (record.HasPath()) {
                    TFsPath basePath = TracePath;
                    TFsPath path = record.GetPath();
                    TFsPath tracePath = basePath / path.GetName();

                    if (tracePath.IsSubpathOf(basePath)) {
                        if (IActor *x = CreateMessageBusTracerStartTrace(msgCtx, basePath / path.GetName())) {
                            TraceActive = true;
                            ActorSystem->Register(x);
                            return;
                        }
                    }
                }
                break;
            case NKikimrClient::TMessageBusTraceRequest::STOP:
                if (IActor *x = CreateMessageBusTracerStopTrace(msgCtx)) {
                    TraceActive = false;
                    ActorSystem->Register(x);
                    return;
                }
                break;
            }
        }
        msgCtx.SendReplyMove(new NMsgBusProxy::TBusResponseStatus(NMsgBusProxy::MSTATUS_ERROR, "undocumented error 2"));
    } else {
        if (TraceActive)
            SaveRequest(msgCtx.GetMessage());
        TMessageBusServer::OnMessage(msgCtx);
    }
}

void TMessageBusTracingServer::OnMessageDied(NBus::TBusKey id) {
    if (TraceActive)
        SaveRequest(nullptr, id);
}

void TMessageBusTracingServer::OnMessageReplied(NBus::TBusKey id, NBus::TBusMessage *response) {
    if (TraceActive)
        SaveRequest(response, id);
}

void TMessageBusTracingServer::SaveRequest(NBus::TBusMessage *msg, NBus::TBusKey replyId) {
    TBuffer content;
    if (msg->GetHeader()->Size > 0)
        content.Reserve(msg->GetHeader()->Size - sizeof(NBus::TBusHeader));
    Protocol.Serialize(msg, content);
    ActorSystem->Send(MessageBusTracerActorID, new TEvMessageBusTracer::TEvTraceEvent(msg, std::move(content), replyId));
}

IActor* TMessageBusTracingServer::CreateMessageBusTraceService() {
    return new TMessageBusTracerService();
}

TEvMessageBusTracer::TEvTraceEvent::TEvTraceEvent(NBus::TBusMessage *msg, TBuffer &&content, NBus::TBusKey replyId)
    : ReplyId(replyId)
    , Header(*msg->GetHeader())
    , Content(content)
{
    if (Header.Size == 0)
        Header.Size = sizeof(Header) + Content.Size();
}

TMessageBusTracerService::TMessageBusTracerService()
    : TActor(&TMessageBusTracerService::StateFunc)
{}

void TMessageBusTracerService::StateFunc(TAutoPtr<IEventHandle> &ev) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvMessageBusTracer::TEvTraceEvent, HandleTraceEvent);
        HFunc(TEvMessageBusTracer::TEvStartTrace, HandleStartTrace);
        HFunc(TEvMessageBusTracer::TEvStopTrace, HandleStopTrace);
    }
}

void TMessageBusTracerService::HandleStartTrace(TEvMessageBusTracer::TEvStartTrace::TPtr &ev, const TActorContext &ctx) {
    const TEvMessageBusTracer::TEvStartTrace *event = ev->Get();
    Path = event->Path;
    Stream = new TFileOutput(Path);
    LOG_NOTICE_S(ctx, NKikimrServices::MSGBUS_TRACER, "MessageBus tracing started, file '"
        << Path << "' "
        << ctx.SelfID.ToString());
    ctx.Send(ev->Sender, new TEvMessageBusTracer::TEvTraceStatus(!!Stream, Path));
}

void TMessageBusTracerService::HandleStopTrace(TEvMessageBusTracer::TEvStopTrace::TPtr &ev, const TActorContext &ctx) {
    const TEvMessageBusTracer::TEvStopTrace *event = ev->Get();
    Y_UNUSED(event);
    LOG_NOTICE_S(ctx, NKikimrServices::MSGBUS_TRACER, "MessageBus tracing stopped, file '" << Path << "' "
        << ctx.SelfID.ToString());
    Path.clear();
    Stream.Reset(nullptr);
    ctx.Send(ev->Sender, new TEvMessageBusTracer::TEvTraceStatus(!!Stream, Path));
}

void TMessageBusTracerService::HandleTraceEvent(TEvMessageBusTracer::TEvTraceEvent::TPtr &ev, const TActorContext &) {
    const TEvMessageBusTracer::TEvTraceEvent *event = ev->Get();
    if (Stream != nullptr) {
        Stream->Write(&event->ReplyId, sizeof(NBus::TBusKey));
        Stream->Write(&event->Header, sizeof(NBus::TBusHeader));
        const TBuffer& content(event->Content);
        Stream->Write(content.Data(), content.Size());
//#ifndef NDEBUG
//        Stream->Flush();
//#endif
    }
}

TEvMessageBusTracer::TEvStartTrace::TEvStartTrace(const TString &path)
    : Path(path)
{
}

TEvMessageBusTracer::TEvTraceStatus::TEvTraceStatus(bool traceActive, const TString &path)
    : TraceActive(traceActive)
    , Path(path)
{
}


namespace {
    const ui32 DefaultTimeout = 90000;
}

template <typename TDerived>
class TMessageBusTraceSimpleActor : public NMsgBusProxy::TMessageBusLocalServiceRequest<TDerived, NKikimrServices::TActivity::MSGBUS_TRACER_ACTOR> {
public:
    TMessageBusTraceSimpleActor(NMsgBusProxy::TBusMessageContext &msg)
        : NMsgBusProxy::TMessageBusLocalServiceRequest<TDerived, NKikimrServices::TActivity::MSGBUS_TRACER_ACTOR>(msg, TDuration::MilliSeconds(DefaultTimeout))
    {}

    void Handle(TEvMessageBusTracer::TEvTraceStatus::TPtr &ev, const TActorContext &ctx) {
        TEvMessageBusTracer::TEvTraceStatus *event = ev->Get();
        TAutoPtr<NMsgBusProxy::TBusMessageBusTraceStatus> response(new NMsgBusProxy::TBusMessageBusTraceStatus());
        response->Record.SetTraceActive(event->TraceActive);
        response->Record.SetPath(event->Path);
        this->SendReplyAndDie(response.Release(), ctx);
    }

    TActorId MakeServiceID(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return MakeMessageBusTraceServiceID();
    }

    NBus::TBusMessage* CreateErrorReply(NMsgBusProxy::EResponseStatus status, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponseStatus(status, "Service not found"));
        return response.Release();
    }

    void HandleTimeout(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponseStatus(NMsgBusProxy::MSTATUS_TIMEOUT, ""));
        this->SendReplyAndDie(response.Release(), ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponseStatus(NMsgBusProxy::MSTATUS_ERROR, "HandleUndelivered"));
        response->Record.SetErrorReason("Cannot deliver request to the service");
        this->SendReplyAndDie(response.Release(), ctx);
    }

    void StateFunc(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMessageBusTracer::TEvTraceStatus,  Handle);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class TMessageBusTracerStartTrace: public TMessageBusTraceSimpleActor<TMessageBusTracerStartTrace> {
    TString Path;
public:
    TMessageBusTracerStartTrace(NMsgBusProxy::TBusMessageContext &msg, const TString& path)
        : TMessageBusTraceSimpleActor<TMessageBusTracerStartTrace>(msg)
        , Path(path)
    {
    }

    TEvMessageBusTracer::TEvStartTrace* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return new TEvMessageBusTracer::TEvStartTrace(Path);
    }
};

IActor* CreateMessageBusTracerStartTrace(NMsgBusProxy::TBusMessageContext &msg, const TString& path) {
    return new TMessageBusTracerStartTrace(msg, path);
}

class TMessageBusTracerStopTrace: public TMessageBusTraceSimpleActor<TMessageBusTracerStopTrace> {
public:
    TMessageBusTracerStopTrace(NMsgBusProxy::TBusMessageContext &msg)
        : TMessageBusTraceSimpleActor<TMessageBusTracerStopTrace>(msg)
    {}

    TEvMessageBusTracer::TEvStopTrace* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return new TEvMessageBusTracer::TEvStopTrace();
    }
};

IActor* CreateMessageBusTracerStopTrace(NMsgBusProxy::TBusMessageContext &msg) {
    return new TMessageBusTracerStopTrace(msg);
}


}

namespace NMsgBusProxy {

IMessageBusServer* CreateMsgBusTracingServer(
    NBus::TBusMessageQueue *queue,
    const NBus::TBusServerSessionConfig &config,
    const TString &tracePath,
    ui32 bindPort
) {
    return new NMessageBusTracer::TMessageBusTracingServer(
        config,
        queue,
        tracePath,
        bindPort
    );
}

}
}
