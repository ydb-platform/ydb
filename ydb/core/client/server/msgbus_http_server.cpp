#include <library/cpp/messagebus/handler.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/public/lib/base/msgbus.h>
#include "msgbus_http_server.h"
#include "http_ping.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TBusHttpIdentity : public NBus::TBusIdentity {
public:
    TBusHttpIdentity() {
        BeginWork();
    }

    void FinishWork() {
        EndWork();
    }
};

class TBusHttpServerSession : public NBus::TBusServerSession {
public:
    TBusHttpServerSession(IMessageBusHttpServer* httpServer, NMonitoring::IMonHttpRequest& request,
                          const TProtocol& protocol, const NBus::TBusServerSessionConfig& config)
        : HttpServer(httpServer)
        , Request(request)
        , Protocol(protocol)
        , Config(config)
        , ReplySent(false)
    {}

    // dummy virtual handlers - just to make class constructable.
    virtual void GetInFlightBulk(::TArrayRef<const NBus::TNetAddr>, TArrayRef<size_t>) const override {}
    virtual void GetConnectSyscallsNumBulkForTest(::TArrayRef<const NBus::TNetAddr>, TArrayRef<size_t>) const override {}
    virtual int GetInFlight() const noexcept override { return 1; }
    virtual TString GetStatus(ui16) override { return TString(); }
    virtual NBus::TConnectionStatusMonRecord GetStatusProtobuf() override { return NBus::TConnectionStatusMonRecord(); }
    virtual NBus::NPrivate::TSessionDumpStatus GetStatusRecordInternal() override { return NBus::NPrivate::TSessionDumpStatus(); }
    virtual TString GetStatusSingleLine() override { return TString(); }
    virtual const NBus::TBusSessionConfig* GetConfig() const noexcept override { return &Config; }
    virtual const NBus::TBusProtocol* GetProto() const noexcept override { return &Protocol; }
    virtual NBus::TBusMessageQueue* GetQueue() const noexcept override { return nullptr; }
    virtual TString GetNameInternal() override { return TString(); }
    virtual void Shutdown() override {}
    virtual void PauseInput(bool) override {}
    virtual unsigned GetActualListenPort() override { return 0; }

    // the only methods used
    virtual NBus::EMessageStatus SendReply(const NBus::TBusIdentity&, NBus::TBusMessage *pRep) override {
        TAutoPtr<NBus::TBusBufferBase> response(static_cast<NBus::TBusBufferBase*>(pRep));
        IOutputStream& out(Request.Output());
        if (response->GetHeader()->Type == MTYPE_CLIENT_RESPONSE) {
            NKikimrClient::TResponse* pbResponse(static_cast<NKikimrClient::TResponse*>(response->GetRecord()));
            NMsgBusProxy::EResponseStatus status = static_cast<NMsgBusProxy::EResponseStatus>(pbResponse->GetStatus());
            switch (status) {
            case MSTATUS_OK:
                out << "HTTP/1.1 200 Ok\r\nContent-Type: application/json; charset=utf-8\r\nConnection: Close\r\n\r\n";
                HttpServer->Status200->Inc();
                break;
            case MSTATUS_TIMEOUT:
                out << "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\nConnection: Close\r\n\r\n";;
                HttpServer->Status504->Inc();
                break;
            case MSTATUS_NOTREADY:
            case MSTATUS_REJECTED:
                out << "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nRetry-After: 1\r\nConnection: Close\r\n\r\n";;
                HttpServer->Status503->Inc();
                break;
            case MSTATUS_INTERNALERROR:
                out << "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: Close\r\n\r\n";;
                HttpServer->Status500->Inc();
                break;
            default:
                // TODO: more status codes
                out << "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nConnection: Close\r\n\r\n";;
                HttpServer->Status400->Inc();
                break;
            }
        } else {
            out << NMonitoring::HTTPOKJSON;
            HttpServer->Status200->Inc();
        }
        if (response->GetRecord()->GetDescriptor()->name() == "TJSON") {
            NKikimrClient::TJSON* json(static_cast<NKikimrClient::TJSON*>(response->GetRecord()));
            const auto& jsonString(json->GetJSON());
            out << jsonString;
            *HttpServer->OutboundSize += jsonString.size();
        } else {
            out << response->GetRecord()->AsJSON();
            // TODO
            //*HttpServer->OutboundSize += ?
        }
        ReplySent = true;
        return NBus::MESSAGE_OK;
    }

    virtual NBus::EMessageStatus ForgetRequest(const NBus::TBusIdentity& ident) override {
        TBusHttpIdentity& httpIdent(const_cast<TBusHttpIdentity&>(static_cast<const TBusHttpIdentity&>(ident)));
        if (!ReplySent) {
            IOutputStream& out(Request.Output());
            out << NMonitoring::HTTPNOCONTENT;
        }
        httpIdent.FinishWork();
        DoneEvent.Signal();
        return NBus::MESSAGE_OK;
    }

    void Wait() {
        DoneEvent.WaitI();
    }

protected:
    IMessageBusHttpServer* HttpServer;
    NMonitoring::IMonHttpRequest& Request;
    const TProtocol& Protocol;
    const NBus::TBusServerSessionConfig& Config;
    TSystemEvent DoneEvent;
    bool ReplySent;
};

class TMessageBusHttpServer : public IMessageBusHttpServer {
public:
    TMessageBusHttpServer(TActorSystem* actorSystem, NBus::IBusServerHandler* handler, const TProtocol& protocol, const NBus::TBusServerSessionConfig& config);
    ~TMessageBusHttpServer();
    virtual void Output(NMonitoring::IMonHttpRequest& request) override;
    virtual void Shutdown() override;

protected:
   TActorSystem* ActorSystem;
    NBus::IBusServerHandler* Handler;
    const TProtocol& Protocol;
    NActors::TMon* Monitor;
    const NBus::TBusServerSessionConfig& Config;
};

TMessageBusHttpServer::TMessageBusHttpServer(TActorSystem* actorSystem, NBus::IBusServerHandler* handler, const TProtocol& protocol, const NBus::TBusServerSessionConfig& config)
    : IMessageBusHttpServer(protocol.GetService(), actorSystem->AppData<TAppData>()->Counters)
    , ActorSystem(actorSystem)
    , Handler(handler)
    , Protocol(protocol)
    , Monitor(actorSystem->AppData<TAppData>()->Mon)
    , Config(config)
{
    HttpGroup = GetServiceCounters(Counters, "proxy")->GetSubgroup("subsystem", "http");
    RequestsActive = HttpGroup->GetCounter("Requests/Active", false);
    RequestsCount = HttpGroup->GetCounter("Requests/Count", true);
    InboundSize = HttpGroup->GetCounter("Requests/InboundSize", true);
    OutboundSize = HttpGroup->GetCounter("Requests/OutboundSize", true);
    Status200 = HttpGroup->GetCounter("200", true);
    Status400 = HttpGroup->GetCounter("400", true);
    Status500 = HttpGroup->GetCounter("500", true);
    Status503 = HttpGroup->GetCounter("503", true);
    Status504 = HttpGroup->GetCounter("504", true);
    RequestTotalTimeHistogram = HttpGroup->GetHistogram("RequestTotalTimeMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));
    RequestPrepareTimeHistogram = HttpGroup->GetHistogram("RequestPrepareTimeMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));
    Monitor->Register(this);
}

TMessageBusHttpServer::~TMessageBusHttpServer() {
    Shutdown();
}

void TMessageBusHttpServer::Shutdown() {
    Handler = nullptr;
}

void TMessageBusHttpServer::Output(NMonitoring::IMonHttpRequest& request) {
    if (Handler != nullptr) {
        THPTimer startTime;
        TString pathInfo{request.GetPathInfo().substr(1)};
        TAutoPtr<NBus::TBusBufferBase> message = Protocol.NewMessage(pathInfo);
        if (message != nullptr) {
            RequestsCount->Inc();
            RequestsActive->Inc();
            TStringBuf postContent(request.GetPostContent());
            *InboundSize += postContent.size();
            const ::google::protobuf::Descriptor* msgDescriptor = message->GetRecord()->GetDescriptor();
            if (msgDescriptor->name() == "TJSON") {
                NKikimrClient::TJSON* json(static_cast<NKikimrClient::TJSON*>(message->GetRecord()));
                json->SetJSON(TString(postContent));
            } else {
                static NJson::TJsonReaderConfig readerConfig;
                static NProtobufJson::TJson2ProtoConfig protoConfig;

                TMemoryInput in(postContent);
                NJson::TJsonValue jsonValue;
                NJson::ReadJsonTree(&in, &readerConfig, &jsonValue, true);
                NProtobufJson::Json2Proto(jsonValue, *message->GetRecord(), protoConfig);
            }
            const ::google::protobuf::FieldDescriptor* fldDescriptor = msgDescriptor->FindFieldByName("SecurityToken");
            if (fldDescriptor != nullptr) {
                TStringBuf authorization = request.GetHeader("Authorization");
                if (!authorization.empty()) {
                    const ::google::protobuf::Reflection* reflection = message->GetRecord()->GetReflection();
                    reflection->SetString(
                                message->GetRecord(),
                                fldDescriptor,
                                TString(authorization));
                }
            }

            LOG_DEBUG_S(*ActorSystem, NActorsServices::HTTP, "HttpRequest "
                        << request.GetMethod()
                        << " "
                        << request.GetUri()
                        << " "
                        << message->GetRecord()->AsJSON());

            TBusHttpServerSession session(this, request, Protocol, Config);
            TBusHttpIdentity identity;
            NBus::TOnMessageContext onMessageContext(message.Release(), identity, &session);
            RequestPrepareTimeHistogram->Collect(startTime.Passed() * 1000/*ms*/);
            Handler->OnMessage(onMessageContext);
            session.Wait();
            RequestsActive->Dec();
        } else {
            IOutputStream& out(request.Output());
            out << NMonitoring::HTTPNOTFOUND;
        }
        RequestTotalTimeHistogram->Collect(startTime.Passed() * 1000/*ms*/);
    }
}

IMessageBusHttpServer* CreateMessageBusHttpServer(TActorSystem* actorSystem, NBus::IBusServerHandler* handler, const TProtocol& protocol, const NBus::TBusServerSessionConfig& config) {
    return new TMessageBusHttpServer(actorSystem, handler, protocol, config);
}

}
}
