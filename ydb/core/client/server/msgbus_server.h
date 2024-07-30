#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/lib/base/defs.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include "msgbus_http_server.h"
#include "msgbus_server_pq_metacache.h"

namespace NMonitoring {
    class TBusNgMonPage;
}

namespace NKikimr {

namespace NGRpcProxy {
    class IRequestContext;
} // NGRpcProxy

namespace NMsgBusProxy {

class IMessageWatcher {
public:
    virtual ~IMessageWatcher() {}
    virtual void OnMessageDied(NBus::TBusKey id) = 0;
    virtual void OnMessageReplied(NBus::TBusKey id, NBus::TBusMessage *response) = 0;
};

class TBusMessageWatcher {
    IMessageWatcher *MessageWatcher;

protected:
    TBusMessageWatcher(IMessageWatcher *messageWatcher = nullptr) : MessageWatcher(messageWatcher) {}
    virtual ~TBusMessageWatcher() {}
    void NotifyForget();
    void NotifyReply(NBus::TBusMessage *response);
    void Swap(TBusMessageWatcher &watcher) { std::swap(MessageWatcher, watcher.MessageWatcher); }
    virtual NBus::TBusKey GetMessageId() = 0;
};

class TBusMessageContext;

class TMessageBusSessionIdentHolder {
    class TImpl;
    THolder<TImpl> Impl;

    class TImplMessageBus;
    class TImplGRpc;

    // to create session
    friend class TBusMessageContext;

protected:
    TMessageBusSessionIdentHolder();
    void InitSession(TBusMessageContext &msg);
    ui64 GetTotalTimeout() const;

public:
    TMessageBusSessionIdentHolder(TBusMessageContext &msg);
    ~TMessageBusSessionIdentHolder();
    void SendReply(NBus::TBusMessage *resp);
    void SendReplyMove(NBus::TBusMessageAutoPtr resp);
    TVector<TStringBuf> FindClientCert() const;

    template <typename U /* <: TBusMessage */>
    void SendReplyAutoPtr(TAutoPtr<U>& resp) { SendReplyMove(resp); }
};

class TBusMessageContext {
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

    class TImplMessageBus;
    class TImplGRpc;

public:
    TBusMessageContext();
    TBusMessageContext(const TBusMessageContext& other);
    TBusMessageContext(NBus::TOnMessageContext &messageContext, IMessageWatcher *messageWatcher = nullptr);
    TBusMessageContext(NGRpcProxy::IRequestContext *requestContext, int type);
    ~TBusMessageContext();

    TBusMessageContext& operator =(TBusMessageContext other);

    NBus::TBusMessage* GetMessage();
    NBus::TBusMessage* ReleaseMessage();
    void SendReplyMove(NBus::TBusMessageAutoPtr response);
    void Swap(TBusMessageContext& msg);
    TVector<TStringBuf> FindClientCert() const;

private:
    friend class TMessageBusSessionIdentHolder;
    THolder<TMessageBusSessionIdentHolder::TImpl> CreateSessionIdentHolder();
};

struct TEvBusProxy {
    enum EEv {
        EvRequest = EventSpaceBegin(TKikimrEvents::ES_PROXY_BUS),
        EvNavigate,
        EvFlatTxRequest,
        EvFlatDescribeRequest,
        EvPersQueue,
        EvInitRoot,
        EvChooseProxy,

        EvStreamIsReadyNotUsed = EvRequest + 512,
        EvStreamIsDeadNotUsed,

        EvEnd
    };

    template<EEv EvId>
    struct TEvMsgBusRequest : public TEventLocal<TEvMsgBusRequest<EvId>, EvId> {
        TBusMessageContext MsgContext;

        TEvMsgBusRequest(TBusMessageContext &msg) {
            MsgContext.Swap(msg);
        }
    };

    typedef TEvMsgBusRequest<EvRequest> TEvRequest;
    typedef TEvMsgBusRequest<EvNavigate> TEvNavigate;
    typedef TEvMsgBusRequest<EvFlatTxRequest> TEvFlatTxRequest;
    typedef TEvMsgBusRequest<EvFlatDescribeRequest> TEvFlatDescribeRequest;
    typedef TEvMsgBusRequest<EvPersQueue> TEvPersQueue;
    typedef TEvMsgBusRequest<EvChooseProxy> TEvChooseProxy;
    typedef TEvMsgBusRequest<EvInitRoot> TEvInitRoot;
};

class TMessageBusServer : public IMessageBusServer, public NBus::IBusServerHandler {
    const NBus::TBusServerSessionConfig &SessionConfig;
    NBus::TBusMessageQueuePtr BusQueue;
    NBus::TBusServerSessionPtr Session;
    TActorId Proxy;
    TActorId Monitor;
protected:
    TProtocol Protocol;
    TActorSystem *ActorSystem = nullptr;
    TIntrusivePtr<IMessageBusHttpServer> HttpServer;
public:
    TMessageBusServer(
        const NBus::TBusServerSessionConfig &sessionConfig,
        NBus::TBusMessageQueue *busQueue,
        ui32 bindPort
    );
    ~TMessageBusServer();

    void InitSession(TActorSystem *actorSystem, const TActorId &proxy);
    void ShutdownSession();

    void RegisterMonPage(NMonitoring::TBusNgMonPage *busMonPage);

protected:
    void OnMessage(NBus::TOnMessageContext &msg) override;
    void OnMessage(TBusMessageContext &msg);
    IActor* CreateMessageBusTraceService() override;
private:
    IActor* CreateProxy() override;
    void OnError(TAutoPtr<NBus::TBusMessage> message, NBus::EMessageStatus status) override;

    void GetTypes(TBusMessageContext &msg);
    void UnknownMessage(TBusMessageContext &msg);

    template<typename TEv>
    void ClientProxyRequest(TBusMessageContext &msg);

    typedef std::function<IActor* (TBusMessageContext &msg)> ActorCreationFunc;
    void ClientActorRequest(ActorCreationFunc func, TBusMessageContext &msg);
};

template <typename TProto>
void CopyProtobufsByFieldName(TProto& protoTo, const TProto& protoFrom) {
    using namespace ::google::protobuf;
    static_assert(std::is_base_of<Message, TProto>::value, "protoTo/protoFrom should be a subclass of protobuf::Message");
    protoTo.CopyFrom(protoFrom);
}

template <typename TProtoTo, typename TProtoFrom>
void CopyProtobufsByFieldName(TProtoTo& protoTo, const TProtoFrom& protoFrom) {
    using namespace ::google::protobuf;
    static_assert(std::is_base_of<Message, TProtoTo>::value, "protoTo should be a subclass of protobuf::Message");
    static_assert(std::is_base_of<Message, TProtoFrom>::value, "protoFrom should be a subclass of protobuf::Message");
    const Descriptor& descriptorTo = *protoTo.GetDescriptor();
    const Reflection& reflectionTo = *protoTo.GetReflection();
    const Reflection& reflectionFrom = *protoFrom.GetReflection();
    std::vector<const FieldDescriptor*> fields;
    protoTo.Clear();
    reflectionFrom.ListFields(protoFrom, &fields);
    for (const FieldDescriptor* fieldFrom : fields) {
        const auto& name = fieldFrom->name();
        const FieldDescriptor* fieldTo = descriptorTo.FindFieldByName(name);
        Y_ABORT_UNLESS(fieldTo != nullptr, "name=%s", name.c_str());
        if (fieldTo != nullptr) {
            FieldDescriptor::CppType type = fieldFrom->cpp_type();
            if (fieldFrom->is_repeated()) {
                int size = reflectionFrom.FieldSize(protoFrom, fieldFrom);

                for (int i = 0; i < size; ++i) {
                    switch (type) {
                    case FieldDescriptor::CPPTYPE_INT32:
                        reflectionTo.AddInt32(&protoTo, fieldTo, reflectionFrom.GetRepeatedInt32(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_INT64:
                        reflectionTo.AddInt64(&protoTo, fieldTo, reflectionFrom.GetRepeatedInt64(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_UINT32:
                        reflectionTo.AddUInt32(&protoTo, fieldTo, reflectionFrom.GetRepeatedUInt32(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_UINT64:
                        reflectionTo.AddUInt64(&protoTo, fieldTo, reflectionFrom.GetRepeatedUInt64(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_DOUBLE:
                        reflectionTo.AddDouble(&protoTo, fieldTo, reflectionFrom.GetRepeatedDouble(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_FLOAT:
                        reflectionTo.AddFloat(&protoTo, fieldTo, reflectionFrom.GetRepeatedFloat(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_BOOL:
                        reflectionTo.AddBool(&protoTo, fieldTo, reflectionFrom.GetRepeatedBool(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_ENUM:
                        reflectionTo.AddEnum(&protoTo, fieldTo, reflectionFrom.GetRepeatedEnum(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_STRING:
                        reflectionTo.AddString(&protoTo, fieldTo, reflectionFrom.GetRepeatedString(protoFrom, fieldFrom, i));
                        break;
                    case FieldDescriptor::CPPTYPE_MESSAGE:
                        reflectionTo.AddMessage(&protoTo, fieldTo)->CopyFrom(reflectionFrom.GetRepeatedMessage(protoFrom, fieldFrom, i));
                        break;
                    }
                }
            } else {
                switch (type) {
                case FieldDescriptor::CPPTYPE_INT32:
                    reflectionTo.SetInt32(&protoTo, fieldTo, reflectionFrom.GetInt32(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_INT64:
                    reflectionTo.SetInt64(&protoTo, fieldTo, reflectionFrom.GetInt64(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_UINT32:
                    reflectionTo.SetUInt32(&protoTo, fieldTo, reflectionFrom.GetUInt32(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_UINT64:
                    reflectionTo.SetUInt64(&protoTo, fieldTo, reflectionFrom.GetUInt64(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                    reflectionTo.SetDouble(&protoTo, fieldTo, reflectionFrom.GetDouble(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                    reflectionTo.SetFloat(&protoTo, fieldTo, reflectionFrom.GetFloat(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_BOOL:
                    reflectionTo.SetBool(&protoTo, fieldTo, reflectionFrom.GetBool(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_ENUM:
                    reflectionTo.SetEnum(&protoTo, fieldTo, reflectionFrom.GetEnum(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_STRING:
                    reflectionTo.SetString(&protoTo, fieldTo, reflectionFrom.GetString(protoFrom, fieldFrom));
                    break;
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    reflectionTo.MutableMessage(&protoTo, fieldTo)->CopyFrom(reflectionFrom.GetMessage(protoFrom, fieldFrom));
                    break;
                }
            }
        }
    }
}


IActor* CreateMessageBusServerProxy(TMessageBusServer *server);

//IActor* CreateMessageBusDatashardSetConfig(TBusMessageContext &msg);
IActor* CreateMessageBusTabletCountersRequest(TBusMessageContext &msg);
IActor* CreateMessageBusLocalMKQL(TBusMessageContext &msg);
IActor* CreateMessageBusLocalSchemeTx(TBusMessageContext &msg);
IActor* CreateMessageBusSchemeInitRoot(TBusMessageContext &msg);
IActor* CreateMessageBusGetTypes(TBusMessageContext &msg);
IActor* CreateMessageBusHiveCreateTablet(TBusMessageContext &msg);
IActor* CreateMessageBusLocalEnumerateTablets(TBusMessageContext &msg);
IActor* CreateMessageBusKeyValue(TBusMessageContext &msg);
IActor* CreateMessageBusPersQueue(TBusMessageContext &msg);
IActor* CreateMessageBusChooseProxy(TBusMessageContext &msg);
IActor* CreateMessageBusTabletStateRequest(TBusMessageContext &msg);
IActor* CreateMessageBusTabletKillRequest(TBusMessageContext &msg);
IActor* CreateMessageBusSchemeOperationStatus(TBusMessageContext &msg);
IActor* CreateMessageBusBlobStorageLoadRequest(TBusMessageContext &msg);
IActor* CreateMessageBusBlobStorageGetRequest(TBusMessageContext &msg);
IActor* CreateMessageBusHiveLookupTablet(TBusMessageContext &msg);
IActor* CreateMessageBusBlobStorageConfig(TBusMessageContext &msg);
IActor* CreateMessageBusDrainNode(TBusMessageContext &msg);
IActor* CreateMessageBusFillNode(TBusMessageContext &msg);
IActor* CreateMessageBusResolveNode(TBusMessageContext &msg);
IActor* CreateMessageBusRegisterNode(TBusMessageContext &msg);
IActor* CreateMessageBusCmsRequest(TBusMessageContext &msg);
IActor* CreateMessageBusSqsRequest(TBusMessageContext &msg);
IActor* CreateMessageBusInterconnectDebug(TBusMessageContext& msg);
IActor* CreateMessageBusConsoleRequest(TBusMessageContext &msg);
IActor* CreateMessageBusTestShardControl(TBusMessageContext &msg);

TBusResponse* ProposeTransactionStatusToResponse(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus &result);

}
}
