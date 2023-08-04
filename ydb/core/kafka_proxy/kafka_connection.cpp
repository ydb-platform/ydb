#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/util/address_classifier.h>

#include "kafka_connection.h"
#include "kafka_events.h"
#include "kafka_messages.h"
#include "kafka_produce_actor.h"
#include "kafka_metadata_actor.h"
#include "kafka_log_impl.h"

#include <strstream>
#include <sstream>
#include <iosfwd>

namespace NKafka {

using namespace NActors;
using namespace NKikimr;

char Hex(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

void Print(const TString& marker, TBuffer& buffer, ssize_t length) {
    TStringBuilder sb;
    for (ssize_t i = 0; i < length; ++i) {
        char c = buffer.Data()[i];
        if (i > 0) {
            sb << ", ";
        }
        sb << "0x" << Hex((c & 0xF0) >> 4) << Hex(c & 0x0F);
    }
    KAFKA_LOG_ERROR("Packet " << marker << ": " << sb);
}

TApiVersionsResponseData GetApiVersions() {
    TApiVersionsResponseData response;
    response.ApiKeys.resize(4);

    response.ApiKeys[0].ApiKey = PRODUCE;
    response.ApiKeys[0].MinVersion = 3; // From version 3 record batch format is 2. Supported only 2 batch format.
    response.ApiKeys[0].MaxVersion = TProduceRequestData::MessageMeta::PresentVersions.Max;

    response.ApiKeys[1].ApiKey = API_VERSIONS;
    response.ApiKeys[1].MinVersion = TApiVersionsRequestData::MessageMeta::PresentVersions.Min;
    response.ApiKeys[1].MaxVersion = TApiVersionsRequestData::MessageMeta::PresentVersions.Max;

    response.ApiKeys[2].ApiKey = METADATA;
    response.ApiKeys[2].MinVersion = TMetadataRequestData::MessageMeta::PresentVersions.Min;
    response.ApiKeys[2].MaxVersion = TMetadataRequestData::MessageMeta::PresentVersions.Max;

    response.ApiKeys[3].ApiKey = INIT_PRODUCER_ID;
    response.ApiKeys[3].MinVersion = TInitProducerIdRequestData::MessageMeta::PresentVersions.Min;
    response.ApiKeys[3].MaxVersion = TInitProducerIdRequestData::MessageMeta::PresentVersions.Max;

    return response;
}

static const TApiVersionsResponseData KAFKA_API_VERSIONS = GetApiVersions();

class TKafkaConnection: public TActorBootstrapped<TKafkaConnection>, public TNetworkConfig {
public:
    using TBase = TActorBootstrapped<TKafkaConnection>;

    struct Msg {
        size_t Size = 0;
        TKafkaInt32 ExpectedSize = 0;
        TBuffer Buffer;
        TRequestHeaderData Header;
        std::unique_ptr<TMessage> Message;
    };

    static constexpr TDuration InactivityTimeout = TDuration::Minutes(10);
    TEvPollerReady* InactivityEvent = nullptr;
    TPollerToken::TPtr PollerToken;

    TIntrusivePtr<TSocketDescriptor> Socket;
    TSocketAddressType Address;
    const NKikimrConfig::TKafkaProxyConfig& Config;

    THPTimer InactivityTimer;

    bool IsAuthRequired = true;
    bool IsSslSupported = true;

    bool ConnectionEstablished = false;
    bool CloseConnection = false;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier;
    TString ClientDC;

    Msg Request;

    enum EReadSteps { SIZE_READ, SIZE_PREPARE, INFLIGTH_CHECK, MESSAGE_READ, MESSAGE_PROCESS };
    EReadSteps Step;

    TReadDemand Demand;

    size_t InflightSize;

    TActorId ProduceActorId;

    std::unordered_map<ui64, Msg> PendingRequests;

    TKafkaConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address,
                     const NKikimrConfig::TKafkaProxyConfig& config)
        : Socket(std::move(socket))
        , Address(address)
        , Config(config)
        , Step(SIZE_READ)
        , Demand(NoDemand)
        , InflightSize(0) {
        SetNonBlock();
        IsSslSupported = IsSslSupported && Socket->IsSslSupported();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TKafkaConnection::StateAccepting);
        Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        KAFKA_LOG_I("incoming connection opened " << Address);

        ProduceActorId = ctx.RegisterWithSameMailbox(new TKafkaProduceActor(SelfId(), ClientDC));

        OnAccept();
    }

    void PassAway() override {
        KAFKA_LOG_D("PassAway");

        if (ConnectionEstablished) {
            ConnectionEstablished = false;
        }
        if (ProduceActorId) {
            Send(ProduceActorId, new TEvents::TEvPoison());
        }
        Shutdown();
        TBase::PassAway();
    }

protected:
    void LogEvent(IEventHandle& ev) {
        KAFKA_LOG_T("Event: " << ev.GetTypeName());
    }

    void SetNonBlock() noexcept {
        Socket->SetNonBlock();
    }

    void Shutdown() {
        KAFKA_LOG_D("Shutdown");

        if (Socket) {
            Socket->Shutdown();
        }
    }

    ssize_t SocketSend(const void* data, size_t size) {
        KAFKA_LOG_T("SocketSend Size=" << size);
        return Socket->Send(data, size);
    }

    ssize_t SocketReceive(void* data, size_t size) {
        return Socket->Receive(data, size);
    }

    void RequestPoller() {
        Socket->RequestPoller(PollerToken);
    }

    SOCKET GetRawSocket() const {
        return Socket->GetRawSocket();
    }

    TString LogPrefix() const {
        TStringBuilder sb;
        sb << "TKafkaConnection " << SelfId() << "(#" << GetRawSocket() << "," << Address->ToString() << ") State: ";
        auto stateFunc = CurrentStateFunc();
        if (stateFunc == &TKafkaConnection::StateConnected) {
            sb << "Connected ";
        } else if (stateFunc == &TKafkaConnection::StateAccepting) {
            sb << "Accepting ";
        } else {
            sb << "Unknown ";
        }
        return sb;
    }

    void OnAccept() {
        InactivityTimer.Reset();
        TBase::Become(&TKafkaConnection::StateConnected);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    void HandleAccepting(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        OnAccept();
    }

    void HandleAccepting(NActors::TEvPollerReady::TPtr) {
        OnAccept();
    }

    STATEFN(StateAccepting) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleAccepting);
            hFunc(TEvPollerRegisterResult, HandleAccepting);
            hFunc(TEvKafka::TEvResponse, Handle);
            default:
                KAFKA_LOG_ERROR("TKafkaConnection: Unexpected " << ev.Get()->GetTypeName());
        }
    }

    void HandleMessage(TRequestHeaderData* header, TApiVersionsRequestData* /*message*/, size_t messageSize) {
        Reply(header, &KAFKA_API_VERSIONS);

        InflightSize -= messageSize;
    }

    void HandleMessage(const TRequestHeaderData* header, const TProduceRequestData* message, size_t /*messageSize*/) {
        PendingRequests[header->CorrelationId] = std::move(Request);
        Send(ProduceActorId, new TEvKafka::TEvProduceRequest(header->CorrelationId, message));
    }

    void Handle(TEvKafka::TEvResponse::TPtr response) {
        auto r = response->Get();
        Reply(r->Cookie, r->Response.get());
    }

    void HandleMessage(const TRequestHeaderData* header, const TInitProducerIdRequestData* /*message*/, size_t messageSize) {
        TInitProducerIdResponseData response;
        response.ProducerEpoch = 1;
        response.ProducerId = 1;
        response.ErrorCode = 0;
        response.ThrottleTimeMs = 0;

        Reply(header, &response);

        InflightSize -= messageSize;
    }

    void HandleMessage(TRequestHeaderData* header, TMetadataRequestData* message, size_t /*messageSize*/) {
        PendingRequests[header->CorrelationId] = std::move(Request);
        Register(new TKafkaMetadataActor(header->CorrelationId, message, SelfId()));
    }

    void ProcessRequest() {
        KAFKA_LOG_D("process message: ApiKey=" << Request.Header.RequestApiKey << ", ExpectedSize=" << Request.ExpectedSize
                                               << ", Size=" << Request.Size);
        switch (Request.Header.RequestApiKey) {
            case PRODUCE:
                HandleMessage(&Request.Header, dynamic_cast<TProduceRequestData*>(Request.Message.get()), Request.ExpectedSize);
                return;

            case API_VERSIONS:
                HandleMessage(&Request.Header, dynamic_cast<TApiVersionsRequestData*>(Request.Message.get()), Request.ExpectedSize);
                return;

            case INIT_PRODUCER_ID:
                HandleMessage(&Request.Header, dynamic_cast<TInitProducerIdRequestData*>(Request.Message.get()), Request.ExpectedSize);
                return;

            case METADATA:
                HandleMessage(&Request.Header, dynamic_cast<TMetadataRequestData*>(Request.Message.get()), Request.ExpectedSize);
                return;

            default:
                KAFKA_LOG_ERROR("Unsupported message: ApiKey=" << Request.Header.RequestApiKey);
        }
    }

    void Reply(const ui64 cookie, const TApiMessage* response) {
        auto it = PendingRequests.find(cookie);
        if (it == PendingRequests.end()) {
            KAFKA_LOG_ERROR("Unexpected cookie " << cookie);
            return;
        }

        auto& request = it->second;
        Reply(&request.Header, response);

        InflightSize -= request.ExpectedSize;

        PendingRequests.erase(it);

        DoRead();
    }

    void Reply(const TRequestHeaderData* header, const TApiMessage* reply) {
        TKafkaVersion headerVersion = ResponseHeaderVersion(header->RequestApiKey, header->RequestApiVersion);
        TKafkaVersion version = header->RequestApiVersion;

        TResponseHeaderData responseHeader;
        responseHeader.CorrelationId = header->CorrelationId;

        TKafkaInt32 size = responseHeader.Size(headerVersion) + reply->Size(version);

        TBufferedWriter buffer(Socket.Get(), Config.GetPacketSize());
        TKafkaWritable writable(buffer);

        writable << size;
        responseHeader.Write(writable, headerVersion);
        reply->Write(writable, version);

        buffer.flush();

        KAFKA_LOG_D("Sent reply: ApiKey=" << header->RequestApiKey << ", Version=" << version << ", Correlation=" << responseHeader.CorrelationId <<  ", Size=" << size);
    }

    void DoRead() {
        KAFKA_LOG_T("DoRead: Demand=" << Demand.Length << ", Step=" << static_cast<i32>(Step));

        for (;;) {
            while (Demand) {
                ssize_t received = 0;
                ssize_t res = SocketReceive(Demand.Buffer, Demand.GetLength());
                if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    return;
                } else if (-res == EINTR) {
                    continue;
                } else if (!res) {
                    KAFKA_LOG_I("connection closed");
                    return PassAway();
                } else if (res < 0) {
                    KAFKA_LOG_I("connection closed - error in recv: " << strerror(-res));
                    return PassAway();
                }
                received = res;

                Request.Size += received;
                Demand.Buffer += received;
                Demand.Length -= received;
            }
            if (!Demand) {
                switch (Step) {
                    case SIZE_READ:
                        Demand = TReadDemand((char*)&(Request.ExpectedSize), sizeof(Request.ExpectedSize));
                        Step = SIZE_PREPARE;
                        break;

                    case SIZE_PREPARE:
                        NormalizeNumber(Request.ExpectedSize);
                        if ((ui64)Request.ExpectedSize > Config.GetMaxMessageSize()) {
                            KAFKA_LOG_ERROR("message is big. Size: " << Request.ExpectedSize);
                            return PassAway();
                        }
                        Step = INFLIGTH_CHECK;

                    case INFLIGTH_CHECK:
                        if (InflightSize + Request.ExpectedSize > Config.GetMaxInflightSize()) {
                            return;
                        }
                        InflightSize += Request.ExpectedSize;
                        Step = MESSAGE_READ;

                    case MESSAGE_READ:
                        KAFKA_LOG_T("start read new message. ExpectedSize=" << Request.ExpectedSize);

                        Request.Buffer.Resize(Request.ExpectedSize);
                        Demand = TReadDemand(Request.Buffer.Data(), Request.ExpectedSize);

                        Step = MESSAGE_PROCESS;
                        break;

                    case MESSAGE_PROCESS:
                        TKafkaInt16 apiKey = *(TKafkaInt16*)Request.Buffer.Data();
                        TKafkaVersion apiVersion = *(TKafkaVersion*)(Request.Buffer.Data() + sizeof(TKafkaInt16));

                        NormalizeNumber(apiKey);
                        NormalizeNumber(apiVersion);

                        KAFKA_LOG_D("received message. ApiKey=" << Request.Header.RequestApiKey
                                                                << ", Version=" << Request.Header.RequestApiVersion);

                        // Print("received", Request.Buffer, Request.ExpectedSize);

                        TKafkaReadable readable(Request.Buffer);

                        Request.Message = CreateRequest(apiKey);
                        try {
                            Request.Header.Read(readable, RequestHeaderVersion(apiKey, apiVersion));
                            Request.Message->Read(readable, apiVersion);
                        } catch(const yexception& e) {
                            KAFKA_LOG_ERROR("error on processing message: ApiKey=" << Request.Header.RequestApiKey
                                                                << ", Version=" << Request.Header.RequestApiVersion 
                                                                << ", Error=" <<  e.what());
                            return PassAway();                     
                        }

                        ProcessRequest();

                        Step = SIZE_READ;
                        break;
                }
            }
        }
    }

    void HandleConnected(TEvPollerReady::TPtr event) {
        if (event->Get()->Read) {
            DoRead();

            if (event->Get() == InactivityEvent) {
                const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
                if (passed >= InactivityTimeout) {
                    KAFKA_LOG_D("connection closed by inactivity timeout");
                    return PassAway(); // timeout
                } else {
                    Schedule(InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
                }
            }
        }
        if (event->Get()->Write) {
            if (!FlushOutput()) {
                return;
            }
        }
        RequestPoller();
    }

    bool FlushOutput() {
        return true; // TODO
    }

    void HandleConnected(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
    }

    STATEFN(StateConnected) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleConnected);
            hFunc(TEvPollerRegisterResult, HandleConnected);
            hFunc(TEvKafka::TEvResponse, Handle);
            default:
                KAFKA_LOG_ERROR("TKafkaConnection: Unexpected " << ev.Get()->GetTypeName());
        }
    }
};

NActors::IActor* CreateKafkaConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address,
                                       const NKikimrConfig::TKafkaProxyConfig& config) {
    return new TKafkaConnection(std::move(socket), std::move(address), config);
}

} // namespace NKafka
