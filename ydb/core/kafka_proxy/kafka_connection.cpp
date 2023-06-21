#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/raw_socket/sock_config.h>

#include "kafka_connection.h"
#include "kafka_messages.h"
#include "kafka_log_impl.h"

#include <strstream>
#include <sstream>
#include <iosfwd>

namespace NKafka {

using namespace NActors;

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
        sb << "0x" << Hex(c >> 4) << Hex(c & 0x0F);
    }
    KAFKA_LOG_T("Packet " << marker << ": " << sb);
}

class TKafkaConnection: public TActorBootstrapped<TKafkaConnection>, public TNetworkConfig {
public:
    using TBase = TActorBootstrapped<TKafkaConnection>;

    // TODO check standard ip packet MTU. On my desktop it is 1500 on eth and wlp interfaces. It is 1300 on the tun interface. It
    // is 1500 and 8950 on the dev server interfaces.
    static constexpr size_t BufferSize = 8950;
    static constexpr size_t MinDirectSize = 256;

    struct Msg {
        size_t Size = 0;
        TKafkaInt32 ExpectedSize = 0;
        TRequestHeaderData Header;
        std::unique_ptr<TMessage> Message;
    };

    static constexpr TDuration InactivityTimeout = TDuration::Minutes(10);
    TEvPollerReady* InactivityEvent = nullptr;
    TPollerToken::TPtr PollerToken;

    TIntrusivePtr<TSocketDescriptor> Socket;
    TSocketAddressType Address;
    THPTimer InactivityTimer;

    bool IsAuthRequired = true;
    bool IsSslSupported = true;

    bool ConnectionEstablished = false;
    bool CloseConnection = false;

    TBuffer Buffer;
    size_t Length;
    size_t Position;

    Msg Request;
    bool HeaderSizeWasRead;
    bool HeaderWasRead;
    bool MessageSizeWasRead;
    bool MessageWasRead;
    std::unique_ptr<TReadContext> Ctx;

    TReadDemand Demand;

    TKafkaConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address)
        : Socket(std::move(socket))
        , Address(address)
        , Length(0)
        , Position(0)
        , HeaderSizeWasRead(false)
        , HeaderWasRead(false)
        , MessageSizeWasRead(false)
        , MessageWasRead(false)
        , Demand(NoDemand) {
        SetNonBlock();
        IsSslSupported = IsSslSupported && Socket->IsSslSupported();
        Buffer.Resize(BufferSize);
    }

    void Bootstrap() {
        Become(&TKafkaConnection::StateAccepting);
        Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        KAFKA_LOG_D("incoming connection opened");
        OnAccept();
    }

    void PassAway() override {
        if (ConnectionEstablished) {
            ConnectionEstablished = false;
        }
        Shutdown();
        TBase::PassAway();
    }

protected:
    void SetNonBlock() noexcept {
        Socket->SetNonBlock();
    }

    void Shutdown() {
        if (Socket) {
            Socket->Shutdown();
        }
    }

    ssize_t SocketSend(const void* data, size_t size) {
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
        return TStringBuilder() << "(#" << GetRawSocket() << "," << Address->ToString() << ") ";
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
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleAccepting);
            hFunc(TEvPollerRegisterResult, HandleAccepting);
        }
    }

    void HandleMessage(TRequestHeaderData* header, TApiVersionsRequestData* /*message*/) {
        TApiVersionsResponseData response;
        response.ApiKeys.resize(4);

        response.ApiKeys[0].ApiKey = PRODUCE;
        response.ApiKeys[0].MinVersion = TProduceRequestData::MessageMeta::PresentVersionMin;
        response.ApiKeys[0].MaxVersion = TProduceRequestData::MessageMeta::PresentVersionMax;

        response.ApiKeys[1].ApiKey = API_VERSIONS;
        response.ApiKeys[1].MinVersion = TApiVersionsRequestData::MessageMeta::PresentVersionMin;
        response.ApiKeys[1].MaxVersion = TApiVersionsRequestData::MessageMeta::PresentVersionMax;

        response.ApiKeys[2].ApiKey = METADATA;
        response.ApiKeys[2].MinVersion = TMetadataRequestData::MessageMeta::PresentVersionMin;
        response.ApiKeys[2].MaxVersion = TMetadataRequestData::MessageMeta::PresentVersionMax;

        response.ApiKeys[3].ApiKey = INIT_PRODUCER_ID;
        response.ApiKeys[3].MinVersion = TInitProducerIdRequestData::MessageMeta::PresentVersionMin;
        response.ApiKeys[3].MaxVersion = TInitProducerIdRequestData::MessageMeta::PresentVersionMax;

        Reply(header, &response);
    }

    void HandleMessage(TRequestHeaderData* header, TProduceRequestData* message) {
        TProduceResponseData response;
        response.Responses.resize(message->TopicData.size());
        int i = 0;
        for (auto& data : message->TopicData) {
            response.Responses[i].Name = data.Name;
            response.Responses[i].PartitionResponses.resize(data.PartitionData.size());
            int j = 0;
            for (auto& p : data.PartitionData) {
                response.Responses[i].PartitionResponses[j].Index = p.Index;
                response.Responses[i].PartitionResponses[j].BaseOffset = 40;

                ++j;
            }
            ++i;
        }

        Reply(header, &response);
    }

    void HandleMessage(TRequestHeaderData* header, TInitProducerIdRequestData* /*message*/) {
        TInitProducerIdResponseData response;
        response.ProducerEpoch = 1;
        response.ProducerId = 1;
        response.ErrorCode = 0;
        response.ThrottleTimeMs = 0;

        Reply(header, &response);
    }

    void HandleMessage(TRequestHeaderData* header, TMetadataRequestData* /*message*/) {
        TMetadataResponseData response;
        response.ThrottleTimeMs = 0;
        response.ClusterId = "cluster-ahjgk";
        response.ControllerId = 1;

        response.Brokers.resize(1);
        response.Brokers[0].NodeId = 1;
        response.Brokers[0].Host = "lbk-dev-02.search.yandex.net";
        response.Brokers[0].Port = 9092;

        response.Topics.resize(1);
        response.Topics[0].TopicId = TKafkaUuid(0, 1);
        response.Topics[0].Name = "topic-1";
        response.Topics[0].Partitions.resize(1);
        response.Topics[0].Partitions[0].LeaderId = 1; // response.Brokers[0].NodeId
        response.Topics[0].Partitions[0].ReplicaNodes.resize(1);
        response.Topics[0].Partitions[0].ReplicaNodes[0] = 1;
        response.Topics[0].Partitions[0].IsrNodes.resize(1);
        response.Topics[0].Partitions[0].IsrNodes[0] = 1;

        Reply(header, &response);
    }

    void ProcessRequest() {
        KAFKA_LOG_D("process message: ApiKey=" << Request.Header.RequestApiKey << ", ExpectedSize=" << Request.ExpectedSize
                                               << ", Size=" << Request.Size);
        switch (Request.Header.RequestApiKey) {
            case PRODUCE:
                HandleMessage(&Request.Header, dynamic_cast<TProduceRequestData*>(Request.Message.get()));
                return;

            case API_VERSIONS:
                HandleMessage(&Request.Header, dynamic_cast<TApiVersionsRequestData*>(Request.Message.get()));
                return;

            case INIT_PRODUCER_ID:
                HandleMessage(&Request.Header, dynamic_cast<TInitProducerIdRequestData*>(Request.Message.get()));
                return;

            case METADATA:
                HandleMessage(&Request.Header, dynamic_cast<TMetadataRequestData*>(Request.Message.get()));
                return;

            default:
                KAFKA_LOG_ERROR("Unsupported message: ApiKey=" << Request.Header.RequestApiKey);
        }
    }

    void Reply(TRequestHeaderData* header, TApiMessage* reply) {
        // TODO improve allocation
        TKafkaVersion headerVersion = ResponseHeaderVersion(header->RequestApiKey, header->RequestApiVersion);
        TKafkaVersion version = header->RequestApiVersion;

        TResponseHeaderData responseHeader;
        responseHeader.CorrelationId = header->CorrelationId;

        TKafkaInt32 size = responseHeader.Size(headerVersion) + reply->Size(version);

        std::stringstream sb;
        TKafkaWritable writable(sb);
        writable << size;
        responseHeader.Write(writable, headerVersion);
        reply->Write(writable, version);

        TBuffer b;
        b.Reserve(size + sizeof(size));
        sb.read(b.data(), size + sizeof(size));

        Print("sent", b, size + sizeof(size));

        SocketSend(b.Data(), size + sizeof(size));
    }

    void DoRead() {
        for (;;) {
            while (Demand) {
                ssize_t received = 0;
                if (Position < Length) {
                    KAFKA_LOG_T("Read from buffer: Position=" << Position << ", Length=" << Length
                                                              << ", Demand=" << Demand.GetLength());
                    received = std::min(Demand.Length, Length - Position);
                    if (!Demand.Skip()) {
                        memcpy(Demand.Buffer, Buffer.Data() + Position, received);
                    }
                    Position += received;
                } else if (!Demand.Skip() && Demand.Length >= MinDirectSize) {
                    ssize_t res = SocketReceive(Demand.Buffer, Demand.GetLength());
                    if (-res == EAGAIN || -res == EWOULDBLOCK) {
                        return;
                    } else if (-res == EINTR) {
                        continue;
                    } else if (!res) {
                        KAFKA_LOG_ERROR("connection closed");
                        return PassAway();
                    } else if (res < 0) {
                        KAFKA_LOG_ERROR("connection closed - error in recv: " << strerror(-res));
                        return PassAway();
                    }
                    received = res;
                    if (!received) {
                        return;
                    }
                } else {
                    Position = 0;
                    Length = 0;
                    ssize_t res = SocketReceive(Buffer.Data(), BufferSize);
                    if (-res == EAGAIN || -res == EWOULDBLOCK) {
                        return;
                    } else if (-res == EINTR) {
                        continue;
                    } else if (!res) {
                        KAFKA_LOG_ERROR("connection closed");
                        return PassAway();
                    } else if (res < 0) {
                        KAFKA_LOG_ERROR("connection closed - error in recv: " << strerror(-res));
                        return PassAway();
                    }
                    Length = res;
                    Print("received", Buffer, Length);
                    if (!Length) {
                        return;
                    }

                    continue;
                }

                Request.Size += received;
                Demand.Buffer += received;
                Demand.Length -= received;
            }
            if (Ctx) {
                Demand = Ctx->Next();
            }
            if (!Demand) {
                if (MessageWasRead) {
                    HeaderSizeWasRead = false;
                    MessageWasRead = false;
                    HeaderWasRead = false;

                    ProcessRequest();

                    Request = Msg();
                    Ctx = nullptr;
                }
                if (!HeaderSizeWasRead) {
                    Demand = TReadDemand((char*)&(Request.ExpectedSize), sizeof(Request.ExpectedSize));
                    HeaderSizeWasRead = true;
                    Ctx = nullptr;
                } else if (!HeaderWasRead) {
                    NPrivate::NormalizeNumber(Request.ExpectedSize);

                    KAFKA_LOG_T("start read new message. ExpectedSize=" << Request.ExpectedSize);

                    Ctx = Request.Header.CreateReadContext(2);

                    HeaderWasRead = true;
                } else {
                    KAFKA_LOG_T("received header. ApiKey=" << Request.Header.RequestApiKey
                                                           << ", Version=" << Request.Header.RequestApiVersion);

                    i16 apiKey = Request.Header.RequestApiKey;
                    TKafkaVersion version = Request.Header.RequestApiVersion;

                    Request.Message = CreateRequest(apiKey);
                    Ctx = Request.Message->CreateReadContext(version);

                    MessageWasRead = true;
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
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleConnected);
            hFunc(TEvPollerRegisterResult, HandleConnected);
        }
    }
};

NActors::IActor* CreateKafkaConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address) {
    return new TKafkaConnection(std::move(socket), std::move(address));
}

} // namespace NKafka