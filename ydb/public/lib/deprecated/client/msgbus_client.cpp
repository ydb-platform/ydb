#include "msgbus_client.h"
#include <util/system/event.h>

namespace NKikimr {
namespace NMsgBusProxy {

void TMsgBusClientConfig::CrackAddress(const TString& address, TString& hostname, ui32& port) {
    size_t first_colon_pos = address.find(':');
    if (first_colon_pos != TString::npos) {
        size_t last_colon_pos = address.rfind(':');
        if (last_colon_pos == first_colon_pos) {
            // only one colon, simple case
            port = FromString<ui32>(address.substr(first_colon_pos + 1));
            hostname = address.substr(0, first_colon_pos);
        } else {
            // ipv6?
            size_t closing_bracket_pos = address.rfind(']');
            if (closing_bracket_pos == TString::npos || closing_bracket_pos > last_colon_pos) {
                // whole address is ipv6 host
                hostname = address;
            } else {
                port = FromString<ui32>(address.substr(last_colon_pos + 1));
                hostname = address.substr(0, last_colon_pos);
            }
            if (hostname.StartsWith('[') && hostname.EndsWith(']')) {
                hostname = hostname.substr(1, hostname.size() - 2);
            }
        }
    } else {
        hostname = address;
    }
}



struct TMessageCookie
{
    virtual void Signal(TAutoPtr<NBus::TBusMessage>& msg, NBus::EMessageStatus errorStatus, TAutoPtr<NBus::TBusMessage> reply) = 0;
    virtual ~TMessageCookie()
    {
    }
};

struct TSyncMessageCookie : public TMessageCookie {
    TAutoPtr<NBus::TBusMessage> Reply;
    NBus::EMessageStatus ErrorStatus = NBus::MESSAGE_UNKNOWN;
    TManualEvent Ev;

    TSyncMessageCookie()
        : Ev()
    {}

    void Wait() {
        Ev.Wait();
    }

    virtual void Signal(TAutoPtr<NBus::TBusMessage>& msg, NBus::EMessageStatus errorStatus, TAutoPtr<NBus::TBusMessage> reply) {
        Y_UNUSED(msg.Release());
        ErrorStatus = errorStatus;
        Reply = reply;
        Ev.Signal();
    }
};


template <typename CallbackType>
struct TAsyncMessageCookie : public TMessageCookie {
    CallbackType Callback;
    void* Data;

    explicit TAsyncMessageCookie(CallbackType callback, void* data)
        : Callback(callback)
        , Data(data)
    {}

    void Signal(TAutoPtr<NBus::TBusMessage>& msg, NBus::EMessageStatus errorStatus, TAutoPtr<NBus::TBusMessage> reply) override;
};

template <>
void TAsyncMessageCookie<TMsgBusClient::TOnCall>::Signal(TAutoPtr<NBus::TBusMessage>& msg, NBus::EMessageStatus errorStatus, TAutoPtr<NBus::TBusMessage> reply) {
    msg->Data = Data;
    Callback(errorStatus, reply);
    delete this; // we must cleanup cookie after use
}

template <>
void TAsyncMessageCookie<TMsgBusClient::TOnCallWithRequest>::Signal(TAutoPtr<NBus::TBusMessage>& msg, NBus::EMessageStatus errorStatus, TAutoPtr<NBus::TBusMessage> reply) {
    msg->Data = Data;
    Callback(errorStatus, msg, reply);
    delete this; // we must cleanup cookie after use
}


TMsgBusClientConfig::TMsgBusClientConfig()
    : Ip("localhost")
    , Port(NMsgBusProxy::TProtocol::DefaultPort)
    , UseCompression(false)
{}

void TMsgBusClientConfig::ConfigureLastGetopt(NLastGetopt::TOpts &opts, const TString& prefix) {
    BusSessionConfig.ConfigureLastGetopt(opts, prefix);
    BusQueueConfig.ConfigureLastGetopt(opts, prefix);
}

TMsgBusClient::TMsgBusClient(const TMsgBusClientConfig &config)
    : Config(config)
    , Protocol(NMsgBusProxy::TProtocol::DefaultPort)
{}

TMsgBusClient::~TMsgBusClient() {
    Shutdown();
}

void TMsgBusClient::Init() {
    NetAddr = new NBus::TNetAddr(Config.Ip, Config.Port); // could throw
    Bus = NBus::CreateMessageQueue(Config.BusQueueConfig);
    Session = NBus::TBusClientSession::Create(&Protocol, this, Config.BusSessionConfig, Bus);
}

void TMsgBusClient::Shutdown() {
    if (Bus) {
        if (Session) {
            Session->Shutdown();
        }

        Session.Drop();
        Bus.Drop();
    }
}

NBus::EMessageStatus TMsgBusClient::SyncCall(TAutoPtr<NBus::TBusMessage> msg, TAutoPtr<NBus::TBusMessage> &reply) {
    Y_ABORT_UNLESS(!msg->Data);
    TAutoPtr<TSyncMessageCookie> cookie(new TSyncMessageCookie());
    msg->Data = cookie.Get();

    // msgbus would recreate second TAutoPtr for our msg pointer (wut?!) Second copy terminates in OnRelease/OnError where we release it.
    NBus::EMessageStatus status = Session->SendMessage(msg.Get(), NetAddr.Get(), true);

    if (status == NBus::MESSAGE_OK) {
        cookie->Wait();
        reply = cookie->Reply;
        return cookie->ErrorStatus;
    } else {
        return status;
    }
}

NBus::EMessageStatus TMsgBusClient::AsyncCall(TAutoPtr<NBus::TBusMessage> msg, TOnCall callback) {
    TAutoPtr<TMessageCookie> cookie(new TAsyncMessageCookie<TOnCall>(callback, msg->Data));
    msg->Data = cookie.Get();

    if (Config.UseCompression) {
        msg->SetCompressed(true);
        msg->SetCompressedResponse(true);
    }

    NBus::EMessageStatus status = Session->SendMessage(msg.Get(), NetAddr.Get(), false);

    if (status == NBus::MESSAGE_OK) {
        // would be destructed in onresult/onerror
        Y_UNUSED(cookie.Release());
        Y_UNUSED(msg.Release());
    }

    return status;
}

NBus::EMessageStatus TMsgBusClient::AsyncCall(TAutoPtr<NBus::TBusMessage> msg, TOnCallWithRequest callback) {
    TAutoPtr<TMessageCookie> cookie(new TAsyncMessageCookie<TOnCallWithRequest>(callback, msg->Data));
    msg->Data = cookie.Get();

    if (Config.UseCompression) {
        msg->SetCompressed(true);
        msg->SetCompressedResponse(true);
    }

    NBus::EMessageStatus status = Session->SendMessage(msg.Get(), NetAddr.Get(), false);

    if (status == NBus::MESSAGE_OK) {
        // would be destructed in onresult/onerror
        Y_UNUSED(cookie.Release());
        Y_UNUSED(msg.Release());
    }

    return status;
}

void TMsgBusClient::OnResult(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status, TAutoPtr<NBus::TBusMessage> pReply) {
    static_cast<TMessageCookie*>(pMessage->Data)->Signal(pMessage, status, pReply);
}

void TMsgBusClient::OnReply(TAutoPtr<NBus::TBusMessage> pMessage, TAutoPtr<NBus::TBusMessage> pReply) {
    OnResult(pMessage, NBus::MESSAGE_OK, pReply);
}

void TMsgBusClient::OnError(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status) {
    if (status == NBus::MESSAGE_UNKNOWN) // timeouted request
        return;

    OnResult(pMessage, status, TAutoPtr<NBus::TBusMessage>());
}

const TMsgBusClientConfig& TMsgBusClient::GetConfig() {
    return Config;
}

EDataReqStatusExcerpt ExtractDataRequestStatus(const NKikimrClient::TResponse *record) {
    if (!record)
        return EDataReqStatusExcerpt::Unknown;

    switch (record->GetStatus()) {
    case MSTATUS_OK:
        return EDataReqStatusExcerpt::Complete;
    case MSTATUS_INPROGRESS:
        return EDataReqStatusExcerpt::InProgress;
    case MSTATUS_ERROR:
        return EDataReqStatusExcerpt::Error;
    case MSTATUS_TIMEOUT:
        return EDataReqStatusExcerpt::LostInSpaceAndTime;
    case MSTATUS_NOTREADY:
    case MSTATUS_REJECTED:
        return EDataReqStatusExcerpt::RejectedForNow;
    case MSTATUS_INTERNALERROR:
        return EDataReqStatusExcerpt::InternalError;
    default:
        return EDataReqStatusExcerpt::Unknown;
    }
}

}

void SetMsgBusDefaults(NBus::TBusSessionConfig& sessionConfig,
                              NBus::TBusQueueConfig& queueConfig) {
   size_t memorySize = NSystemInfo::TotalMemorySize();
   if (!memorySize) {
       memorySize = 1 << 30;
   }
   sessionConfig.MaxInFlightBySize =
           (memorySize / 20);
   sessionConfig.MaxInFlight =
           sessionConfig.MaxInFlightBySize / 1024;
   queueConfig.NumWorkers =
           ((NSystemInfo::CachedNumberOfCpus() - 1) / 4 + 1);
   sessionConfig.TotalTimeout = TDuration::Minutes(5).MilliSeconds();
   sessionConfig.ConnectTimeout = TDuration::Seconds(15).MilliSeconds();
}

}
