#pragma once

#include "msgbus_client_config.h"

#include <ydb/public/lib/base/defs.h>
#include <ydb/public/lib/base/msgbus.h>

#include <functional>
#include <util/system/info.h>
#include <library/cpp/messagebus/ybus.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TMsgBusClient : NBus::IBusClientHandler {
    TMsgBusClientConfig Config;
    TProtocol Protocol;
    TAutoPtr<NBus::TNetAddr> NetAddr;
    NBus::TBusMessageQueuePtr Bus;
    NBus::TBusClientSessionPtr Session;
private:
    void OnReply(TAutoPtr<NBus::TBusMessage> pMessage, TAutoPtr<NBus::TBusMessage> pReply) override;
    void OnError(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status) override;
    void OnResult(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status, TAutoPtr<NBus::TBusMessage> pReply);

public:
    typedef std::function<void (NBus::EMessageStatus status, TAutoPtr<NBus::TBusMessage> reply)> TOnCall;
    typedef std::function<void (NBus::EMessageStatus status,
                                TAutoPtr<NBus::TBusMessage> message,
                                TAutoPtr<NBus::TBusMessage> reply)> TOnCallWithRequest;

    TMsgBusClient(const TMsgBusClientConfig &config);
    ~TMsgBusClient();

    NBus::EMessageStatus SyncCall(TAutoPtr<NBus::TBusMessage> msg, TAutoPtr<NBus::TBusMessage> &reply);
    NBus::EMessageStatus AsyncCall(TAutoPtr<NBus::TBusMessage> msg, TOnCall callback);
    NBus::EMessageStatus AsyncCall(TAutoPtr<NBus::TBusMessage> msg, TOnCallWithRequest callback);
    void Init();
    void Shutdown();
    const TMsgBusClientConfig& GetConfig();
};

EDataReqStatusExcerpt ExtractDataRequestStatus(const NKikimrClient::TResponse *response);

} // NMsgBusProxy

void SetMsgBusDefaults(NBus::TBusSessionConfig& sessionConfig,
                              NBus::TBusQueueConfig& queueConfig);

}
