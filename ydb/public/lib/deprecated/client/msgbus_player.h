#pragma once
#include "msgbus_client.h"

namespace NKikimr {
namespace NMessageBusTracer {


class TMsgBusPlayer {
protected:
    using TMsgBusClient = NMsgBusProxy::TMsgBusClient;
    TMsgBusClient &MsgBusClient;
public:
    TMsgBusPlayer(TMsgBusClient &msgBusClient);
    ui32 PlayTrace(const TString &traceFile, ui32 maxInFlight = 1000, std::function<void(int)> progressReporter = std::function<void(int)>());
};

}
}
