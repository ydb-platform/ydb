#pragma once

#include <library/cpp/messagebus/queue_config.h>
#include <library/cpp/messagebus/session_config.h>
#include <library/cpp/getopt/last_getopt.h>

namespace NKikimr {
namespace NMsgBusProxy {

struct TMsgBusClientConfig {
    NBus::TBusQueueConfig BusQueueConfig;
    NBus::TBusClientSessionConfig BusSessionConfig;

    TString Ip;
    ui32 Port;
    bool UseCompression;

    TMsgBusClientConfig();

    void ConfigureLastGetopt(NLastGetopt::TOpts &opts, const TString& prefix = TString());
    static void CrackAddress(const TString& address, TString& hostname, ui32& port);
};

}}
