#pragma once
#include <ydb/public/lib/base/defs.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <ydb/core/mon/mon.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NMsgBusProxy {

class IMessageBusHttpServer : public NMonitoring::IMonPage {
public:
    IMessageBusHttpServer(const TString& path, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : NMonitoring::IMonPage(path)
        , Counters(counters)
    {}

    virtual void Shutdown() = 0;

    // counters
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> HttpGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsActive;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InboundSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutboundSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr Status200;
    ::NMonitoring::TDynamicCounters::TCounterPtr Status400;
    ::NMonitoring::TDynamicCounters::TCounterPtr Status500;
    ::NMonitoring::TDynamicCounters::TCounterPtr Status503;
    ::NMonitoring::TDynamicCounters::TCounterPtr Status504;
    NMonitoring::THistogramPtr RequestTotalTimeHistogram;
    NMonitoring::THistogramPtr RequestPrepareTimeHistogram;
};

IMessageBusHttpServer* CreateMessageBusHttpServer(TActorSystem* actorSystem, NBus::IBusServerHandler* handler, const TProtocol& protocol, const NBus::TBusServerSessionConfig& config);

}
}
