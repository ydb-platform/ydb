///////////////////////////////////////////////////////////
/// \file
/// \brief Starter session implementation

/// Starter session will generate emtpy message to insert
/// into local session that are registered under same protocol

/// Starter (will one day) automatically adjust number
/// of message inflight to make sure that at least one of source
/// sessions within message queue is at the limit (bottle neck)

/// Maximum number of messages that starter will instert into
/// the pipeline is configured by NBus::TBusSessionConfig::MaxInFlight

#include "startsession.h"

#include "module.h"

#include <library/cpp/messagebus/ybus.h>

namespace NBus {
    void* TBusStarter::_starter(void* data) {
        TBusStarter* pThis = static_cast<TBusStarter*>(data);
        pThis->Starter();
        return nullptr;
    }

    TBusStarter::TBusStarter(TBusModule* module, const TBusSessionConfig& config)
        : Module(module)
        , Config(config)
        , StartThread(_starter, this)
        , Exiting(false)
    {
        StartThread.Start();
    }

    TBusStarter::~TBusStarter() {
        Shutdown();
    }

    void TBusStarter::Shutdown() {
        {
            TGuard<TMutex> g(ExitLock);
            Exiting = true;
            ExitSignal.Signal();
        }
        StartThread.Join();
    }

    void TBusStarter::Starter() {
        TGuard<TMutex> g(ExitLock);
        while (!Exiting) {
            TAutoPtr<TBusMessage> empty(new TBusMessage(0));

            EMessageStatus status = Module->StartJob(empty);

            if (Config.SendTimeout > 0) {
                ExitSignal.WaitT(ExitLock, TDuration::MilliSeconds(Config.SendTimeout));
            } else {
                ExitSignal.WaitT(ExitLock, (status == MESSAGE_BUSY) ? TDuration::MilliSeconds(1) : TDuration::Zero());
            }
        }
    }

}
