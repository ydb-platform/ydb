#pragma once

#include <library/cpp/messagebus/ybus.h>

#include <util/system/thread.h>

namespace NBus {
    class TBusModule;

    class TBusStarter {
    private:
        TBusModule* Module;
        TBusSessionConfig Config;
        TThread StartThread;
        bool Exiting;
        TCondVar ExitSignal;
        TMutex ExitLock;

        static void* _starter(void* data);

        void Starter();

        TString GetStatus(ui16 /*flags=YBUS_STATUS_CONNS*/) {
            return "";
        }

    public:
        TBusStarter(TBusModule* module, const TBusSessionConfig& config);
        ~TBusStarter();

        void Shutdown();
    };

}
