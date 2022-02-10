#pragma once

#include "mon_messagebus.h"

#include <library/cpp/monlib/service/monservice.h>

#include <util/system/mutex.h>

namespace NMonitoring {
    class TMonServiceMessageBus: public TMonService2 {
    private:
        TMutex Mtx;
        TIntrusivePtr<NMonitoring::TBusNgMonPage> BusNgMonPage;

    public:
        TMonServiceMessageBus(ui16 port, const TString& title);

    private:
        NBus::TBusWww* RegisterBusNgMonPage() {
            TGuard<TMutex> g(Mtx);
            if (!BusNgMonPage) {
                BusNgMonPage = new NMonitoring::TBusNgMonPage();
                Register(BusNgMonPage.Get());
            }
            return BusNgMonPage->BusWww.Get();
        }

    public:
        void RegisterClientSession(NBus::TBusClientSessionPtr clientSession) {
            RegisterBusNgMonPage()->RegisterClientSession(clientSession);
        }

        void RegisterServerSession(NBus::TBusServerSessionPtr serverSession) {
            RegisterBusNgMonPage()->RegisterServerSession(serverSession);
        }

        void RegisterQueue(NBus::TBusMessageQueuePtr queue) {
            RegisterBusNgMonPage()->RegisterQueue(queue);
        }

        void RegisterModule(NBus::TBusModule* module) {
            RegisterBusNgMonPage()->RegisterModule(module);
        }
    };

}
