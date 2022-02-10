#pragma once

#include "example.h"

#include <library/cpp/messagebus/oldmodule/module.h>

namespace NBus {
    namespace NTest {
        struct TExampleModule: public TBusModule {
            TExampleProtocol Proto;
            TBusMessageQueuePtr Queue;

            TExampleModule();

            void StartModule();

            bool Shutdown() override;

            // nop by default
            TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override;
        };

        struct TExampleServerModule: public TExampleModule {
            TNetAddr ServerAddr;
            TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override;
        };

        struct TExampleClientModule: public TExampleModule {
            TBusClientSessionPtr Source;

            TExampleClientModule();

            TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override;
        };

    }
}
