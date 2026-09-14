#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/atexit.h>
#include <util/thread/factory.h>

#include <chrono>
#include <cstdlib>
#include <future>

using namespace NYdb;

Y_UNIT_TEST_SUITE(RuntimeThreadLifetimeTests) {
    Y_UNIT_TEST(NetworkCanStartWorkerAfterUtilSingletonDestruction) {
        // AtExit priority zero runs after the default util singleton priority.
        // Initialize the old factory even when the SDK no longer needs it.
        (void)SystemThreadFactory();
        NYdbGrpc::TGRpcClientLow client(1);
        AtExit([] {
            try {
                NYdbGrpc::TGRpcClientLow lateClient(1);
                lateClient.AddWorkerThreadForTest();
            } catch (...) {
                std::_Exit(EXIT_FAILURE);
            }
        }, 0);
    }

    Y_UNIT_TEST(BackgroundCanStartWorkerAfterUtilSingletonDestruction) {
        struct TState {
            std::promise<void> Entered;
            std::promise<void> Release;
            std::promise<void> Done;
        };
        // The callbacks deliberately retain this state until process exit.
        auto* state = new TState;
        auto entered = state->Entered.get_future();
        auto release = state->Release.get_future().share();
        (void)SystemThreadFactory();
        GetRuntime().Post([state, release] {
            state->Entered.set_value();
            release.wait();
        });
        if (entered.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
            state->Release.set_value();
            UNIT_FAIL("The initial background worker did not start");
        }
        AtExit([](void* data) {
            auto* state = static_cast<TState*>(data);
            try {
                auto done = state->Done.get_future();
                // The first worker is still occupied, forcing a new worker.
                GetRuntime().Post([state] { state->Done.set_value(); });
                if (done.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
                    std::_Exit(EXIT_FAILURE);
                }
                state->Release.set_value();
            } catch (...) {
                std::_Exit(EXIT_FAILURE);
            }
        }, state, 0);
    }
}
