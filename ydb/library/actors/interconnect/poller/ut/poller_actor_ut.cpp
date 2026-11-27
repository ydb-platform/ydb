#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/network/pair.h>
#include <util/network/socket.h>

namespace NActors {
    namespace {

        class TTestSocket: public TSharedDescriptor {
        public:
            explicit TTestSocket(SOCKET fd)
                : Fd_(fd)
            {
            }

            ~TTestSocket() override {
                closesocket(Fd_);
            }

            int GetDescriptor() override {
                return Fd_;
            }

        private:
            const SOCKET Fd_;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(TPollerWakeupTest) {
        Y_UNIT_TEST(ReadNotificationWithoutSyncOperation) {
            TTestActorRuntimeBase runtime;
            runtime.Initialize();

            SOCKET fds[2];
            UNIT_ASSERT_VALUES_EQUAL(SocketPair(fds), 0);
            auto reader = MakeIntrusive<TTestSocket>(fds[0]);
            auto writer = MakeIntrusive<TTestSocket>(fds[1]);
            SetNonBlock(fds[0]);
            SetNonBlock(fds[1]);

            const auto clientId = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakePollerActorId(), clientId,
                                          new TEvPollerRegister(reader, clientId, {})));
            auto registered = runtime.GrabEdgeEvent<TEvPollerRegisterResult>(clientId);
            auto token = registered->Get()->PollerToken;

            for (int i = 0; i < 3; ++i) {
                // On Windows, Request waits for its sync operation to finish. The following
                // readiness notification must not try to drain that empty queue.
                token->Request(true, false);
                UNIT_ASSERT_VALUES_EQUAL(send(writer->GetDescriptor(), "x", 1, 0), 1);
                auto ready = runtime.GrabEdgeEvent<TEvPollerReady>(clientId);
                UNIT_ASSERT_EQUAL(ready->Get()->Socket, reader);
                UNIT_ASSERT(ready->Get()->Read);
                UNIT_ASSERT(!ready->Get()->Write);
                char value = 0;
                UNIT_ASSERT_VALUES_EQUAL(recv(reader->GetDescriptor(), &value, 1, 0), 1);
                UNIT_ASSERT_VALUES_EQUAL(value, 'x');
            }
        }
    } // Y_UNIT_TEST_SUITE(TPollerWakeupTest)

} // namespace NActors
