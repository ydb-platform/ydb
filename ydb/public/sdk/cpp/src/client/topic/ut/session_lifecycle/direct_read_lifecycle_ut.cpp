#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests {

    class TDirectReadSessionImplTestSetup {
    public:
        static void Abort(TDirectReadSession& session) {
            TDeferredActions<false> deferred;
            with_lock (session.Lock) {
                session.AbortImpl(TPlainStatus(EStatus::UNAVAILABLE, "Connection failed"), deferred);
            }
        }

        static bool TryClose(TDirectReadSession& session) {
            if (!session.Lock.TryAcquire()) {
                return false;
            }
            session.Lock.Release();
            session.Close();
            return true;
        }
    };

    Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle) {
        Y_UNIT_TEST(TerminalCallbackCanCloseDirectReader) {
            struct TControlCallbacks: IDirectReadSessionControlCallbacks {
                std::function<void()> OnAbort;

                void AbortSession(TSessionClosedEvent&&) override {
                    OnAbort();
                }
            };
            auto callbacks = std::make_shared<TControlCallbacks>();
            TDirectReadSession session(0, "", TReadSessionSettings{}, callbacks, nullptr, nullptr, TLog{});
            bool callbackCalled = false;
            bool closedWithoutLock = false;
            callbacks->OnAbort = [&] {
                callbackCalled = true;
                closedWithoutLock = TDirectReadSessionImplTestSetup::TryClose(session);
            };

            TDirectReadSessionImplTestSetup::Abort(session);

            UNIT_ASSERT(callbackCalled);
            UNIT_ASSERT_C(closedWithoutLock, "Terminal callback must run after releasing the direct reader lock");
        }
    } // Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle)

} // namespace NYdb::inline Dev::NTopic::NTests
