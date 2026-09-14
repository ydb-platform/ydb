#include <ydb/library/actors/async/callback_coroutine.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NAsyncTest {

    using namespace NActors::NDetail;

    Y_UNIT_TEST_SUITE(CallbackCoroutine) {

        struct TSimpleVoidCallback {
            int Value = 0;

            void operator()() noexcept {
                ++Value;
            }
        };

        struct TSimpleSymmetricCallback {
            int Value = 0;
            std::coroutine_handle<> Next = std::noop_coroutine();

            std::coroutine_handle<> operator()() noexcept {
                ++Value;
                return Next;
            }
        };

        struct TExceptionFromCoroutine : public yexception {};

        struct TSimpleThrowingVoidCallback {
            int Value = 0;

            void operator()() {
                ++Value;
                throw TExceptionFromCoroutine();
            }
        };

        struct TSimpleThrowingSymmetricCallback {
            int Value = 0;
            std::coroutine_handle<> Next = std::noop_coroutine();

            std::coroutine_handle<> operator()() {
                ++Value;
                throw TExceptionFromCoroutine();
            }
        };

        Y_UNIT_TEST(Resume) {
            auto coro = MakeCallbackCoroutine<TSimpleVoidCallback>();
            UNIT_ASSERT(!coro.done());
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 0);
            coro();
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 1);
            coro.resume();
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 2);
            std::coroutine_handle<> h = coro;
            h.resume();
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 3);
            UNIT_ASSERT(!coro.done());

            auto coro2 = MakeCallbackCoroutine<TSimpleSymmetricCallback>();
            UNIT_ASSERT_VALUES_EQUAL(coro2->Value, 0);
            coro2.resume();
            UNIT_ASSERT_VALUES_EQUAL(coro2->Value, 1);
            coro2->Next = coro;
            coro2.resume();
            UNIT_ASSERT_VALUES_EQUAL(coro2->Value, 2);
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 4);
            UNIT_ASSERT(!coro2.done());
        }

        Y_UNIT_TEST(Throwing) {
            auto coro = MakeCallbackCoroutine<TSimpleThrowingVoidCallback>();
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 0);
            UNIT_ASSERT_EXCEPTION(coro.resume(), TExceptionFromCoroutine);
            UNIT_ASSERT_VALUES_EQUAL(coro->Value, 1);
            UNIT_ASSERT(coro.done());

            auto coro2 = MakeCallbackCoroutine<TSimpleThrowingSymmetricCallback>();
            UNIT_ASSERT_VALUES_EQUAL(coro2->Value, 0);
            UNIT_ASSERT_EXCEPTION(coro2.resume(), TExceptionFromCoroutine);
            UNIT_ASSERT_VALUES_EQUAL(coro2->Value, 1);
            UNIT_ASSERT(coro2.done());
        }

    } // Y_UNIT_TEST_SUITE(CallbackCoroutine)

} // namespace NAsyncTest
