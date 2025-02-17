#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncSemaphoreTest, CancelReadyEvent)
{
    auto semaphore = New<TAsyncSemaphore>(1);
    semaphore->Acquire(); // Drain single slot.

    auto readyOne = semaphore->GetReadyEvent();
    auto readyTwo = semaphore->GetReadyEvent();

    readyOne.Cancel(TError("canceled"));
    semaphore->Release();
    EXPECT_TRUE(readyTwo.IsSet());
    EXPECT_TRUE(WaitFor(readyTwo).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
