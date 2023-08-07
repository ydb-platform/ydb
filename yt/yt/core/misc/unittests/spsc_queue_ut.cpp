#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/spsc_queue.h>

#include <util/system/thread.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSpscQueueTest
    : public ::testing::Test
{
protected:
    TSpscQueue<int> A;
    TSpscQueue<int> B;

    std::atomic<bool> Stopped = {false};

    TSpscQueueTest()
    { }

    void AtoB()
    {
        while (auto item = A.Front()) {
            if (Stopped.load()) {
                break;
            }
            B.Push(std::move(*item));
            A.Pop();
        }
    }

    void BtoA()
    {
        while (auto item = B.Front()) {
            if (Stopped.load()) {
                break;
            }
            A.Push(std::move(*item));
            B.Pop();
        }
    }

};

TEST_F(TSpscQueueTest, TwoThreads)
{
    constexpr int N = 1000;

    using TThis = typename std::remove_reference<decltype(*this)>::type;
    TThread thread1([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        this_->AtoB();
        return nullptr;
    }, this);
    TThread thread2([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        this_->BtoA();
        return nullptr;
    }, this);

    for (size_t i = 1; i <= N; ++i) {
        if (i & 1) {
            A.Push(i);
        } else {
            B.Push(i);
        }
    }

    thread1.Start();
    thread2.Start();

    Sleep(TDuration::Seconds(2));

    Stopped.store(true);

    thread1.Join();
    thread2.Join();

    std::vector<int> elements;
    while (auto item = A.Front()) {
        elements.push_back(*item);
        A.Pop();
    }
    while (auto item = B.Front()) {
        elements.push_back(*item);
        B.Pop();
    }

    std::sort(elements.begin(), elements.end());

    EXPECT_EQ(std::ssize(elements), N);
    for (ssize_t i = 1; i <= N; ++i) {
        EXPECT_EQ(elements[i - 1], i);
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
