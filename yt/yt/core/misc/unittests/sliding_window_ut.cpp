#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/sliding_window.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size>
class TSlidingWindowTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Window_ = std::make_unique<TSlidingWindow<size_t>>(Size);
    }

    void PushElement(size_t value)
    {
        Window_->AddPacket(value, size_t(value), [&] (size_t&& value) {
            ReceivedElements_.push_back(value);
        });
    }

    bool CheckElements(size_t minValue, size_t expectedSize)
    {
        if (ReceivedElements_.size() != expectedSize ||
            Window_->GetNextSequenceNumber() != static_cast<ssize_t>(expectedSize))
        {
            return false;
        }

        for (size_t value = minValue; value < expectedSize; ++value) {
            if (ReceivedElements_[value] != value) {
                return false;
            }
        }
        return true;
    }

protected:
    std::unique_ptr<TSlidingWindow<size_t>> Window_;
    std::vector<size_t> ReceivedElements_;
};

using TSingleSlidingWindowTest = TSlidingWindowTest<1>;

TEST_F(TSingleSlidingWindowTest, SingleTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->IsEmpty());

    PushElement(0);
    PushElement(1);
    EXPECT_ANY_THROW(PushElement(1));
    PushElement(2);
    EXPECT_TRUE(CheckElements(0, 3));
    EXPECT_TRUE(Window_->IsEmpty());
}

constexpr ssize_t WindowSize = 1024;
using TFiniteSlidingWindowTest = TSlidingWindowTest<WindowSize>;

TEST_F(TFiniteSlidingWindowTest, FiniteTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->IsEmpty());

    PushElement(0);
    for (auto element = WindowSize; element > 1; --element) {
        PushElement(element);
    }
    EXPECT_TRUE(CheckElements(0, 1));
    EXPECT_FALSE(Window_->IsEmpty());
    EXPECT_ANY_THROW(PushElement(2));
    EXPECT_ANY_THROW(PushElement(WindowSize));

    PushElement(1);
    EXPECT_TRUE(CheckElements(1, WindowSize + 1));
    EXPECT_TRUE(Window_->IsEmpty());

    PushElement(2 * WindowSize);
    EXPECT_TRUE(CheckElements(WindowSize + 1, WindowSize + 1));
    EXPECT_FALSE(Window_->IsEmpty());
}

using TInfiniteSlidingWindowTest = TSlidingWindowTest<std::numeric_limits<ssize_t>::max()>;

TEST_F(TInfiniteSlidingWindowTest, TInfiniteTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->IsEmpty());

    PushElement(WindowSize);
    PushElement(0);
    for (auto element = WindowSize - 1; element > 1; --element) {
        PushElement(element);
    }
    EXPECT_TRUE(CheckElements(0, 1));
    EXPECT_FALSE(Window_->IsEmpty());
    EXPECT_ANY_THROW(PushElement(2));
    EXPECT_ANY_THROW(PushElement(WindowSize));

    PushElement(1);
    EXPECT_TRUE(CheckElements(1, WindowSize + 1));
    EXPECT_TRUE(Window_->IsEmpty());

    PushElement(std::numeric_limits<ssize_t>::max());
    EXPECT_ANY_THROW(PushElement(std::numeric_limits<ssize_t>::max()));
    EXPECT_TRUE(CheckElements(WindowSize + 1, WindowSize + 1));
    EXPECT_FALSE(Window_->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

