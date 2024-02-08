#include <gtest/gtest.h>

#include <yt/yt/library/ytprof/queue.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(StaticQueue, PushPop)
{
    TStaticQueue queue(10);

    int a, b, c;
    void* aptr = reinterpret_cast<void*>(&a);
    void* bptr = reinterpret_cast<void*>(&b);
    void* cptr = reinterpret_cast<void*>(&c);

    std::vector<void*> backtrace;
    auto getBacktrace = [&] () -> std::pair<void*, bool> {
        if (backtrace.empty()) {
            return {nullptr, false};
        }

        auto ip = backtrace.front();
        backtrace.erase(backtrace.begin());
        return {ip, true};
    };

    ASSERT_TRUE(queue.TryPush(getBacktrace));

    backtrace = {aptr, bptr, cptr};
    ASSERT_TRUE(queue.TryPush(getBacktrace));

    backtrace.push_back(aptr);
    ASSERT_TRUE(queue.TryPush(getBacktrace));

    auto readBacktrace = [&] (void *ip) {
        backtrace.push_back(ip);
    };

    ASSERT_TRUE(queue.TryPop(readBacktrace));
    ASSERT_EQ(backtrace, std::vector<void*>{});

    backtrace.clear();
    ASSERT_TRUE(queue.TryPop(readBacktrace));
    ASSERT_EQ(backtrace, (std::vector<void*>{aptr, bptr, cptr}));

    backtrace.clear();
    ASSERT_TRUE(queue.TryPop(readBacktrace));
    ASSERT_EQ(backtrace, (std::vector<void*>{aptr}));

    ASSERT_FALSE(queue.TryPop(readBacktrace));
}

TEST(StaticQueue, Overflow)
{
    TStaticQueue queue(10);

    ASSERT_FALSE(queue.TryPush([] () -> std::pair<void*, bool> {
        return {nullptr, true};
    }));
}

} // namespace
} // namespace NYT::NYTProf
