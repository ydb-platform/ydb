#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TMoveCounter
{
public:
    TMoveCounter() = default;
    TMoveCounter(const TMoveCounter&) = delete;
    TMoveCounter& operator=(const TMoveCounter&) = delete;

    TMoveCounter(TMoveCounter&&)
    {
        ++Moves_;
    }

    TMoveCounter& operator=(TMoveCounter&&)
    {
        ++Moves_;
        return *this;
    }

    DEFINE_BYREF_RW_PROPERTY(int, Moves);
};

////////////////////////////////////////////////////////////////////////////////

TEST(TCollectionHelpersTest, EmplaceDefault)
{
    THashMap<int, TMoveCounter> map;
    EmplaceDefault(map, 0);
    EXPECT_EQ(map[0].Moves(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
