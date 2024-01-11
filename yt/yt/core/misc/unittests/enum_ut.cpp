#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EColor,
    ((Red)  (10))
    ((Green)(20))
    ((Blue) (30))
    (Black)
    (White)
);

TEST(TEnumTest, SaveAndLoad)
{
    TStringStream stream;
    TStreamSaveContext saveContext(&stream);
    TStreamLoadContext loadContext(&stream);

    auto first = EColor::Red;
    auto second = EColor::Black;
    auto third = EColor(0);
    auto fourth = EColor(0);

    Save(saveContext, first);
    Save(saveContext, second);

    saveContext.Finish();

    Load(loadContext, third);
    Load(loadContext, fourth);

    EXPECT_EQ(first, third);
    EXPECT_EQ(second, fourth);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

