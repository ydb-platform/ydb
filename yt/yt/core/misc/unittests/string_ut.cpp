#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/string/enum.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EColor,
    (Red)
    (BlackAndWhite)
);

DEFINE_BIT_ENUM(ELangs,
    ((None)       (0x00))
    ((Cpp)        (0x01))
    ((Go)         (0x02))
    ((Rust)       (0x04))
    ((Python)     (0x08))
    ((JavaScript) (0x10))
);

TEST(TStringTest, Enum)
{
    EXPECT_EQ("red", FormatEnum(EColor::Red));
    EXPECT_EQ(ParseEnum<EColor>("red"), EColor::Red);

    EXPECT_EQ("black_and_white", FormatEnum(EColor::BlackAndWhite));
    EXPECT_EQ(ParseEnum<EColor>("black_and_white"), EColor::BlackAndWhite);

    EXPECT_EQ("EColor(100)", FormatEnum(EColor(100)));

    EXPECT_EQ("java_script", FormatEnum(ELangs::JavaScript));
    EXPECT_EQ(ParseEnum<ELangs>("java_script"), ELangs::JavaScript);

    EXPECT_EQ("none", FormatEnum(ELangs::None));
    EXPECT_EQ(ParseEnum<ELangs>("none"), ELangs::None);

    EXPECT_EQ("cpp | go", FormatEnum(ELangs::Cpp | ELangs::Go));
    EXPECT_EQ(ParseEnum<ELangs>("cpp | go"), ELangs::Cpp | ELangs::Go);

    auto four = ELangs::Cpp | ELangs::Go | ELangs::Python | ELangs::JavaScript;
    EXPECT_EQ("cpp | go | python | java_script", FormatEnum(four));
    EXPECT_EQ(ParseEnum<ELangs>("cpp | go | python | java_script"), four);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

