#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/error/error_code.h>

#include <library/cpp/yt/string/format.h>

#include <ostream>

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((Global1) (-5))
    ((Global2) (-6))
);

namespace NExternalWorld {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((X) (-11))
    ((Y) (-22))
    ((Z) (-33))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExternalWorld

namespace NYT {

void PrintTo(const TErrorCodeRegistry::TErrorCodeInfo& errorCodeInfo, std::ostream* os)
{
    *os << ToString(errorCodeInfo);
}

namespace NInternalLittleWorld {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((A) (-1))
    ((B) (-2))
    ((C) (-3))
    ((D) (-4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMyOwnLittleWorld

namespace {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((Kek)     (-57))
    ((Haha)    (-179))
    ((Muahaha) (-1543))
    ((Kukarek) (-2007))
);

std::string TestErrorCodeFormatter(int code)
{
    return Format("formatted%v", code);
}

YT_DEFINE_ERROR_CODE_RANGE(-4399, -4200, "NYT::Test", TestErrorCodeFormatter);

DEFINE_ENUM(EDifferentTestErrorCode,
    ((ErrorNumberOne)   (-10000))
    ((ErrorNumberTwo)   (-10001))
    ((ErrorNumberThree) (-10002))
);

std::string DifferentTestErrorCodeFormatter(int code)
{
    return TEnumTraits<EDifferentTestErrorCode>::ToString(static_cast<EDifferentTestErrorCode>(code));
}

YT_DEFINE_ERROR_CODE_RANGE(-10005, -10000, "NYT::DifferentTest", DifferentTestErrorCodeFormatter);

TEST(TErrorCodeRegistryTest, Basic)
{
#ifdef _unix_
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-1543),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::(anonymous namespace)", "Muahaha"}));
#else
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-1543),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::`anonymous namespace'", "Muahaha"}));
#endif
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-3),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::NInternalLittleWorld", "C"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-33),
        (TErrorCodeRegistry::TErrorCodeInfo{"NExternalWorld", "Z"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-5),
        (TErrorCodeRegistry::TErrorCodeInfo{"", "Global1"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-4300),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::Test", "formatted-4300"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-10002),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::DifferentTest", "ErrorNumberThree"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-10005),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::DifferentTest", "EDifferentTestErrorCode(-10005)"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-111),
        (TErrorCodeRegistry::TErrorCodeInfo{"NUnknown", "ErrorCode-111"}));
}

DEFINE_ENUM(ETestEnumOne,
    ((VariantOne) (0))
    ((VariantTwo) (1))
);

DEFINE_ENUM(ETestEnumTwo,
    ((DifferentVariantOne) (0))
    ((DifferentVariantTwo) (1))
);

template <class T, class K>
concept EquallyComparable = requires(T a, K b)
{
    { static_cast<T>(0) == static_cast<K>(0) };
};

TEST(TErrorCodeTest, ImplicitCastTest)
{
    // assert TErrorCode is in scope
    using NYT::TErrorCode;
    bool equallyComparable = EquallyComparable<ETestEnumOne, ETestEnumTwo>;
    EXPECT_FALSE(equallyComparable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
