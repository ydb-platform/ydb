#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFirstOption
{
    TString Value;
};

bool operator==(const TFirstOption& lhs, const TFirstOption& rhs)
{
    return lhs.Value == rhs.Value;
}

struct TSecondOption
{
    i64 Value;
};

bool operator==(const TSecondOption& lhs, const TSecondOption& rhs)
{
    return lhs.Value == rhs.Value;
}

struct TTestStruct
    : public TYsonStruct
{
    TString Nothing;
    i64 First;
    double Second;
    TString Both;

    REGISTER_YSON_STRUCT(TTestStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nothing", &TThis::Nothing)
            .Default();
        registrar.Parameter("first", &TThis::First)
            .Default()
            .AddOption(TFirstOption{"something"});
        registrar.Parameter("second", &TThis::Second)
            .Default()
            .AddOption(TSecondOption{42});
        registrar.Parameter("both", &TThis::Both)
            .Default()
            .AddOption(TFirstOption{"other"})
            .AddOption(TSecondOption{24});
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructOptionsTest, Options)
{
    auto ysonStruct = New<TTestStruct>();
    auto parameters = ysonStruct->GetMeta()->GetParameterMap();

    EXPECT_EQ(parameters["nothing"]->FindOption<TFirstOption>(), std::nullopt);
    EXPECT_EQ(parameters["nothing"]->FindOption<TSecondOption>(), std::nullopt);
    EXPECT_EQ(parameters["first"]->FindOption<TFirstOption>(), std::make_optional(TFirstOption{"something"}));
    EXPECT_EQ(parameters["first"]->FindOption<TSecondOption>(), std::nullopt);
    EXPECT_EQ(parameters["second"]->FindOption<TFirstOption>(), std::nullopt);
    EXPECT_EQ(parameters["second"]->FindOption<TSecondOption>(), std::make_optional(TSecondOption{42}));
    EXPECT_EQ(parameters["both"]->GetOptionOrThrow<TFirstOption>(), TFirstOption{"other"});
    EXPECT_EQ(parameters["both"]->GetOptionOrThrow<TSecondOption>(), TSecondOption{24});
    EXPECT_THROW(parameters["second"]->GetOptionOrThrow<TFirstOption>(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
