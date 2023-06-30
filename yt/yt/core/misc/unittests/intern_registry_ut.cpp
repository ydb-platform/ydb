#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/intern_registry.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TStringRegistry = TInternRegistry<TString>;
using TInternedString = TInternedObject<TString>;

TEST(TInternRegistry, TestEmptyRegistry)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());
}

TEST(TInternRegistry, TestEmptyInstance)
{
    TInternedString s;
    EXPECT_EQ(0u, s->length());
}

TEST(TInternRegistry, Simple)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());

    auto s1 = registry->Intern(TString("hello"));
    EXPECT_EQ(1, registry->GetSize());

    auto s2 = registry->Intern(TString("world"));
    EXPECT_EQ(2, registry->GetSize());

    auto s3 = registry->Intern(TString("hello"));
    EXPECT_EQ(2, registry->GetSize());

    EXPECT_TRUE(*s1 == *s3);
    EXPECT_FALSE(*s1 == *s2);

    auto s4 = registry->Intern(TString("test"));
    EXPECT_EQ(3, registry->GetSize());

    s4 = TInternedString();
    EXPECT_EQ(2, registry->GetSize());

    s3 = TInternedString();
    EXPECT_EQ(2, registry->GetSize());

    s2 = TInternedString();
    EXPECT_EQ(1, registry->GetSize());

    s1 = TInternedString();
    EXPECT_EQ(0, registry->GetSize());
}


TEST(TInternRegistry, Default)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());

    auto s1 = TInternedString();

    auto s2 = registry->Intern(TString());
    EXPECT_EQ(0, registry->GetSize());

    EXPECT_TRUE(*s1 == *s2);
}

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NYT
