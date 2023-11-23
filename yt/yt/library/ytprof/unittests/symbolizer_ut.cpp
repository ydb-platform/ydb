#include <gtest/gtest.h>

#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/build_info.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

Y_NO_INLINE void* GetIP()
{
    return __builtin_return_address(0);
}

TEST(Symbolize, EmptyProfile)
{
    NProto::Profile profile;
    profile.add_string_table();

    Symbolize(&profile);
    AddBuildInfo(&profile, TBuildInfo::GetDefault());
}

TEST(Symbolize, SingleLocation)
{
    NProto::Profile profile;
    profile.add_string_table();

    auto thisIP = GetIP();

    {
        auto location = profile.add_location();
        location->set_address(reinterpret_cast<ui64>(thisIP));

        auto line = location->add_line();
        line->set_function_id(reinterpret_cast<ui64>(thisIP));

        auto function = profile.add_function();
        function->set_id(reinterpret_cast<ui64>(thisIP));
    }

    Symbolize(&profile);

    ASSERT_EQ(1, profile.function_size());
    auto function = profile.function(0);

    auto name = profile.string_table(function.name());
    ASSERT_TRUE(name.find("SingleLocation") != TString::npos)
        << "function name is " << name;
}

TEST(Symbolize, GetBuildId)
{
    if (!IsProfileBuild()) {
        GTEST_SKIP();
    }

    return;

    auto buildId = GetBuildId();
    ASSERT_TRUE(buildId);
    ASSERT_NE(*buildId, TString{""});
}

TEST(BuildInfo, Test)
{
    if (!IsProfileBuild()) {
        GTEST_SKIP();
    }

    auto info = TBuildInfo::GetDefault();
    if (IsProfileBuild()) {
        ASSERT_EQ(info.BuildType, "profile");
    }

    ASSERT_NE(info.ArcRevision, "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
