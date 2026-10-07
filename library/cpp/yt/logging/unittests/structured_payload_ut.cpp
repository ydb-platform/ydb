#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/structured_payload.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NLogging {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonString MakeMapFragment(TStringBuf yson)
{
    return TYsonString(yson, EYsonType::MapFragment);
}

TEST(TStructuredPayloadTest, RoundTrip)
{
    auto payload = MakeStructuredPayloadFromYson(MakeMapFragment("\"key\"=\"value\""));
    auto view = GetYsonFromStructuredPayload(payload);
    EXPECT_EQ(view.GetType(), EYsonType::MapFragment);
    EXPECT_EQ(view.AsStringBuf(), "\"key\"=\"value\"");
}

TEST(TStructuredPayloadTest, EmptyFragment)
{
    auto payload = MakeStructuredPayloadFromYson(MakeMapFragment(""));
    EXPECT_EQ(GetYsonFromStructuredPayload(payload).AsStringBuf(), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
