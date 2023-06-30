#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TGuidTest, SerializationToProto)
{
    auto guid = TGuid::Create();
    auto protoGuid = ToProto<NProto::TGuid>(guid);
    auto deserializedGuid = FromProto<TGuid>(protoGuid);
    EXPECT_EQ(guid, deserializedGuid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
