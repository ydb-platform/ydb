#include <ydb/core/blobstorage/base/data_center_states.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_base.pb.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <algorithm>

namespace NKikimr {
namespace {

using TDataCenterState = NKikimrBlobStorage::TDataCenterState;

TEST(DataCenterStates, ParseValidateAndRoundTrip) {
    NKikimrBlobStorage::TDefineDataCenterStates command;
    auto* included = command.AddDataCenter();
    included->SetName("included");
    included->SetState(TDataCenterState::INCLUDED);
    auto* excluded = command.AddDataCenter();
    excluded->SetName("excluded");
    excluded->SetState(TDataCenterState::EXCLUDED);
    auto* recovering = command.AddDataCenter();
    recovering->SetName("recovering");
    recovering->SetState(TDataCenterState::RECOVERING);
    const auto states = ParseDataCenterStates(command);
    ASSERT_TRUE(states.has_value()) << states.error();
    EXPECT_EQ(states->size(), 3);
    EXPECT_EQ(states->at("included"), TDataCenterState::INCLUDED);
    EXPECT_EQ(states->at("excluded"), TDataCenterState::EXCLUDED);
    EXPECT_EQ(states->at("recovering"), TDataCenterState::RECOVERING);
    const auto serialized = SerializeDataCenterStates(*states);
    EXPECT_EQ(serialized.DataCenterSize(), 3);
    const auto restored = ParseDataCenterStates(serialized);
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(*states, *restored);

    // Duplicate names must be rejected even when their first definition is INCLUDED.
    command.AddDataCenter()->CopyFrom(*included);
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter()->RemoveLast();
    command.MutableDataCenter(0)->ClearState();
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter(0)->SetState(static_cast<TDataCenterState::EState>(77));
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter(0)->SetState(TDataCenterState::INCLUDED);
    command.MutableDataCenter(0)->SetName("");
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
}

TEST(FailRealmStates, RoleNamesRoundTrip) {
    NKikimrBlobStorage::TNodeWardenCache cache;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(R"(
        ServiceSet { Groups { Rings { State: INCLUDED } Rings { State: EXCLUDED } Rings { State: RECOVERING } Rings {} } }
    )", &cache));
    const auto& group = cache.GetServiceSet().GetGroups(0);
    const TDataCenterState::EState expected[] = {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::RECOVERING, TDataCenterState::INCLUDED};
    ASSERT_EQ(group.RingsSize(), std::size(expected));
    for (ui32 realm = 0; realm < std::size(expected); ++realm) {
        EXPECT_EQ(group.GetRings(realm).GetState(), expected[realm]);
    }
}

} // namespace
} // namespace NKikimr
