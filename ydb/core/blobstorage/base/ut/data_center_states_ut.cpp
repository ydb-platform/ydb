#include <ydb/core/blobstorage/base/data_center_states.h>
#include <ydb/core/protos/blobstorage.pb.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <algorithm>

namespace NKikimr {
namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using DC = NKikimrBlobStorage::TDefineDataCenterStates::TDataCenter;

TEST(DataCenterStates, ParseValidateAndRoundTrip) {
    NKikimrBlobStorage::TDefineDataCenterStates command;
    auto* included = command.AddDataCenter();
    included->SetName("included");
    included->SetState(DC::INCLUDED);
    auto* excluded = command.AddDataCenter();
    excluded->SetName("excluded");
    excluded->SetState(DC::EXCLUDED);
    const auto states = ParseDataCenterStates(command);
    ASSERT_TRUE(states.has_value()) << states.error();
    EXPECT_EQ(states->size(), 2);
    EXPECT_EQ(states->at("included"), DC::INCLUDED);
    EXPECT_EQ(states->at("excluded"), DC::EXCLUDED);
    const auto serialized = SerializeDataCenterStates(*states);
    EXPECT_EQ(serialized.DataCenterSize(), 2);
    const auto restored = ParseDataCenterStates(serialized);
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(*states, *restored);

    // Duplicate names must be rejected even when their first definition is INCLUDED.
    command.AddDataCenter()->CopyFrom(*included);
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter()->RemoveLast();
    command.MutableDataCenter(0)->ClearState();
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter(0)->SetState(static_cast<DC::EDataCenterState>(2));
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
    command.MutableDataCenter(0)->SetState(DC::INCLUDED);
    command.MutableDataCenter(0)->SetName("");
    EXPECT_FALSE(ParseDataCenterStates(command).has_value());
}

TEST(FailRealmStates, LegacyNodeWardenCacheTextFormat) {
    NKikimrBlobStorage::TNodeWardenCache cache;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(R"(
        ServiceSet {
            Groups {
                Rings { State: N0 }
                Rings { State: N1 }
                Rings { State: N2 }
                Rings { State: F }
                Rings { State: R }
                Rings {}
            }
        }
    )", &cache));
    const auto& group = cache.GetServiceSet().GetGroups(0);
    const R::EState expected[] = {R::Invariant0, R::Invariant1, R::Invariant2, R::Excluded, R::Recovering, R::Invariant0};
    ASSERT_EQ(group.RingsSize(), std::size(expected));
    for (ui32 realm = 0; realm < std::size(expected); ++realm) {
        EXPECT_EQ(group.GetRings(realm).GetState(), expected[realm]);
    }
}

TEST(FailRealmStates, Rfc190TransitionTable) {
    struct TCase {
        TFailRealmStates Before;
        TFailRealmStates After;
        bool Allowed;
    };
    const TCase cases[] = {
        {{R::Invariant0, R::Invariant0, R::Invariant0}, {R::Invariant0, R::Invariant0, R::Excluded}, true},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant0, R::Excluded, R::Excluded}, false},
        {{R::Invariant1, R::Invariant1, R::Excluded}, {R::Invariant1, R::Excluded, R::Excluded}, true},
        {{R::Invariant2, R::Invariant2, R::Excluded}, {R::Invariant2, R::Excluded, R::Excluded}, true},
        {{R::Invariant1, R::Recovering, R::Excluded}, {R::Invariant1, R::Excluded, R::Excluded}, true},
        // Recovery can always be aborted, even when repairing a state installed with force.
        {{R::Recovering, R::Excluded, R::Excluded}, {R::Excluded, R::Excluded, R::Excluded}, true},
        {{R::Recovering, R::Recovering, R::Excluded}, {R::Excluded, R::Recovering, R::Excluded}, true},
        {{R::Invariant1, R::Excluded, R::Excluded}, {R::Invariant1, R::Recovering, R::Excluded}, true},
        {{R::Invariant1, R::Recovering, R::Excluded}, {R::Invariant1, R::Excluded, R::Recovering}, false},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant0, R::Invariant0, R::Recovering}, true},
        {{R::Invariant0, R::Invariant0, R::Recovering}, {R::Invariant0, R::Invariant0, R::Invariant0}, true},
        {{R::Invariant1, R::Invariant1, R::Recovering}, {R::Invariant0, R::Invariant0, R::Invariant0}, true},
        {{R::Invariant2, R::Invariant2, R::Recovering}, {R::Invariant0, R::Invariant0, R::Invariant0}, true},
        {{R::Invariant1, R::Invariant1, R::Recovering}, {R::Invariant1, R::Invariant1, R::Invariant0}, false},
        {{R::Invariant1, R::Recovering, R::Excluded}, {R::Invariant1, R::Invariant1, R::Excluded}, true},
        {{R::Invariant2, R::Recovering, R::Excluded}, {R::Invariant2, R::Invariant2, R::Excluded}, true},
        {{R::Invariant1, R::Recovering, R::Excluded}, {R::Invariant1, R::Invariant2, R::Excluded}, false},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant1, R::Invariant1, R::Excluded}, true},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant2, R::Invariant2, R::Excluded}, true},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant1, R::Invariant0, R::Excluded}, false},
        {{R::Invariant1, R::Invariant1, R::Excluded}, {R::Invariant2, R::Invariant2, R::Excluded}, true},
        {{R::Invariant2, R::Invariant2, R::Excluded}, {R::Invariant1, R::Invariant1, R::Excluded}, false},
        {{R::Invariant0, R::Invariant0, R::Recovering}, {R::Invariant1, R::Invariant1, R::Recovering}, false},
        {{R::Invariant0, R::Invariant0, R::Excluded}, {R::Invariant0, R::Invariant0, R::Invariant0}, false},
        {{R::Invariant0, R::Invariant0, R::Invariant0}, {R::Invariant0, R::Invariant0, R::Recovering}, false},
        {{R::Invariant0, R::Invariant0, R::Invariant0}, {R::Invariant1, R::Invariant1, R::Invariant1}, false},
        {{R::Invariant1, R::Invariant1, R::Excluded}, {R::Invariant0, R::Invariant0, R::Excluded}, false},
        {{R::Invariant1, R::Excluded, R::Excluded}, {R::Invariant1, R::Recovering, R::Recovering}, false},
        {{R::Invariant1, R::Excluded, R::Excluded}, {R::Excluded, R::Excluded, R::Excluded}, false},
    };
    for (const auto& test : cases) {
        std::array<unsigned, 3> permutation{0, 1, 2};
        do {
            TFailRealmStates before, after;
            for (unsigned realm = 0; realm < 3; ++realm) {
                before[realm] = test.Before[permutation[realm]];
                after[realm] = test.After[permutation[realm]];
            }
            const auto result = ValidateFailRealmStateTransition(before, after);
            EXPECT_EQ(result.has_value(), test.Allowed) << (result ? TString() : result.error());
        } while (std::next_permutation(permutation.begin(), permutation.end()));
    }
}

TEST(FailRealmStates, IdempotencyAndUnknownStates) {
    for (int first = 0; first < 5; ++first) {
        for (int second = 0; second < 5; ++second) {
            for (int third = 0; third < 5; ++third) {
                const TFailRealmStates states{static_cast<R::EState>(first), static_cast<R::EState>(second), static_cast<R::EState>(third)};
                EXPECT_TRUE(ValidateFailRealmStateTransition(states, states).has_value());
            }
        }
    }
    const TFailRealmStates normal{R::Invariant0, R::Invariant0, R::Invariant0};
    const TFailRealmStates invalid{R::Invariant0, R::Invariant0, static_cast<R::EState>(77)};
    EXPECT_FALSE(ValidateFailRealmStateTransition(normal, invalid).has_value());
    EXPECT_FALSE(ValidateFailRealmStateTransition(invalid, normal).has_value());
}

} // namespace
} // namespace NKikimr
