#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NKikimr::NPQ {

TEvPQ::TMessageGroupsPtr CreateExplicitMessageGroups(const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                                     const NKikimrPQ::TPartitions& partitionsData,
                                                     const NKikimrPQ::TPQTabletConfig& tabletConfig,
                                                     const TPartitionGraph& graph,
                                                     ui32 partitionId);

namespace {

TString SerializeKey(ui64 value) {
    const TVector<TCell> cells = {TCell::Make<ui64>(value)};
    return TSerializedCellVec::Serialize(cells);
}

NKikimrPQ::TPQTabletConfig MakeTabletConfig(bool withKeySchema) {
    NKikimrPQ::TPQTabletConfig config;
    if (withKeySchema) {
        auto& key = *config.AddPartitionKeySchema();
        key.SetName("key");
        key.SetTypeId(NScheme::NTypeIds::Uint64);
    }

    auto& first = *config.AddPartitions();
    first.SetPartitionId(0);
    first.MutableKeyRange()->SetToBound(SerializeKey(100));

    auto& second = *config.AddPartitions();
    second.SetPartitionId(1);
    second.MutableKeyRange()->SetFromBound(SerializeKey(100));
    second.MutableKeyRange()->SetToBound(SerializeKey(200));
    return config;
}

NKikimrPQ::TBootstrapConfig MakeBootstrapConfig(bool withKeyRanges) {
    NKikimrPQ::TBootstrapConfig config;

    auto& first = *config.AddExplicitMessageGroups();
    first.SetId("ds1");
    auto& second = *config.AddExplicitMessageGroups();
    second.SetId("ds2");
    auto& third = *config.AddExplicitMessageGroups();
    third.SetId("ds3");

    if (withKeyRanges) {
        first.MutableKeyRange()->SetToBound(SerializeKey(100));
        second.MutableKeyRange()->SetFromBound(SerializeKey(100));
        second.MutableKeyRange()->SetToBound(SerializeKey(200));
        third.MutableKeyRange()->SetFromBound(SerializeKey(200));
    }

    return config;
}

TString GetGroupIds(const TEvPQ::TMessageGroupsPtr& groups) {
    TVector<TString> ids;
    for (const auto& [id, _] : *groups) {
        ids.push_back(id);
    }
    Sort(ids);
    return JoinSeq(",", ids);
}

} // anonymous

Y_UNIT_TEST_SUITE(TExplicitMessageGroupsTest) {
    Y_UNIT_TEST(ShouldRegisterOnlyIntersectingGroupsWhenGroupsHaveKeyRanges) {
        // given
        const auto tabletConfig = MakeTabletConfig(true);
        const auto bootstrapConfig = MakeBootstrapConfig(true);

        // when
        const auto groups = CreateExplicitMessageGroups(bootstrapConfig, {}, tabletConfig, TPartitionGraph(), 1);

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIds(groups), "ds2");
    }

    Y_UNIT_TEST(ShouldRegisterAllGroupsWhenGroupsHaveNoKeyRanges) {
        // given
        const auto tabletConfig = MakeTabletConfig(true);
        const auto bootstrapConfig = MakeBootstrapConfig(false);

        // when
        const auto groups = CreateExplicitMessageGroups(bootstrapConfig, {}, tabletConfig, TPartitionGraph(), 1);

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIds(groups), "ds1,ds2,ds3");
    }

    Y_UNIT_TEST(ShouldRegisterAllGroupsWhenTopicHasNoKeySchema) {
        // given
        const auto tabletConfig = MakeTabletConfig(false);
        const auto bootstrapConfig = MakeBootstrapConfig(true);

        // when
        const auto groups = CreateExplicitMessageGroups(bootstrapConfig, {}, tabletConfig, TPartitionGraph(), 1);

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIds(groups), "ds1,ds2,ds3");
    }
}

} // namespace NKikimr::NPQ
