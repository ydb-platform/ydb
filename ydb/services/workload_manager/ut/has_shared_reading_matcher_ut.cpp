#include <ydb/services/workload_manager/has_shared_reading_matcher.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/any.pb.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using NKqpProto::TKqpPhyQuery;

namespace {

NKqpProto::TKqpPhyStage* GetOrAddStage(TKqpPhyQuery& phy) {
    auto* tx = phy.TransactionsSize() ? phy.MutableTransactions(0) : phy.AddTransactions();
    return tx->StagesSize() ? tx->MutableStages(0) : tx->AddStages();
}

void AddPqTopicSource(TKqpPhyQuery& phy, const TString& topicPath, bool sharedReading) {
    auto* source = GetOrAddStage(phy)->AddSources();
    NYql::NPq::NProto::TDqPqTopicSource pqSource;
    pqSource.SetTopicPath(topicPath);
    pqSource.SetSharedReading(sharedReading);
    source->MutableExternalSource()->MutableSettings()->PackFrom(pqSource);
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TSharedReadingMatcher) {

    Y_UNIT_TEST(EmptyQueryHasNoSharedReading) {
        TKqpPhyQuery phy;
        UNIT_ASSERT(!UsesSharedReading(phy));
    }

    Y_UNIT_TEST(PqSourceWithoutSharedReading) {
        TKqpPhyQuery phy;
        AddPqTopicSource(phy, "/Root/topic", false);
        UNIT_ASSERT(!UsesSharedReading(phy));
    }

    Y_UNIT_TEST(PqSourceWithSharedReading) {
        TKqpPhyQuery phy;
        AddPqTopicSource(phy, "/Root/topic", true);
        UNIT_ASSERT(UsesSharedReading(phy));
    }

    Y_UNIT_TEST(AnySourceWithSharedReadingIsEnough) {
        TKqpPhyQuery phy;
        AddPqTopicSource(phy, "/Root/topic1", false);
        AddPqTopicSource(phy, "/Root/topic2", true);
        UNIT_ASSERT(UsesSharedReading(phy));
    }

}

}  // namespace NKikimr::NWorkloadManager
