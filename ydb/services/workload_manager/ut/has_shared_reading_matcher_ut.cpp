#include <ydb/services/workload_manager/has_shared_reading_matcher.h>

#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/any.pb.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using NKqpProto::TKqpPhyQuery;

namespace {

NKqpProto::TKqpPhyStage* GetOrAddStage(TKqpPhyQuery& phy, size_t txIndex = 0, size_t stageIndex = 0) {
    while (phy.TransactionsSize() <= txIndex) {
        phy.AddTransactions();
    }
    auto* tx = phy.MutableTransactions(txIndex);
    while (tx->StagesSize() <= stageIndex) {
        tx->AddStages();
    }
    return tx->MutableStages(stageIndex);
}

void AddPqTopicSourceAt(
    TKqpPhyQuery& phy,
    size_t txIndex,
    size_t stageIndex,
    const TString& topicPath,
    bool sharedReading)
{
    auto* source = GetOrAddStage(phy, txIndex, stageIndex)->AddSources();
    NYql::NPq::NProto::TDqPqTopicSource pqSource;
    pqSource.SetTopicPath(topicPath);
    pqSource.SetSharedReading(sharedReading);
    source->MutableExternalSource()->MutableSettings()->PackFrom(pqSource);
    source->MutableExternalSource()->SetType(TString{NYql::PqSource});
}

void AddPqTopicSource(TKqpPhyQuery& phy, const TString& topicPath, bool sharedReading) {
    AddPqTopicSourceAt(phy, /*txIndex=*/0, /*stageIndex=*/0, topicPath, sharedReading);
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

    Y_UNIT_TEST(SharedReadingInSecondTransactionIsDetected) {
        TKqpPhyQuery phy;
        AddPqTopicSourceAt(phy, /*txIndex=*/1, /*stageIndex=*/0, "/Root/topic", true);
        UNIT_ASSERT(UsesSharedReading(phy));
    }

    Y_UNIT_TEST(SharedReadingInSecondStageIsDetected) {
        TKqpPhyQuery phy;
        AddPqTopicSourceAt(phy, /*txIndex=*/0, /*stageIndex=*/0, "/Root/topic1", false);
        AddPqTopicSourceAt(phy, /*txIndex=*/0, /*stageIndex=*/1, "/Root/topic2", true);
        UNIT_ASSERT(UsesSharedReading(phy));
    }

    Y_UNIT_TEST(AllSourcesWithoutSharedReadingIsFalse) {
        // Multiple PqSources across transactions/stages, none with shared reading.
        TKqpPhyQuery phy;
        AddPqTopicSourceAt(phy, /*txIndex=*/0, /*stageIndex=*/0, "/Root/topic1", false);
        AddPqTopicSourceAt(phy, /*txIndex=*/0, /*stageIndex=*/1, "/Root/topic2", false);
        AddPqTopicSourceAt(phy, /*txIndex=*/1, /*stageIndex=*/0, "/Root/topic3", false);
        UNIT_ASSERT(!UsesSharedReading(phy));
    }

}

}  // namespace NKikimr::NWorkloadManager
