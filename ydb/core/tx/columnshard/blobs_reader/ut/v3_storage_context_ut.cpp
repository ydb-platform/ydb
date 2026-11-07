#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {
namespace {
TReadStatsContext IndexContext(
    const std::shared_ptr<NScanStats::TCollector>& collector, std::optional<ui32> indexId, TString meta = "IndexFilter") {
    TReadStatsContext context;
    context.Collector = collector;
    context.DataKind =
        indexId ? NKqpProto::TColumnShardStepDetails::TStorageRead::index : NKqpProto::TColumnShardStepDetails::TStorageRead::metadata;
    context.Parent = collector->Start(NScanStats::TStep::FetchColumns, "fetch", "fetch");
    context.Parent->Update([&](auto& record) {
        record.SetMetaStep(meta);
        if (indexId) {
            auto* descriptor = record.AddIndexDescriptors();
            descriptor->SetIndexId(*indexId);
            descriptor->SetName(TStringBuilder() << "index-" << *indexId);
            descriptor->SetType("BLOOM_FILTER");
            descriptor->AddColumns("value");
        }
    });
    return context;
}
}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardStorageContextsV3) {
    Y_UNIT_TEST(SameIndexWithDifferentOperationIdsIsNotSharedInput) {
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        auto first = IndexContext(collector, 11);
        const auto second = IndexContext(collector, 11);
        UNIT_ASSERT(first.Parent->GetStepId() != second.Parent->GetStepId());
        first.Merge(second);
        UNIT_ASSERT(!first.SharedIndexInputs);
        UNIT_ASSERT_VALUES_EQUAL(first.IndexDescriptors.size(), 1);
    }

    Y_UNIT_TEST(TwoIndexOwnersUseOneSharedDescriptorSet) {
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        auto first = IndexContext(collector, 11);
        const auto second = IndexContext(collector, 12);
        first.Merge(second);
        first.Merge(second);
        UNIT_ASSERT(first.SharedIndexInputs);
        UNIT_ASSERT_VALUES_EQUAL(first.IndexDescriptors.size(), 2);
    }

    Y_UNIT_TEST(IndexAndMetadataIsSharedEvenWithOneIndex) {
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        auto first = IndexContext(collector, 11);
        first.Merge(IndexContext(collector, std::nullopt, "Filter"));
        UNIT_ASSERT(first.SharedIndexInputs);
        UNIT_ASSERT_VALUES_EQUAL(first.IndexDescriptors.size(), 1);
        UNIT_ASSERT(first.DataKind == NKqpProto::TColumnShardStepDetails::TStorageRead::mixed);
    }

    Y_UNIT_TEST(ForeignQueryIndexMetadataIsNotCopiedToOwner) {
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        auto foreign = std::make_shared<NScanStats::TCollector>(1);
        auto first = IndexContext(collector, 11);
        first.Merge(IndexContext(foreign, 12));
        UNIT_ASSERT(first.Collector == collector);
        UNIT_ASSERT(!first.SharedIndexInputs);
        UNIT_ASSERT_VALUES_EQUAL(first.IndexDescriptors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first.IndexDescriptors.Get(0).GetIndexId(), 11);
    }

    Y_UNIT_TEST(EmptyAccumulatorPreservesSharedContext) {
        auto collector = std::make_shared<NScanStats::TCollector>(1);
        auto shared = IndexContext(collector, 11);
        shared.Merge(IndexContext(collector, 12));
        TReadStatsContext accumulator;
        accumulator.Merge(shared);
        UNIT_ASSERT(accumulator.SharedIndexInputs);
        UNIT_ASSERT_VALUES_EQUAL(accumulator.IndexDescriptors.size(), 2);
    }
}
}   // namespace NKikimr::NOlap
