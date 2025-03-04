#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <reader/abstract/read_context.h>
#include <reader/common_reader/iterator/context.h>
#include <reader/common_reader/iterator/fetching.h>
#include <reader/simple_reader/iterator/context.h>

/*
    TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const NColumnShard::TConcreteScanCounters& counters, const TReadMetadataBase::TConstPtr& readMetadata, const TActorId& scanActorId,
        const TActorId& resourceSubscribeActorId, const TActorId& readCoordinatorActorId, const TComputeShardingPolicy& computeShardingPolicy,
        const ui64 scanId);
*/

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

Y_UNIT_TEST_SUITE(TestScript) {
    NSimple::TSpecialReadContext MakeTestReadContext(const THashMap<ui32, NTable::TColumn>& columns, const std::vector<ui32> pkIds = {0}) {
        auto columnsCopy = columns;
        for (ui64 i = 0; i < pkIds.size(); ++i) {
            TValidator::CheckNotNull(columnsCopy.FindPtr(pkIds[i]))->KeyOrder = i;
        }

        const auto versionedIndex = std::make_shared<TVersionedIndex>();
        auto cache = std::make_shared<TSchemaObjectsCache>();
        TIndexInfo info = TIndexInfo::BuildDefault(TTestStoragesManager::GetInstance(), columns, pkIds);
        AFL_VERIFY(info.BuildFromProto({}, nullptr, cache));
        versionedIndex->AddIndex(TSnapshot::Zero(), cache->UpsertIndexInfo(0, std::move(info)));

        const TReadDescription read(TSnapshot::Max(), false);

        const auto base = std::make_shared<NKikimr::NOlap::NReader::TReadContext>(TTestStoragesManager::GetInstance(), nullptr,
            NKikimr::NColumnShard::TConcreteScanCounters(NKikimr::NColumnShard::TScanCounters()),
            std::make_shared<NSimple::TReadMetadata>(versionedIndex, read), NActors::TActorId(), NActors::TActorId(), NActors::TActorId(),
            NKikimr::NOlap::NReader::TComputeShardingPolicy(), 0);
        return NSimple::TSpecialReadContext(base);
    }

    Y_UNIT_TEST(StepMerging) {
        NCommon::TFetchingScriptBuilder acc(
            MakeTestReadContext({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 1, NTable::TColumn("c1", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 2, NTable::TColumn("c2", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } }));

        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NSimple::EStageFeaturesIndexes::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NSimple::EStageFeaturesIndexes::Filter);
        acc.AddAssembleStep(std::vector<ui32>({ 0 }), "", NSimple::EStageFeaturesIndexes::Filter, false);

        acc.AddFetchingStep(std::vector<ui32>({ 0, 1 }), NSimple::EStageFeaturesIndexes::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 1, 2 }), NSimple::EStageFeaturesIndexes::Fetching);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NSimple::EStageFeaturesIndexes::Fetching);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NSimple::EStageFeaturesIndexes::Merge);

        auto script = std::move(acc).Build();
        UNIT_ASSERT_STRINGS_EQUAL(script->DebugString(), "");
    }
}
