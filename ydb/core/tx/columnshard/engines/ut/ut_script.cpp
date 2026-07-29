#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <reader/common_reader/iterator/fetching.h>
#include <reader/simple_reader/iterator/fetching.h>
#include <scheme/versions/snapshot_scheme.h>

#include <thread>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

Y_UNIT_TEST_SUITE(TestScript) {
    std::shared_ptr<ISnapshotSchema> MakeTestSchema(THashMap<ui32, NTable::TColumn> columns, const std::vector<ui32> pkIds = { 0 }) {
        for (ui64 i = 0; i < pkIds.size(); ++i) {
            TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
        }

        auto cache = std::make_shared<TSchemaObjectsCache>();
        TIndexInfo info = TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
        return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(info)), TSnapshot(1, 1));
    }

    Y_UNIT_TEST(StepMerging) {
        NCommon::TFetchingScriptBuilder acc = NCommon::TFetchingScriptBuilder::MakeForTests(
            MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 1, NTable::TColumn("c1", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 2, NTable::TColumn("c2", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } }));

        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddAssembleStep(std::vector<ui32>({ 0 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        acc.AddStep(std::make_shared<NSimple::TDeletionFilter>());
        acc.AddFetchingStep(std::vector<ui32>({ 0, 1 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 1, 2 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddAssembleStep(std::vector<ui32>({ 0, 1, 2 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, false);
        acc.AddStep(std::make_shared<NSimple::TDeletionFilter>());
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Merge);

        auto script = std::move(acc).Build();
        UNIT_ASSERT_STRINGS_EQUAL(script->DebugString(),
            "{branch:UNDEFINED;steps:["
            "{name=ALLOCATE_MEMORY::FILTER;details={stage=FILTER;column_ids=[Blob:0,Raw:0];};};"
            "{name=FETCHING_COLUMNS;details={columns=0;};};"
            "{name=ASSEMBLER;details={columns=(column_ids=0;column_names=c0;);;};};"
            "{name=DELETION;details={};};"
            "{name=ALLOCATE_MEMORY::FILTER;details={stage=FILTER;column_ids=[Blob:1];};};"
            "{name=ALLOCATE_MEMORY::FETCHING;details={stage=FETCHING;column_ids=[Blob:2,Raw:1,Raw:2];};};"
            "{name=FETCHING_COLUMNS;details={columns=1,2;};};"
            "{name=ASSEMBLER;details={columns=(column_ids=1,2;column_names=c1,c2;);;};};"
            "{name=DELETION;details={};};]}");
    }

    // regression for #47943: reading the owner from another thread while the script is being
    // published must not observe a torn shared_ptr
    Y_UNIT_TEST(OwnerConcurrentInitialization) {
        static constexpr ui32 RacesCount = 5000;
        static constexpr ui32 ReaderAttemptsCount = 100000;
        auto schema = MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } });
        for (ui32 i = 0; i < RacesCount; ++i) {
            NCommon::TFetchingScriptBuilder acc = NCommon::TFetchingScriptBuilder::MakeForTests(schema);
            acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
            acc.AddAssembleStep(std::vector<ui32>({ 0 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            auto script = std::move(acc).Build();

            const NCommon::TFetchingScript* published = script.get();
            NCommon::TFetchingScriptOwner owner;
            std::atomic<bool> readerStarted = false;
            std::atomic<const NCommon::TFetchingScript*> observed = nullptr;
            std::thread reader([&]() {
                readerStarted = true;
                for (ui32 j = 0; j < ReaderAttemptsCount; ++j) {
                    if (owner.HasScript()) {
                        observed = owner.GetScriptVerified().get();
                        break;
                    }
                }
            });
            while (!readerStarted) {
            }
            auto guard = owner.StartInitialization();
            UNIT_ASSERT(guard);
            guard->InitializationFinished(std::move(script));
            reader.join();
            UNIT_ASSERT(!owner.NeedInitialization());
            if (observed) {
                UNIT_ASSERT_VALUES_EQUAL((const void*)observed.load(), (const void*)published);
            }
            UNIT_ASSERT_VALUES_EQUAL((const void*)owner.GetScriptVerified().get(), (const void*)published);
            UNIT_ASSERT(!owner.GetScriptVerified()->IsFinished(0));
        }
    }
}
