#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <reader/common_reader/iterator/fetching.h>
#include <reader/common_reader/iterator/source.h>
#include <reader/simple_reader/iterator/fetching.h>
#include <scheme/versions/snapshot_scheme.h>

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

namespace {

struct TSourceHandoffState {
    std::mutex Mutex;
    std::condition_variable Ready;
    std::shared_ptr<NCommon::IDataSource> Pending;
    const NCommon::IDataSource* Observed = nullptr;
    bool Acquired = false;
    bool Release = false;
};

class TAsyncOwnershipStep: public NCommon::IFetchingStep {
private:
    NCommon::TExecutionContext& ExecutionContext;
    const std::shared_ptr<TSourceHandoffState> Handoff;

protected:
    virtual TConclusion<bool> DoExecuteInplace(
        std::shared_ptr<NCommon::IDataSource>&& source, const NCommon::TFetchingScriptCursor& /*cursor*/) const override {
        auto ownershipGuard = ExecutionContext.GuardSourceOwnership(std::move(source), source);
        auto continuation = ExecutionContext.ExtractSourceOwnership();
        {
            std::lock_guard guard(Handoff->Mutex);
            UNIT_ASSERT(!Handoff->Pending);
            Handoff->Pending = std::move(continuation);
        }
        Handoff->Ready.notify_one();

        std::unique_lock lock(Handoff->Mutex);
        Handoff->Ready.wait(lock, [&]() {
            return Handoff->Acquired;
        });
        return false;
    }

public:
    TAsyncOwnershipStep(NCommon::TExecutionContext& executionContext, std::shared_ptr<TSourceHandoffState> handoff)
        : IFetchingStep("ASYNC_OWNERSHIP_TEST")
        , ExecutionContext(executionContext)
        , Handoff(std::move(handoff))
    {
    }
};

}   // namespace

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

    Y_UNIT_TEST(AsyncStepTransfersSourceOwnership) {
        NCommon::TExecutionContext executionContext;
        auto handoff = std::make_shared<TSourceHandoffState>();
        auto step = std::make_shared<TAsyncOwnershipStep>(executionContext, handoff);
        std::vector<std::shared_ptr<NCommon::IFetchingStep>> steps = { step };
        auto script = std::make_shared<NCommon::TFetchingScript>("OWNERSHIP_TEST", std::move(steps));
        NCommon::TFetchingScriptCursor cursor(script, 0);

        auto backing = std::make_shared<std::max_align_t>();
        std::weak_ptr<std::max_align_t> weakBacking = backing;
        auto source = std::shared_ptr<NCommon::IDataSource>(backing, reinterpret_cast<NCommon::IDataSource*>(backing.get()));
        const auto* expected = source.get();
        backing.reset();

        std::thread continuation([handoff]() {
            std::shared_ptr<NCommon::IDataSource> owned;
            std::unique_lock lock(handoff->Mutex);
            handoff->Ready.wait(lock, [&]() {
                return !!handoff->Pending;
            });
            owned = std::move(handoff->Pending);
            handoff->Observed = owned.get();
            handoff->Acquired = true;
            handoff->Ready.notify_one();
            handoff->Ready.wait(lock, [&]() {
                return handoff->Release;
            });
        });

        const auto result = step->ExecuteInplace(std::move(source), cursor);
        const bool sourceWasTransferred = !source;
        const bool contextWasEmptied = !executionContext.HasSourceOwnership();
        const NCommon::IDataSource* observed = nullptr;
        long ownersWhileContinuationBlocked = 0;
        {
            std::lock_guard guard(handoff->Mutex);
            observed = handoff->Observed;
            ownersWhileContinuationBlocked = weakBacking.use_count();
            handoff->Release = true;
        }
        handoff->Ready.notify_one();
        continuation.join();

        UNIT_ASSERT(!result.IsFail());
        UNIT_ASSERT(!*result);
        UNIT_ASSERT(sourceWasTransferred);
        UNIT_ASSERT(contextWasEmptied);
        UNIT_ASSERT_VALUES_EQUAL((const void*)observed, (const void*)expected);
        UNIT_ASSERT_VALUES_EQUAL(ownersWhileContinuationBlocked, 1);
        UNIT_ASSERT(weakBacking.expired());
    }

    Y_UNIT_TEST(SourceOwnershipIsRestoredOnException) {
        NCommon::TExecutionContext executionContext;
        auto backing = std::make_shared<std::max_align_t>();
        std::weak_ptr<std::max_align_t> weakBacking = backing;
        auto source = std::shared_ptr<NCommon::IDataSource>(backing, reinterpret_cast<NCommon::IDataSource*>(backing.get()));
        const auto* expected = source.get();
        backing.reset();

        try {
            auto ownershipGuard = executionContext.GuardSourceOwnership(std::move(source), source);
            UNIT_ASSERT(!source);
            UNIT_ASSERT(executionContext.HasSourceOwnership());
            throw std::runtime_error("test exception");
        } catch (const std::runtime_error&) {
        }

        UNIT_ASSERT_VALUES_EQUAL((const void*)source.get(), (const void*)expected);
        UNIT_ASSERT(!executionContext.HasSourceOwnership());
        UNIT_ASSERT_VALUES_EQUAL(weakBacking.use_count(), 1);
        source.reset();
        UNIT_ASSERT(weakBacking.expired());
    }
}
