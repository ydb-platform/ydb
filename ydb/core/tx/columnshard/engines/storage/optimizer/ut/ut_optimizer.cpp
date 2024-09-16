#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/splitter/rb_splitter.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/intervals/optimizer.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/serializer/full.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

Y_UNIT_TEST_SUITE(StorageOptimizer) {

    using namespace NKikimr::NArrow;

    class TPortionsMaker {
    private:
        ui64 PortionId = 0;
    public:

        static TReplaceKey MakeKey(const i64 value) {
            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int64Type>>>(
                "pk", NConstruction::TIntSeqFiller<arrow::Int64Type>(value));
            return TReplaceKey({column->BuildArray(1)}, 0);
        }

        std::shared_ptr<NKikimr::NOlap::TPortionInfo> Make(const i64 pkStart, const i64 pkFinish, const i64 size) {
            auto result = std::make_shared<NKikimr::NOlap::TPortionInfo>(0, ++PortionId, NKikimr::NOlap::TSnapshot(1, 1));

            result->MutableMeta().IndexKeyStart = MakeKey(pkStart);
            result->MutableMeta().IndexKeyEnd = MakeKey(pkFinish);

            result->Records.emplace_back(NKikimr::NOlap::TColumnRecord::TTestInstanceBuilder::Build(1, 0, 0, size, 1, 1));
            return result;
        }
    };

    Y_UNIT_TEST(Empty) {
        TPortionsMaker maker;
        NKikimr::NOlap::NStorageOptimizer::TIntervalsOptimizerPlanner planner(0);
        planner.AddPortion(maker.Make(1, 100, 10000));
        Cerr << planner.GetDescription() << Endl;
        NKikimr::NOlap::TCompactionLimits limits;
        auto task = planner.GetOptimizationTask(limits, nullptr);
        Y_ABORT_UNLESS(!task);
    }

    Y_UNIT_TEST(MergeSmall) {
        TPortionsMaker maker;
        NKikimr::NOlap::NStorageOptimizer::TIntervalsOptimizerPlanner planner(0);
        planner.AddPortion(maker.Make(1, 100, 10000));
        planner.AddPortion(maker.Make(101, 200, 10000));
        Cerr << planner.GetDescription() << Endl;
        NKikimr::NOlap::TCompactionLimits limits;
        auto task = dynamic_pointer_cast<NKikimr::NOlap::TCompactColumnEngineChanges>(planner.GetOptimizationTask(limits, nullptr));
        Y_ABORT_UNLESS(task);
        Y_ABORT_UNLESS(task->SwitchedPortions.size() == 2);
    }

    Y_UNIT_TEST(MergeSmall1) {
        TPortionsMaker maker;
        NKikimr::NOlap::NStorageOptimizer::TIntervalsOptimizerPlanner planner(0);
        planner.AddPortion(maker.Make(1, 100, 10000));
        planner.AddPortion(maker.Make(2, 101, 20000));
        planner.AddPortion(maker.Make(10, 200, 30000));
        planner.AddPortion(maker.Make(10, 20, 40000));
        Cerr << planner.GetDescription() << Endl;
        NKikimr::NOlap::TCompactionLimits limits;
        auto task = dynamic_pointer_cast<NKikimr::NOlap::TCompactColumnEngineChanges>(planner.GetOptimizationTask(limits, nullptr));
        Y_ABORT_UNLESS(task);
        Y_ABORT_UNLESS(task->SwitchedPortions.size() == 4);
    }

    Y_UNIT_TEST(MergeSmall2) {
        TPortionsMaker maker;
        NKikimr::NOlap::NStorageOptimizer::TIntervalsOptimizerPlanner planner(0);
        planner.AddPortion(maker.Make(1, 10000, 10000000));
        planner.AddPortion(maker.Make(1, 100, 10000));
        planner.AddPortion(maker.Make(2, 101, 20000));
        planner.AddPortion(maker.Make(1000, 20000, 30000));
        planner.AddPortion(maker.Make(1000, 2000, 40000));
        Cerr << planner.GetDescription() << Endl;
        NKikimr::NOlap::TCompactionLimits limits;
        auto task = dynamic_pointer_cast<NKikimr::NOlap::TCompactColumnEngineChanges>(planner.GetOptimizationTask(limits, nullptr));
        Y_ABORT_UNLESS(task);
        Y_ABORT_UNLESS(task->SwitchedPortions.size() == 2);
        Y_ABORT_UNLESS(task->SwitchedPortions[0].GetPortion() == 1);
        Y_ABORT_UNLESS(task->SwitchedPortions[1].GetPortion() == 2);
    }

};
