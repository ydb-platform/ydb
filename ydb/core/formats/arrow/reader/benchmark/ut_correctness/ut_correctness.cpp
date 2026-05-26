#include <library/cpp/testing/unittest/registar.h>
#include "../merges.h"
// ===========================================================================
// Юнит-тесты: TMergePartialStream и hash_first-pipeline должны выдавать
// идентичный отдедупленный результат на одних и тех же данных.
// ===========================================================================

Y_UNIT_TEST_SUITE(MergeBenchmarkEquivalence) {

    void CheckEquivalenceArrow20(int numSources) {
        TFixture fxOld{numSources};
        TFixture20 fxNew{numSources};

        auto resOld = MergeOnce(fxOld);
        auto resNew = HashFirstMergeOnce(fxNew);

        UNIT_ASSERT_VALUES_EQUAL(resOld->num_rows(), resNew->num_rows());
        UNIT_ASSERT_VALUES_EQUAL(resOld->num_columns(), 4);
        UNIT_ASSERT_VALUES_EQUAL(resNew->num_columns(), 4);

        // Сортируем resNew по ts чтобы привести к каноническому порядку:
        // GrouperFastImpl использует hash-таблицу — порядок group_id совпадает
        // с порядком первого encounter, но при батч-обработке он может отличаться
        // от входного порядка. Проверяем множество строк, а не позиционный порядок.
        {
            arrow20::compute::SortOptions tsSort({
                arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
            });
            auto idx = arrow20::compute::SortIndices(arrow20::Datum(resNew), tsSort).ValueOrDie();
            resNew = arrow20::compute::Take(arrow20::Datum(resNew), arrow20::Datum(idx))
                         .ValueOrDie().record_batch();
        }

        const auto& tsOld = static_cast<const arrow::TimestampArray&>(*resOld->column(0));
        const auto& aOld  = static_cast<const arrow::StringArray&>   (*resOld->column(1));
        const auto& bOld  = static_cast<const arrow::StringArray&>   (*resOld->column(2));
        const auto& vOld  = static_cast<const arrow::Int64Array&>    (*resOld->column(3));

        const auto& tsNew = static_cast<const arrow20::TimestampArray&>(*resNew->column(0));
        const auto& aNew  = static_cast<const arrow20::StringArray&>   (*resNew->column(1));
        const auto& bNew  = static_cast<const arrow20::StringArray&>   (*resNew->column(2));
        const auto& vNew  = static_cast<const arrow20::Int64Array&>    (*resNew->column(3));

        for (int64_t i = 0; i < resOld->num_rows(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(tsOld.Value(i), tsNew.Value(i), "row " << i << " ts");
            UNIT_ASSERT_VALUES_EQUAL_C(std::string(aOld.GetView(i)), std::string(aNew.GetView(i)),
                                       "row " << i << " a");
            UNIT_ASSERT_VALUES_EQUAL_C(std::string(bOld.GetView(i)), std::string(bNew.GetView(i)),
                                       "row " << i << " b");
            UNIT_ASSERT_VALUES_EQUAL_C(vOld.Value(i), vNew.Value(i), "row " << i << " ver");
        }
    }

    Y_UNIT_TEST(Arrow20TwoSourcesSmall)   { CheckEquivalenceArrow20(2); }
    Y_UNIT_TEST(Arrow20TenSourcesSmall)   { CheckEquivalenceArrow20(5); }
    Y_UNIT_TEST(Arrow20TwoSourcesLarger)  { CheckEquivalenceArrow20(10); }
    Y_UNIT_TEST(Arrow20TwentySourcesMid)  { CheckEquivalenceArrow20(20); }

    void CheckEquivalenceArrow5(int numSources) {
        TFixture fx{numSources};
        // AFL_VERIFY(false);

        auto resOld = arrow::Table::FromRecordBatches({MergeOnce(fx)}).ValueOrDie();
        auto resNew = MergeOnceArrow(fx);

        UNIT_ASSERT_VALUES_EQUAL(resOld->num_rows(), resNew->num_rows());
        UNIT_ASSERT_VALUES_EQUAL(resOld->num_columns(), 4);
        UNIT_ASSERT_VALUES_EQUAL(resNew->num_columns(), 4);

        // Сортируем resNew по ts чтобы привести к каноническому порядку:
        // GrouperFastImpl использует hash-таблицу — порядок group_id совпадает
        // с порядком первого encounter, но при батч-обработке он может отличаться
        // от входного порядка. Проверяем множество строк, а не позиционный порядок.
        {
            arrow::compute::SortOptions tsSort({
                arrow::compute::SortKey("ts", arrow::compute::SortOrder::Ascending),
            });
            auto idx = arrow::compute::SortIndices(arrow::Datum(resNew), tsSort).ValueOrDie();
            resNew = arrow::compute::Take(arrow::Datum(resNew), arrow::Datum(idx))
                         .ValueOrDie().table();
        }
        Cerr << "correct: " << resOld->ToString() << '\n';
        Cerr << "new: " << resNew->ToString() << '\n';

        UNIT_ASSERT(resOld->Equals(*resNew));
    }

    Y_UNIT_TEST(Arrow5TwoSourcesSmall)   { CheckEquivalenceArrow5(2); }
    Y_UNIT_TEST(Arrow5TenSourcesSmall)   { CheckEquivalenceArrow5(5); }
    Y_UNIT_TEST(Arrow5TwoSourcesLarger)  { CheckEquivalenceArrow5(10); }
    Y_UNIT_TEST(Arrow5TwentySourcesMid)  { CheckEquivalenceArrow5(20); }

}
