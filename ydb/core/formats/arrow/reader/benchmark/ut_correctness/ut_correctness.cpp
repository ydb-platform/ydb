#include <library/cpp/testing/unittest/registar.h>
#include "../merges.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <tuple>
// ===========================================================================
// Юнит-тесты: TMergePartialStream и hash_first-pipeline должны выдавать
// идентичный отдедупленный результат на одних и тех же данных.
// ===========================================================================

Y_UNIT_TEST_SUITE(MergeBenchmarkEquivalence) {

    struct TRow {
        int64_t Ts = 0;
        TString A;
        TString B;
        int64_t Ver = 0;
    };

    bool RowLess(const TRow& l, const TRow& r) {
        if (l.Ts != r.Ts) {
            return l.Ts < r.Ts;
        }
        if (l.A != r.A) {
            return l.A < r.A;
        }
        if (l.B != r.B) {
            return l.B < r.B;
        }
        return l.Ver > r.Ver;
    }

    bool SameKey(const TRow& l, const TRow& r) {
        return std::tie(l.Ts, l.A, l.B) == std::tie(r.Ts, r.A, r.B);
    }

    std::shared_ptr<arrow::RecordBatch> MakeBatchFromRows(const std::vector<TRow>& rows) {
        arrow::TimestampBuilder tsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        arrow::StringBuilder aBuilder, bBuilder;
        arrow::Int64Builder verBuilder;

        for (const auto& row : rows) {
            Y_ABORT_UNLESS(tsBuilder.Append(row.Ts).ok());
            Y_ABORT_UNLESS(aBuilder.Append(row.A.data(), row.A.size()).ok());
            Y_ABORT_UNLESS(bBuilder.Append(row.B.data(), row.B.size()).ok());
            Y_ABORT_UNLESS(verBuilder.Append(row.Ver).ok());
        }

        std::shared_ptr<arrow::Array> tsArr, aArr, bArr, verArr;
        Y_ABORT_UNLESS(tsBuilder.Finish(&tsArr).ok());
        Y_ABORT_UNLESS(aBuilder.Finish(&aArr).ok());
        Y_ABORT_UNLESS(bBuilder.Finish(&bArr).ok());
        Y_ABORT_UNLESS(verBuilder.Finish(&verArr).ok());

        return arrow::RecordBatch::Make(MakeFullSchema(), rows.size(), {tsArr, aArr, bArr, verArr});
    }

    std::shared_ptr<arrow20::RecordBatch> MakeBatch20FromRows(const std::vector<TRow>& rows) {
        arrow20::TimestampBuilder tsBuilder(arrow20::timestamp(arrow20::TimeUnit::MICRO), arrow20::default_memory_pool());
        arrow20::StringBuilder aBuilder, bBuilder;
        arrow20::Int64Builder verBuilder;

        for (const auto& row : rows) {
            Y_ABORT_UNLESS(tsBuilder.Append(row.Ts).ok());
            Y_ABORT_UNLESS(aBuilder.Append(row.A.data(), row.A.size()).ok());
            Y_ABORT_UNLESS(bBuilder.Append(row.B.data(), row.B.size()).ok());
            Y_ABORT_UNLESS(verBuilder.Append(row.Ver).ok());
        }

        std::shared_ptr<arrow20::Array> tsArr, aArr, bArr, verArr;
        Y_ABORT_UNLESS(tsBuilder.Finish(&tsArr).ok());
        Y_ABORT_UNLESS(aBuilder.Finish(&aArr).ok());
        Y_ABORT_UNLESS(bBuilder.Finish(&bArr).ok());
        Y_ABORT_UNLESS(verBuilder.Finish(&verArr).ok());

        return arrow20::RecordBatch::Make(MakeFullSchema20(), rows.size(), {tsArr, aArr, bArr, verArr});
    }

    std::pair<TFixture, TFixture20> MakeRandomFixtures(int numSources, int numKeys, int duplicatesPerKey, ui64 seed) {
        UNIT_ASSERT_C(duplicatesPerKey <= numSources, "duplicatesPerKey must fit into distinct sources");

        std::mt19937_64 rng(seed);
        std::vector<std::vector<TRow>> rowsBySource(numSources);
        std::vector<int> sources(numSources);
        std::iota(sources.begin(), sources.end(), 0);

        for (int key = 0; key < numKeys; ++key) {
            std::shuffle(sources.begin(), sources.end(), rng);
            for (int version = 0; version < duplicatesPerKey; ++version) {
                rowsBySource[sources[version]].push_back({
                    .Ts = static_cast<int64_t>(key / 3) * 1000,
                    .A = ToString(key % 17),
                    .B = ToString(key),
                    .Ver = version,
                });
            }
        }

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::vector<std::shared_ptr<arrow20::RecordBatch>> batches20;
        batches.reserve(numSources);
        batches20.reserve(numSources);
        for (auto& rows : rowsBySource) {
            std::sort(rows.begin(), rows.end(), RowLess);
            for (size_t i = 1; i < rows.size(); ++i) {
                UNIT_ASSERT_C(!SameKey(rows[i - 1], rows[i]), "source batch contains duplicated PK");
            }
            batches.push_back(MakeBatchFromRows(rows));
            batches20.push_back(MakeBatch20FromRows(rows));
        }

        return {TFixture(std::move(batches)), TFixture20(std::move(batches20))};
    }

    void AssertEqualRows(const std::shared_ptr<arrow::RecordBatch>& resOld, const std::shared_ptr<arrow20::RecordBatch>& resNew) {
        UNIT_ASSERT_VALUES_EQUAL(resOld->num_rows(), resNew->num_rows());
        UNIT_ASSERT_VALUES_EQUAL(resOld->num_columns(), 4);
        UNIT_ASSERT_VALUES_EQUAL(resNew->num_columns(), 4);

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
            UNIT_ASSERT_VALUES_EQUAL_C(std::string(aOld.GetView(i)), std::string(aNew.GetView(i)), "row " << i << " a");
            UNIT_ASSERT_VALUES_EQUAL_C(std::string(bOld.GetView(i)), std::string(bNew.GetView(i)), "row " << i << " b");
            UNIT_ASSERT_VALUES_EQUAL_C(vOld.Value(i), vNew.Value(i), "row " << i << " ver");
        }
    }

    std::shared_ptr<arrow20::RecordBatch> SortBatch20(const std::shared_ptr<arrow20::RecordBatch>& batch) {
        arrow20::compute::SortOptions sort({
            arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
            arrow20::compute::SortKey("a", arrow20::compute::SortOrder::Ascending),
            arrow20::compute::SortKey("b", arrow20::compute::SortOrder::Ascending),
        });
        auto idx = arrow20::compute::SortIndices(arrow20::Datum(batch), sort).ValueOrDie();
        return arrow20::compute::Take(arrow20::Datum(batch), arrow20::Datum(idx)).ValueOrDie().record_batch();
    }

    std::shared_ptr<arrow20::RecordBatch> SortTable20(const std::shared_ptr<arrow20::Table>& table) {
        arrow20::compute::SortOptions sort({
            arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
            arrow20::compute::SortKey("a", arrow20::compute::SortOrder::Ascending),
            arrow20::compute::SortKey("b", arrow20::compute::SortOrder::Ascending),
        });
        auto idx = arrow20::compute::SortIndices(arrow20::Datum(table), sort).ValueOrDie();
        auto sortedTable = arrow20::compute::Take(arrow20::Datum(table), arrow20::Datum(idx)).ValueOrDie().table();
        auto combined = sortedTable->CombineChunks().ValueOrDie();

        std::vector<std::shared_ptr<arrow20::Array>> columns;
        columns.reserve(combined->num_columns());
        for (int i = 0; i < combined->num_columns(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(combined->column(i)->num_chunks(), 1);
            columns.push_back(combined->column(i)->chunk(0));
        }
        return arrow20::RecordBatch::Make(combined->schema(), combined->num_rows(), columns);
    }

    void CheckRandomEquivalence(int numSources, int numKeys, int duplicatesPerKey, ui64 seed) {
        auto [fxOld, fxNew] = MakeRandomFixtures(numSources, numKeys, duplicatesPerKey, seed);
        auto resOld = MergeOnce(fxOld);

        auto resArrow20 = SortTable20(MergeOnceArrow20(fxNew));
        AssertEqualRows(resOld, resArrow20);

        auto resHashFirst = SortBatch20(HashFirstMergeOnce(fxNew));
        AssertEqualRows(resOld, resHashFirst);
    }

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
        TFixture20 fx20{numSources};
        // AFL_VERIFY(false);

        auto resOld = arrow::Table::FromRecordBatches({MergeOnce(fx)}).ValueOrDie();
        auto resNew = MergeOnceArrow20(fx20);

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
            arrow20::compute::SortOptions tsSort20({
                arrow20::compute::SortKey("ts", arrow20::compute::SortOrder::Ascending),
            });
            auto idx = arrow20::compute::SortIndices(arrow20::Datum(resNew), tsSort20).ValueOrDie();
            resNew = arrow20::compute::Take(arrow20::Datum(resNew), arrow20::Datum(idx))
                         .ValueOrDie().table();
        }
        Cerr << "correct: " << resOld->ToString() << '\n';
        Cerr << "new: " << resNew->ToString() << '\n';
        
        UNIT_ASSERT_VALUES_EQUAL(resOld->num_rows(), resNew->num_rows());
    }

    Y_UNIT_TEST(Arrow5TwoSourcesSmall)   { CheckEquivalenceArrow5(2); }
    Y_UNIT_TEST(Arrow5TenSourcesSmall)   { CheckEquivalenceArrow5(5); }
    Y_UNIT_TEST(Arrow5TwoSourcesLarger)  { CheckEquivalenceArrow5(10); }
    Y_UNIT_TEST(Arrow5TwentySourcesMid)  { CheckEquivalenceArrow5(20); }

    Y_UNIT_TEST(RandomTwoSourcesDuplicates) {
        CheckRandomEquivalence(2, 1000, 2, 0x6d6572676532ULL);
    }

    Y_UNIT_TEST(RandomFiveSourcesDuplicates) {
        CheckRandomEquivalence(5, 5000, 3, 0x6d6572676535ULL);
    }

    Y_UNIT_TEST(RandomTwentySourcesDuplicates) {
        CheckRandomEquivalence(20, 10000, 4, 0x6d65726765320ULL);
    }

}
