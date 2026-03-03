#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/test_tools/fmr_block_iterator/yql_yt_fmr_block_iterator.h>

namespace NYql::NFmr {

TString GetSortedResult(const TVector<IBlockIterator::TPtr>& inputs) {
    auto reader = MakeIntrusive<TFmrSortingBlockReader>(inputs);
    const TString sortedBinaryYson = reader->ReadAll();
    return GetTextYson(sortedBinaryYson);
}

Y_UNIT_TEST_SUITE(SortingBlockReaderTests) {
    Y_UNIT_TEST(SortInputsInWrongOrder) {
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=3;\"v\"=\"c\"};\n"
                    "{\"k\"=1;\"v\"=\"a\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=4;\"v\"=\"d\"};\n"
                    "{\"k\"=5;\"v\"=\"e\"};\n"
                    "{\"k\"=2;\"v\"=\"b\"};\n"
            },
        };

        TVector<IBlockIterator::TPtr> inputs = MakeTestBlockIterators(rawTestTables);
        const TString sortedText = GetSortedResult(inputs);

        const TString expected =
            "{\"k\"=1;\"v\"=\"a\"};\n"
            "{\"k\"=2;\"v\"=\"b\"};\n"
            "{\"k\"=3;\"v\"=\"c\"};\n"
            "{\"k\"=4;\"v\"=\"d\"};\n"
            "{\"k\"=5;\"v\"=\"e\"};\n";

        UNIT_ASSERT_NO_DIFF(sortedText, expected);
    }

    Y_UNIT_TEST(SortFromTdsDifferentSourceOrder) {
        auto tds = MakeLocalTableDataService();
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=5;\"v\"=\"e\"};\n"
                    "{\"k\"=4;\"v\"=\"a\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::TableDataService,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=2;\"v\"=\"b\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=6;\"v\"=\"f\"};\n"
                    "{\"k\"=3;\"v\"=\"c\"};\n"
                    "{\"k\"=7;\"v\"=\"g\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::TableDataService,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=8;\"v\"=\"h\"};\n"
                    "{\"k\"=1;\"v\"=\"d\"};\n"
            }
        };

        TVector<IBlockIterator::TPtr> inputs = MakeTestBlockIterators(rawTestTables, tds);
        const TString sortedText = GetSortedResult(inputs);

        const TString expected =
            "{\"k\"=1;\"v\"=\"d\"};\n"
            "{\"k\"=2;\"v\"=\"b\"};\n"
            "{\"k\"=3;\"v\"=\"c\"};\n"
            "{\"k\"=4;\"v\"=\"a\"};\n"
            "{\"k\"=5;\"v\"=\"e\"};\n"
            "{\"k\"=6;\"v\"=\"f\"};\n"
            "{\"k\"=7;\"v\"=\"g\"};\n"
            "{\"k\"=8;\"v\"=\"h\"};\n";

        UNIT_ASSERT_NO_DIFF(sortedText, expected);
    }

    Y_UNIT_TEST(SeveralKeyColumnsAndDescendingSortOrder) {
        auto tds = MakeLocalTableDataService();
        TVector<TString> keyColumns = {"k", "z"};
        TVector<TString> neededColumns = {"k", "v", "z"};
        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Descending};
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=1};\n"
                    "{\"k\"=3;\"v\"={\"name\"=\"Bob\";\"age\"=25};\"z\"=2};\n"
                    "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=2};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=4;\"v\"={\"name\"=\"Bob\";\"age\"=25};\"z\"=2};\n"
                    "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=3};\n"
                    "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=6};\n"
            }
        };
        TVector<IBlockIterator::TPtr> inputs = MakeTestBlockIterators(rawTestTables, tds, {}, sortOrders);
        const TString sortedText = GetSortedResult(inputs);

        const TString expected =
            "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=2};\n"
            "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=1};\n"
            "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=6};\n"
            "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21};\"z\"=3};\n"
            "{\"k\"=3;\"v\"={\"name\"=\"Bob\";\"age\"=25};\"z\"=2};\n"
            "{\"k\"=4;\"v\"={\"name\"=\"Bob\";\"age\"=25};\"z\"=2};\n";

        UNIT_ASSERT_NO_DIFF(sortedText, expected);
    }
}

} // namespace NYql::NFmr
