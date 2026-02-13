#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/yql/providers/yt/fmr/test_tools/fmr_merge_reader/yql_yt_fmr_merge_reader.cpp>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(TSortedMergeReaderTests) {
    Y_UNIT_TEST(MergeFromFileGatewayDifferentYtSourceOrder) {
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<ESortOrder> sortOrders(keyColumns.size(), ESortOrder::Ascending);
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"=\"a\"};\n"
                    "{\"k\"=3;\"v\"=\"c\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=2;\"v\"=\"b\"};\n"
                    "{\"k\"=4;\"v\"=\"d\"};\n"
            },
        };

        TVector<IBlockIterator::TPtr> inputs = MakeTestIterators(rawTestTables);
        const TString mergedText = GetMergeResult(inputs, sortOrders);

        const TString expected =
            "{\"k\"=1;\"v\"=\"a\"};\n"
            "{\"k\"=2;\"v\"=\"b\"};\n"
            "{\"k\"=3;\"v\"=\"c\"};\n"
            "{\"k\"=4;\"v\"=\"d\"};\n";

        UNIT_ASSERT_NO_DIFF(mergedText, expected);
    }

    Y_UNIT_TEST(MergeStableSortByInputOrder) {
        auto tds = MakeLocalTableDataService();
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<ESortOrder> sortOrders(keyColumns.size(), ESortOrder::Ascending);
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"=\"1\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::TDS,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"=\"2\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"=\"3\"};\n"
            }
        };

        TVector<IBlockIterator::TPtr> inputs = MakeTestIterators(rawTestTables, tds);
        const TString mergedText = GetMergeResult(inputs, sortOrders);

        const TString expected =
            "{\"k\"=1;\"v\"=\"1\"};\n"
            "{\"k\"=1;\"v\"=\"2\"};\n"
            "{\"k\"=1;\"v\"=\"3\"};\n";

        UNIT_ASSERT_NO_DIFF(mergedText, expected);
    }

    Y_UNIT_TEST(MergeFromTdsDifferentSourceOrder) {
        auto tds = MakeLocalTableDataService();
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<ESortOrder> sortOrders(keyColumns.size(), ESortOrder::Ascending);
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"=\"a\"};\n"
                    "{\"k\"=5;\"v\"=\"e\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::TDS,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=2;\"v\"=\"b\"};\n"
                    "{\"k\"=6;\"v\"=\"f\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=3;\"v\"=\"c\"};\n"
                    "{\"k\"=7;\"v\"=\"g\"};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::TDS,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=4;\"v\"=\"d\"};\n"
                    "{\"k\"=8;\"v\"=\"h\"};\n"
            }
        };

        TVector<IBlockIterator::TPtr> inputs = MakeTestIterators(rawTestTables, tds);
        const TString mergedText = GetMergeResult(inputs, sortOrders);

        const TString expected =
            "{\"k\"=1;\"v\"=\"a\"};\n"
            "{\"k\"=2;\"v\"=\"b\"};\n"
            "{\"k\"=3;\"v\"=\"c\"};\n"
            "{\"k\"=4;\"v\"=\"d\"};\n"
            "{\"k\"=5;\"v\"=\"e\"};\n"
            "{\"k\"=6;\"v\"=\"f\"};\n"
            "{\"k\"=7;\"v\"=\"g\"};\n"
            "{\"k\"=8;\"v\"=\"h\"};\n";

        UNIT_ASSERT_NO_DIFF(mergedText, expected);
    }

    Y_UNIT_TEST(MergeFromYtWithNestedStructures) {
        auto tds = MakeLocalTableDataService();
        TVector<TString> keyColumns = {"k"};
        TVector<TString> neededColumns = {"k", "v"};
        TVector<ESortOrder> sortOrders(keyColumns.size(), ESortOrder::Ascending);
        TVector<TMergeTestTable> rawTestTables = {
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21}};\n"
                    "{\"k\"=3;\"v\"={\"name\"=\"Bob\";\"age\"=25}};\n"
            },
            {
                .SourceType = EMergeReaderSourceType::YT,
                .KeyColumns = keyColumns,
                .NeededColumns = neededColumns,
                .RawTableBody =
                    "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21}};\n"
                    "{\"k\"=4;\"v\"={\"name\"=\"Bob\";\"age\"=25}};\n"
            }
        };
        TVector<IBlockIterator::TPtr> inputs = MakeTestIterators(rawTestTables, tds);
        const TString mergedText = GetMergeResult(inputs, sortOrders);

        const TString expected =
            "{\"k\"=1;\"v\"={\"name\"=\"Alice\";\"age\"=21}};\n"
            "{\"k\"=2;\"v\"={\"name\"=\"Alice\";\"age\"=21}};\n"
            "{\"k\"=3;\"v\"={\"name\"=\"Bob\";\"age\"=25}};\n"
            "{\"k\"=4;\"v\"={\"name\"=\"Bob\";\"age\"=25}};\n";


        UNIT_ASSERT_NO_DIFF(mergedText, expected);
    }
}

} // namespace NYql::NFmr

