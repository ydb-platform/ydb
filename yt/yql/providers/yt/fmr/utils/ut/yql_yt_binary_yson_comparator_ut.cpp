#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(TBinaryYsonComparatorTests) {

    Y_UNIT_TEST(CompareEqualRows) {
        TString textYson = "{name=\"Alice\";age=30};{name=\"Alice\";age=30}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT_EQUAL(result, 0);
    }

    Y_UNIT_TEST(CompareStringAscending) {
        TString textYson = "{name=\"Alice\";age=30};{name=\"Bob\";age=25}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // Alice < Bob

        result = comparator.CompareRows(rows[1], rows[0]);
        UNIT_ASSERT(result > 0);  // Bob > Alice
    }

    Y_UNIT_TEST(CompareStringDescending) {
        TString textYson = "{name=\"Alice\";age=30};{name=\"Bob\";age=25}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Descending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result > 0);  // Alice > Bob (descending)
    }

    Y_UNIT_TEST(CompareIntegersAscending) {
        TString textYson = "{name=\"Alice\";age=25};{name=\"Alice\";age=30}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0); // 25 < 30
    }

    Y_UNIT_TEST(CompareIntegersDescending) {
        TString textYson = "{name=\"Alice\";age=25};{name=\"Alice\";age=30}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Descending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result > 0);  // 25 > 30 (descending)
    }

    Y_UNIT_TEST(CompareMultipleColumns) {
        TString textYson = "{name=\"Alice\";age=30};{name=\"Alice\";age=25};{name=\"Bob\";age=20}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 3);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result > 0); // 30 > 25

        result = comparator.CompareRows(rows[0], rows[2]);
        UNIT_ASSERT(result < 0); // Alise < Bob
    }

    Y_UNIT_TEST(CompareWithNegativeIntegers) {
        TString textYson = "{id=-100;value=1};{id=50;value=2}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"id", "value"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // -100 < 50
    }

    Y_UNIT_TEST(CompareWithUnsignedIntegers) {
        TString textYson = "{id=100u;value=1};{id=200u;value=2}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"id", "value"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // 100u < 200u
    }

    Y_UNIT_TEST(CompareWithDoubles) {
        TString textYson = "{price=3.14;quantity=10};{price=2.71;quantity=20}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"price", "quantity"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result > 0);  // 3.14 > 2.71
    }

    Y_UNIT_TEST(CompareWithBooleans) {
        TString textYson = "{flag=%false;id=1};{flag=%true;id=2}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"flag", "id"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // false < true
    }

    Y_UNIT_TEST(CompareWithEntity) {
        TString textYson = "{value=#;id=1};{value=#;id=2}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"value", "id"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // id: 1 < 2
    }

    Y_UNIT_TEST(CompareEmptyStrings) {
        TString textYson = "{name=\"\";age=30};{name=\"Alice\";age=25}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // "" < "Alice"
    }

    Y_UNIT_TEST(CompareManyRows) {
        TString textYson =
            "{name=\"Alice\";age=30};"
            "{name=\"Bob\";age=25};"
            "{name=\"Charlie\";age=35};"
            "{name=\"David\";age=20};"
            "{name=\"Eve\";age=28}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name", "age"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 5);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        for (size_t i = 0; i < rows.size(); ++i) {
            for (size_t j = i + 1; j < rows.size(); ++j) {
                UNIT_ASSERT(comparator.CompareRows(rows[i], rows[j]) < 0);
            }
        }

        // Alice < Bob
        UNIT_ASSERT(comparator.CompareRows(rows[0], rows[1]) < 0);
        // Bob < Charlie
        UNIT_ASSERT(comparator.CompareRows(rows[1], rows[2]) < 0);
        // Alice < Charlie
        UNIT_ASSERT(comparator.CompareRows(rows[0], rows[2]) < 0);
    }

    Y_UNIT_TEST(CompareSingleColumn) {
        TString textYson = "{name=\"Alice\"};{name=\"Bob\"}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"name"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // Alice < Bob
    }

    Y_UNIT_TEST(CompareWithComplexTypes) {
        TString textYson = "{tags=[\"a\";\"b\"];id=1};{tags=[\"a\";\"c\"];id=2}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"tags", "id"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 2);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // [a,b] < [a,c]
    }

    Y_UNIT_TEST(CompareMixedSortOrders) {
        TString textYson =
            "{category=\"A\";priority=10};"
            "{category=\"A\";priority=5};"
            "{category=\"B\";priority=10}";
        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"category", "priority"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();
        const auto& rows = parser.GetRows();

        UNIT_ASSERT_EQUAL(rows.size(), 3);

        TVector<ESortOrder> sortOrders = {ESortOrder::Ascending, ESortOrder::Descending};
        TBinaryYsonComparator comparator(binaryYson, sortOrders);

        // A,10 vs A,5 -  10 < 5
        int result = comparator.CompareRows(rows[0], rows[1]);
        UNIT_ASSERT(result < 0);  // 10 < 5 в descending порядке

        // A,10 vs B,10
        result = comparator.CompareRows(rows[0], rows[2]);
        UNIT_ASSERT(result < 0);  // A < B
    }
}

} // namespace NYql::NFmr

