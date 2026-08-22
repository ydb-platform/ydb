#include <yt/yql/providers/yt/fmr/utils/yql_yt_index_serialisation.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/str.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(TIndexSerialisationTests) {

    Y_UNIT_TEST(SerializeDeserializeSimple) {
        TString textYson =
            "{column1=\"value1\";column2=\"value2\";column3=\"value3\"};"
            "{column1=\"value4\";column2=\"value5\";column3=\"value6\"}";

        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"column1", "column2", "column3"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();

        const auto& parsedRows = parser.GetRows();
        UNIT_ASSERT_EQUAL(parsedRows.size(), 2);

        TSortedRowMetadata originalData;
        originalData.KeyColumns = keyColumns;
        originalData.Rows = parsedRows;

        TString serialized;
        TStringOutput out(serialized);
        originalData.Save(&out);

        UNIT_ASSERT(!serialized.empty());

        TSortedRowMetadata data;
        TStringInput in(serialized);
        data.Load(&in, keyColumns);

        UNIT_ASSERT_EQUAL(data.Rows.size(), 2);

        for (size_t i = 0; i < data.Rows.size(); ++i) {
            UNIT_ASSERT_EQUAL(data.Rows[i].size(), parsedRows[i].size());
            for (size_t j = 0; j < data.Rows[i].size(); ++j) {
                UNIT_ASSERT_EQUAL(data.Rows[i][j].StartOffset, parsedRows[i][j].StartOffset);
                UNIT_ASSERT_EQUAL(data.Rows[i][j].EndOffset, parsedRows[i][j].EndOffset);
            }
        }
    }

    Y_UNIT_TEST(SerializeDeserializeWithLargeOffsets) {
        TStringStream textYsonStream;
        for (int i = 0; i < 100; ++i) {
            textYsonStream << "{key=\"key_" << i << "\";value=\"";
            for (int j = 0; j < 10000; ++j) {
                textYsonStream << "data";
            }
            textYsonStream << "\"};";
        }

        TString binaryYson = GetBinaryYson(textYsonStream.Str());

        TVector<TString> keyColumns = {"key", "value"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();

        const auto& parsedRows = parser.GetRows();
        UNIT_ASSERT_EQUAL(parsedRows.size(), 100);

        TSortedRowMetadata originalData;
        originalData.KeyColumns = keyColumns;
        for (size_t i = 0; i < 3 && i < parsedRows.size(); ++i) {
            originalData.Rows.push_back(parsedRows[i]);
        }

        TString serialized;
        TStringOutput out(serialized);
        originalData.Save(&out);

        TSortedRowMetadata data;
        TStringInput in(serialized);
        data.Load(&in, keyColumns);

        UNIT_ASSERT_EQUAL(data.Rows.size(), 3);

        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(data.Rows[i].size(), 2 + 1);
            UNIT_ASSERT_EQUAL(data.Rows[i][0].StartOffset, originalData.Rows[i][0].StartOffset);
            UNIT_ASSERT_EQUAL(data.Rows[i][0].EndOffset, originalData.Rows[i][0].EndOffset);
            UNIT_ASSERT_EQUAL(data.Rows[i][1].StartOffset, originalData.Rows[i][1].StartOffset);
            UNIT_ASSERT_EQUAL(data.Rows[i][1].EndOffset, originalData.Rows[i][1].EndOffset);

            if (i > 0) {
                UNIT_ASSERT(data.Rows[i][0].StartOffset > 1000);
            }
        }
    }


    Y_UNIT_TEST(SerializeDeserializeMissingColumns) {
        TString textYson =
            "{col_a=\"a1\";col_b=\"b1\";col_c=\"c1\"};"
            "{col_a=\"a2\";col_c=\"c2\"}";  // col_b is missing

        TString binaryYson = GetBinaryYson(textYson);

        TVector<TString> keyColumns = {"col_a", "col_b", "col_c"};
        TParserFragmentListIndex parser(binaryYson, keyColumns);
        parser.Parse();

        const auto& parsedRows = parser.GetRows();
        UNIT_ASSERT_EQUAL(parsedRows.size(), 2);

        TSortedRowMetadata originalData;
        originalData.KeyColumns = keyColumns;
        originalData.Rows = parsedRows;

        TString serialized;
        TStringOutput out(serialized);
        originalData.Save(&out);

        TSortedRowMetadata data;
        TStringInput in(serialized);
        data.Load(&in, keyColumns);

        UNIT_ASSERT_EQUAL(data.Rows.size(), 2);

        // Row 1: all columns present
        UNIT_ASSERT_EQUAL(data.Rows[0].size(), 3 + 1);
        UNIT_ASSERT(data.Rows[0][0].IsValid());
        UNIT_ASSERT(data.Rows[0][1].IsValid());
        UNIT_ASSERT(data.Rows[0][2].IsValid());

        // Row 2: col_b should be missing (0, 0)
        UNIT_ASSERT_EQUAL(data.Rows[1].size(), 3 + 1);
        UNIT_ASSERT(data.Rows[1][0].IsValid());
        UNIT_ASSERT(!data.Rows[1][1].IsValid());
        UNIT_ASSERT(data.Rows[1][2].IsValid());

        for (size_t i = 0; i < data.Rows.size(); ++i) {
            for (size_t j = 0; j < data.Rows[i].size(); ++j) {
                UNIT_ASSERT_EQUAL(data.Rows[i][j].StartOffset, parsedRows[i][j].StartOffset);
                UNIT_ASSERT_EQUAL(data.Rows[i][j].EndOffset, parsedRows[i][j].EndOffset);
            }
        }
    }

    Y_UNIT_TEST(VarIntEncodingEfficiency) {
        TStringStream smallTextYson;
        for (int i = 0; i < 10; ++i) {
            smallTextYson << "{col=\"v" << i << "\"};";
        }
        TString smallBinaryYson = GetBinaryYson(smallTextYson.Str());

        TVector<TString> keyColumns = {"col"};
        TParserFragmentListIndex parserSmall(smallBinaryYson, keyColumns);
        parserSmall.Parse();
        const auto& smallParsedRows = parserSmall.GetRows();

        TSortedRowMetadata smallOffsets;
        smallOffsets.KeyColumns = keyColumns;
        smallOffsets.Rows = smallParsedRows;

        TStringStream largeTextYson;
        for (int i = 0; i < 10; ++i) {
            largeTextYson << "{col=\"";
            for (int j = 0; j < 10000; ++j) {
                largeTextYson << "x";
            }
            largeTextYson << "\"};";
        }
        TString largeBinaryYson = GetBinaryYson(largeTextYson.Str());

        TParserFragmentListIndex parserLarge(largeBinaryYson, keyColumns);
        parserLarge.Parse();
        const auto& largeParsedRows = parserLarge.GetRows();

        TSortedRowMetadata largeOffsets;
        largeOffsets.KeyColumns = keyColumns;
        largeOffsets.Rows = largeParsedRows;

        TString serializedSmall;
        TStringOutput outSmall(serializedSmall);
        smallOffsets.Save(&outSmall);

        TString serializedLarge;
        TStringOutput outLarge(serializedLarge);
        largeOffsets.Save(&outLarge);

        UNIT_ASSERT(serializedSmall.size() < serializedLarge.size());

        TSortedRowMetadata dataSmall;
        TStringInput inSmall(serializedSmall);
        dataSmall.Load(&inSmall,keyColumns);

        TSortedRowMetadata dataLarge;
        TStringInput inLarge(serializedLarge);
        dataLarge.Load(&inLarge,keyColumns);

        UNIT_ASSERT_EQUAL(dataSmall.Rows.size(), 10);
        UNIT_ASSERT_EQUAL(dataLarge.Rows.size(), 10);

        for (size_t i = 0; i < 10; ++i) {
            UNIT_ASSERT_EQUAL(dataSmall.Rows[i][0].StartOffset, smallOffsets.Rows[i][0].StartOffset);
            UNIT_ASSERT_EQUAL(dataSmall.Rows[i][0].EndOffset, smallOffsets.Rows[i][0].EndOffset);

            UNIT_ASSERT_EQUAL(dataLarge.Rows[i][0].StartOffset, largeOffsets.Rows[i][0].StartOffset);
            UNIT_ASSERT_EQUAL(dataLarge.Rows[i][0].EndOffset, largeOffsets.Rows[i][0].EndOffset);
        }

        // Verify that large offsets are actually large (> 127 for multi-byte varint)
        UNIT_ASSERT(dataLarge.Rows[1][0].StartOffset > 127);
    }

} // Y_UNIT_TEST_SUITE

} // namespace NYql::NFmr

