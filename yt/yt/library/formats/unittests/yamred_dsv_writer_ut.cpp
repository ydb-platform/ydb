#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/library/formats/yamred_dsv_writer.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <util/string/vector.h>

#include <cstdio>


namespace NYT::NFormats {
namespace {

using VectorStrok = TVector<TString>;

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

class TSchemalessWriterForYamredDsvTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_;
    TYamredDsvFormatConfigPtr Config_;
    IUnversionedRowsetWriterPtr Writer_;

    TStringStream OutputStream_;

    int KeyAId_;
    int KeyBId_;
    int KeyCId_;
    int ValueXId_;
    int ValueYId_;
    int TableIndexId_;
    int RangeIndexId_;
    int RowIndexId_;

    TSchemalessWriterForYamredDsvTest()
    {
        NameTable_ = New<TNameTable>();
        KeyAId_ = NameTable_->RegisterName("key_a");
        KeyBId_ = NameTable_->RegisterName("key_b");
        KeyCId_ = NameTable_->RegisterName("key_c");
        ValueXId_ = NameTable_->RegisterName("value_x");
        ValueYId_ = NameTable_->RegisterName("value_y");
        TableIndexId_ = NameTable_->RegisterName(TableIndexColumnName);
        RowIndexId_ = NameTable_->RegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->RegisterName(RangeIndexColumnName);
        Config_ = New<TYamredDsvFormatConfig>();
    }

    void CreateStandardWriter(TControlAttributesConfigPtr controlAttributes = New<TControlAttributesConfig>())
    {
        Writer_ = CreateSchemalessWriterForYamredDsv(
            Config_,
            NameTable_,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)),
            false, /* enableContextSaving */
            controlAttributes,
            0 /* keyColumnCount */);
    }

    // Splits output into key and sorted vector of values that are entries of the last YAMR column.
    // Returns true if success (there are >= 2 values after splitting by field separator), otherwise false.
    bool ExtractKeyValue(TString output, TString& key, VectorStrok& value, char fieldSeparator = '\t')
    {
        char delimiter[2] = {fieldSeparator, 0};
        // Splitting by field separator.
        value = SplitString(output, delimiter, 0 /* maxFields */, KEEP_EMPTY_TOKENS);
        // We should at least have key and the rest of values.
        if (value.size() < 2)
            return false;
        key = value[0];
        value.erase(value.begin());
        std::sort(value.begin(), value.end());
        return true;
    }

    // The same function as previous, version with subkey.
    bool ExtractKeySubkeyValue(TString output, TString& key, TString& subkey, VectorStrok& value, char fieldSeparator = '\t')
    {
        char delimiter[2] = {fieldSeparator, 0};
        // Splitting by field separator.
        value = SplitString(output, delimiter, 0 /* maxFields */, KEEP_EMPTY_TOKENS);
        // We should at least have key, subkey and the rest of values.
        if (value.size() < 3)
            return false;
        key = value[0];
        subkey = value[1];
        value.erase(value.begin(), value.end());
        std::sort(value.begin(), value.end());
        return true;
    }

    // Compares output and expected output ignoring the order of entries in YAMR value column.
    void CompareKeyValue(TString output, TString expected, char recordSeparator = '\n', char fieldSeparator = '\t')
    {
        char delimiter[2] = {recordSeparator, 0};
        VectorStrok outputRows = SplitString(output, delimiter, 0 /* maxFields */ , KEEP_EMPTY_TOKENS);
        VectorStrok expectedRows = SplitString(expected, delimiter, 0 /* maxFields */, KEEP_EMPTY_TOKENS);
        EXPECT_EQ(outputRows.size(), expectedRows.size());
        // Since there is \n after each row, there will be an extra empty string in both vectors.
        EXPECT_EQ(outputRows.back(), "");
        ASSERT_EQ(expectedRows.back(), "");
        outputRows.pop_back();
        expectedRows.pop_back();

        TString outputKey;
        TString expectedKey;
        VectorStrok outputValue;
        VectorStrok expectedValue;
        for (int rowIndex = 0; rowIndex < static_cast<int>(outputRows.size()); rowIndex++) {
            EXPECT_TRUE(ExtractKeyValue(outputRows[rowIndex], outputKey, outputValue, fieldSeparator));
            ASSERT_TRUE(ExtractKeyValue(expectedRows[rowIndex], expectedKey, expectedValue, fieldSeparator));
            EXPECT_EQ(outputKey, expectedKey);
            EXPECT_EQ(outputValue, expectedValue);
        }
    }

    // The same function as previous, version with subkey.
    void CompareKeySubkeyValue(TString output, TString expected, char recordSeparator = '\n', char fieldSeparator = '\t')
    {
        char delimiter[2] = {recordSeparator, 0};
        VectorStrok outputRows = SplitString(output, delimiter, 0 /* maxFields */ , KEEP_EMPTY_TOKENS);
        VectorStrok expectedRows = SplitString(expected, delimiter, 0 /* maxFields */, KEEP_EMPTY_TOKENS);
        EXPECT_EQ(outputRows.size(), expectedRows.size());
        // Since there is \n after each row, there will be an extra empty string in both vectors.
        EXPECT_EQ(outputRows.back(), "");
        ASSERT_EQ(expectedRows.back(), "");
        outputRows.pop_back();
        expectedRows.pop_back();

        TString outputKey;
        TString expectedKey;
        TString outputSubkey;
        TString expectedSubkey;
        VectorStrok outputValue;
        VectorStrok expectedValue;
        for (int rowIndex = 0; rowIndex < static_cast<int>(outputRows.size()); rowIndex++) {
            EXPECT_TRUE(ExtractKeySubkeyValue(outputRows[rowIndex], outputKey, outputSubkey, outputValue, fieldSeparator));
            ASSERT_TRUE(ExtractKeySubkeyValue(expectedRows[rowIndex], expectedKey, expectedSubkey, expectedValue, fieldSeparator));
            EXPECT_EQ(outputKey, expectedKey);
            EXPECT_EQ(outputSubkey, expectedSubkey);
            EXPECT_EQ(outputValue, expectedValue);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, Simple)
{
    Config_->KeyColumnNames.emplace_back("key_a");
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("a1", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("x", ValueXId_));
    row1.AddValue(MakeUnversionedSentinelValue(EValueType::Null, ValueYId_));

    // Ignore system columns.
    row1.AddValue(MakeUnversionedInt64Value(2, TableIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(42, RowIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(1, RangeIndexId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("a2", KeyAId_));
    row2.AddValue(MakeUnversionedStringValue("y", ValueYId_));
    row2.AddValue(MakeUnversionedStringValue("b", KeyBId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput =
        "a1\tvalue_x=x\n"
        "a2\tvalue_y=y\tkey_b=b\n";

    TString output = OutputStream_.Str();

    CompareKeyValue(expectedOutput, output);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, SimpleWithSubkey)
{
    Config_->HasSubkey = true;
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->KeyColumnNames.emplace_back("key_b");
    Config_->SubkeyColumnNames.emplace_back("key_c");
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("a", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("b1", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("c", KeyCId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("a", KeyAId_));
    row2.AddValue(MakeUnversionedStringValue("b2", KeyBId_));
    row2.AddValue(MakeUnversionedStringValue("c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput =
        "a b1\tc\t\n"
        "a b2\tc\t\n";

    TString output = OutputStream_.Str();

    CompareKeySubkeyValue(expectedOutput, output);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, Lenval)
{
    Config_->Lenval = true;
    Config_->HasSubkey = true;
    Config_->EnableTableIndex = true;
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->KeyColumnNames.emplace_back("key_b");
    Config_->SubkeyColumnNames.emplace_back("key_c");

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    controlAttributes->EnableRowIndex = true;
    controlAttributes->EnableRangeIndex = true;
    CreateStandardWriter(controlAttributes);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("a", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("b1", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("x", ValueXId_));

    row1.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(23, RangeIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(17, RowIndexId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("a", KeyAId_));
    row2.AddValue(MakeUnversionedStringValue("b2", KeyBId_));
    row2.AddValue(MakeUnversionedStringValue("c", KeyCId_));

    row2.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));
    row2.AddValue(MakeUnversionedInt64Value(23, RangeIndexId_));
    row2.AddValue(MakeUnversionedInt64Value(18, RowIndexId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = TString(
        "\xff\xff\xff\xff" "\x2a\x00\x00\x00" // Table index.
        "\xfd\xff\xff\xff" "\x17\x00\x00\x00" // Range index.
        "\xfc\xff\xff\xff" "\x11\x00\x00\x00\x00\x00\x00\x00" // Row index.

        "\x04\x00\x00\x00" "a b1"
        "\x01\x00\x00\x00" "c"
        "\x09\x00\x00\x00" "value_x=x"

        "\x04\x00\x00\x00" "a b2"
        "\x01\x00\x00\x00" "c"
        "\x00\x00\x00\x00" "",

        13 * 4 + 4 + 1 + 9 + 4 + 1 + 0);

    TString output = OutputStream_.Str();
    EXPECT_EQ(expectedOutput, output)
        << "expected length: " << expectedOutput.length()
        << ", "
        << "actual length: " << output.length();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, Escaping)
{
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->KeyColumnNames.emplace_back("key_b");
    int columnWithEscapedNameId = NameTable_->GetIdOrRegisterName("value\t_t");
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("a\n", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("\nb\t", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("\nva\\lue\t", columnWithEscapedNameId));

    std::vector<TUnversionedRow> rows = {row1.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "a\\n \\nb\\t\tvalue\\t_t=\\nva\\\\lue\\t\n";
    TString output = OutputStream_.Str();

    EXPECT_EQ(expectedOutput, output);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, SkippedKey)
{
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->KeyColumnNames.emplace_back("key_b");
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("b", KeyBId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_FALSE(Writer_->Write(rows));

    EXPECT_THROW(Writer_->Close()
        .Get()
        .ThrowOnError(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, SkippedSubkey)
{
    Config_->HasSubkey = true;
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->SubkeyColumnNames.emplace_back("key_c");
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("a", KeyAId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_FALSE(Writer_->Write(rows));

    EXPECT_THROW(Writer_->Close()
        .Get()
        .ThrowOnError(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, NonStringValues)
{
    Config_->HasSubkey = true;
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->SubkeyColumnNames.emplace_back("key_c");
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedInt64Value(-42, KeyAId_));
    row.AddValue(MakeUnversionedUint64Value(18, KeyCId_));
    row.AddValue(MakeUnversionedBooleanValue(true, KeyBId_));
    row.AddValue(MakeUnversionedDoubleValue(3.14, ValueXId_));
    row.AddValue(MakeUnversionedStringValue("yt", ValueYId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "-42\t18\tkey_b=true\tvalue_x=3.14\tvalue_y=yt\n";
    TString output = OutputStream_.Str();

    EXPECT_EQ(expectedOutput, output);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessWriterForYamredDsvTest, ErasingSubkeyColumnsWhenHasSubkeyIsFalse)
{
    Config_->KeyColumnNames.emplace_back("key_a");
    Config_->SubkeyColumnNames.emplace_back("key_b");
    // Config->HasSubkey = false by default.
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("a", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("b", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("x", ValueXId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "a\tkey_c=c\tvalue_x=x\n";
    TString output = OutputStream_.Str();

    EXPECT_EQ(expectedOutput, output);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
