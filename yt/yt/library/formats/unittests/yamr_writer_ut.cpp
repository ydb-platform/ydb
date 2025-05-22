#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/library/formats/yamr_writer.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

class TSchemalessWriterForYamrTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_;
    int KeyId_;
    int SubkeyId_;
    int ValueId_;
    int TableIndexId_;
    int RangeIndexId_;
    int RowIndexId_;

    TYamrFormatConfigPtr Config_;

    IUnversionedRowsetWriterPtr Writer_;

    TStringStream OutputStream_;

    TSchemalessWriterForYamrTest()
    {
        NameTable_ = New<TNameTable>();
        KeyId_ = NameTable_->RegisterName("key");
        SubkeyId_ = NameTable_->RegisterName("subkey");
        ValueId_ = NameTable_->RegisterName("value");
        TableIndexId_ = NameTable_->RegisterName(TableIndexColumnName);
        RowIndexId_ = NameTable_->RegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->RegisterName(RangeIndexColumnName);

        Config_ = New<TYamrFormatConfig>();
    }

    void CreateStandardWriter(TControlAttributesConfigPtr controlAttributes = New<TControlAttributesConfig>())
    {
        Writer_ = CreateSchemalessWriterForYamr(
            Config_,
            NameTable_,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)),
            false, /* enableContextSaving */
            controlAttributes,
            0 /* keyColumnCount */);
    }
};

TEST_F(TSchemalessWriterForYamrTest, Simple)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));

    // Ignore system columns.
    row1.AddValue(MakeUnversionedInt64Value(2, TableIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(42, RowIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(1, RangeIndexId_));

    // Note that key and value follow not in order.
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "key1\tvalue1\n"
        "key2\tvalue2\n";

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SimpleWithSubkey)
{
    Config_->HasSubkey = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedStringValue("subkey1", SubkeyId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("subkey2", SubkeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SubkeyCouldBeSkipped)
{
    Config_->HasSubkey = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", KeyId_));
    row.AddValue(MakeUnversionedStringValue("value", ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = "key\t\tvalue\n";
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SubkeyCouldBeNull)
{
    Config_->HasSubkey = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", KeyId_));
    row.AddValue(MakeUnversionedSentinelValue(EValueType::Null, SubkeyId_));
    row.AddValue(MakeUnversionedStringValue("value", ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = "key\t\tvalue\n";
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, NonNullTerminatedStrings)
{
    Config_->HasSubkey = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    const char* longString = "trashkeytrashsubkeytrashvalue";
    row.AddValue(MakeUnversionedStringValue(TStringBuf(longString + 5, 3), KeyId_));
    row.AddValue(MakeUnversionedStringValue(TStringBuf(longString + 13, 6), SubkeyId_));
    row.AddValue(MakeUnversionedStringValue(TStringBuf(longString + 24, 5), ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = "key\tsubkey\tvalue\n";
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SkippedKey)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("value", ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_FALSE(Writer_->Write(rows));

    EXPECT_THROW(Writer_->Close()
        .Get()
        .ThrowOnError(), std::exception);
}

TEST_F(TSchemalessWriterForYamrTest, SkippedValue)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", KeyId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_FALSE(Writer_->Write(rows));

    EXPECT_THROW(Writer_->Close()
        .Get()
        .ThrowOnError(), std::exception);
}

TEST_F(TSchemalessWriterForYamrTest, NotStringType) {
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", KeyId_));
    row.AddValue(MakeUnversionedInt64Value(42, ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_FALSE(Writer_->Write(rows));

    EXPECT_THROW(Writer_->Close()
        .Get()
        .ThrowOnError(), std::exception);
}

TEST_F(TSchemalessWriterForYamrTest, ExtraItem)
{
    int trashId = NameTable_->RegisterName("trash");
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", KeyId_));
    row.AddValue(MakeUnversionedStringValue("value", ValueId_));
    // This value will be ignored.
    row.AddValue(MakeUnversionedStringValue("trash", trashId));
    // This value will also be ignored because Config_->HasSubkey is off,
    // despite the fact it has non-string type.
    row.AddValue(MakeUnversionedInt64Value(42, SubkeyId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = "key\tvalue\n";
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, Escaping)
{
    Config_->HasSubkey = true;
    Config_->EnableEscaping = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("\n", KeyId_));
    row.AddValue(MakeUnversionedStringValue("\t", SubkeyId_));
    row.AddValue(MakeUnversionedStringValue("\n", ValueId_));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = "\\n\t\\t\t\\n\n";
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SimpleWithTableIndex)
{
    Config_->EnableTableIndex = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    CreateStandardWriter(controlAttributes);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("value3", ValueId_));
    row3.AddValue(MakeUnversionedInt64Value(23, TableIndexId_));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "42\n"
        "key1\tvalue1\n"
        "key2\tvalue2\n"
        "23\n"
        "key3\tvalue3\n";

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, SimpleWithRowIndexAndTableIndex)
{
    Config_->EnableTableIndex = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    controlAttributes->EnableRowIndex = true;
    CreateStandardWriter(controlAttributes);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(0, RowIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(0, RangeIndexId_));
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("value3", ValueId_));
    row3.AddValue(MakeUnversionedInt64Value(5, RowIndexId_));
    row3.AddValue(MakeUnversionedInt64Value(1, RangeIndexId_));
    rows = { row3.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row4;
    row4.AddValue(MakeUnversionedStringValue("key4", KeyId_));
    row4.AddValue(MakeUnversionedStringValue("value4", ValueId_));
    row4.AddValue(MakeUnversionedInt64Value(23, TableIndexId_));
    row4.AddValue(MakeUnversionedInt64Value(10, RowIndexId_));
    row4.AddValue(MakeUnversionedInt64Value(2, RangeIndexId_));
    rows = { row4.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output =
        "42\n0\n"
        "key1\tvalue1\n"
        "key2\tvalue2\n"
        "42\n5\n"
        "key3\tvalue3\n"
        "23\n10\n"
        "key4\tvalue4\n";

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, Lenval)
{
    Config_->HasSubkey = true;
    Config_->Lenval = true;
    CreateStandardWriter();

    // Note that order in both rows is unusual.
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("subkey1", SubkeyId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedStringValue("subkey2", SubkeyId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = TString(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x06\x00\x00\x00" "value2",

        2 * (3 * 4 + 4 + 6 + 7)); // all i32 + lengths of keys
    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, LenvalWithEmptyFields)
{
    Config_->HasSubkey = true;
    Config_->Lenval = true;
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("subkey1", SubkeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("", SubkeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("subkey3", SubkeyId_));
    row3.AddValue(MakeUnversionedStringValue("", ValueId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow(), row3.GetRow() };

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = TString(
        "\x00\x00\x00\x00" ""
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x00\x00\x00\x00" ""
        "\x06\x00\x00\x00" "value2"

        "\x04\x00\x00\x00" "key3"
        "\x07\x00\x00\x00" "subkey3"
        "\x00\x00\x00\x00" "",

        9 * 4 + (7 + 6) + (4 + 6) + (4 + 7)); // all i32 + lengths of keys

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, LenvalWithKeySwitch)
{
    Config_->HasSubkey = true;
    Config_->Lenval = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableKeySwitch = true;

    Writer_ = CreateSchemalessWriterForYamr(
        Config_,
        NameTable_,
        CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)),
        false, /* enableContextSaving */
        controlAttributes,
        1 /* keyColumnCount */);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("subkey1", SubkeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("subkey21", SubkeyId_));
    row2.AddValue(MakeUnversionedStringValue("value21", ValueId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("subkey22", SubkeyId_));
    row3.AddValue(MakeUnversionedStringValue("value22", ValueId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow(), row3.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row4;
    row4.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row4.AddValue(MakeUnversionedStringValue("subkey3", SubkeyId_));
    row4.AddValue(MakeUnversionedStringValue("value3", ValueId_));

    rows = { row4.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output = TString(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\xfe\xff\xff\xff" // key switch

        "\x04\x00\x00\x00" "key2"
        "\x08\x00\x00\x00" "subkey21"
        "\x07\x00\x00\x00" "value21"

        "\x04\x00\x00\x00" "key2"
        "\x08\x00\x00\x00" "subkey22"
        "\x07\x00\x00\x00" "value22"

        "\xfe\xff\xff\xff"

        "\x04\x00\x00\x00" "key3"
        "\x07\x00\x00\x00" "subkey3"
        "\x06\x00\x00\x00" "value3",

        14 * 4 + (4 + 7 + 6) + (4 + 8 + 7) + (4 + 8 + 7) + (4 + 7 + 6)); // all i32 + lengths of keys

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, LenvalWithTableIndex)
{
    Config_->EnableTableIndex = true;
    Config_->Lenval = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    CreateStandardWriter(controlAttributes);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedInt64Value(42, TableIndexId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("value3", ValueId_));
    row3.AddValue(MakeUnversionedInt64Value(23, TableIndexId_));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output(
        "\xff\xff\xff\xff" "\x2a\x00\x00\x00" // 42

        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\xff\xff\xff\xff" "\x17\x00\x00\x00" // 23

        "\x04\x00\x00\x00" "key3"
        "\x06\x00\x00\x00" "value3",

        10 * 4 + 3 * (4 + 6));

    EXPECT_EQ(output, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForYamrTest, LenvalWithRangeAndRowIndex)
{
    Config_->Lenval = true;

    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableRowIndex = true;
    controlAttributes->EnableRangeIndex = true;
    CreateStandardWriter(controlAttributes);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", KeyId_));
    row1.AddValue(MakeUnversionedStringValue("value1", ValueId_));
    row1.AddValue(MakeUnversionedInt64Value(42, RangeIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(23, RowIndexId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", KeyId_));
    row2.AddValue(MakeUnversionedStringValue("value2", ValueId_));
    row2.AddValue(MakeUnversionedInt64Value(42, RangeIndexId_));
    row2.AddValue(MakeUnversionedInt64Value(24, RowIndexId_));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", KeyId_));
    row3.AddValue(MakeUnversionedStringValue("value3", ValueId_));
    row3.AddValue(MakeUnversionedInt64Value(42, RangeIndexId_));
    row3.AddValue(MakeUnversionedInt64Value(25, RowIndexId_));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, Writer_->Write(rows));

    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString output(
        "\xfd\xff\xff\xff" "\x2a\x00\x00\x00" // 42
        "\xfc\xff\xff\xff" "\x17\x00\x00\x00\x00\x00\x00\x00" // 23

        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"

        "\x04\x00\x00\x00" "key3"
        "\x06\x00\x00\x00" "value3",

        11 * 4 + 3 * (4 + 6));

    EXPECT_EQ(output, OutputStream_.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
