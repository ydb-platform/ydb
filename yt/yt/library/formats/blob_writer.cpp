#include "blob_writer.h"

#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TBlobWriter
    : public TSchemalessFormatWriterBase
{
public:
    TBlobWriter(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        TBlobFormatConfigPtr config);

private:
    const TBlobFormatConfigPtr Config_;

    const std::string DataColumnName_;
    const int DataColumnId_;

    const std::optional<std::string> PartIndexColumnName_;
    const std::optional<int> PartIndexColumnId_;

    std::optional<i64> LastPartIndex_;

    void DoWrite(TRange<TUnversionedRow> rows) override;

    TUnversionedValue GetTypedValue(
        TUnversionedRow row,
        int columnId,
        std::string_view columnName,
        EValueType expectedType) const;

    void ValidatePartIndex(i64 currentPartIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

TBlobWriter::TBlobWriter(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    TBlobFormatConfigPtr config)
    : TSchemalessFormatWriterBase(
        nameTable,
        std::move(output),
        enableContextSaving,
        controlAttributesConfig,
        /*keyColumnCount*/ 0)
    , Config_(config)
    , DataColumnName_(Config_->DataColumnName.value_or(TBlobTableSchema::DataColumn))
    , DataColumnId_(NameTable_->GetIdOrRegisterName(DataColumnName_))
    , PartIndexColumnName_(Config_->EnablePartIndex
        ? std::optional<std::string>(Config_->PartIndexColumnName.value_or(TBlobTableSchema::PartIndexColumn))
        : std::nullopt)
    , PartIndexColumnId_(PartIndexColumnName_
        ? std::optional<int>(NameTable_->GetIdOrRegisterName(*PartIndexColumnName_))
        : std::nullopt)
{ }

void TBlobWriter::DoWrite(TRange<TUnversionedRow> rows)
{
    auto* output = GetOutputStream();
    for (auto row : rows) {
        if (Config_->EnablePartIndex) {
            auto partIndexValue = GetTypedValue(row, *PartIndexColumnId_, *PartIndexColumnName_, EValueType::Int64);
            i64 currentPartIndex = partIndexValue.Data.Int64;
            ValidatePartIndex(currentPartIndex);
            LastPartIndex_ = currentPartIndex;
        }

        auto dataValue = GetTypedValue(row, DataColumnId_, DataColumnName_, EValueType::String);
        output->Write(dataValue.AsStringBuf());

        MaybeFlushBuffer(/*force*/ false);
    }
    MaybeFlushBuffer(/*force*/ true);
}

TUnversionedValue TBlobWriter::GetTypedValue(
    TUnversionedRow row,
    int columnId,
    std::string_view columnName,
    EValueType expectedType) const
{
    std::optional<TUnversionedValue> foundValue;
    for (const auto& value : row) {
        if (value.Id == columnId) {
            foundValue = value;
        }
    }

    if (!foundValue) {
        THROW_ERROR_EXCEPTION("Column %Qv not found", columnName);
    }

    if (foundValue->Type != expectedType) {
        THROW_ERROR_EXCEPTION("Column %Qv must be of type %Qlv but has type %Qlv",
            columnName,
            expectedType,
            foundValue->Type);
    }

    return *foundValue;
}

void TBlobWriter::ValidatePartIndex(i64 currentPartIndex) const
{
    if (LastPartIndex_ && *LastPartIndex_ + 1 != currentPartIndex) {
        THROW_ERROR_EXCEPTION("Values of column %Qv must be consecutive but values %v and %v violate this property",
            PartIndexColumnName_,
            *LastPartIndex_,
            currentPartIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForBlob(
    TBlobFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    /*keyColumnCount*/ int)
{
    if (controlAttributesConfig->EnableKeySwitch) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in blob format");
    }

    if (controlAttributesConfig->EnableRangeIndex) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in blob format");
    }

    if (controlAttributesConfig->EnableRowIndex) {
        THROW_ERROR_EXCEPTION("Row indices are not supported in blob format");
    }

    if (controlAttributesConfig->EnableTableIndex) {
        THROW_ERROR_EXCEPTION("Table indices are not supported in blob format");
    }

    if (controlAttributesConfig->EnableTabletIndex) {
        THROW_ERROR_EXCEPTION("Tablet indices are not supported in blob format");
    }

    return New<TBlobWriter>(nameTable, output, enableContextSaving, controlAttributesConfig, config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForBlob(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    try {
        auto config = ConvertTo<TBlobFormatConfigPtr>(&attributes);
        return CreateSchemalessWriterForBlob(
            config,
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NFormats::EErrorCode::InvalidFormat, "Failed to parse config for blob format") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
