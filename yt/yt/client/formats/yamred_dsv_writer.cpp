#include "yamred_dsv_writer.h"

#include "escape.h"

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NFormats {

using namespace NYTree;
using namespace NTableClient;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamredDsv
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamredDsv(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamredDsvFormatConfigPtr config = New<TYamredDsvFormatConfig>())
        : TSchemalessWriterForYamrBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount,
            config)
        , Config_(config)
    {
        ConfigureEscapeTables(config, true /* addCarriageReturn */, &KeyEscapeTable_, &ValueEscapeTable_);
        try {
            // We register column names in order to have correct size of NameTable_ in DoWrite method.
            for (const auto& columnName : config->KeyColumnNames) {
                KeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
            }

            for (const auto& columnName : config->SubkeyColumnNames) {
                SubkeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
            }
        } catch (const std::exception& ex) {
            auto error = TError("Failed to add columns to name table for YAMRed DSV format")
                << ex;
            RegisterError(error);
        }

        UpdateEscapedColumnNames();
    }

private:
    const TYamredDsvFormatConfigPtr Config_;

    std::vector<const TUnversionedValue*> RowValues_;

    std::vector<int> KeyColumnIds_;
    std::vector<int> SubkeyColumnIds_;
    std::vector<TString> EscapedColumnNames_;

    // In lenval mode key, subkey and value are first written into
    // this buffer in order to calculate their length.
    TBlobOutput LenvalBuffer_;

    // We capture size of name table at the beginning of #WriteRows
    // and refer to the captured value, since name table may change asynchronously.
    int NameTableSize_ = 0;

    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;


    // ISchemalessFormatWriter implementation
    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        TableIndexWasWritten_ = false;

        auto* stream = GetOutputStream();

        UpdateEscapedColumnNames();
        RowValues_.resize(NameTableSize_);
        // Invariant: at the beginning of each loop iteration RowValues contains
        // nullptr in each element.
        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; index++) {
            auto row = rows[index];
            if (CheckKeySwitch(row, index + 1 == rowCount /* isLastRow */)) {
                YT_VERIFY(!Config_->Lenval);
                WritePod(*stream, static_cast<ui32>(-2));
            }

            WriteControlAttributes(row);

            for (const auto* item = row.Begin(); item != row.End(); ++item) {
                if (IsSystemColumnId(item->Id) || item->Type == EValueType::Null) {
                    // Ignore null values and system columns.
                    continue;
                }
                YT_VERIFY(item->Id < NameTableSize_);
                RowValues_[item->Id] = item;
            }

            WriteYamrKey(KeyColumnIds_);
            if (Config_->HasSubkey) {
                WriteYamrKey(SubkeyColumnIds_);
            } else {
                // Due to YAMRed DSV format logic, when there is no subkey, but still some
                // columns are marked as subkey columns, we should explicitly remove them
                // from the row (i. e. don't print as a rest of values in YAMR value column).
                for (int id : SubkeyColumnIds_)
                    RowValues_[id] = nullptr;
            }
            WriteYamrValue();
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    void WriteYamrKey(const std::vector<int>& columnIds)
    {
        auto* stream = Config_->Lenval ? &LenvalBuffer_ : GetOutputStream();

        bool firstColumn = true;
        for (int id : columnIds) {
            if (!firstColumn) {
                stream->Write(Config_->YamrKeysSeparator);
            } else {
                firstColumn = false;
            }
            if (!RowValues_[id]) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Key column %Qv is missing",
                    NameTableReader_->GetName(id));
            }
            WriteUnversionedValue(*RowValues_[id], stream, ValueEscapeTable_);
            RowValues_[id] = nullptr;
        }

        if (Config_->Lenval) {
            WritePod(*GetOutputStream(), static_cast<ui32>(LenvalBuffer_.Size()));
            GetOutputStream()->Write(LenvalBuffer_.Begin(), LenvalBuffer_.Size());
            LenvalBuffer_.Clear();
        } else {
            GetOutputStream()->Write(Config_->FieldSeparator);
        }
    }

    void WriteYamrValue()
    {
        auto* stream = Config_->Lenval ? &LenvalBuffer_ : GetOutputStream();

        bool firstColumn = true;
        for (int id = 0; id < NameTableSize_; ++id) {
            const auto* value = RowValues_[id];
            if (!value) {
                continue;
            }
            bool skip = Config_->SkipUnsupportedTypesInValue && IsAnyOrComposite(value->Type);
            if (!skip) {
                if (!firstColumn) {
                    stream->Write(Config_->FieldSeparator);
                } else {
                    firstColumn = false;
                }
                stream->Write(EscapedColumnNames_[id]);
                stream->Write(Config_->KeyValueSeparator);
                WriteUnversionedValue(*RowValues_[id], stream, ValueEscapeTable_);
            }
            RowValues_[id] = nullptr;
        }

        if (Config_->Lenval) {
            WritePod(*GetOutputStream(), static_cast<ui32>(LenvalBuffer_.Size()));
            GetOutputStream()->Write(LenvalBuffer_.Begin(), LenvalBuffer_.Size());
            LenvalBuffer_.Clear();
        } else {
            GetOutputStream()->Write(Config_->RecordSeparator);
        }
    }

    void UpdateEscapedColumnNames()
    {
        // We store escaped column names in order to not re-escape them each time we write a column name.
        NameTableSize_ = NameTableReader_->GetSize();
        EscapedColumnNames_.reserve(NameTableSize_);
        for (int columnIndex = EscapedColumnNames_.size(); columnIndex < NameTableSize_; ++columnIndex) {
            EscapedColumnNames_.emplace_back(Escape(NameTableReader_->GetName(columnIndex), KeyEscapeTable_));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    TYamredDsvFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    if (controlAttributesConfig->EnableKeySwitch && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMRed DSV format");
    }

    if (controlAttributesConfig->EnableRangeIndex && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMRed DSV format");
    }

    if (controlAttributesConfig->EnableEndOfStream) {
        THROW_ERROR_EXCEPTION("End of stream control attribute is not supported in YAMRed DSV format");
    }

    return New<TSchemalessWriterForYamredDsv>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    try {
        auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&attributes);
        return CreateSchemalessWriterForYamredDsv(
            config,
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidFormat, "Failed to parse config for YAMRed DSV format") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

