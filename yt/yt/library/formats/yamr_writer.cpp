#include "yamr_writer.h"

#include "escape.h"
#include "helpers.h"

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/format.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamr
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamr(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamrFormatConfigPtr config = New<TYamrFormatConfig>())
        : TSchemalessWriterForYamrBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount,
            config)
    {
        ConfigureEscapeTables(
            config,
            config->EnableEscaping /* enableKeyEscaping */,
            config->EnableEscaping /* enableValueEscaping */,
            true /* escapingForWriter */,
            &KeyEscapeTable_,
            &ValueEscapeTable_);

        try {
            KeyId_ = nameTable->GetIdOrRegisterName(config->Key);
            SubkeyId_ = Config_->HasSubkey ? nameTable->GetIdOrRegisterName(config->Subkey) : -1;
            ValueId_ = nameTable->GetIdOrRegisterName(config->Value);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to add columns to name table for YAMR format")
                << ex;
            RegisterError(error);
        }
    }

private:
    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;

    int KeyId_;
    int SubkeyId_;
    int ValueId_;

    // ISchemalessFormatWriter override.
    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        TableIndexWasWritten_ = false;

        auto* stream = GetOutputStream();
        // This nasty line is needed to use Config as TYamrFormatConfigPtr
        // without extra serializing/deserializing.
        TYamrFormatConfigPtr config(static_cast<TYamrFormatConfig*>(Config_.Get()));

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; index++) {
            auto row = rows[index];
            if (CheckKeySwitch(row, index + 1 == rowCount /* isLastRow */)) {
                YT_VERIFY(config->Lenval);
                WritePod(*stream, static_cast<ui32>(-2));
            }

            WriteControlAttributes(row);

            std::optional<TStringBuf> key;
            std::optional<TStringBuf> subkey;
            std::optional<TStringBuf> value;

            for (const auto* item = row.Begin(); item != row.End(); ++item) {
                if (item->Id == KeyId_) {
                    ValidateColumnType(item, TStringBuf("key"));
                    key = item->AsStringBuf();
                } else if (item->Id == SubkeyId_) {
                    if (item->Type != EValueType::Null) {
                        ValidateColumnType(item, TStringBuf("subkey"));
                        subkey = item->AsStringBuf();
                    }
                } else if (item->Id == ValueId_) {
                    ValidateColumnType(item, TStringBuf("value"));
                    value = item->AsStringBuf();
                } else {
                    // Ignore unknown columns.
                    continue;
                }
            }

            if (!key) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Missing key column %Qv in YAMR record",
                    config->Key);
            }

            if (!subkey) {
                subkey = "";
            }

            if (!value) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Missing value column %Qv in YAMR record",
                    config->Value);
            }

            if (!config->Lenval) {
                EscapeAndWrite(*key, stream, KeyEscapeTable_);
                stream->Write(config->FieldSeparator);
                if (config->HasSubkey) {
                    EscapeAndWrite(*subkey, stream, KeyEscapeTable_);
                    stream->Write(config->FieldSeparator);
                }
                EscapeAndWrite(*value, stream, ValueEscapeTable_);
                stream->Write(config->RecordSeparator);
            } else {
                WriteInLenvalMode(*key);
                if (config->HasSubkey) {
                    WriteInLenvalMode(*subkey);
                }
                WriteInLenvalMode(*value);
            }

            TryFlushBuffer(false);
        }

        TryFlushBuffer(true);
    }

    void ValidateColumnType(const TUnversionedValue* value, TStringBuf columnName)
    {
        if (value->Type != EValueType::String) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Wrong type %Qlv of column %Qv in YAMR record",
                value->Type,
                columnName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamr(
    TYamrFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    if (controlAttributesConfig->EnableKeySwitch && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format");
    }

    if (controlAttributesConfig->EnableRangeIndex && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }

    if (controlAttributesConfig->EnableEndOfStream) {
        THROW_ERROR_EXCEPTION("End of stream control attribute is not supported in YAMR format");
    }

    return New<TSchemalessWriterForYamr>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamr(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    try {
        auto config = ConvertTo<TYamrFormatConfigPtr>(&attributes);
        return CreateSchemalessWriterForYamr(
            config,
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount);
    } catch (const std::exception& exc) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidFormat, "Failed to parse config for YAMR format") << exc;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
