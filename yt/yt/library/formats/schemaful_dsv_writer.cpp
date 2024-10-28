#include "schemaful_dsv_writer.h"

#include "escape.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/format.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <limits>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// This class contains methods common for TSchemafulWriterForSchemafulDsv and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TSchemafulDsvFormatConfigPtr Config_;

    // This array indicates on which position should each
    // column stay in the resulting row.
    std::vector<int> IdToIndexInRow_;

    // This array contains TUnversionedValue's reordered
    // according to the desired order.
    std::vector<const TUnversionedValue*> CurrentRowValues_;

    TEscapeTable EscapeTable_;

    TSchemafulDsvWriterBase(TSchemafulDsvFormatConfigPtr config, std::vector<int> idToIndexInRow)
        : Config_(config)
        , IdToIndexInRow_(idToIndexInRow)
    {
        ConfigureEscapeTable(Config_, &EscapeTable_);
        if (!IdToIndexInRow_.empty()) {
            CurrentRowValues_.resize(
                *std::max_element(IdToIndexInRow_.begin(), IdToIndexInRow_.end()) + 1);
        }
        YT_VERIFY(Config_->Columns);
    }

    int FindMissingValueIndex() const
    {
        for (int valueIndex = 0; valueIndex < static_cast<int>(CurrentRowValues_.size()); ++valueIndex) {
            const auto* value = CurrentRowValues_[valueIndex];
            if (!value || value->Type == EValueType::Null) {
                return valueIndex;
            }
        }
        return -1;
    }

    template <class TFunction>
    void WriteColumnNamesHeader(TFunction writeCallback)
    {
        if (Config_->EnableColumnNamesHeader && *Config_->EnableColumnNamesHeader) {
            const auto& columns = *Config_->Columns;
            for (size_t index = 0; index < columns.size(); ++index) {
                writeCallback(
                    columns[index],
                    (index + 1 == columns.size()) ? Config_->RecordSeparator : Config_->FieldSeparator);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForSchemafulDsv
    : public TSchemalessFormatWriterBase
    , public TSchemafulDsvWriterBase
{
public:
    TSchemalessWriterForSchemafulDsv(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> idToIndexInRow)
        : TSchemalessFormatWriterBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            0 /* keyColumnCount */)
        , TSchemafulDsvWriterBase(
            config,
            idToIndexInRow)
        , TableIndexColumnId_(Config_->EnableTableIndex && controlAttributesConfig->EnableTableIndex
            ? nameTable->GetId(TableIndexColumnName)
            : -1)
    {
        BlobOutput_ = GetOutputStream();
        WriteColumnNamesHeader([this] (TStringBuf buf, char c) {
            WriteRaw(buf);
            WriteRaw(c);
        });
    }

private:
    TBlobOutput* BlobOutput_;
    const int TableIndexColumnId_;

    // ISchemalessFormatWriter overrides.
    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        for (const auto& row : rows) {
            CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
            for (auto item = row.Begin(); item != row.End(); ++item) {
                if (item->Id < IdToIndexInRow_.size() && IdToIndexInRow_[item->Id] != -1) {
                    CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
                }
            }

            if (Config_->EnableTableIndex && ControlAttributesConfig_->EnableTableIndex &&
                !CurrentRowValues_[IdToIndexInRow_[TableIndexColumnId_]])
            {
                THROW_ERROR_EXCEPTION("Table index column is missing");
            }

            int missingValueIndex = FindMissingValueIndex();
            if (missingValueIndex != -1) {
                if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                    continue;
                } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) {
                    THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
                }
            }

            bool firstValue = true;
            for (const auto* item : CurrentRowValues_) {
                if (!firstValue) {
                    WriteRaw(Config_->FieldSeparator);
                } else {
                    firstValue = false;
                }
                if (!item || item->Type == EValueType::Null) {
                    // If we got here, MissingValueMode is PrintSentinel.
                    WriteRaw(Config_->MissingValueSentinel);
                } else {
                    WriteUnversionedValue(*item, BlobOutput_, EscapeTable_);
                }
            }
            WriteRaw(Config_->RecordSeparator);
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    void WriteRaw(TStringBuf str)
    {
        BlobOutput_->Write(str.begin(), str.length());
    }

    void WriteRaw(char ch)
    {
        BlobOutput_->Write(ch);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriterForSchemafulDsv
    : public IUnversionedRowsetWriter
    , public TSchemafulDsvWriterBase
{
public:
    TSchemafulWriterForSchemafulDsv(
        IAsyncOutputStreamPtr stream,
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> IdToIndexInRow)
        : TSchemafulDsvWriterBase(
            config,
            IdToIndexInRow)
        , Output_(CreateBufferedSyncAdapter(stream))
    {
        WriteColumnNamesHeader([this] (TStringBuf buf, char c) {
            Output_->Write(buf);
            Output_->Write(c);
        });
    }

    TFuture<void> Close() override
    {
        DoFlushBuffer();
        return VoidFuture;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        for (const auto& row : rows) {
            if (!row) {
                THROW_ERROR_EXCEPTION("Empty rows are not supported by schemaful dsv writer");
            }

            CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
            for (auto item = row.Begin(); item != row.End(); ++item) {
                YT_ASSERT(item->Id >= 0 && item->Id < IdToIndexInRow_.size());
                if (IdToIndexInRow_[item->Id] != -1) {
                    CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
                }
            }

            int missingValueIndex = FindMissingValueIndex();
            if (missingValueIndex != -1) {
                if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                    continue;
                } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) {
                    THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
                }
            }

            bool firstValue = true;
            for (const auto* item : CurrentRowValues_) {
                if (!firstValue) {
                    Output_->Write(Config_->FieldSeparator);
                } else {
                    firstValue = false;
                }
                if (!item || item->Type == EValueType::Null) {
                    // If we got here, MissingValueMode is PrintSentinel.
                    Output_->Write(Config_->MissingValueSentinel);
                } else {
                    WriteUnversionedValue(*item, Output_.get(), EscapeTable_);
                }
            }
            Output_->Write(Config_->RecordSeparator);
        }
        DoFlushBuffer();

        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return Result_;
    }

    std::optional<NCrypto::TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    std::unique_ptr<IOutputStream> Output_;

    void DoFlushBuffer()
    {
        Output_->Flush();
    }

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateDuplicateColumns(const std::vector<TString>& columns)
{
    THashSet<TString> names;
    for (const auto& name : columns) {
        if (!names.insert(name).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in schemaful DSV config",
                name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */)
{
    if (controlAttributesConfig->EnableKeySwitch) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in schemaful DSV format");
    }

    if (controlAttributesConfig->EnableRangeIndex) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in schemaful DSV format");
    }

    if (controlAttributesConfig->EnableRowIndex) {
        THROW_ERROR_EXCEPTION("Row indices are not supported in schemaful DSV format");
    }

    if (controlAttributesConfig->EnableTabletIndex) {
        THROW_ERROR_EXCEPTION("Tablet indices are not supported in schemaful DSV format");
    }

    if (!config->Columns) {
        THROW_ERROR_EXCEPTION("Config must contain columns for schemaful DSV schemaless writer");
    }

    std::vector<int> idToIndexInRow;
    auto columns = *config->Columns;

    if (config->EnableTableIndex && controlAttributesConfig->EnableTableIndex) {
        columns.insert(columns.begin(), TableIndexColumnName);
    }

    ValidateDuplicateColumns(columns);

    try {
        for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
            nameTable->GetIdOrRegisterName(columns[columnIndex]);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to add columns to name table for schemaful DSV format")
            << ex;
    }

    idToIndexInRow.resize(nameTable->GetSize(), -1);
    for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
        idToIndexInRow[nameTable->GetId(columns[columnIndex])] = columnIndex;
    }

    return New<TSchemalessWriterForSchemafulDsv>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        config,
        idToIndexInRow);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    try {
        auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
        return CreateSchemalessWriterForSchemafulDsv(
            config,
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidFormat, "Failed to parse config for schemaful DSV format") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    TTableSchemaPtr schema,
    IAsyncOutputStreamPtr stream)
{
    std::vector<int> idToIndexInRow(schema->GetColumnCount(), -1);
    if (config->Columns) {
        ValidateDuplicateColumns(*config->Columns);
        for (int columnIndex = 0; columnIndex < static_cast<int>(config->Columns->size()); ++columnIndex) {
            idToIndexInRow[schema->GetColumnIndexOrThrow((*config->Columns)[columnIndex])] = columnIndex;
        }
    } else {
        std::iota(idToIndexInRow.begin(), idToIndexInRow.end(), 0);
    }

    return New<TSchemafulWriterForSchemafulDsv>(
        std::move(stream),
        std::move(config),
        idToIndexInRow);
}

IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const IAttributeDictionary& attributes,
    TTableSchemaPtr schema,
    IAsyncOutputStreamPtr stream)
{
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return CreateSchemafulWriterForSchemafulDsv(
        std::move(config),
        std::move(schema),
        std::move(stream));
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT::NFormats
