#include "schemaless_writer_adapter.h"

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFormats {

using namespace NComplexTypes;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const i64 ContextBufferSize = static_cast<i64>(128 * 7) * 1024;
static const i64 ContextBufferCapacity = static_cast<i64>(1024) * 1024;

////////////////////////////////////////////////////////////////////////////////

TSchemalessFormatWriterBase::TSchemalessFormatWriterBase(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
    : NameTable_(std::move(nameTable))
    , Output_(std::move(output))
    , EnableContextSaving_(enableContextSaving)
    , ControlAttributesConfig_(std::move(controlAttributesConfig))
    , KeyColumnCount_(keyColumnCount)
    , NameTableReader_(std::make_unique<TNameTableReader>(NameTable_))
{
    CurrentBuffer_.Reserve(ContextBufferCapacity);

    EnableRowControlAttributes_ = ControlAttributesConfig_->EnableTableIndex ||
        ControlAttributesConfig_->EnableRangeIndex ||
        ControlAttributesConfig_->EnableRowIndex ||
        ControlAttributesConfig_->EnableTabletIndex;

    try {
        RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
        TabletIndexId_ = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to add system columns to name table for a format writer") << ex;
    }
}

TFuture<void> TSchemalessFormatWriterBase::GetReadyEvent()
{
    return MakeFuture(Error_);
}

TFuture<void> TSchemalessFormatWriterBase::Close()
{
    if (Closed_) {
        return MakeFuture(Error_);
    }

    try {
        if (ControlAttributesConfig_->EnableEndOfStream) {
            WriteEndOfStream();
        }
        DoFlushBuffer();
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
    }

    Closed_ = true;

    return MakeFuture(Error_);
}

TBlobOutput* TSchemalessFormatWriterBase::GetOutputStream()
{
    return &CurrentBuffer_;
}

TBlob TSchemalessFormatWriterBase::GetContext() const
{
    TBlob result;
    result.Append(PreviousBuffer_);
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

void TSchemalessFormatWriterBase::TryFlushBuffer(bool force)
{
    if (CurrentBuffer_.Size() > ContextBufferSize || (!EnableContextSaving_ && force)) {
        DoFlushBuffer();
    }
}

i64 TSchemalessFormatWriterBase::GetWrittenSize() const
{
    return WrittenSize_;
}

void TSchemalessFormatWriterBase::FlushWriter()
{ }

void TSchemalessFormatWriterBase::DoFlushBuffer()
{
    FlushWriter();

    if (CurrentBuffer_.Size() == 0) {
        return;
    }

    WrittenSize_ += CurrentBuffer_.Size();

    PreviousBuffer_ = CurrentBuffer_.Flush();
    WaitFor(Output_->Write(PreviousBuffer_))
        .ThrowOnError();

    CurrentBuffer_.Reserve(ContextBufferCapacity);
}

bool TSchemalessFormatWriterBase::Write(TRange<TUnversionedRow> rows)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWrite(rows);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

bool TSchemalessFormatWriterBase::WriteBatch(IUnversionedRowBatchPtr rowBatch)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWriteBatch(rowBatch);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

void TSchemalessFormatWriterBase::DoWriteBatch(IUnversionedRowBatchPtr rowBatch)
{
    return DoWrite(rowBatch->MaterializeRows());
}

bool TSchemalessFormatWriterBase::CheckKeySwitch(TUnversionedRow row, bool isLastRow)
{
    if (!ControlAttributesConfig_->EnableKeySwitch) {
        return false;
    }

    bool needKeySwitch = false;
    try {
        needKeySwitch = CurrentKey_ && CompareRows(row, CurrentKey_, KeyColumnCount_);
        CurrentKey_ = row;
    } catch (const std::exception& ex) {
        // COMPAT(psushin): composite values are not comparable anymore.
        THROW_ERROR_EXCEPTION("Cannot inject key switch into output stream") << ex;
    }

    if (isLastRow && CurrentKey_) {
        // After processing last row we create a copy of CurrentKey.
        LastKey_ = GetKeyPrefix(CurrentKey_, KeyColumnCount_);
        CurrentKey_ = LastKey_;
    }

    return needKeySwitch;
}

bool TSchemalessFormatWriterBase::IsSystemColumnId(int id) const
{
    return IsTableIndexColumnId(id) ||
        IsRangeIndexColumnId(id) ||
        IsRowIndexColumnId(id) ||
        IsTabletIndexColumnId(id);
}

bool TSchemalessFormatWriterBase::IsTableIndexColumnId(int id) const
{
    return id == TableIndexId_;
}

bool TSchemalessFormatWriterBase::IsRowIndexColumnId(int id) const
{
    return id == RowIndexId_;
}

bool TSchemalessFormatWriterBase::IsRangeIndexColumnId(int id) const
{
    return id == RangeIndexId_;
}

bool TSchemalessFormatWriterBase::IsTabletIndexColumnId(int id) const
{
    return id == TabletIndexId_;
}

int TSchemalessFormatWriterBase::GetRangeIndexColumnId() const
{
    return RangeIndexId_;
}

int TSchemalessFormatWriterBase::GetRowIndexColumnId() const
{
    return RowIndexId_;
}

int TSchemalessFormatWriterBase::GetTableIndexColumnId() const
{
    return TableIndexId_;
}

int TSchemalessFormatWriterBase::GetTabletIndexColumnId() const
{
    return TabletIndexId_;
}

void TSchemalessFormatWriterBase::WriteControlAttributes(TUnversionedRow row)
{
    if (!EnableRowControlAttributes_) {
        return;
    }

    ++RowIndex_;

    std::optional<i64> tableIndex;
    std::optional<i64> rangeIndex;
    std::optional<i64> rowIndex;
    std::optional<i64> tabletIndex;

    for (const auto* it = row.Begin(); it != row.End(); ++it) {
        if (it->Id == TableIndexId_) {
            tableIndex = it->Data.Int64;
        } else if (it->Id == RowIndexId_) {
            rowIndex = it->Data.Int64;
        } else if (it->Id == RangeIndexId_) {
            rangeIndex = it->Data.Int64;
        } else if (it->Id == TabletIndexId_) {
            tabletIndex = it->Data.Int64;
        }
    }

    bool needRowIndex = false;
    if (tableIndex && *tableIndex != TableIndex_) {
        if (ControlAttributesConfig_->EnableTableIndex) {
            WriteTableIndex(*tableIndex);
        }
        TableIndex_ = *tableIndex;
        needRowIndex = true;
    }

    if (rangeIndex && *rangeIndex != RangeIndex_) {
        if (ControlAttributesConfig_->EnableRangeIndex) {
            WriteRangeIndex(*rangeIndex);
        }
        RangeIndex_ = *rangeIndex;
        needRowIndex = true;
    }

    if (tabletIndex && *tabletIndex != TabletIndex_) {
        if (ControlAttributesConfig_->EnableTabletIndex) {
            WriteTabletIndex(*tabletIndex);
        }
        TabletIndex_ = *tabletIndex;
        needRowIndex = true;
    }

    if (rowIndex) {
        needRowIndex = needRowIndex || (*rowIndex != RowIndex_);
        RowIndex_ = *rowIndex;
        if (ControlAttributesConfig_->EnableRowIndex && needRowIndex) {
            WriteRowIndex(*rowIndex);
        }
    }
}

void TSchemalessFormatWriterBase::WriteTableIndex(i64 /* tableIndex */)
{ }

void TSchemalessFormatWriterBase::WriteRangeIndex(i64 /* rangeIndex */)
{ }

void TSchemalessFormatWriterBase::WriteRowIndex(i64 /* rowIndex */)
{ }

void TSchemalessFormatWriterBase::WriteTabletIndex(i64 /* tabletIndex */)
{ }

void TSchemalessFormatWriterBase::WriteEndOfStream()
{ }

bool TSchemalessFormatWriterBase::HasError() const
{
    return !Error_.IsOK();
}

const TError& TSchemalessFormatWriterBase::GetError() const
{
    return Error_;
}

void TSchemalessFormatWriterBase::RegisterError(const TError& error)
{
    Error_ = error;
}

TFuture<void> TSchemalessFormatWriterBase::Flush()
{
    TryFlushBuffer(/*force*/ true);
    return MakeFuture(Error_);
}

std::optional<TMD5Hash> TSchemalessFormatWriterBase::GetDigest() const {
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterAdapter::TSchemalessWriterAdapter(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
    : TSchemalessFormatWriterBase(
        std::move(nameTable),
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount)
{ }

// CreateConsumerForFormat may throw an exception if there is no consumer for the given format,
// so we set Consumer_ inside Init function rather than inside the constructor.
void TSchemalessWriterAdapter::Init(const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas, const TFormat& format)
{
    // This is generic code for those formats, that support skipping nulls.
    // See #TYsonFormatConfig and #TJsonFormatConfig.
    SkipNullValues_ = format.Attributes().Get("skip_null_values", false);
    TYsonConverterConfig config{
        .ComplexTypeMode = format.Attributes().Get("complex_type_mode", EComplexTypeMode::Named),
        .StringKeyedDictMode = format.Attributes().Get("string_keyed_dict_mode", EDictMode::Positional),
        .DecimalMode = format.Attributes().Get("decimal_mode", EDecimalMode::Binary),
        .TimeMode = format.Attributes().Get("time_mode", ETimeMode::Binary),
        .UuidMode = format.Attributes().Get("uuid_mode", EUuidMode::Binary),
        .SkipNullValues = SkipNullValues_,
    };

    Consumer_ = CreateConsumerForFormat(format, EDataType::Tabular, GetOutputStream());

    ValueWriters_.reserve(tableSchemas.size());
    for (const auto& schema : tableSchemas) {
        ValueWriters_.emplace_back(NameTable_, schema, config);
    }
}

void TSchemalessWriterAdapter::DoWrite(TRange<TUnversionedRow> rows)
{
    int count = static_cast<int>(rows.Size());
    for (int index = 0; index < count; ++index) {
        if (CheckKeySwitch(rows[index], index + 1 == count /* isLastRow */)) {
            WriteControlAttribute(EControlAttribute::KeySwitch, true);
        }

        ConsumeRow(rows[index]);
        FlushWriter();
        TryFlushBuffer(false);
    }

    TryFlushBuffer(true);
}

void TSchemalessWriterAdapter::FlushWriter()
{
    Consumer_->Flush();
}

template <class T>
void TSchemalessWriterAdapter::WriteControlAttribute(
    EControlAttribute controlAttribute,
    T value)
{
    BuildYsonListFragmentFluently(Consumer_.get())
        .Item()
        .BeginAttributes()
            .Item(FormatEnum(controlAttribute)).Value(value)
        .EndAttributes()
        .Entity();
}

void TSchemalessWriterAdapter::WriteTableIndex(i64 tableIndex)
{
    CurrentTableIndex_ = tableIndex;
    WriteControlAttribute(EControlAttribute::TableIndex, tableIndex);
}

void TSchemalessWriterAdapter::WriteRowIndex(i64 rowIndex)
{
    WriteControlAttribute(EControlAttribute::RowIndex, rowIndex);
}

void TSchemalessWriterAdapter::WriteRangeIndex(i64 rangeIndex)
{
    WriteControlAttribute(EControlAttribute::RangeIndex, rangeIndex);
}

void TSchemalessWriterAdapter::WriteTabletIndex(i64 tabletIndex)
{
    WriteControlAttribute(EControlAttribute::TabletIndex, tabletIndex);
}

void TSchemalessWriterAdapter::WriteEndOfStream()
{
    WriteControlAttribute(EControlAttribute::EndOfStream, true);
}

void TSchemalessWriterAdapter::ConsumeRow(TUnversionedRow row)
{
    WriteControlAttributes(row);

    Consumer_->OnListItem();
    Consumer_->OnBeginMap();
    for (const auto* it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;

        if (IsSystemColumnId(value.Id)) {
            continue;
        }

        if (value.Type == EValueType::Null && SkipNullValues_) {
            continue;
        }

        Consumer_->OnKeyedItem(NameTableReader_->GetName(value.Id));
        ValueWriters_[CurrentTableIndex_].WriteValue(value, Consumer_.get());
    }
    Consumer_->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
