#pragma once

#include "format.h"
#include "helpers.h"
#include "unversioned_value_yson_writer.h"

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/public.h>

#include <memory>
#include <limits>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessFormatWriterBase
    : public ISchemalessFormatWriter
{
public:
    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;
    bool WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close() override;

    TBlob GetContext() const override;

    i64 GetWrittenSize() const override;

    TFuture<void> Flush() override;

    std::optional<NCrypto::TMD5Hash> GetDigest() const override;

protected:
    const NTableClient::TNameTablePtr NameTable_;
    const NConcurrency::IAsyncOutputStreamPtr Output_;
    const bool EnableContextSaving_;
    const TControlAttributesConfigPtr ControlAttributesConfig_;
    const int KeyColumnCount_;

    const std::unique_ptr<NTableClient::TNameTableReader> NameTableReader_;

    NTableClient::TLegacyOwningKey LastKey_;
    NTableClient::TLegacyKey CurrentKey_;

    TSchemalessFormatWriterBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount);

    TBlobOutput* GetOutputStream();

    void TryFlushBuffer(bool force);
    virtual void FlushWriter();

    virtual void DoWrite(TRange<NTableClient::TUnversionedRow> rows) = 0;
    virtual void DoWriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch);

    bool CheckKeySwitch(NTableClient::TUnversionedRow row, bool isLastRow);

    bool IsSystemColumnId(int id) const;
    bool IsTableIndexColumnId(int id) const;
    bool IsRangeIndexColumnId(int id) const;
    bool IsRowIndexColumnId(int id) const;
    bool IsTabletIndexColumnId(int id) const;

    int GetRangeIndexColumnId() const;
    int GetRowIndexColumnId() const;
    int GetTableIndexColumnId() const;
    int GetTabletIndexColumnId() const;

    // This is suitable only for switch-based control attributes,
    // e.g. in such formats as YAMR or YSON.
    void WriteControlAttributes(NTableClient::TUnversionedRow row);
    virtual void WriteTableIndex(i64 tableIndex);
    virtual void WriteRangeIndex(i64 rangeIndex);
    virtual void WriteRowIndex(i64 rowIndex);
    virtual void WriteTabletIndex(i64 tabletIndex);
    virtual void WriteEndOfStream();

    bool HasError() const;
    const TError& GetError() const;
    void RegisterError(const TError& error);

private:
    TBlobOutput CurrentBuffer_;
    TSharedRef PreviousBuffer_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;
    int TabletIndexId_ = -1;

    i64 RangeIndex_ = std::numeric_limits<i64>::min();
    i64 TableIndex_ = std::numeric_limits<i64>::min();
    i64 RowIndex_ = std::numeric_limits<i64>::min();
    i64 TabletIndex_ = std::numeric_limits<i64>::min();

    bool EnableRowControlAttributes_;

    TError Error_;

    i64 WrittenSize_ = 0;

    bool Closed_ = false;

    void DoFlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterAdapter(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount);

    void Init(const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas, const TFormat& format);

private:
    template <class T>
    void WriteControlAttribute(
        NTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(NTableClient::TUnversionedRow row);

    void DoWrite(TRange<NTableClient::TUnversionedRow> rows) override;
    void FlushWriter() override;

    void WriteTableIndex(i64 tableIndex) override;
    void WriteRangeIndex(i64 rangeIndex) override;
    void WriteRowIndex(i64 rowIndex) override;
    void WriteTabletIndex(i64 tabletIndex) override;
    void WriteEndOfStream() override;

private:
    std::vector<TUnversionedValueYsonWriter> ValueWriters_;
    std::unique_ptr<NYson::IFlushableYsonConsumer> Consumer_;
    i64 CurrentTableIndex_ = 0;
    bool SkipNullValues_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
