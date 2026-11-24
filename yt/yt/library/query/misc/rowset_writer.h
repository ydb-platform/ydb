#pragma once

#include <yt/yt/client/table_client/unversioned_writer.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TSimpleRowsetWriter
    : public NTableClient::IUnversionedRowsetWriter
{
public:
    explicit TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider);

    TSharedRange<NTableClient::TUnversionedRow> GetRows() const;

    TFuture<TSharedRange<NTableClient::TUnversionedRow>> GetResult() const;

    TFuture<void> Close() override;

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;

    TFuture<void> GetReadyEvent() override;

    void Fail(const TError& error);

    std::optional<NTableClient::TRowsDigest> GetDigest() const override;

private:
    struct TSchemafulRowsetWriterBufferTag
    { };

    const TPromise<TSharedRange<NTableClient::TUnversionedRow>> Result_ = NewPromise<TSharedRange<NTableClient::TUnversionedRow>>();
    const NTableClient::TRowBufferPtr RowBuffer_;
    std::vector<NTableClient::TUnversionedRow> Rows_;
};

DEFINE_REFCOUNTED_TYPE(TSimpleRowsetWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
