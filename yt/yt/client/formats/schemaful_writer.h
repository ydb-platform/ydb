#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriter
    : public NTableClient::IUnversionedRowsetWriter
{
public:
    TSchemafulWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        NTableClient::TTableSchemaPtr schema,
        const std::function<std::unique_ptr<NYson::IFlushableYsonConsumer>(IZeroCopyOutput*)>& consumerBuilder);

    TFuture<void> Close() override;

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;

    TFuture<void> GetReadyEvent() override;

    std::optional<NCrypto::TMD5Hash> GetDigest() const override;

private:
    const NConcurrency::IAsyncOutputStreamPtr Stream_;
    const NTableClient::TTableSchemaPtr Schema_;

    TBlobOutput Buffer_;

    TFuture<void> Result_;

    const std::unique_ptr<NYson::IFlushableYsonConsumer> Consumer_;

    THashMap<int, NComplexTypes::TYsonServerToClientConverter> ColumnConverters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
