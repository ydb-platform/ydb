#include "arrow_row_stream_encoder.h"

#include <yt/yt/client/api/rpc_proxy/row_stream.h>
#include <yt/yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/columnar.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/range.h>

#include <util/system/align.h>

namespace NYT::NArrow {

using namespace NApi::NRpcProxy;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ArrowLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TArrowRowStreamEncoder)

class TArrowRowStreamEncoder
    : public IRowStreamEncoder
{
public:
    TArrowRowStreamEncoder(
        TTableSchemaPtr schema,
        std::optional<std::vector<std::string>> columns,
        TNameTablePtr nameTable,
        IRowStreamEncoderPtr fallbackEncoder,
        NFormats::TControlAttributesConfigPtr controlAttributesConfig)
        : Schema_(std::move(schema))
        , Columns_(std::move(columns))
        , NameTable_(std::move(nameTable))
        , FallbackEncoder_(std::move(fallbackEncoder))
        , ControlAttributesConfig_(controlAttributesConfig)
        , OutputStream_(Data_)
        , AsyncOutputStream_(NConcurrency::CreateAsyncAdapter(&OutputStream_))
    {
        YT_LOG_DEBUG("Row stream encoder created (Schema: %v)",
            *Schema_);
    }

    TSharedRef Encode(
        const IUnversionedRowBatchPtr& batch,
        const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics) override;

private:
    const TTableSchemaPtr Schema_;
    const std::optional<std::vector<std::string>> Columns_;
    const TNameTablePtr NameTable_;
    const IRowStreamEncoderPtr FallbackEncoder_;
    const NFormats::TControlAttributesConfigPtr ControlAttributesConfig_;

    NFormats::ISchemalessFormatWriterPtr Writer_ = nullptr;
    TString Data_;
    TStringOutput OutputStream_;
    NConcurrency::IFlushableAsyncOutputStreamPtr AsyncOutputStream_;
};

DEFINE_REFCOUNTED_TYPE(TArrowRowStreamEncoder)


TSharedRef TArrowRowStreamEncoder::Encode(
    const IUnversionedRowBatchPtr& batch,
    const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics)
{
    auto columnarBatch = batch->TryAsColumnar();
    if (!columnarBatch) {
        YT_LOG_DEBUG("Encoding non-columnar batch; running fallback");
        return FallbackEncoder_->Encode(batch, statistics);
    }
    YT_LOG_DEBUG("Encoding columnar batch (RowCount: %v)",
        batch->GetRowCount());

    NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
    descriptor.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
    descriptor.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);
    descriptor.set_rowset_format(NApi::NRpcProxy::NProto::RF_ARROW);

    if (!Writer_) {
        // The writer is created lazily to avoid unnecessary errors in the constructor when using fallbackEncoder
        Writer_ = CreateStaticTableWriterForFormat(
            NFormats::EFormatType::Arrow,
            NameTable_,
            {Schema_},
            {Columns_},
            AsyncOutputStream_,
            /*enableContextSaving*/ false,
            ControlAttributesConfig_,
            /*keyColumnCount*/ 0);
    }
    Data_.clear();
    Writer_->WriteBatch(batch);
    NConcurrency::WaitFor(Writer_->Flush())
        .ThrowOnError();

    auto rowRefs = TSharedRef::FromString(Data_);

    auto [block, payloadRef] = SerializeRowStreamBlockEnvelope(
        rowRefs.Size(),
        descriptor,
        statistics);

    MergeRefsToRef(std::vector<TSharedRef>{rowRefs}, payloadRef);

    return block;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IRowStreamEncoderPtr CreateArrowRowStreamEncoder(
    TTableSchemaPtr schema,
    std::optional<std::vector<std::string>> columns,
    TNameTablePtr nameTable,
    IRowStreamEncoderPtr fallbackEncoder,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig)
{
    return New<TArrowRowStreamEncoder>(
        std::move(schema),
        std::move(columns),
        std::move(nameTable),
        std::move(fallbackEncoder),
        std::move(controlAttributesConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
