#include "file_reader.h"

#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TFileReader
    : public IFileReader
{
public:
    TFileReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        TObjectId id,
        NHydra::TRevision revision)
        : Underlying_(std::move(underlying))
        , Id_(id)
        , Revision_(revision)
    {
        YT_VERIFY(Underlying_);
    }

    TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read();
    }

    NObjectClient::TObjectId GetId() const override
    {
        return Id_;
    }

    NHydra::TRevision GetRevision() const override
    {
        return Revision_;
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const TObjectId Id_;
    const NHydra::TRevision Revision_;
};

template <class TMeta, class TRequestPtr>
TFuture<IFileReaderPtr> DoCreateFileReader(TRequestPtr request)
{
    return NRpc::CreateRpcClientInputStream(std::move(request))
        .Apply(BIND([] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
            return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
                TMeta meta;
                if (!TryDeserializeProto(&meta, metaRef)) {
                    THROW_ERROR_EXCEPTION("Failed to deserialize file stream header");
                }

                return New<TFileReader>(
                    inputStream,
                    FromProto<TObjectId>(meta.id()),
                    FromProto<NHydra::TRevision>(meta.revision()));
            })).template As<IFileReaderPtr>();
        }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> CreateFileReader(
    TApiServiceProxy::TReqReadFilePtr request)
{
    return DoCreateFileReader<NApi::NRpcProxy::NProto::TReadFileMeta>(std::move(request));
}

TFuture<IFileReaderPtr> CreateFilePartitionReader(
    TApiServiceProxy::TReqReadFilePartitionPtr request)
{
    return DoCreateFileReader<NApi::NRpcProxy::NProto::TRspReadFilePartitionMeta>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
