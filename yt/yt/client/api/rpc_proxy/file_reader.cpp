#include "file_reader.h"

#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

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

TFuture<IFileReaderPtr> CreateFileReader(
    TApiServiceProxy::TReqReadFilePtr request)
{
    return NRpc::CreateRpcClientInputStream(std::move(request))
        .Apply(BIND([=] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
            return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
                NApi::NRpcProxy::NProto::TReadFileMeta meta;
                if (!TryDeserializeProto(&meta, metaRef)) {
                    THROW_ERROR_EXCEPTION("Failed to deserialize file stream header");
                }

                return New<TFileReader>(inputStream, FromProto<TObjectId>(meta.id()), meta.revision());
            })).As<IFileReaderPtr>();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

