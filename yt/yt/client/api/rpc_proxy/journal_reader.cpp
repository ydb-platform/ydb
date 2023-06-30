#include "journal_reader.h"

#include <yt/yt/client/api/journal_reader.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TJournalReader
    : public IJournalReader
{
public:
    explicit TJournalReader(
        TApiServiceProxy::TReqReadJournalPtr request)
        : Request_(std::move(request))
    {
        YT_VERIFY(Request_);
    }

    TFuture<void> Open() override
    {
        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateRpcClientInputStream(Request_)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
                    Underlying_ = inputStream;
                }));
        }

        return OpenResult_;
    }

    TFuture<std::vector<TSharedRef>> Read() override
    {
        ValidateOpened();

        return Underlying_->Read().Apply(BIND ([] (const TSharedRef& packedRows) {
            std::vector<TSharedRef> rows;
            if (packedRows) {
                UnpackRefs(packedRows, &rows);
            }
            return rows;
        }));
    }

private:
    const TApiServiceProxy::TReqReadJournalPtr Request_;

    IAsyncZeroCopyInputStreamPtr Underlying_;
    TFuture<void> OpenResult_;

    void ValidateOpened()
    {
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Cannot read from an unopened journal reader");
        }
        OpenResult_.Get().ThrowOnError();
    }
};

IJournalReaderPtr CreateJournalReader(
    TApiServiceProxy::TReqReadJournalPtr request)
{
    return New<TJournalReader>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

