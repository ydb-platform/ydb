#include "file_writer.h"

#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public IFileWriter
{
public:
    explicit TFileWriter(TApiServiceProxy::TReqWriteFilePtr request)
        : Request_(std::move(request))
    {
        YT_VERIFY(Request_);
    }

    TFuture<void> Open() override
    {
        ValidateNotClosed();

        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateRpcClientOutputStream(Request_)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
                    Underlying_ = outputStream;
                })).As<void>();
        }

        return OpenResult_;
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateOpened();
        ValidateNotClosed();

        if (!data) {
            return VoidFuture;
        }

        return Underlying_->Write(data);
    }

    TFuture<void> Close() override
    {
        ValidateOpened();
        ValidateNotClosed();

        Closed_ = true;
        return Underlying_->Close();
    }

private:
    const TApiServiceProxy::TReqWriteFilePtr Request_;

    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> OpenResult_;
    bool Closed_ = false;

    void ValidateOpened()
    {
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Cannot write into an unopened file writer");
        }
        OpenResult_.Get().ThrowOnError();
    }

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("File writer is closed");
        }
    }
};

IFileWriterPtr CreateFileWriter(
    TApiServiceProxy::TReqWriteFilePtr request)
{
    return New<TFileWriter>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

class TFileFragmentWriter
    : public IFileFragmentWriter
{
public:
    explicit TFileFragmentWriter(TApiServiceProxy::TReqWriteFileFragmentPtr request)
        : Request_(std::move(request))
    {
        YT_VERIFY(Request_);
    }

    TFuture<void> Open() override
    {
        using TRspPtr = TIntrusivePtr<NRpc::TTypedClientResponse<NProto::TRspWriteFileFragment>>;

        ValidateNotClosed();

        if (!OpenResult_) {
            WriteResultPromise_ = NewPromise<TSignedWriteFileFragmentResultPtr>();
            WriteResultFuture_ = WriteResultPromise_.ToFuture();

            OpenResult_ = NRpc::CreateRpcClientOutputStream(
                /*request*/ Request_,
                /*rspHandler*/ BIND([this, this_ = MakeStrong(this)] (TRspPtr&& rsp){
                    YT_VERIFY(rsp->has_signed_write_result());
                    WriteResultPromise_.Set(ConvertTo<TSignedWriteFileFragmentResultPtr>(NYson::TYsonString(rsp->signed_write_result())));
                }))
            .Apply(BIND([this, this_ = MakeStrong(this)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
                Underlying_ = outputStream;
            })).As<void>();
        }

        return OpenResult_;
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateOpened();
        ValidateNotClosed();

        if (!data) {
            return VoidFuture;
        }

        return Underlying_->Write(data);
    }

    TFuture<void> Close() override
    {
        ValidateOpened();
        ValidateNotClosed();

        Closed_ = true;
        return AllSucceeded<void>({Underlying_->Close(), WriteResultFuture_.AsVoid()});
    }

    TSignedWriteFileFragmentResultPtr GetWriteFragmentResult() const override
    {
        if (!WriteResultFuture_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't get unset write fragment result");
        }
        return WriteResultFuture_.Get()
            .ValueOrThrow();
    }

private:
    const TApiServiceProxy::TReqWriteFileFragmentPtr Request_;

    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> OpenResult_;

    TPromise<TSignedWriteFileFragmentResultPtr> WriteResultPromise_;
    TFuture<TSignedWriteFileFragmentResultPtr> WriteResultFuture_;

    bool Closed_ = false;

    void ValidateOpened()
    {
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Cannot write into an unopened file writer");
        }
        OpenResult_.Get().ThrowOnError();
    }

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("File writer is closed");
        }
    }
};

IFileFragmentWriterPtr CreateFileFragmentWriter(TApiServiceProxy::TReqWriteFileFragmentPtr request)
{
    return New<TFileFragmentWriter>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
