#include "file_fragment_writer.h"

#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TFileFragmentWriter
    : public IFileFragmentWriter
{
public:
    explicit TFileFragmentWriter(std::unique_ptr<IOutputStreamWithResponse> stream)
        : Underlying_(std::move(stream))
    { }

    TWriteFileFragmentResult GetWriteFragmentResult() const override
    {
        return TWriteFileFragmentResult(NodeFromYsonString(Underlying_->GetResponse()));
    }

private:
    std::unique_ptr<IOutputStreamWithResponse> Underlying_;

    void DoWrite(const void* buf, size_t len) override
    {
        Underlying_->Write(buf, len);
    }

    void DoFinish() override
    {
        Underlying_->Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileFragmentWriterPtr CreateFileFragmentWriter(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TDistributedWriteFileCookie& cookie,
    const TFileFragmentWriterOptions& options)
{
    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IOutputStreamWithResponse>>(
        retryPolicy,
        [&] (TMutationId /*mutationId*/) {
            return rawClient->WriteFileFragment(cookie, options);
        }
    );

    return MakeIntrusive<TFileFragmentWriter>(std::move(stream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
