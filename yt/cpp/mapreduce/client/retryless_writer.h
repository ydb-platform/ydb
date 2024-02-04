#pragma once

#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/io/helpers.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetrylessWriter
    : public TRawTableWriter
{
public:
    template <class TWriterOptions>
    TRetrylessWriter(
        const TClientContext& context,
        const TTransactionId& parentId,
        const TString& command,
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        size_t bufferSize,
        const TWriterOptions& options)
        : BufferSize_(bufferSize)
        , AutoFinish_(options.AutoFinish_)
    {
        THttpHeader header("PUT", command);
        header.SetInputFormat(format);
        header.MergeParameters(FormIORequestParameters(path, options));
        header.AddTransactionId(parentId);
        header.SetRequestCompression(ToString(context.Config->ContentEncoding));
        if (context.ServiceTicketAuth) {
            header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
        } else {
            header.SetToken(context.Token);
        }

        if (context.ImpersonationUser) {
            header.SetImpersonationUser(*context.ImpersonationUser);
        }

        TString requestId = CreateGuidAsString();

        auto hostName = GetProxyForHeavyRequest(context);
        UpdateHeaderForProxyIfNeed(hostName, context, header);
        Request_ = context.HttpClient->StartRequest(GetFullUrl(hostName, context, header), requestId, header);
        BufferedOutput_.Reset(new TBufferedOutput(Request_->GetStream(), BufferSize_));
    }

    ~TRetrylessWriter() override;
    void NotifyRowEnd() override;
    void Abort() override;

    size_t GetBufferMemoryUsage() const override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    const size_t BufferSize_ = 0;
    const bool AutoFinish_;

    bool Running_ = true;
    NHttpClient::IHttpRequestPtr Request_;
    THolder<TBufferedOutput> BufferedOutput_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
