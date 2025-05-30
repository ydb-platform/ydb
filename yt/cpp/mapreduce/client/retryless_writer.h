#pragma once

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>
#include <yt/cpp/mapreduce/http/requests.h>

#include <yt/cpp/mapreduce/http_client/raw_requests.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/io/helpers.h>

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
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        size_t bufferSize,
        const TWriterOptions& options)
        : BufferSize_(bufferSize)
        , AutoFinish_(options.AutoFinish_)
    {
        Output_ = NDetail::NRawClient::WriteTable(context, parentId, path, format, options);
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
    std::unique_ptr<IOutputStream> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
