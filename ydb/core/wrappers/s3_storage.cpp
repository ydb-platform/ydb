#include "s3_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/VersionConfig.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/string/cast.h>

#include <algorithm>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;

namespace {

static const TString OkStatusName = "OK";
static const TString UnknownStatusName = "Unknown";
static const TString StatusCountersGroup = "status";
static const TString HttpCodeCountersGroup = "http_code";
static const TString AwsCodeCountersGroup = "aws_code";
static const TString RequestsCountCounter = "RequestsCount";
static const TString BytesWrittenCounter = "BytesWritten";
static const TString BytesReadCounter = "BytesRead";
static const TString LatencyCounter = "LatencyMs";

NMonitoring::IHistogramCollectorPtr GetLatencyCollector() {
    return NMonitoring::ExplicitHistogram({
        5, 10, 25, 50, 100, 500, 1000, 2500, 5000,
        10'000, 30'000, 60'000, 120'000, 180'000, 300'000, 600'000});
}

template <typename TEvRequest, typename TEvResponse>
class TContextBase: public AsyncCallerContext {
public:
    explicit TContextBase(
            const TActorSystem* sys,
            const TActorId& sender,
            IRequestContext::TPtr requestContext,
            const Aws::S3::Model::StorageClass storageClass,
            const TReplyAdapterContainer& replyAdapter,
            TIntrusivePtr<TS3ExternalStorage::TS3RequestCounters> counters)
        : AsyncCallerContext()
        , ActorSystem(sys)
        , Sender(sender)
        , Counters(std::move(counters))
        , RequestContext(requestContext)
        , StorageClass(storageClass)
        , ReplyAdapter(replyAdapter)
    {
    }

    const TActorSystem* GetActorSystem() const {
        return ActorSystem;
    }

    virtual const typename TEvRequest::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) {
        return ev->Get()->GetRequest();
    }

protected:
    void Send(const TActorId& recipient, std::unique_ptr<IEventBase>&& ev) const {
        ActorSystem->Send(ReplyAdapter.GetRecipient(recipient), ev.release());
    }

    void Send(std::unique_ptr<IEventBase>&& ev) const {
        Send(Sender, std::move(ev));
    }

    void IncrementCounters(const typename TEvResponse::TOutcome& outcome) const {
        const TDuration latency = TInstant::Now() - Start;
        const bool success = outcome.IsSuccess();

        ReplyAdapter.CollectStats({
            .RequestName = TEvRequest::RequestName,
            .Latency = latency,
            .Success = success,
            .HttpResponseCode = success ? 0 : static_cast<int>(outcome.GetError().GetResponseCode()),
            .BytesRead = success ? BytesRead.value_or(0) : 0,
            .BytesWritten = success ? BytesWritten.value_or(0) : 0,
        });

        if (!Counters) {
            return;
        }

        // Error code: requests per second
        NMonitoring::TCounterForPtr* requestsCount = nullptr;
        if (outcome.IsSuccess()) {
            requestsCount = Counters->GetSuccessRequestsCountCounter();
        } else {
            requestsCount = Counters->GetRequestsCountCounter(
                TString(outcome.GetError().GetExceptionName()),
                static_cast<int>(outcome.GetError().GetResponseCode()),
                static_cast<int>(outcome.GetError().GetErrorType()));
        }
        if (requestsCount) {
            ++*requestsCount;
        }

        // Latency
        if (auto* hist = Counters->GetLatency()) {
            hist->Collect(latency.MilliSeconds());
        }

        if (outcome.IsSuccess() && BytesWritten) {
            if (auto* bw = Counters->GetBytesWritten()) {
                *bw += *BytesWritten;
            }
        }

        if (outcome.IsSuccess() && BytesRead) {
            if (auto* br = Counters->GetBytesRead()) {
                *br += *BytesRead;
            }
        }
    }

private:
    const TActorSystem* ActorSystem;
    const TActorId Sender;

protected:
    const TInstant Start = TInstant::Now();
    // Metrics for some special methods
    std::optional<size_t> BytesWritten;
    std::optional<size_t> BytesRead;
    TIntrusivePtr<TS3ExternalStorage::TS3RequestCounters> Counters;
    mutable bool Replied = false;
    IRequestContext::TPtr RequestContext;
    const Aws::S3::Model::StorageClass StorageClass;
    const TReplyAdapterContainer& ReplyAdapter;

}; // TContextBase

template <typename TEvRequest, typename TEvResponse>
class TBasicContext: public TContextBase<TEvRequest, TEvResponse> {
public:
    using TContextBase<TEvRequest, TEvResponse>::TContextBase;

    void Reply(const typename TEvRequest::TRequest&, const typename TEvResponse::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(this->Replied, true), "Double-reply");
        this->IncrementCounters(outcome);
        this->Send(std::make_unique<TEvResponse>(outcome, this->RequestContext));
    }
};

template <typename TEvRequest, typename TEvResponse>
class TGenericContext: public TContextBase<TEvRequest, TEvResponse> {
public:
    using TContextBase<TEvRequest, TEvResponse>::TContextBase;

    void Reply(const typename TEvRequest::TRequest& request, const typename TEvResponse::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(this->Replied, true), "Double-reply");

        typename TEvResponse::TKey key;
        if (request.KeyHasBeenSet()) {
            key = request.GetKey();
        }

        this->IncrementCounters(outcome);
        this->Send(MakeResponse(key, outcome));
    }

protected:
    virtual std::unique_ptr<IEventBase> MakeResponse(
            const typename TEvResponse::TKey& key,
            const typename TEvResponse::TOutcome& outcome) const
    {
        return this->ReplyAdapter.RebuildReplyEvent(std::make_unique<TEvResponse>(key, outcome));
    }
};

template <typename TEvRequest, typename TEvResponse>
class TOutputStreamContext: public TGenericContext<TEvRequest, TEvResponse> {
    using TBase = TGenericContext<TEvRequest, TEvResponse>;

    class TOutputStreamBuf: public std::streambuf {
    public:
        explicit TOutputStreamBuf(TString& buffer) {
            char* begin = buffer.Detach();
            setp(begin, begin + buffer.size());
            setg(begin, begin, begin);
        }

    private:
        // S3 uses the same response stream for the requested range and for an XML error document.
        // Only allocate extra storage when the server actually sends more than the requested range.
        static constexpr size_t MaxErrorBodySize = 64 * 1024;
        std::unique_ptr<char[]> ErrorBody;

        int_type overflow(int_type ch) override {
            if (traits_type::eq_int_type(ch, traits_type::eof())) {
                return traits_type::not_eof(ch);
            }
            const size_t written = pptr() - pbase();
            if (written >= MaxErrorBodySize) {
                return traits_type::eof();
            }
            const auto read = gptr() - eback();
            ErrorBody = std::make_unique<char[]>(MaxErrorBodySize);
            char* begin = ErrorBody.get();
            std::copy_n(pbase(), written, begin);
            setp(begin, begin + MaxErrorBodySize);
            pbump(static_cast<int>(written));
            setg(begin, begin + read, begin + written);
            return sputc(traits_type::to_char_type(ch));
        }

        int_type underflow() override {
            setg(eback(), gptr(), pptr());
            return gptr() == egptr() ? traits_type::eof() : traits_type::to_int_type(*gptr());
        }

        pos_type seekoff(off_type off, std::ios_base::seekdir dir, std::ios_base::openmode mode) override {
            if (mode == std::ios_base::out && dir == std::ios_base::cur && off == 0) {
                return pptr() - pbase();
            }
            if (mode == std::ios_base::in) {
                if (dir == std::ios_base::cur) {
                    off += gptr() - eback();
                } else if (dir == std::ios_base::end) {
                    off += pptr() - pbase();
                }
                return seekpos(off, mode);
            }
            return pos_type(off_type(-1));
        }

        pos_type seekpos(pos_type pos, std::ios_base::openmode mode) override {
            const off_type offset = pos;
            if (mode != std::ios_base::in || offset < 0 || offset > pptr() - pbase()) {
                return pos_type(off_type(-1));
            }
            setg(pbase(), pbase() + offset, pptr());
            return pos;
        }
    };

public:
    using TBase::TBase;

    const typename TEvRequest::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->Request;

        std::pair<ui64, ui64> range;
        Y_ABORT_UNLESS(request.RangeHasBeenSet() && TEvGetObjectResponse::TryParseRange(request.GetRange().c_str(), range));
        Range = range;

        Buffer.resize(range.second - range.first + 1);
        this->BytesRead = Buffer.size();
        request.SetResponseStreamFactory([this]() {
            return Aws::New<DefaultUnderlyingStream>("StreamContext",
                MakeUnique<TOutputStreamBuf>("StreamContext", Buffer));
            }
        );

        return request;
    }

    void Reply(const typename TEvRequest::TRequest& request, const typename TEvResponse::TOutcome& outcome) const {
        if (outcome.IsSuccess()) {
            const auto written = static_cast<std::streamoff>(outcome.GetResult().GetBody().tellp());
            if (written != static_cast<std::streamoff>(Buffer.size())) {
                const TString reason = TStringBuilder() << "Invalid S3 range response length. Expected " << Buffer.size()
                    << " bytes, stream position " << written;
                auto error = AWSError<S3Errors>(S3Errors::NETWORK_CONNECTION, "InvalidRangeResponse", reason.c_str(), true);
                return TBase::Reply(request, typename TEvResponse::TOutcome(S3Error(std::move(error))));
            }
        }
        TBase::Reply(request, outcome);
    }

protected:
    std::unique_ptr<IEventBase> MakeResponse(
            const typename TEvResponse::TKey& key,
            const typename TEvResponse::TOutcome& outcome) const override
    {
        Y_ABORT_UNLESS(Range);
        std::unique_ptr<TEvResponse> response;
        if (outcome.IsSuccess()) {
            response = std::make_unique<TEvResponse>(key, *Range, outcome, std::move(Buffer));
        } else {
            response = std::make_unique<TEvResponse>(key, *Range, outcome);
        }
        return this->ReplyAdapter.RebuildReplyEvent(std::move(response));
    }

private:
    std::optional<std::pair<ui64, ui64>> Range;
    mutable TString Buffer;

}; // TOutputStreamContext

template <typename TEvRequest, typename TEvResponse>
class TInputStreamContext: public TGenericContext<TEvRequest, TEvResponse> {
    class TInputStreamBuf: public PreallocatedStreamBuf {
        TInputStreamBuf(char* data, size_t size)
            : PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(data), size)
        {
        }

        TInputStreamBuf(const char* data, size_t size)
            : TInputStreamBuf(const_cast<char*>(data), size)
        {
        }

    public:
        explicit TInputStreamBuf(const TStringBuf buf)
            : TInputStreamBuf(buf.data(), buf.size())
        {
        }
    };

public:
    using TGenericContext<TEvRequest, TEvResponse>::TGenericContext;

    const typename TEvRequest::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->MutableRequest();
        Buffer = std::move(ev->Get()->Body);
        this->BytesWritten = Buffer.size();
        request.SetBody(MakeShared<DefaultUnderlyingStream>("StreamContext",
            MakeUnique<TInputStreamBuf>("StreamContext", Buffer)));

        return request;
    }

private:
    TString Buffer;

}; // TInputStreamContext

template <typename TEvRequest, typename TEvResponse, template <typename, typename> typename TContext = TGenericContext>
class TContextWithStorageClass: public TContext<TEvRequest, TEvResponse> {
public:
    using TContext<TEvRequest, TEvResponse>::TContext;

    const typename TEvRequest::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->MutableRequest();
        if (this->StorageClass != Aws::S3::Model::StorageClass::NOT_SET) {
            request.WithStorageClass(this->StorageClass);
        }
        return TContext<TEvRequest, TEvResponse>::PrepareRequest(ev);
    }
};

template <typename TEvRequest, typename TEvResponse>
class TPutObjectContext: public TContextWithStorageClass<TEvRequest, TEvResponse, TInputStreamContext> {
public:
    using TContextWithStorageClass<TEvRequest, TEvResponse, TInputStreamContext>::TContextWithStorageClass;
};

template <typename TEvRequest, typename TEvResponse>
class TCreateMultipartUploadContext: public TContextWithStorageClass<TEvRequest, TEvResponse> {
public:
    using TContextWithStorageClass<TEvRequest, TEvResponse>::TContextWithStorageClass;
};

} // anonymous

NMonitoring::THistogramCounter* TS3ExternalStorage::TS3RequestCounters::GetLatency() const {
    if (auto* hist = LatencyHist.load(std::memory_order_acquire)) {
        return hist;
    }
    if (!RequestGroup) {
        return nullptr;
    }

    auto* hist = RequestGroup->GetHistogram(LatencyCounter, GetLatencyCollector()).Get();
    LatencyHist.store(hist, std::memory_order_release);
    return hist;
}

NMonitoring::TCounterForPtr* TS3ExternalStorage::TS3RequestCounters::GetBytesWritten() const {
    if (auto* counter = BytesWritten.load(std::memory_order_acquire)) {
        return counter;
    }
    if (!RequestGroup) {
        return nullptr;
    }

    auto* counter = RequestGroup->GetCounter(BytesWrittenCounter, true).Get();
    BytesWritten.store(counter, std::memory_order_release);
    return counter;
}

NMonitoring::TCounterForPtr* TS3ExternalStorage::TS3RequestCounters::GetBytesRead() const {
    if (auto* counter = BytesRead.load(std::memory_order_acquire)) {
        return counter;
    }
    if (!RequestGroup) {
        return nullptr;
    }

    auto* counter = RequestGroup->GetCounter(BytesReadCounter, true).Get();
    BytesRead.store(counter, std::memory_order_release);
    return counter;
}

NMonitoring::TDynamicCounterPtr TS3ExternalStorage::TS3RequestCounters::GetStatusSubgroupImpl(const TString& statusName, int httpResponseCode, int awsErrorType) const {
    return RequestGroup
        ->GetSubgroup(StatusCountersGroup, statusName ? statusName : UnknownStatusName)
        ->GetSubgroup(HttpCodeCountersGroup, ToString(httpResponseCode))
        ->GetSubgroup(AwsCodeCountersGroup, ToString(awsErrorType));
}

NMonitoring::TCounterForPtr* TS3ExternalStorage::TS3RequestCounters::GetRequestsCountCounter(const TString& statusName, int httpResponseCode, int awsErrorType) const {
    if (!RequestGroup) {
        return nullptr;
    }

    return GetStatusSubgroupImpl(statusName, httpResponseCode, awsErrorType)->GetCounter(RequestsCountCounter, true).Get();
}

NMonitoring::TCounterForPtr* TS3ExternalStorage::TS3RequestCounters::GetSuccessRequestsCountCounter() const {
    if (auto* counter = SuccessRequests.load(std::memory_order_acquire)) {
        return counter;
    }

    auto* counter = GetRequestsCountCounter(OkStatusName, 200, -1);
    SuccessRequests.store(counter, std::memory_order_release);
    return counter;
}

TS3ExternalStorage::~TS3ExternalStorage() {
    if (Client) {
        Client->DisableRequestProcessing();
        std::unique_lock guard(RunningQueriesMutex);
        RunningQueriesNotifier.wait(guard, [&] { return RunningQueriesCount == 0; });
    }
}

void TS3ExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    Call<TEvGetObjectRequest, TEvGetObjectResponse, TOutputStreamContext>(
        ev, &S3Client::GetObjectAsync);
}

void TS3ExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    Call<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse, TBasicContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::HeadObjectAsync<>);
#else
        ev, &S3Client::HeadObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    Call<TEvListObjectsRequest, TEvListObjectsResponse, TBasicContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::ListObjectsAsync<>);
#else
        ev, &S3Client::ListObjectsAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    Call<TEvHeadObjectRequest, TEvHeadObjectResponse, TGenericContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::HeadObjectAsync<>);
#else
        ev, &S3Client::HeadObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    Call<TEvPutObjectRequest, TEvPutObjectResponse, TPutObjectContext>(
        ev, &S3Client::PutObjectAsync);
}

void TS3ExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    Call<TEvDeleteObjectRequest, TEvDeleteObjectResponse, TGenericContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::DeleteObjectAsync<>);
#else
        ev, &S3Client::DeleteObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    Call<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse, TBasicContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::DeleteObjectsAsync<>);
#else
        ev, &S3Client::DeleteObjectsAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    Call<TEvCreateMultipartUploadRequest, TEvCreateMultipartUploadResponse, TCreateMultipartUploadContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::CreateMultipartUploadAsync<>);
#else
        ev, &S3Client::CreateMultipartUploadAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    Call<TEvUploadPartRequest, TEvUploadPartResponse, TInputStreamContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::UploadPartAsync<>);
#else
        ev, &S3Client::UploadPartAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    Call<TEvCompleteMultipartUploadRequest, TEvCompleteMultipartUploadResponse, TGenericContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::CompleteMultipartUploadAsync<>);
#else
        ev, &S3Client::CompleteMultipartUploadAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    Call<TEvAbortMultipartUploadRequest, TEvAbortMultipartUploadResponse, TGenericContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::AbortMultipartUploadAsync<>);
#else
        ev, &S3Client::AbortMultipartUploadAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    Call<TEvUploadPartCopyRequest, TEvUploadPartCopyResponse, TGenericContext>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::UploadPartCopyAsync<>);
#else
        ev, &S3Client::UploadPartCopyAsync);
#endif
}

}

#endif // KIKIMR_DISABLE_S3_OPS
