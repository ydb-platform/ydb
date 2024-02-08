#include "s3_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/VersionConfig.h>
#include <contrib/libs/curl/include/curl/curl.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/cast.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;

namespace {

template <typename TEvRequest, typename TEvResponse>
class TCommonContextBase: public AsyncCallerContext {
protected:
    using TRequest = typename TEvRequest::TRequest;
    using TOutcome = typename TEvResponse::TOutcome;
public:
    explicit TCommonContextBase(const TActorSystem* sys, const TActorId& sender, IRequestContext::TPtr requestContext, const Aws::S3::Model::StorageClass storageClass, const TReplyAdapterContainer& replyAdapter)
        : AsyncCallerContext()
        , RequestContext(requestContext)
        , StorageClass(storageClass)
        , ReplyAdapter(replyAdapter)
        , ActorSystem(sys)
        , Sender(sender)
    {
    }

    const TActorSystem* GetActorSystem() const {
        return ActorSystem;
    }

    virtual const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) {
        return ev->Get()->GetRequest();
    }

protected:
    void Send(const TActorId& recipient, std::unique_ptr<IEventBase>&& ev) const {
        ActorSystem->Send(ReplyAdapter.GetRecipient(recipient), ev.release());
    }

    void Send(std::unique_ptr<IEventBase>&& ev) const {
        Send(Sender, std::move(ev));
    }

    mutable bool Replied = false;
    IRequestContext::TPtr RequestContext;
    const Aws::S3::Model::StorageClass StorageClass;
    const TReplyAdapterContainer& ReplyAdapter;
private:
    const TActorSystem* ActorSystem;
    const TActorId Sender;
}; // TCommonContextBase

template <typename TEvRequest, typename TEvResponse>
class TContextBase: public TCommonContextBase<TEvRequest, TEvResponse> {
private:
    using TBase = TCommonContextBase<TEvRequest, TEvResponse>;
protected:
    virtual std::unique_ptr<IEventBase> MakeResponse(const typename TEvResponse::TKey& key, const typename TBase::TOutcome& outcome) const {
        return TBase::ReplyAdapter.RebuildReplyEvent(std::make_unique<TEvResponse>(key, outcome));
    }

public:
    using TBase::Send;
    using TBase::TBase;
    void Reply(const typename TBase::TRequest& request, const typename TBase::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(TBase::Replied, true), "Double-reply");

        typename TEvResponse::TKey key;
        if (request.KeyHasBeenSet()) {
            key = request.GetKey();
        }
        Send(MakeResponse(key, outcome));
    }
};

template <>
class TContextBase<TEvListObjectsRequest, TEvListObjectsResponse>
    : public TCommonContextBase<TEvListObjectsRequest, TEvListObjectsResponse> {
private:
    using TBase = TCommonContextBase<TEvListObjectsRequest, TEvListObjectsResponse>;
public:
    using TBase::Send;
    using TBase::TBase;
    void Reply(const typename TBase::TRequest& /*request*/, const typename TBase::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(TBase::Replied, true), "Double-reply");

        Send(std::make_unique<TEvListObjectsResponse>(outcome));
    }
};

template <>
class TContextBase<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse>
    : public TCommonContextBase<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse> {
private:
    using TBase = TCommonContextBase<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse>;
public:
    using TBase::Send;
    using TBase::TBase;
    void Reply(const typename TBase::TRequest& /*request*/, const typename TBase::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(TBase::Replied, true), "Double-reply");

        Send(std::make_unique<TEvDeleteObjectsResponse>(outcome));
    }
};

template <>
class TContextBase<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse>
    : public TCommonContextBase<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse> {
private:
    using TBase = TCommonContextBase<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse>;
public:
    using TBase::Send;
    using TBase::TBase;
    void Reply(const typename TBase::TRequest& /*request*/, const typename TBase::TOutcome& outcome) const {
        Y_ABORT_UNLESS(!std::exchange(TBase::Replied, true), "Double-reply");
        Send(std::make_unique<TEvCheckObjectExistsResponse>(outcome, RequestContext));
    }
};

template <typename TEvRequest, typename TEvResponse>
class TOutputStreamContext: public TContextBase<TEvRequest, TEvResponse> {
private:
    using TBase = TContextBase<TEvRequest, TEvResponse>;
protected:
    using TRequest = typename TEvRequest::TRequest;
    using TOutcome = typename TEvResponse::TOutcome;

private:
    class TOutputStreamBuf: public PreallocatedStreamBuf {
        TOutputStreamBuf(char* data, size_t size)
            : PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(data), size)
        {
        }

    public:
        explicit TOutputStreamBuf(TString& buffer)
            : TOutputStreamBuf(buffer.Detach(), buffer.Size())
        {
        }
    };

    static bool TryParseRange(const TString& str, std::pair<ui64, ui64>& range) {
        TStringBuf buf(str);
        if (!buf.SkipPrefix("bytes=")) {
            return false;
        }

        ui64 start;
        if (!TryFromString(buf.NextTok('-'), start)) {
            return false;
        }

        ui64 end;
        if (!TryFromString(buf, end)) {
            return false;
        }

        range = std::make_pair(start, end);
        return true;
    }

    std::optional<std::pair<ui64, ui64>> Range;
public:
    using TContextBase<TEvRequest, TEvResponse>::TContextBase;

    const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->Request;

        std::pair<ui64, ui64> range;
        Y_ABORT_UNLESS(request.RangeHasBeenSet() && TryParseRange(request.GetRange().c_str(), range));
        Range = range;

        Buffer.resize(range.second - range.first + 1);
        request.SetResponseStreamFactory([this]() {
            return Aws::New<DefaultUnderlyingStream>("StreamContext",
                MakeUnique<TOutputStreamBuf>("StreamContext", Buffer));
            });

        return request;
    }

protected:
    std::unique_ptr<IEventBase> MakeResponse(const typename TEvResponse::TKey& key, const TOutcome& outcome) const override {
        Y_ABORT_UNLESS(Range);
        std::unique_ptr<TEvResponse> response;
        if (outcome.IsSuccess()) {
            response = std::make_unique<TEvResponse>(key, *Range, outcome, std::move(Buffer));
        } else {
            response = std::make_unique<TEvResponse>(key, *Range, outcome);
        }
        return TBase::ReplyAdapter.RebuildReplyEvent(std::move(response));
    }

private:
    mutable TString Buffer;

}; // TOutputStreamContext

template <typename TEvRequest, typename TEvResponse>
class TInputStreamContext: public TContextBase<TEvRequest, TEvResponse> {
private:
    using TBase = TContextBase<TEvRequest, TEvResponse>;
protected:
    using TRequest = typename TEvRequest::TRequest;
    using TOutcome = typename TEvResponse::TOutcome;

private:
    class TInputStreamBuf: public PreallocatedStreamBuf {
        TInputStreamBuf(char* data, size_t size)
            : PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(data), size) {
        }

        TInputStreamBuf(const char* data, size_t size)
            : TInputStreamBuf(const_cast<char*>(data), size) {
        }

    public:
        explicit TInputStreamBuf(const TStringBuf buf)
            : TInputStreamBuf(buf.data(), buf.size()) {
        }
    };

public:
    using TBase::TBase;

    const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->MutableRequest();
        Buffer = std::move(ev->Get()->Body);
        request.SetBody(MakeShared<DefaultUnderlyingStream>("StreamContext",
            MakeUnique<TInputStreamBuf>("StreamContext", Buffer)));

        return request;
    }

private:
    TString Buffer;

}; // TInputStreamContext

template <typename TEvRequest, typename TEvResponse>
class TPutInputStreamContext: public TInputStreamContext<TEvRequest, TEvResponse> {
private:
    using TBase = TInputStreamContext<TEvRequest, TEvResponse>;
public:
    using TBase::TBase;

    const typename TBase::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->MutableRequest();
        auto storageClass = TBase::StorageClass;

        // workaround for minio.
        // aws s3 treats NOT_SET as STANDARD
        // but internally sdk just doesn't set corresponding header, while adds it to SignedHeaders
        // and minio implementation treats it as error, returning to client error
        // which literally can't be debugged e.g. "There were headers present in the request which were not signed"
        if (storageClass == Aws::S3::Model::StorageClass::NOT_SET) {
            storageClass = Aws::S3::Model::StorageClass::STANDARD;
        }

        request.WithStorageClass(storageClass);
        return TBase::PrepareRequest(ev);
    }
}; // TPutInputStreamContext

} // anonymous

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
    Call<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::HeadObjectAsync<>);
#else
        ev, &S3Client::HeadObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    Call<TEvListObjectsRequest, TEvListObjectsResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::ListObjectsAsync<>);
#else
        ev, &S3Client::ListObjectsAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    Call<TEvHeadObjectRequest, TEvHeadObjectResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::HeadObjectAsync<>);
#else
        ev, &S3Client::HeadObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    Call<TEvPutObjectRequest, TEvPutObjectResponse, TPutInputStreamContext>(
        ev, &S3Client::PutObjectAsync);
}

void TS3ExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    Call<TEvDeleteObjectRequest, TEvDeleteObjectResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::DeleteObjectAsync<>);
#else
        ev, &S3Client::DeleteObjectAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    Call<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::DeleteObjectsAsync<>);
#else
        ev, &S3Client::DeleteObjectsAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    Call<TEvCreateMultipartUploadRequest, TEvCreateMultipartUploadResponse, TContextBase>(
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
    Call<TEvCompleteMultipartUploadRequest, TEvCompleteMultipartUploadResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::CompleteMultipartUploadAsync<>);
#else
        ev, &S3Client::CompleteMultipartUploadAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    Call<TEvAbortMultipartUploadRequest, TEvAbortMultipartUploadResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::AbortMultipartUploadAsync<>);
#else
        ev, &S3Client::AbortMultipartUploadAsync);
#endif
}

void TS3ExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    Call<TEvUploadPartCopyRequest, TEvUploadPartCopyResponse, TContextBase>(
#if AWS_SDK_VERSION_MAJOR == 1 && AWS_SDK_VERSION_MINOR >= 11
        ev, &S3Client::UploadPartCopyAsync<>);
#else
        ev, &S3Client::UploadPartCopyAsync);
#endif
}

}

#endif // KIKIMR_DISABLE_S3_OPS
