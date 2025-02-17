#include "s3_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/VersionConfig.h>

#include <ydb/library/actors/core/actorsystem.h>

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
class TContextBase: public AsyncCallerContext {
public:
    explicit TContextBase(
            const TActorSystem* sys,
            const TActorId& sender,
            IRequestContext::TPtr requestContext,
            const Aws::S3::Model::StorageClass storageClass,
            const TReplyAdapterContainer& replyAdapter)
        : AsyncCallerContext()
        , ActorSystem(sys)
        , Sender(sender)
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

private:
    const TActorSystem* ActorSystem;
    const TActorId Sender;

protected:
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
    class TOutputStreamBuf: public PreallocatedStreamBuf {
        TOutputStreamBuf(char* data, size_t size)
            : PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(data), size)
        {
        }

    public:
        explicit TOutputStreamBuf(TString& buffer)
            : TOutputStreamBuf(buffer.Detach(), buffer.size())
        {
        }
    };

public:
    using TGenericContext<TEvRequest, TEvResponse>::TGenericContext;

    const typename TEvRequest::TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->Request;

        std::pair<ui64, ui64> range;
        Y_ABORT_UNLESS(request.RangeHasBeenSet() && TEvGetObjectResponse::TryParseRange(request.GetRange().c_str(), range));
        Range = range;

        Buffer.resize(range.second - range.first + 1);
        request.SetResponseStreamFactory([this]() {
            return Aws::New<DefaultUnderlyingStream>("StreamContext",
                MakeUnique<TOutputStreamBuf>("StreamContext", Buffer));
            }
        );

        return request;
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
