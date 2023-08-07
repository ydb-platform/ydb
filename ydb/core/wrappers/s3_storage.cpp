#include "s3_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
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
    explicit TCommonContextBase(const TActorSystem* sys, const TActorId& sender, IRequestContext::TPtr requestContext, const Aws::S3::Model::StorageClass storageClass)
        : AsyncCallerContext()
        , RequestContext(requestContext)
        , StorageClass(storageClass)
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
    void Send(const TActorId& recipient, IEventBase* ev) const {
        ActorSystem->Send(recipient, ev);
    }

    void Send(IEventBase* ev) const {
        Send(Sender, ev);
    }

    mutable bool Replied = false;
    IRequestContext::TPtr RequestContext;
    const Aws::S3::Model::StorageClass StorageClass;
private:
    const TActorSystem* ActorSystem;
    const TActorId Sender;
}; // TCommonContextBase

template <typename TEvRequest, typename TEvResponse>
class TContextBase: public TCommonContextBase<TEvRequest, TEvResponse> {
private:
    using TBase = TCommonContextBase<TEvRequest, TEvResponse>;
protected:
    virtual THolder<IEventBase> MakeResponse(const typename TEvResponse::TKey& key, const typename TBase::TOutcome& outcome) const {
        return MakeHolder<TEvResponse>(key, outcome);
    }

public:
    using TBase::Send;
    using TBase::TBase;
    void Reply(const typename TBase::TRequest& request, const typename TBase::TOutcome& outcome) const {
        Y_VERIFY(!std::exchange(TBase::Replied, true), "Double-reply");

        typename TEvResponse::TKey key;
        if (request.KeyHasBeenSet()) {
            key = request.GetKey();
        }
        Send(MakeResponse(key, outcome).Release());
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
        Y_VERIFY(!std::exchange(TBase::Replied, true), "Double-reply");

        Send(MakeHolder<TEvListObjectsResponse>(outcome).Release());
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
        Y_VERIFY(!std::exchange(TBase::Replied, true), "Double-reply");

        Send(MakeHolder<TEvDeleteObjectsResponse>(outcome).Release());
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
        Y_VERIFY(!std::exchange(TBase::Replied, true), "Double-reply");
        Send(MakeHolder<TEvCheckObjectExistsResponse>(outcome, RequestContext).Release());
    }
};

template <typename TEvRequest, typename TEvResponse>
class TOutputStreamContext: public TContextBase<TEvRequest, TEvResponse> {
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

public:
    using TContextBase<TEvRequest, TEvResponse>::TContextBase;

    const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
        auto& request = ev->Get()->Request;

        std::pair<ui64, ui64> range;
        Y_VERIFY(request.RangeHasBeenSet() && TryParseRange(request.GetRange().c_str(), range));

        Buffer.resize(range.second - range.first + 1);
        request.SetResponseStreamFactory([this]() {
            return Aws::New<DefaultUnderlyingStream>("StreamContext",
                MakeUnique<TOutputStreamBuf>("StreamContext", Buffer));
            });

        return request;
    }

protected:
    THolder<IEventBase> MakeResponse(const typename TEvResponse::TKey& key, const TOutcome& outcome) const override {
        if (outcome.IsSuccess()) {
            return MakeHolder<TEvResponse>(key, outcome, std::move(Buffer));
        } else {
            return MakeHolder<TEvResponse>(key, outcome);
        }
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
        request.WithStorageClass(TBase::StorageClass);
        return TBase::PrepareRequest(ev);
    }
}; // TPutInputStreamContext

} // anonymous

TS3ExternalStorage::~TS3ExternalStorage() {
    if (Client) {
        Client->DisableRequestProcessing();
    }
}

void TS3ExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    Call<TEvGetObjectRequest, TEvGetObjectResponse, TOutputStreamContext>(
        ev, &S3Client::GetObjectAsync);
}

void TS3ExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    Call<TEvCheckObjectExistsRequest, TEvCheckObjectExistsResponse, TContextBase>(
        ev, &S3Client::HeadObjectAsync);
}

void TS3ExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    Call<TEvListObjectsRequest, TEvListObjectsResponse, TContextBase>(
        ev, &S3Client::ListObjectsAsync);
}

void TS3ExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    Call<TEvHeadObjectRequest, TEvHeadObjectResponse, TContextBase>(
        ev, &S3Client::HeadObjectAsync);
}

void TS3ExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    Call<TEvPutObjectRequest, TEvPutObjectResponse, TPutInputStreamContext>(
        ev, &S3Client::PutObjectAsync);
}

void TS3ExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    Call<TEvDeleteObjectRequest, TEvDeleteObjectResponse, TContextBase>(
        ev, &S3Client::DeleteObjectAsync);
}

void TS3ExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    Call<TEvDeleteObjectsRequest, TEvDeleteObjectsResponse, TContextBase>(
        ev, &S3Client::DeleteObjectsAsync);
}

void TS3ExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    Call<TEvCreateMultipartUploadRequest, TEvCreateMultipartUploadResponse, TContextBase>(
        ev, &S3Client::CreateMultipartUploadAsync);
}

void TS3ExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    Call<TEvUploadPartRequest, TEvUploadPartResponse, TInputStreamContext>(
        ev, &S3Client::UploadPartAsync);
}

void TS3ExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    Call<TEvCompleteMultipartUploadRequest, TEvCompleteMultipartUploadResponse, TContextBase>(
        ev, &S3Client::CompleteMultipartUploadAsync);
}

void TS3ExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    Call<TEvAbortMultipartUploadRequest, TEvAbortMultipartUploadResponse, TContextBase>(
        ev, &S3Client::AbortMultipartUploadAsync);
}

void TS3ExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    Call<TEvUploadPartCopyRequest, TEvUploadPartCopyResponse, TContextBase>(
        ev, &S3Client::UploadPartCopyAsync);
}

}

#endif // KIKIMR_DISABLE_S3_OPS
