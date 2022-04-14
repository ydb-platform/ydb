#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "s3_wrapper.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NWrappers {

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;

namespace {

    struct TCurlInitializer {
        TCurlInitializer() {
            curl_global_init(CURL_GLOBAL_ALL);
        }

        ~TCurlInitializer() {
            curl_global_cleanup();
        }
    };

    struct TApiInitializer {
        TApiInitializer() {
            Options.httpOptions.initAndCleanupCurl = false;
            InitAPI(Options);

            Internal::CleanupEC2MetadataClient(); // speeds up config construction
        }

        ~TApiInitializer() {
            ShutdownAPI(Options);
        }

    private:
        SDKOptions Options;
    };

    class TApiOwner {
    public:
        void Ref() {
            auto guard = Guard(Mutex);
            if (!RefCount++) {
                if (!CurlInitializer) {
                    CurlInitializer.Reset(new TCurlInitializer);
                }
                ApiInitializer.Reset(new TApiInitializer);
            }
        }

        void UnRef() {
            auto guard = Guard(Mutex);
            if (!--RefCount) {
                ApiInitializer.Destroy();
            }
        }

    private:
        ui64 RefCount = 0;
        TMutex Mutex;
        THolder<TCurlInitializer> CurlInitializer;
        THolder<TApiInitializer> ApiInitializer;
    };

} // anonymous

TS3User::TS3User() {
    Singleton<TApiOwner>()->Ref();
}

TS3User::~TS3User() {
    Singleton<TApiOwner>()->UnRef();
}

class TS3Wrapper: public TActor<TS3Wrapper>, private TS3User {
    template <typename TEvRequest, typename TEvResponse>
    class TContextBase: public AsyncCallerContext {
    protected:
        using TRequest = typename TEvRequest::TRequest;
        using TOutcome = typename TEvResponse::TOutcome;

    public:
        explicit TContextBase(const TActorSystem* sys, const TActorId& sender)
            : AsyncCallerContext()
            , ActorSystem(sys)
            , Sender(sender)
            , Replied(false)
        {
        }

        const TActorSystem* GetActorSystem() const {
            return ActorSystem;
        }

        virtual const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) {
            return ev->Get()->Request;
        }

        void Reply(const TRequest& request, const TOutcome& outcome) const {
            Y_VERIFY(!std::exchange(Replied, true), "Double-reply");

            typename TEvResponse::TKey key;
            if (request.KeyHasBeenSet()) {
                key = request.GetKey();
            }

            Send(MakeResponse(key, outcome).Release());
        }

    protected:
        void Send(const TActorId& recipient, IEventBase* ev) const {
            ActorSystem->Send(recipient, ev);
        }

        void Send(IEventBase* ev) const {
            Send(Sender, ev);
        }

        virtual THolder<IEventBase> MakeResponse(const typename TEvResponse::TKey& key, const TOutcome& outcome) const {
            return MakeHolder<TEvResponse>(key, outcome);
        }

    private:
        const TActorSystem* ActorSystem;
        const TActorId Sender;

        mutable bool Replied;

    }; // TContextBase

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
    protected:
        using TRequest = typename TEvRequest::TRequest;
        using TOutcome = typename TEvResponse::TOutcome;

    private:
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
        using TContextBase<TEvRequest, TEvResponse>::TContextBase;

        const TRequest& PrepareRequest(typename TEvRequest::TPtr& ev) override {
            auto& request = ev->Get()->Request;

            Buffer = std::move(ev->Get()->Body);
            request.SetBody(MakeShared<DefaultUnderlyingStream>("StreamContext",
                MakeUnique<TInputStreamBuf>("StreamContext", Buffer)));

            return request;
        }

    private:
        TString Buffer;

    }; // TInputStreamContext

    template <typename TRequest, typename TOutcome>
    using THandler = std::function<void(const S3Client*, const TRequest&, const TOutcome&, const std::shared_ptr<const AsyncCallerContext>&)>;

    template <typename TRequest, typename TOutcome>
    using TFunc = std::function<void(const S3Client*, const TRequest&, THandler<TRequest, TOutcome>, const std::shared_ptr<const AsyncCallerContext>&)>;

    template <typename TEvRequest, typename TEvResponse, template <typename...> typename TContext = TContextBase>
    void Call(typename TEvRequest::TPtr& ev, TFunc<typename TEvRequest::TRequest, typename TEvResponse::TOutcome> func) {
        using TCtx = TContext<TEvRequest, TEvResponse>;

        auto ctx = std::make_shared<TCtx>(TlsActivationContext->ActorSystem(), ev->Sender);
        auto callback = [](
                const S3Client*,
                const typename TEvRequest::TRequest& request,
                const typename TEvResponse::TOutcome& outcome,
                const std::shared_ptr<const AsyncCallerContext>& context) {
            const auto* ctx = static_cast<const TCtx*>(context.get());

            LOG_NOTICE_S(*ctx->GetActorSystem(), NKikimrServices::S3_WRAPPER, "Response"
                << ": uuid# " << ctx->GetUUID()
                << ", response# " << outcome);
            ctx->Reply(request, outcome);
        };

        LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER, "Request"
            << ": uuid# " << ctx->GetUUID()
            << ", request# " << ev->Get()->Request);
        func(Client.Get(), ctx->PrepareRequest(ev), callback, ctx);
    }

    void Handle(TEvS3Wrapper::TEvGetObjectRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvGetObjectRequest, TEvS3Wrapper::TEvGetObjectResponse, TOutputStreamContext>(
            ev, &S3Client::GetObjectAsync);
    }

    void Handle(TEvS3Wrapper::TEvHeadObjectRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvHeadObjectRequest, TEvS3Wrapper::TEvHeadObjectResponse>(
            ev, &S3Client::HeadObjectAsync);
    }

    void Handle(TEvS3Wrapper::TEvPutObjectRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvPutObjectRequest, TEvS3Wrapper::TEvPutObjectResponse, TInputStreamContext>(
            ev, &S3Client::PutObjectAsync);
    }

    void Handle(TEvS3Wrapper::TEvDeleteObjectRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvDeleteObjectRequest, TEvS3Wrapper::TEvDeleteObjectResponse>(
            ev, &S3Client::DeleteObjectAsync);
    }

    void Handle(TEvS3Wrapper::TEvCreateMultipartUploadRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvCreateMultipartUploadRequest, TEvS3Wrapper::TEvCreateMultipartUploadResponse>(
            ev, &S3Client::CreateMultipartUploadAsync);
    }

    void Handle(TEvS3Wrapper::TEvUploadPartRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvUploadPartRequest, TEvS3Wrapper::TEvUploadPartResponse, TInputStreamContext>(
            ev, &S3Client::UploadPartAsync);
    }

    void Handle(TEvS3Wrapper::TEvCompleteMultipartUploadRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvCompleteMultipartUploadRequest, TEvS3Wrapper::TEvCompleteMultipartUploadResponse>(
            ev, &S3Client::CompleteMultipartUploadAsync);
    }

    void Handle(TEvS3Wrapper::TEvAbortMultipartUploadRequest::TPtr& ev) {
        Call<TEvS3Wrapper::TEvAbortMultipartUploadRequest, TEvS3Wrapper::TEvAbortMultipartUploadResponse>(
            ev, &S3Client::AbortMultipartUploadAsync);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::S3_WRAPPER_ACTOR;
    }

    explicit TS3Wrapper(const AWSCredentials& credentials, const ClientConfiguration& config)
        : TActor(&TThis::StateWork)
        , Client(new S3Client(credentials, config))
    {
    }

    virtual ~TS3Wrapper() {
        if (Client) {
            Client->DisableRequestProcessing();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvS3Wrapper::TEvGetObjectRequest, Handle);
            hFunc(TEvS3Wrapper::TEvHeadObjectRequest, Handle);
            hFunc(TEvS3Wrapper::TEvPutObjectRequest, Handle);
            hFunc(TEvS3Wrapper::TEvDeleteObjectRequest, Handle);
            hFunc(TEvS3Wrapper::TEvCreateMultipartUploadRequest, Handle);
            hFunc(TEvS3Wrapper::TEvUploadPartRequest, Handle);
            hFunc(TEvS3Wrapper::TEvCompleteMultipartUploadRequest, Handle);
            hFunc(TEvS3Wrapper::TEvAbortMultipartUploadRequest, Handle);

            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    THolder<S3Client> Client;

}; // TS3Wrapper

IActor* CreateS3Wrapper(const AWSCredentials& credentials, const ClientConfiguration& config) {
    return new TS3Wrapper(credentials, config);
}

} // NWrappers
} // NKikimr
