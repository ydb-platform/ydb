#include "my_service.h"

#include <gtest/gtest.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <random>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TMyService
    : public IMyService
    , public TServiceBase
{
public:
    TMyService(IInvokerPtr invoker, bool secure)
        : TServiceBase(
            invoker,
            TMyProxy::GetDescriptor(),
            NLogging::TLogger("Main"))
        , Secure_(secure)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PassCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegularAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NullAndEmptyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Compression));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DoNothing));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCall)
            .SetCancelable(true)
            .SetConcurrencyLimit(10)
            .SetQueueSizeLimit(20));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCanceledCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestBytesThrottledCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NoReply));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlakyCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequireCoolFeature));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StreamingEcho)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ServerStreamsAborted)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ServerNotReading)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ServerNotWriting)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTraceBaggage));
        // NB: NotRegisteredCall is not registered intentionally

        DeclareServerFeature(EMyFeature::Great);
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall)
    {
        context->SetRequestInfo();
        int a = request->a();
        response->set_b(a + 100);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, PassCall)
    {
        context->SetRequestInfo();
        WriteAuthenticationIdentityToProto(response, context->GetAuthenticationIdentity());
        ToProto(response->mutable_mutation_id(), context->GetMutationId());
        response->set_retry(context->IsRetry());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, RegularAttachments)
    {
        for (const auto& attachment : request->Attachments()) {
            auto data = TBlob();
            data.Append(attachment);
            data.Append("_", 1);
            response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NullAndEmptyAttachments)
    {
        const auto& attachments = request->Attachments();
        EXPECT_EQ(2u, attachments.size());
        EXPECT_FALSE(attachments[0]);
        EXPECT_TRUE(attachments[1]);
        EXPECT_TRUE(attachments[1].Empty());
        response->Attachments() = attachments;
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, Compression)
    {
        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto serializedRequestBody = SerializeProtoToRefWithCompression(*request, requestCodecId);
        const auto& compressedRequestBody = context->GetRequestBody();
        EXPECT_TRUE(TRef::AreBitwiseEqual(serializedRequestBody, compressedRequestBody));

        const auto& attachments = request->Attachments();
        const auto& compressedAttachments = context->RequestAttachments();
        EXPECT_TRUE(attachments.size() == compressedAttachments.size());
        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        for (int i = 0; i < std::ssize(attachments); ++i) {
            auto compressedAttachment = requestCodec->Compress(attachments[i]);
            EXPECT_TRUE(TRef::AreBitwiseEqual(compressedAttachments[i], compressedAttachment));
        }

        response->set_message(request->message());
        response->Attachments() = attachments;
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, DoNothing)
    {
        context->SetRequestInfo();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError)
    {
        context->SetRequestInfo();
        context->Reply(TError(NYT::EErrorCode(42), "Some Error"));
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCall)
    {
        context->SetRequestInfo();
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCanceledCall)
    {
        try {
            context->SetRequestInfo();
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(2));
            context->Reply();
        } catch (const TFiberCanceledException&) {
            SlowCallCanceled_.Set();
            throw;
        }
    }

    TFuture<void> GetSlowCallCanceled() const override
    {
        return SlowCallCanceled_.ToFuture();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, RequestBytesThrottledCall)
    {
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NoReply)
    { }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, StreamingEcho)
    {
        context->SetRequestInfo();

        bool delayed = request->delayed();
        std::vector<TSharedRef> receivedData;

        ssize_t totalSize = 0;
        while (true) {
            auto data = WaitFor(request->GetAttachmentsStream()->Read())
                .ValueOrThrow();
            if (!data) {
                break;
            }
            totalSize += data.size();

            if (delayed) {
                receivedData.push_back(data);
            } else {
                WaitFor(response->GetAttachmentsStream()->Write(data))
                    .ThrowOnError();
            }
        }

        if (delayed) {
            for (const auto& data : receivedData) {
                WaitFor(response->GetAttachmentsStream()->Write(data))
                    .ThrowOnError();
            }
        }

        WaitFor(response->GetAttachmentsStream()->Close())
            .ThrowOnError();

        response->set_total_size(totalSize);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ServerStreamsAborted)
    {
        context->SetRequestInfo();

        auto promise = NewPromise<void>();
        context->SubscribeCanceled(BIND([=] () mutable {
            promise.Set();
        }));

        promise
            .ToFuture()
            .Get()
            .ThrowOnError();

        EXPECT_THROW({
            response->GetAttachmentsStream()->Write(TSharedMutableRef::Allocate(100))
                .Get()
                .ThrowOnError();
        }, TErrorException);

        EXPECT_THROW({
            request->GetAttachmentsStream()->Read()
                .Get()
                .ThrowOnError();
        }, TErrorException);

        ServerStreamsAborted_.Set();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ServerNotReading)
    {
        context->SetRequestInfo();

        WaitFor(context->GetRequestAttachmentsStream()->Read())
            .ThrowOnError();

        try {
            auto sleep = request->sleep();
            if (sleep) {
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            }

            WaitFor(context->GetRequestAttachmentsStream()->Read())
                .ThrowOnError();
            context->Reply();
        } catch (const TFiberCanceledException&) {
            SlowCallCanceled_.Set();
            throw;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ServerNotWriting)
    {
        context->SetRequestInfo();

        auto data = TSharedRef::FromString("abacaba");
        WaitFor(context->GetResponseAttachmentsStream()->Write(data))
            .ThrowOnError();

        try {
            auto sleep = request->sleep();
            if (sleep) {
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            }

            WaitFor(context->GetResponseAttachmentsStream()->Close())
                .ThrowOnError();
            context->Reply();
        } catch (const TFiberCanceledException&) {
            SlowCallCanceled_.Set();
            throw;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, FlakyCall)
    {
        static std::atomic<int> callCount;

        context->SetRequestInfo();

        if (callCount.fetch_add(1) % 2) {
            context->Reply();
        } else {
            context->Reply(TError(EErrorCode::TransportError, "Flaky call iteration"));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, RequireCoolFeature)
    {
        context->SetRequestInfo();
        context->ValidateClientFeature(EMyFeature::Cool);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, GetTraceBaggage)
    {
        context->SetRequestInfo();
        auto* traceContext = NTracing::TryGetCurrentTraceContext();
        response->set_baggage(NYson::ConvertToYsonString(traceContext->UnpackBaggage()).ToString());
        context->Reply();
    }

    TFuture<void> GetServerStreamsAborted() const override
    {
        return ServerStreamsAborted_.ToFuture();
    }

private:
    const bool Secure_;

    TPromise<void> SlowCallCanceled_ = NewPromise<void>();
    TPromise<void> ServerStreamsAborted_ = NewPromise<void>();


    void BeforeInvoke(IServiceContext* context) override
    {
        TServiceBase::BeforeInvoke(context);
        if (Secure_) {
            const auto& ext = context->GetRequestHeader().GetExtension(NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext);
            EXPECT_EQ("localhost", ext.peer_identity());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IMyServicePtr CreateMyService(IInvokerPtr invoker, bool secure)
{
    return New<TMyService>(invoker, secure);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
