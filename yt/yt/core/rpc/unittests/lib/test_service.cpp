#include "test_service.h"

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

class TTestService
    : public ITestService
    , public TServiceBase
{
public:
    TTestService(
        IInvokerPtr invoker,
        bool secure,
        TTestCreateChannelCallback createChannel,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : TServiceBase(
            invoker,
            TTestProxy::GetDescriptor(),
            std::move(memoryUsageTracker),
            NLogging::TLogger("Main"))
        , Secure_(secure)
        , CreateChannel_(createChannel)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PassCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocationCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegularAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NullAndEmptyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Compression));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DoNothing));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCall)
            .SetCancelable(true)
            .SetConcurrencyLimit(10)
            .SetQueueSizeLimit(20)
            .SetConcurrencyByteLimit(10_MB)
            .SetQueueByteSizeLimit(20_MB));
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMetadata));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChannelFailureError));
        // NB: NotRegisteredCall is not registered intentionally

        DeclareServerFeature(ETestFeature::Great);
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, SomeCall)
    {
        context->SetRequestInfo();
        int a = request->a();
        response->set_b(a + 100);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, PassCall)
    {
        context->SetRequestInfo();
        WriteAuthenticationIdentityToProto(response, context->GetAuthenticationIdentity());
        ToProto(response->mutable_mutation_id(), context->GetMutationId());
        response->set_retry(context->IsRetry());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, AllocationCall)
    {
        context->SetRequestInfo();
        response->set_allocated_string(TString("r", request->size()));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, RegularAttachments)
    {
        for (const auto& attachment : request->Attachments()) {
            auto data = TBlob();
            data.Append(attachment);
            data.Append("_", 1);
            response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, NullAndEmptyAttachments)
    {
        const auto& attachments = request->Attachments();
        EXPECT_EQ(2u, attachments.size());
        EXPECT_FALSE(attachments[0]);
        EXPECT_TRUE(attachments[1]);
        EXPECT_TRUE(attachments[1].Empty());
        response->Attachments() = attachments;
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, Compression)
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, DoNothing)
    {
        context->SetRequestInfo();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, CustomMessageError)
    {
        context->SetRequestInfo();
        context->Reply(TError(NYT::EErrorCode(42), "Some Error"));
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, SlowCall)
    {
        context->SetRequestInfo();
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, SlowCanceledCall)
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, RequestBytesThrottledCall)
    {
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, NoReply)
    { }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, StreamingEcho)
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, ServerStreamsAborted)
    {
        context->SetRequestInfo();

        auto promise = NewPromise<void>();
        context->SubscribeCanceled(BIND([=] (const TError&) mutable {
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, ServerNotReading)
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, ServerNotWriting)
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

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, FlakyCall)
    {
        static std::atomic<int> callCount;

        context->SetRequestInfo();

        if (callCount.fetch_add(1) % 2) {
            context->Reply();
        } else {
            context->Reply(TError(EErrorCode::TransportError, "Flaky call iteration"));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, RequireCoolFeature)
    {
        context->SetRequestInfo();
        context->ValidateClientFeature(ETestFeature::Cool);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, GetTraceBaggage)
    {
        context->SetRequestInfo();
        auto* traceContext = NTracing::TryGetCurrentTraceContext();
        response->set_baggage(NYson::ConvertToYsonString(traceContext->UnpackBaggage()).ToString());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, CustomMetadata)
    {
        auto customMetadataExt = context->GetRequestHeader().GetExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
        for (const auto& [key, value] : customMetadataExt.entries()) {
            (*response->mutable_parsed_custom_metadata())[key] = value;
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTestRpc, GetChannelFailureError)
    {
        context->SetRequestInfo();

        if (request->has_redirection_address()) {
            YT_VERIFY(CreateChannel_);
            TTestProxy proxy(CreateChannel_(request->redirection_address()));
            auto req = proxy.GetChannelFailureError();
            context->ReplyFrom(req->Invoke().AsVoid());
        } else {
            TError channelFailureError(NRpc::EErrorCode::Unavailable, "Test channel failure error");
            YT_VERIFY(IsChannelFailureError(channelFailureError));
            context->Reply(channelFailureError);
        }
    }

    TFuture<void> GetServerStreamsAborted() const override
    {
        return ServerStreamsAborted_.ToFuture();
    }

private:
    const bool Secure_;
    const TTestCreateChannelCallback CreateChannel_;

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

ITestServicePtr CreateTestService(
    IInvokerPtr invoker,
    bool secure,
    TTestCreateChannelCallback createChannel,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return New<TTestService>(
        invoker,
        secure,
        createChannel,
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
