#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/server_detail.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NRpc {
namespace {

using namespace NBus;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TTestServiceContext
    : public TServiceContextBase
{
public:
    explicit TTestServiceContext(TSharedRefArray requestMessage)
        : TServiceContextBase(
            std::move(requestMessage),
            {},
            nullptr,
            NLogging::TLogger("TestServiceContext"),
            NLogging::ELogLevel::Debug)
    { }

private:
    void DoReply() override
    { }

    void LogResponse() override
    { }
};

TEST(TServiceTest, OversizedResponse)
{
    // Reserve address space without touching the oversized payload.
    auto oversizedPart = TSharedMutableRef::AllocateViaMmap(
        MaxMessagePartSize + 1,
        {.InitializeStorage = false});

    for (bool oversizedBody : {true, false}) {
        SCOPED_TRACE(oversizedBody);

        auto requestId = TRequestId::Create();
        NProto::TRequestHeader requestHeader;
        ToProto(requestHeader.mutable_request_id(), requestId);
        requestHeader.set_service("TestService");
        requestHeader.set_method("OversizedResponse");
        auto context = New<TTestServiceContext>(CreateRequestMessage(
            requestHeader,
            TSharedRef::MakeEmpty(),
            {}));

        context->SetResponseBody(oversizedBody ? oversizedPart : TSharedRef::MakeEmpty());
        if (!oversizedBody) {
            context->ResponseAttachments().push_back(oversizedPart);
        }
        context->Reply(TError());

        NProto::TResponseHeader responseHeader;
        ASSERT_TRUE(TryParseResponseHeader(context->GetResponseMessage(), &responseHeader));
        EXPECT_EQ(requestId, NYT::FromProto<TRequestId>(responseHeader.request_id()));
        EXPECT_EQ(requestHeader.service(), responseHeader.service());
        EXPECT_EQ(requestHeader.method(), responseHeader.method());
        EXPECT_EQ(NRpc::EErrorCode::TransportError, NYT::FromProto<TError>(responseHeader.error()).GetCode());
        EXPECT_EQ(NRpc::EErrorCode::TransportError, context->GetError().GetCode());
        EXPECT_EQ(1u, context->GetResponseMessage().Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
