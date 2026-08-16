#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/ymq/actor/action.h>
#include <ydb/core/ymq/actor/actor.h>
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/core/ymq/base/action.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {
namespace {

const TString UserName = "source-address-user";
const TString QueueName = "source-address-queue";
const TString Ticket = "source-address-ticket";

class TNoopReplyCallback final : public IReplyCallback {
    void DoSendReply(const NKikimrClient::TSqsResponse&) override {}
};

class TSourceAddressProbeActor final
    : public TActionActor<TSourceAddressProbeActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TSourceAddressProbeActor(const NKikimrClient::TSqsRequest& request, TString expectedSourceAddress)
        : TActionActor(request, EAction::CreateQueue, MakeHolder<TNoopReplyCallback>())
        , ExpectedSourceAddress_(std::move(expectedSourceAddress))
    {}

private:
    void DoAction() override {
        SendReplyAndDie();
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableCreateQueue()->MutableError();
    }

    TString DoGetQueueName() const override {
        UNIT_ASSERT_VALUES_EQUAL(SourceAddress_, ExpectedSourceAddress_);
        return QueueName;
    }

private:
    const TString ExpectedSourceAddress_;
};

struct TRequestCase {
    TString Name;
    NKikimrClient::TSqsRequest Request;
};

template <class TRequest>
void FillCommonAuth(TRequest& request, const TString& sourceAddress) {
    request.MutableCredentials()->SetOAuthToken(Ticket);
    request.MutableAuth()->SetUserName(UserName);
    request.MutableAuth()->SetSourceAddress(sourceAddress);
}

template <class TRequest>
void FillCommonAuthWithoutSourceAddress(TRequest& request) {
    request.MutableCredentials()->SetOAuthToken(Ticket);
    request.MutableAuth()->SetUserName(UserName);
}

TVector<TRequestCase> MakeRequestsWithAuthSourceAddress(const TString& sourceAddress) {
    TVector<TRequestCase> requests;

#define ADD_REQUEST(action) \
    { \
        NKikimrClient::TSqsRequest request; \
        request.SetRequestId(#action); \
        FillCommonAuth(*request.Mutable##action(), sourceAddress); \
        requests.push_back({#action, std::move(request)}); \
    }

    ENUMERATE_ALL_ACTIONS(ADD_REQUEST)

#undef ADD_REQUEST

    return requests;
}

TVector<TRequestCase> MakeRequestsWithLegacySourceAddress(const TString& sourceAddress) {
    TVector<TRequestCase> requests;

#define ADD_LEGACY_REQUEST(action) \
    { \
        NKikimrClient::TSqsRequest request; \
        request.SetRequestId(#action); \
        auto& actionRequest = *request.Mutable##action(); \
        FillCommonAuthWithoutSourceAddress(actionRequest); \
        actionRequest.SetSourceAddress(sourceAddress); \
        requests.push_back({#action, std::move(request)}); \
    }

    ADD_LEGACY_REQUEST(CreateQueue)
    ADD_LEGACY_REQUEST(DeleteQueue)
    ADD_LEGACY_REQUEST(DeleteUser)
    ADD_LEGACY_REQUEST(TagQueue)
    ADD_LEGACY_REQUEST(UntagQueue)

#undef ADD_LEGACY_REQUEST

    return requests;
}

class TSourceAddressProbeRuntime {
public:
    TSourceAddressProbeRuntime() {
        Runtime_.Initialize(NKikimr::TAppPrepare().Unwrap());
        Runtime_.SetLogPriority(NKikimrServices::SQS, NLog::PRI_TRACE);
        Runtime_.SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        Runtime_.GetAppData().SqsConfig.SetRequestTimeoutMs(0);
        EdgeActor_ = Runtime_.AllocateEdgeActor();
        Runtime_.RegisterService(MakeSqsServiceID(Runtime_.GetNodeId()), EdgeActor_);
        Runtime_.RegisterService(MakeTicketParserID(), EdgeActor_);

        Runtime_.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            return Observe(ev);
        });
    }

    TString Run(const NKikimrClient::TSqsRequest& request, const TString& expectedSourceAddress) {
        PeerName_.clear();
        GotAuthorizeTicket_ = false;

        Runtime_.Register(new TSourceAddressProbeActor(request, expectedSourceAddress));

        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [this] {
            return GotAuthorizeTicket_;
        };
        Runtime_.DispatchEvents(options, TDuration::Seconds(30));

        UNIT_ASSERT_C(GotAuthorizeTicket_, "TEvAuthorizeTicket was not sent for " << request.GetRequestId());
        return PeerName_;
    }

private:
    NActors::TTestActorRuntimeBase::EEventAction Observe(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TSqsEvents::EvGetConfiguration: {
                auto configuration = MakeHolder<TSqsEvents::TEvConfiguration>();
                configuration->UserExists = true;
                configuration->QueueExists = true;
                configuration->RootUrl = "http://localhost:8771";
                configuration->SchemeCache = EdgeActor_;
                configuration->QueueLeader = EdgeActor_;
                configuration->TablesFormat = 1;
                configuration->QueueVersion = 1;
                configuration->Shards = 1;

                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient, configuration.Release()));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            case TEvTicketParser::EvAuthorizeTicket: {
                auto* authorizeTicket = ev->CastAsLocal<TEvTicketParser::TEvAuthorizeTicket>();
                UNIT_ASSERT(authorizeTicket);
                UNIT_ASSERT_VALUES_EQUAL(authorizeTicket->Ticket, Ticket);
                PeerName_ = authorizeTicket->PeerName;
                GotAuthorizeTicket_ = true;
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            default:
                return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
        }
    }

private:
    NActors::TTestBasicRuntime Runtime_;
    TActorId EdgeActor_;
    TString PeerName_;
    bool GotAuthorizeTicket_ = false;
};

} // namespace

Y_UNIT_TEST_SUITE(SourceAddress) {
    Y_UNIT_TEST(AuthSourceAddressIsSentAsTicketParserPeerNameForEveryRequest) {
        const TString sourceAddress = "192.168.0.101";

        TSourceAddressProbeRuntime probe;
        for (const auto& requestCase : MakeRequestsWithAuthSourceAddress(sourceAddress)) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                probe.Run(requestCase.Request, sourceAddress),
                sourceAddress,
                requestCase.Name);
        }
    }

    Y_UNIT_TEST(LegacySourceAddressFallbackIsSentAsTicketParserPeerName) {
        const TString sourceAddress = "192.168.0.101";

        TSourceAddressProbeRuntime probe;
        for (const auto& requestCase : MakeRequestsWithLegacySourceAddress(sourceAddress)) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                probe.Run(requestCase.Request, sourceAddress),
                sourceAddress,
                requestCase.Name);
        }
    }
}

} // namespace NKikimr::NSQS
