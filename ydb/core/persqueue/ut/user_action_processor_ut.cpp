#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TUserActionProcessorTests) {

class TUserActionProcessorFixture : public NUnitTest::TBaseFixture {
protected:
    struct TUserInfoMatcher {
        TMaybe<TString> Session;
        TMaybe<ui64> Offset;
        TMaybe<ui32> Generation;
        TMaybe<ui32> Step;
    };

    struct TProxyResponseMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NMsgBusProxy::EResponseStatus> Status;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
    };

    struct TErrorMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<TString> Error;
    };

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    TActorId CreatePartitionActor();

    void SendCreateSession(ui64 cookie,
                           const TString& clientId,
                           ui64 offset,
                           const TString& sessionId,
                           ui32 generation,
                           ui32 step);
    void SendSetOffset(ui64 cookie,
                       const TString& clientId,
                       ui64 offset,
                       const TString& sessionId);
    void WaitCmdWrite(size_t count,
                      const THashMap<size_t, TUserInfoMatcher>& matchers = {});
    void SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status);
    void WaitProxyResponse(const TProxyResponseMatcher &matcher = {});
    void WaitErrorResponse(const TErrorMatcher& matcher = {});

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TActorId ActorId;
};

void TUserActionProcessorFixture::SetUp(NUnitTest::TTestContext&)
{
    Ctx.ConstructInPlace();
    Finalizer.ConstructInPlace(*Ctx);

    Ctx->Prepare();
    Ctx->Runtime->SetScheduledLimit(5'000);

    ActorId = CreatePartitionActor();
}

void TUserActionProcessorFixture::TearDown(NUnitTest::TTestContext&)
{
}

TActorId TUserActionProcessorFixture::CreatePartitionActor()
{
    using TKeyValueCounters = TProtobufTabletCounters<
        NKeyValue::ESimpleCounters_descriptor,
        NKeyValue::ECumulativeCounters_descriptor,
        NKeyValue::EPercentileCounters_descriptor,
        NKeyValue::ETxTypes_descriptor
    >;
    using TPersQueueCounters = TAppProtobufTabletCounters<
        NPQ::ESimpleCounters_descriptor,
        NPQ::ECumulativeCounters_descriptor,
        NPQ::EPercentileCounters_descriptor
    >;
    using TCounters = TProtobufTabletCountersPair<
        TKeyValueCounters,
        TPersQueueCounters
    >;

    TAutoPtr<TCounters> counters(new TCounters());
    TAutoPtr<TTabletCountersBase> tabletCounters = counters->GetSecondTabletCounters().Release();

    NPersQueue::TTopicNamesConverterFactory factory(true, "/Root/PQ", "dc1");
    NPersQueue::TTopicConverterPtr topicConverter;
    NKikimrPQ::TPQTabletConfig config;

    config.SetTopicName("rt3.dc1--account--topic");
    config.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
    config.SetFederationAccount("account");
    config.SetLocalDC(true);
    config.SetYdbDatabasePath("");

    topicConverter = factory.MakeTopicConverter(config);

    auto actor = new NPQ::TPartition(Ctx->TabletId,
                                     0,
                                     Ctx->Edge,
                                     Ctx->Edge,
                                     topicConverter,
                                     true,
                                     "dcId",
                                     config,
                                     *tabletCounters,
                                     true);

    return Ctx->Runtime->Register(actor);
}

void TUserActionProcessorFixture::SendCreateSession(ui64 cookie,
                                                    const TString& clientId,
                                                    ui64 offset,
                                                    const TString& sessionId,
                                                    ui32 generation,
                                                    ui32 step)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     offset,
                                                     sessionId,
                                                     generation,
                                                     step,
                                                     TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendSetOffset(ui64 cookie,
                                                const TString& clientId,
                                                ui64 offset,
                                                const TString& sessionId)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     offset,
                                                     sessionId,
                                                     0,
                                                     0);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitCmdWrite(size_t count,
                                               const THashMap<size_t, TUserInfoMatcher>& matchers)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetCookie(), 1);             // SET_OFFSET_COOKIE
    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdWriteSize(), count);

    for (auto& [index, matcher] : matchers) {
        UNIT_ASSERT(index < count);

        NKikimrPQ::TUserInfo ud;
        UNIT_ASSERT(ud.ParseFromString(event->Record.GetCmdWrite(index).GetValue()));

        if (matcher.Session) {
            UNIT_ASSERT(ud.HasSession());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Session, ud.GetSession());
        }
        if (matcher.Generation) {
            UNIT_ASSERT(ud.HasGeneration());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Generation, ud.GetGeneration());
        }
        if (matcher.Step) {
            UNIT_ASSERT(ud.HasStep());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, ud.GetStep());
        }
        if (matcher.Offset) {
            UNIT_ASSERT(ud.HasOffset());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Offset, ud.GetOffset());
        }
    }
}

void TUserActionProcessorFixture::SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(status);
    event->Record.SetCookie(1); // SET_OFFSET_COOKIE

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitProxyResponse(const TProxyResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Cookie);
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Response.HasStatus());
        UNIT_ASSERT(*matcher.Status == event->Response.GetStatus());
    }

    if (matcher.ErrorCode) {
        UNIT_ASSERT(event->Response.HasErrorCode());
        UNIT_ASSERT(*matcher.ErrorCode == event->Response.GetErrorCode());
    }
}

void TUserActionProcessorFixture::WaitErrorResponse(const TErrorMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Cookie);
    }

    if (matcher.ErrorCode) {
        UNIT_ASSERT(*matcher.ErrorCode == event->ErrorCode);
    }

    if (matcher.Error) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Error, event->Error);
    }
}

Y_UNIT_TEST_F(Batching, TUserActionProcessorFixture)
{
    SendCreateSession(4, "client-1", 0, "session-id-1", 2, 3);

    WaitCmdWrite(2, {{0, {.Session = "session-id-1", .Offset=0, .Generation=2, .Step=3}}});

    SendCreateSession(5, "client-2", 0, "session-id-2", 4, 5);
    SendCreateSession(6, "client-3", 0, "session-id-3", 6, 7);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=4});

    WaitCmdWrite(4, {
                 {0, {.Session = "session-id-2", .Offset=0, .Generation=4, .Step=5}},
                 {2, {.Session = "session-id-3", .Offset=0, .Generation=6, .Step=7}}
                 });

    SendSetOffset(7, "client-1", 0, "session-id-1");
    SendCreateSession(8, "client-1", 0, "session-id-2", 8, 9);
    SendSetOffset(9, "client-1", 0, "session-id-1");
    SendSetOffset(10, "client-1", 0, "session-id-2");
    SendCreateSession(11, "client-1", 0, "session-id-3", 7, 10);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=5});
    WaitProxyResponse({.Cookie=6});

    WaitCmdWrite(2, {
                 {0, {.Session = "session-id-2", .Offset=0, .Generation=8, .Step=9}},
                 });

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=7, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitProxyResponse({.Cookie=8, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitErrorResponse({.Cookie=9, .ErrorCode=NPersQueue::NErrorCode::WRONG_COOKIE});
    WaitProxyResponse({.Cookie=10, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitErrorResponse({.Cookie=11, .ErrorCode=NPersQueue::NErrorCode::WRONG_COOKIE});
}

}

}
