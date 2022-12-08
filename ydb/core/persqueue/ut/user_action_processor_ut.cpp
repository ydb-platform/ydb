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
        TMaybe<ui64> Offset;
    };

    struct TErrorMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<TString> Error;
    };

    struct TProposeTransactionResponseMatcher {
        TMaybe<ui64> TxId;
        TMaybe<NKikimrPQ::TEvProposeTransactionResult::EStatus> Status;
    };

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    void CreatePartitionActor(ui32 partition = 1, bool newPartition = true);
    void CreatePartition(ui32 partition = 1, ui64 begin = 0, ui64 end = 0);

    void CreateSession(const TString& clientId,
                       const TString& sessionId,
                       ui32 generation = 1, ui32 step = 1,
                       ui64 cookie = 1);
    void SetOffset(const TString& clientId,
                   const TString& sessionId,
                   ui64 offset,
                   TMaybe<ui64> expected = Nothing(),
                   ui64 cookie = 1);

    void SendCreateSession(ui64 cookie,
                           const TString& clientId,
                           const TString& sessionId,
                           ui32 generation,
                           ui32 step);
    void SendSetOffset(ui64 cookie,
                       const TString& clientId,
                       ui64 offset,
                       const TString& sessionId);
    void SendGetOffset(ui64 cookie,
                       const TString& clientId);
    void WaitCmdWrite(size_t count,
                      const THashMap<size_t, TUserInfoMatcher>& matchers = {});
    void SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status);
    void WaitProxyResponse(const TProxyResponseMatcher &matcher = {});
    void WaitErrorResponse(const TErrorMatcher& matcher = {});

    void WaitDiskStatusRequest();
    void SendDiskStatusResponse();
    void WaitMetaReadRequest();
    void SendMetaReadResponse();
    void WaitInfoRangeRequest();
    void SendInfoRangeResponse();
    void WaitDataRangeRequest();
    void SendDataRangeResponse(ui64 begin, ui64 end);
    void WaitDataReadRequest();
    void SendDataReadResponse();

    void SendProposeTransactionRequest(ui32 partition,
                                       ui64 begin, ui64 end,
                                       const TString& client,
                                       const TString& topic,
                                       bool immediate,
                                       ui64 txId);
    void WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher = {});

private:
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
}

void TUserActionProcessorFixture::TearDown(NUnitTest::TTestContext&)
{
}

void TUserActionProcessorFixture::CreatePartitionActor(ui32 id, bool newPartition)
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
                                     id,
                                     Ctx->Edge,
                                     Ctx->Edge,
                                     topicConverter,
                                     true,
                                     "dcId",
                                     false,
                                     config,
                                     *tabletCounters,
                                     newPartition);
    ActorId = Ctx->Runtime->Register(actor);
}

void TUserActionProcessorFixture::CreatePartition(ui32 id, ui64 begin, ui64 end)
{
    if ((begin == 0) && (end == 0)) {
        CreatePartitionActor(id, true);
    } else {
        CreatePartitionActor(id, false);

        WaitDiskStatusRequest();
        SendDiskStatusResponse();

        WaitMetaReadRequest();
        SendMetaReadResponse();

        WaitInfoRangeRequest();
        SendInfoRangeResponse();

        WaitDataRangeRequest();
        SendDataRangeResponse(begin, end);
    }
}

void TUserActionProcessorFixture::CreateSession(const TString& clientId,
                                                const TString& sessionId,
                                                ui32 generation, ui32 step,
                                                ui64 cookie)
{
    SendCreateSession(cookie,clientId,sessionId, generation, step);
    WaitCmdWrite(2, {{0, {.Session = sessionId, .Offset = 0}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie = cookie});
}

void TUserActionProcessorFixture::SetOffset(const TString& clientId,
                                            const TString& sessionId,
                                            ui64 offset,
                                            TMaybe<ui64> expected,
                                            ui64 cookie)
{
    SendSetOffset(cookie, clientId, offset, sessionId);
    WaitCmdWrite(2, {{0, {.Session = sessionId, .Offset = (expected ? *expected : offset)}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie = cookie});
}

void TUserActionProcessorFixture::SendCreateSession(ui64 cookie,
                                                    const TString& clientId,
                                                    const TString& sessionId,
                                                    ui32 generation,
                                                    ui32 step)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     0,
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

void TUserActionProcessorFixture::SendGetOffset(ui64 cookie,
                                                const TString& clientId)
{
    auto event = MakeHolder<TEvPQ::TEvGetClientOffset>(cookie,
                                                       clientId);
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

    if (matcher.Offset) {
        UNIT_ASSERT(event->Response.HasPartitionResponse());
        UNIT_ASSERT(event->Response.GetPartitionResponse().HasCmdGetClientOffsetResult());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Offset, event->Response.GetPartitionResponse().GetCmdGetClientOffsetResult().GetOffset());
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

void TUserActionProcessorFixture::WaitDiskStatusRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT(event->Record.CmdGetStatusSize() > 0);
}

void TUserActionProcessorFixture::SendDiskStatusResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto result = event->Record.AddGetStatusResult();
    result->SetStatus(NKikimrProto::OK);
    result->SetStatusFlags(NKikimrBlobStorage::StatusIsValid);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitMetaReadRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadSize(), 1);
}

void TUserActionProcessorFixture::SendMetaReadResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadResult();
    read->SetStatus(NKikimrProto::NODATA);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitInfoRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TUserActionProcessorFixture::SendInfoRangeResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::NODATA);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitDataRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TUserActionProcessorFixture::SendDataRangeResponse(ui64 begin, ui64 end)
{
    Y_VERIFY(begin <= end);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);
    auto pair = read->AddPair();
    NPQ::TKey key(NPQ::TKeyPrefix::TypeData, 1, begin, 0, end - begin, 0);
    pair->SetStatus(NKikimrProto::OK);
    pair->SetKey(key.ToString());
    //pair->SetValueSize();
    pair->SetCreationUnixTime(0);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendProposeTransactionRequest(ui32 partition,
                                                                ui64 begin, ui64 end,
                                                                const TString& client,
                                                                const TString& topic,
                                                                bool immediate,
                                                                ui64 txId)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();

    ActorIdToProto(Ctx->Edge, event->Record.MutableSource());
    auto* body = event->Record.MutableTxBody();
    auto* operation = body->MutableOperations()->Add();
    operation->SetPartitionId(partition);
    operation->SetBegin(begin);
    operation->SetEnd(end);
    operation->SetConsumer(client);
    operation->SetPath(topic);
    body->SetImmediate(immediate);
    event->Record.SetTxId(txId);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.TxId) {
        UNIT_ASSERT(event->Record.HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->Record.GetTxId());
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Record.HasStatus());
        UNIT_ASSERT(*matcher.Status == event->Record.GetStatus());
    }
}

Y_UNIT_TEST_F(Batching, TUserActionProcessorFixture)
{
    CreatePartition();

    SendCreateSession(4, "client-1", "session-id-1", 2, 3);

    WaitCmdWrite(2, {{0, {.Session = "session-id-1", .Offset=0, .Generation=2, .Step=3}}});

    SendCreateSession(5, "client-2", "session-id-2", 4, 5);
    SendCreateSession(6, "client-3", "session-id-3", 6, 7);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=4});

    WaitCmdWrite(4, {
                 {0, {.Session = "session-id-2", .Offset=0, .Generation=4, .Step=5}},
                 {2, {.Session = "session-id-3", .Offset=0, .Generation=6, .Step=7}}
                 });

    SendSetOffset(7, "client-1", 0, "session-id-1");
    SendCreateSession(8, "client-1", "session-id-2", 8, 9);
    SendSetOffset(9, "client-1", 0, "session-id-1");
    SendSetOffset(10, "client-1", 0, "session-id-2");
    SendCreateSession(11, "client-1", "session-id-3", 7, 10);

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

Y_UNIT_TEST_F(SetOffset, TUserActionProcessorFixture)
{
    const ui32 partition = 0;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    CreatePartition(partition, begin, end);

    //
    // create session
    //
    CreateSession(client, session);

    //
    // regular commit (5 <= end)
    //
    SendSetOffset(1, client, 5, session);
    WaitCmdWrite(2, {{0, {.Session=session, .Offset=5}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=1, .Status=NMsgBusProxy::MSTATUS_OK});

    //
    // offset is 5
    //
    SendGetOffset(2, client);
    WaitProxyResponse({.Cookie=2, .Status=NMsgBusProxy::MSTATUS_OK, .Offset=5});

    //
    // commit to back (1 < 5)
    //
    SendSetOffset(3, client, 1, session);
    WaitCmdWrite(2, {{0, {.Session=session, .Offset=5}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=3, .Status=NMsgBusProxy::MSTATUS_OK});

    //
    // the offset has not changed
    //
    SendGetOffset(4, client);
    WaitProxyResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK, .Offset=5});

    //
    // commit to future (13 > end)
    //
    SendSetOffset(5, client, 13, session);
    WaitCmdWrite(2, {{0, {.Session=session, .Offset=end}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
}

Y_UNIT_TEST_F(CommitOffsetRanges, TUserActionProcessorFixture)
{
    const ui32 partition = 0;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    CreatePartition(partition, begin, end);

    //
    // create session
    //
    CreateSession(client, session);

    SendProposeTransactionRequest(partition,
                                  0, 2,  // 0 --> 2
                                  client,
                                  "topic-path",
                                  true,
                                  1);
    WaitCmdWrite(2, {{0, {.Session="", .Offset=2}}});

    SendProposeTransactionRequest(partition,
                                  2, 0,          // begin > end
                                  client,
                                  "topic-path",
                                  true,
                                  2);
    SendProposeTransactionRequest(partition,
                                  4, 6,          // begin > client.end
                                  client,
                                  "topic-path",
                                  true,
                                  3);
    SendProposeTransactionRequest(partition,
                                  1, 4,          // begin < client.end
                                  client,
                                  "topic-path",
                                  true,
                                  4);
    SendProposeTransactionRequest(partition,
                                  2, 4,          // begin == client.end
                                  client,
                                  "topic-path",
                                  true,
                                  5);
    SendProposeTransactionRequest(partition,
                                  4, 13,         // end > partition.end
                                  client,
                                  "topic-path",
                                  true,
                                  6);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProposeTransactionResponse({.TxId=1, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitCmdWrite(2, {{0, {.Session="", .Offset=4}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProposeTransactionResponse({.TxId=2, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});
    WaitProposeTransactionResponse({.TxId=3, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=4, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=5, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitProposeTransactionResponse({.TxId=6, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});

    SendGetOffset(6, client);
    WaitProxyResponse({.Cookie=6, .Offset=4});
}

} // Y_UNIT_TEST_SUITE(TUserActionProcessorTests)

} // namespace NKikimr::NPQ
