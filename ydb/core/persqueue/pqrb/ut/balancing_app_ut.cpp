#include "pqrb_ut_common.h"

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/hash.h>

namespace NKikimr::NPQ {

namespace {

void SendBalancerHttp(TTestContext& tc, const TString& extraQuery = {}) {
    TStringBuilder query;
    query << "/app?TabletID=" << tc.BalancerTabletId;
    if (extraQuery) {
        query << "&" << extraQuery;
    }
    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new NMon::TEvRemoteHttpInfo(query)
    );
}

TString FetchBalancerHtml(TTestContext& tc, const TString& extraQuery = {}) {
    SendBalancerHttp(tc, extraQuery);
    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    return res->Html;
}

void AddParam(google::protobuf::RepeatedPtrField<NActorsProto::TRemoteHttpInfo::TQueryParam>* params,
        const TString& key, const TString& value)
{
    auto* param = params->Add();
    param->SetKey(key);
    param->SetValue(value);
}

void SendBalancerKillPost(TTestContext& tc, const TString& consumer, const TString& session) {
    NActorsProto::TRemoteHttpInfo pb;
    pb.SetMethod(HTTP_METHOD_POST);
    pb.SetPath("/app");
    AddParam(pb.MutableQueryParams(), "TabletID", ToString(tc.BalancerTabletId));
    AddParam(pb.MutablePostParams(), "TabletID", ToString(tc.BalancerTabletId));
    AddParam(pb.MutablePostParams(), "action", "kill_session");
    AddParam(pb.MutablePostParams(), "consumer", consumer);
    AddParam(pb.MutablePostParams(), "session", session);
    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new NMon::TEvRemoteHttpInfo(std::move(pb))
    );
}

TStringBuf QuotedAttr(TStringBuf src, TStringBuf name) {
    const TString needle = TString::Join(name, "=\"");
    const auto pos = src.find(needle);
    if (pos == TStringBuf::npos) {
        return {};
    }
    src = src.substr(pos + needle.size());
    return src.Before('"');
}

struct TPostedForm {
    TString Method;
    TString Action;
    THashMap<TString, TString> Fields;
};

TPostedForm ParseKillSessionForm(const TString& html, const TString& sessionName) {
    const TString marker = TStringBuilder() << "name=\"session\" value=\"" << sessionName << "\"";
    const auto sessionPos = html.find(marker);
    UNIT_ASSERT_C(sessionPos != TString::npos, html.substr(0, 2000));

    const auto formStart = html.rfind("<form ", sessionPos);
    UNIT_ASSERT(formStart != TString::npos);
    const auto formEnd = html.find("</form>", sessionPos);
    UNIT_ASSERT(formEnd != TString::npos);

    const TStringBuf form = TStringBuf(html).substr(formStart, formEnd - formStart);
    TPostedForm parsed;
    parsed.Method = TString(QuotedAttr(form, "method"));
    parsed.Action = TString(QuotedAttr(form, "action"));

    TStringBuf rest = form;
    while (true) {
        const auto inputPos = rest.find("<input ");
        if (inputPos == TStringBuf::npos) {
            break;
        }
        rest = rest.substr(inputPos);
        const auto tagEnd = rest.find('>');
        if (tagEnd == TStringBuf::npos) {
            break;
        }
        const auto tag = rest.substr(0, tagEnd);
        if (const auto name = QuotedAttr(tag, "name")) {
            parsed.Fields[TString(name)] = TString(QuotedAttr(tag, "value"));
        }
        rest = rest.substr(tagEnd + 1);
    }
    return parsed;
}

void SendFormAsMonitoringProxy(TTestContext& tc, const TPostedForm& form) {
    UNIT_ASSERT_VALUES_EQUAL(form.Method, "POST");
    const TString* tabletIdParam = form.Fields.FindPtr("TabletID");
    const TString* actionParam = form.Fields.FindPtr("action");
    const TString* consumerParam = form.Fields.FindPtr("consumer");
    const TString* sessionParam = form.Fields.FindPtr("session");
    UNIT_ASSERT(tabletIdParam);
    UNIT_ASSERT(actionParam);
    UNIT_ASSERT(consumerParam);
    UNIT_ASSERT(sessionParam);
    UNIT_ASSERT_VALUES_EQUAL(*actionParam, "kill_session");

    const ui64 tabletId = FromString<ui64>(*tabletIdParam);
    UNIT_ASSERT_VALUES_EQUAL(tabletId, tc.BalancerTabletId);

    TStringBuf actionQuery = form.Action;
    if (actionQuery.StartsWith('?')) {
        actionQuery = actionQuery.substr(1);
    }
    actionQuery = actionQuery.Before('#');
    TCgiParameters queryParams(actionQuery);
    UNIT_ASSERT(queryParams.Get("action") != "kill_session");

    NActorsProto::TRemoteHttpInfo pb;
    pb.SetMethod(HTTP_METHOD_POST);
    pb.SetPath("/app");
    for (const auto& [key, value] : queryParams) {
        AddParam(pb.MutableQueryParams(), key, value);
    }
    for (const auto& [key, value] : form.Fields) {
        AddParam(pb.MutablePostParams(), key, value);
    }

    ForwardToTablet(
        *tc.Runtime,
        tabletId,
        tc.Edge,
        new NMon::TEvRemoteHttpInfo(std::move(pb))
    );
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbBalancingApp) {

Y_UNIT_TEST(RenderAppCoversFamilyPartitionAndSessionStates) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    // Finish/Commit are only valid on partitions that already have children.
    // 1 and 2 get grandchildren so later Finish events cover HTML description
    // branches without tripping the leaf-partition debug abort.
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
            {3, {tc.TabletId, 4}},
            {4, {tc.TabletId, 5}},
        },
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .Consumers = {
            {"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING},
            {"other", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING},
        },
        .ParentPartitionIds = {{1, {0}}, {2, {0}}, {3, {1}}, {4, {2}}},
        .ChildPartitionIds = {{0, {1, 2}}, {1, {3}}, {2, {4}}},
        .NextPartitionId = 5,
    });
    WaitBalancerReady(tc);

    auto pipe0 = RegisterReadSession("session-0", tc);
    auto lock0 = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock0);
    UNIT_ASSERT_VALUES_EQUAL(lock0->Record.GetPartition(), 0u);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 0, /*scaleAwareSDK=*/true, /*startedReadingFromEndOffset=*/false),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );

    absl::flat_hash_set<ui32> lockedChildren;
    for (ui32 i = 0; i < 2; ++i) {
        auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
        UNIT_ASSERT(lock);
        lockedChildren.insert(lock->Record.GetPartition());
    }
    UNIT_ASSERT(lockedChildren.contains(1));
    UNIT_ASSERT(lockedChildren.contains(2));

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvReadingPartitionStatusRequest("user", 0, 1, 1),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 1, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/true),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 2, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/false),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );
    DispatchFor(tc);

    auto pipe1 = RegisterReadSession("session-1", tc);
    Y_UNUSED(pipe1);
    DispatchFor(tc, TDuration::MilliSeconds(200));

    const TString html = FetchBalancerHtml(tc);
    UNIT_ASSERT_C(html.Contains("Families"), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("Partitions"));
    UNIT_ASSERT(html.Contains("Statistics"));
    UNIT_ASSERT(html.Contains("Sessions"));
    UNIT_ASSERT(html.Contains("session-0"));
    UNIT_ASSERT(html.Contains("Total:"));
    UNIT_ASSERT(html.Contains("committed") || html.Contains("reading child") || html.Contains("finished"));
    UNIT_ASSERT(html.Contains("scheduled. iteration:") || html.Contains("iteration:"));
    UNIT_ASSERT(html.Contains("Free") || html.Contains("Ready") || html.Contains("Read") || html.Contains("Finished"));
    UNIT_ASSERT(html.Contains("Active"));
    UNIT_ASSERT(html.Contains("Inactive"));
    UNIT_ASSERT(html.Contains("?TabletID="));
    UNIT_ASSERT_C(html.Contains("method=\"POST\""), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("name=\"TabletID\""));
    UNIT_ASSERT(html.Contains("name=\"action\""));
    UNIT_ASSERT(html.Contains("value=\"kill_session\""));
    UNIT_ASSERT(html.Contains("Kill"));
    UNIT_ASSERT(html.Contains("value=\"session-0\""));
    UNIT_ASSERT(!html.Contains("location.href"));
}

Y_UNIT_TEST(RenderAppAfterSessionShowsConsumerTab) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = RegisterReadSession("lonely-session", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    Y_UNUSED(pipe);

    const TString html = FetchBalancerHtml(tc);
    UNIT_ASSERT_C(html.Contains("Families"), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("Ready") || html.Contains("Free") || html.Contains("Read"));
    UNIT_ASSERT(html.Contains("lonely-session"));
    UNIT_ASSERT(html.Contains("method=\"POST\""));
    UNIT_ASSERT(html.Contains("value=\"kill_session\""));
    UNIT_ASSERT(html.Contains("value=\"lonely-session\""));
    UNIT_ASSERT(!html.Contains("location.href"));
}

Y_UNIT_TEST(KillSessionSendsErrorAndKeepsSessionUntilDisconnect) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = RegisterReadSession("lonely-session", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);

    SendBalancerKillPost(tc, "user", "lonely-session");
    auto error = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT(error->Record.GetCode() == NPersQueue::NErrorCode::ERROR);
    UNIT_ASSERT_VALUES_EQUAL(error->Record.GetDescription(), "Reading session stopped from tablet monitoring");

    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    UNIT_ASSERT_C(res->Html.Contains("lonely-session"), res->Html.substr(0, 2000));

    // Emulate the read session actor closing the balancer pipe after TEvError.
    tc.Runtime->ClosePipe(pipe, tc.Edge, 0);
    DispatchFor(tc);

    const TString html = FetchBalancerHtml(tc);
    UNIT_ASSERT_C(!html.Contains("lonely-session"), html.substr(0, 2000));
    UNIT_ASSERT(!html.Contains("value=\"kill_session\""));
}

Y_UNIT_TEST(KillUnknownSessionRendersPage) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    const TString html = FetchBalancerHtml(tc, "action=kill_session&consumer=missing&session=no-such-session");
    UNIT_ASSERT_C(html.Contains("Generic Info"), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("Tablet info"));
}

Y_UNIT_TEST(KillSessionGetDoesNotStopSession) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = RegisterReadSession("lonely-session", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    Y_UNUSED(pipe);

    SendBalancerHttp(tc, "action=kill_session&consumer=user&session=lonely-session");
    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    UNIT_ASSERT_C(res->Html.Contains("lonely-session"), res->Html.substr(0, 2000));

    auto error = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(TDuration::MilliSeconds(1));
    UNIT_ASSERT(!error);
}

Y_UNIT_TEST(KillEmptyConsumerOrSessionDoesNotMatchUnregisteredPipe) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
    DispatchFor(tc);
    Y_UNUSED(pipe);

    SendBalancerKillPost(tc, "", "");
    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    UNIT_ASSERT_C(res->Html.Contains("Generic Info"), res->Html.substr(0, 2000));

    auto error = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(TDuration::MilliSeconds(1));
    UNIT_ASSERT(!error);
}

Y_UNIT_TEST(KillUnknownSessionPostRendersPage) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    SendBalancerKillPost(tc, "missing", "no-such-session");
    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    UNIT_ASSERT_C(res->Html.Contains("Generic Info"), res->Html.substr(0, 2000));
    UNIT_ASSERT(res->Html.Contains("Tablet info"));
}

Y_UNIT_TEST(KillSessionHtmlFormIsPostedLikeMonitoringProxy) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = RegisterReadSession("lonely-session", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    Y_UNUSED(pipe);

    const TString html = FetchBalancerHtml(tc);
    const auto form = ParseKillSessionForm(html, "lonely-session");
    UNIT_ASSERT_VALUES_EQUAL(*form.Fields.FindPtr("consumer"), "user");
    UNIT_ASSERT_VALUES_EQUAL(*form.Fields.FindPtr("session"), "lonely-session");

    SendFormAsMonitoringProxy(tc, form);
    auto error = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT(error->Record.GetCode() == NPersQueue::NErrorCode::ERROR);
    UNIT_ASSERT_VALUES_EQUAL(error->Record.GetDescription(), "Reading session stopped from tablet monitoring");
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancingApp)

} // namespace NKikimr::NPQ
