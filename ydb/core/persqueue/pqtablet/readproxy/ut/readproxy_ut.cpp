#include <ydb/core/persqueue/pqtablet/readproxy/readproxy.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

using namespace NActors;

NKikimrClient::TPersQueueRequest MakeReadRequest(ui64 offset) {
    NKikimrClient::TPersQueueRequest request;
    auto* cmdRead = request.MutablePartitionRequest()->MutableCmdRead();
    cmdRead->SetOffset(offset);
    cmdRead->SetPartNo(0);
    cmdRead->SetCount(10);
    cmdRead->SetCanReadBatches(true);
    return request;
}

void AddResult(
    NKikimrClient::TCmdReadResult* readResult,
    ui64 offset,
    ui64 seqNo,
    ui32 partNo,
    ui32 totalParts,
    TStringBuf data)
{
    auto* result = readResult->AddResult();
    result->SetOffset(offset);
    result->SetSeqNo(seqNo);
    result->SetPartNo(partNo);
    result->SetTotalParts(totalParts);
    result->SetData(TString{data});
    result->SetSourceId("src");
    result->SetWriteTimestampMS(1);
    result->SetCreateTimestampMS(1);
    result->SetUncompressedSize(data.size());
    if (partNo == 0 && totalParts > 1) {
        result->SetTotalSize(data.size() * totalParts);
    }
}

THolder<TEvPersQueue::TEvResponse> MakeOkReadResponse(std::function<void(NKikimrClient::TCmdReadResult*)> fill) {
    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto* readResult = response->Record.MutablePartitionResponse()->MutableCmdReadResult();
    readResult->SetRealReadOffset(0);
    readResult->SetMaxOffset(100);
    readResult->SetStartOffset(0);
    readResult->SetEndOffset(100);
    fill(readResult);
    return response;
}

Y_UNIT_TEST_SUITE(TReadProxyTest) {

// Reproduces: after an incomplete multipart message, follow-up returns PartNo==0
// (remaining parts gone). Proxy should drop the incomplete message and finish,
// but currently re-issues the same follow-up instead of calling removeIncomplete.
Y_UNIT_TEST(DropsIncompleteMessageWhenFollowupStartsNewMessage) {
    TTestBasicRuntime runtime(1);
    runtime.Initialize(TAppPrepare().Unwrap());

    const TActorId sender = runtime.AllocateEdgeActor();
    const TActorId tablet = runtime.AllocateEdgeActor();
    constexpr ui64 tabletId = 1;
    constexpr ui32 tabletGeneration = 1;

    const auto request = MakeReadRequest(/*offset=*/9);
    const TActorId proxy = runtime.Register(CreateReadProxy(
        sender,
        tabletId,
        tablet,
        tabletGeneration,
        TDirectReadKey{},
        request,
        TActorId{}));
    runtime.EnableScheduleForActor(proxy);

    {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(opts);
    }

    // Initial tablet answer: complete message + first part of a 3-part message.
    runtime.Send(new IEventHandle(proxy, tablet, MakeOkReadResponse([](auto* readResult) {
        AddResult(readResult, /*offset=*/9, /*seqNo=*/100, /*partNo=*/0, /*totalParts=*/1, "complete");
        AddResult(readResult, /*offset=*/10, /*seqNo=*/101, /*partNo=*/0, /*totalParts=*/3, "part0");
    }).Release()), 0, true);

    {
        auto followup = runtime.GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
        UNIT_ASSERT(followup);
        const auto& cmdRead = followup->Record.GetPartitionRequest().GetCmdRead();
        UNIT_ASSERT_VALUES_EQUAL(cmdRead.GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(cmdRead.GetPartNo(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cmdRead.GetCount(), 1u);
    }

    // Remaining parts of offset=10 were compacted away; tablet returns next message.
    runtime.Send(new IEventHandle(proxy, tablet, MakeOkReadResponse([](auto* readResult) {
        AddResult(readResult, /*offset=*/11, /*seqNo=*/102, /*partNo=*/0, /*totalParts=*/1, "next");
    }).Release()), 0, true);

    // Correct behaviour: finish with only the complete message (offset 9).
    // Buggy behaviour: another follow-up for offset=10 PartNo=1.
    TAutoPtr<IEventHandle> handle;
    auto [response, followupAgain] = runtime.GrabEdgeEvents<
        TEvPersQueue::TEvResponse,
        TEvPersQueue::TEvRequest>(handle, TDuration::Seconds(5));

    UNIT_ASSERT_C(response && !followupAgain,
        TStringBuilder()
            << "expected final response after incomplete multipart was superseded by PartNo==0; "
            << (followupAgain
                    ? "got another follow-up request instead"
                    : "got neither response nor follow-up"));

    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(response->Record.GetStatus()), static_cast<ui32>(NMsgBusProxy::MSTATUS_OK));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(response->Record.GetErrorCode()), static_cast<ui32>(NPersQueue::NErrorCode::OK));

    const auto& result = response->Record.GetPartitionResponse().GetCmdReadResult();
    UNIT_ASSERT_VALUES_EQUAL(result.ResultSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(result.GetResult(0).GetOffset(), 9u);
    UNIT_ASSERT_VALUES_EQUAL(result.GetResult(0).GetData(), "complete");
}

} // Y_UNIT_TEST_SUITE(TReadProxyTest)

} // namespace
} // namespace NKikimr::NPQ
