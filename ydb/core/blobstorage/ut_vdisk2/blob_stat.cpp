#include "env.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

using namespace NKikimr;

namespace {

    constexpr ui64 RequestCookie = 42;

    void SendRequest(TTestEnv& env, const TActorId& edge) {
        auto request = std::make_unique<TEvGetLogoBlobIndexStatRequest>();
        request->Record.set_stream(true);
        env.GetRuntime()->Send(new IEventHandle(
            env.GetVDiskServiceId(), edge, request.release(), 0, RequestCookie), 1);
    }

    std::unique_ptr<TEventHandle<TEvGetLogoBlobIndexStatResponse>> WaitForResponse(
            TTestEnv& env,
            const TActorId& edge)
    {
        return env.GetRuntime()->WaitForEdgeActorEvent<TEvGetLogoBlobIndexStatResponse>(edge);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(VDiskBlobStatTests) {

    Y_UNIT_TEST(OnlyOneStreamingScanIsAdmitted) {
        // A one-byte response limit makes the first completed tablet flush a
        // nonterminal batch, leaving the scan alive while it waits for an ACK.
        TTestEnv env(nullptr, false, 1);
        const TString data(1, 'x');
        for (ui64 tabletId : {1, 2, 3}) {
            const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.Put(id, data).GetStatus(), NKikimrProto::OK);
        }

        const TActorId firstEdge = env.GetRuntime()->AllocateEdgeActor(1);
        SendRequest(env, firstEdge);
        auto first = WaitForResponse(env, firstEdge);
        UNIT_ASSERT_VALUES_EQUAL(
            first->Get()->Record.status(),
            NKikimrProto::EReplyStatus_Name(NKikimrProto::OK));
        UNIT_ASSERT(first->Get()->Record.has_more());
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.sequence_id(), 1);

        const TActorId secondEdge = env.GetRuntime()->AllocateEdgeActor(1);
        SendRequest(env, secondEdge);
        auto second = WaitForResponse(env, secondEdge);
        UNIT_ASSERT_VALUES_EQUAL(
            second->Get()->Record.status(),
            NKikimrProto::EReplyStatus_Name(NKikimrProto::TRYLATER));
        UNIT_ASSERT(!second->Get()->Record.has_more());

        env.GetRuntime()->Send(new IEventHandle(
            first->Sender,
            firstEdge,
            new TEvGetLogoBlobIndexStatResponseAck(1, true),
            0,
            RequestCookie), 1);

        // Cancellation releases the admission slot. TEvGone races with a new
        // request, so retry TRYLATER responses just as a real client would.
        const TActorId retryEdge = env.GetRuntime()->AllocateEdgeActor(1);
        bool admitted = false;
        for (ui32 attempt = 0; attempt < 100 && !admitted; ++attempt) {
            SendRequest(env, retryEdge);
            auto retry = WaitForResponse(env, retryEdge);
            const auto& status = retry->Get()->Record.status();
            if (status == NKikimrProto::EReplyStatus_Name(NKikimrProto::OK)) {
                admitted = true;
                UNIT_ASSERT(retry->Get()->Record.has_more());
                env.GetRuntime()->Send(new IEventHandle(
                    retry->Sender,
                    retryEdge,
                    new TEvGetLogoBlobIndexStatResponseAck(1, true),
                    0,
                    RequestCookie), 1);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    status,
                    NKikimrProto::EReplyStatus_Name(NKikimrProto::TRYLATER));
            }
        }
        UNIT_ASSERT_C(admitted, "the scan admission slot was not released after cancellation");
    }

}
