#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/fake_scheme_shard.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/public/lib/base/msgbus.h>


namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPQTestSlow) {

Y_UNIT_TEST(TestWriteVeryBigMessage) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        tc.Runtime->SetScheduledLimit(200);
        activeZone = false;

        PQTabletPrepare({}, {}, tc); //always delete

        TVector<std::pair<ui64, TString>> data;
        data.push_back({1, TString{10, 'b'}});
        CmdWrite(1, "sourceIdx", data, tc, false, {}, false, "", -1, 40'000);
        data.clear();
        const ui32 size  = PlainOrSoSlow(40_MB, 1_MB);
        const ui32 so = PlainOrSoSlow(1, 0);
        data.push_back({2, TString{size, 'a'}});
        CmdWrite(1, "sourceIdx", data, tc, false, {}, false, "", -1, 80'000);
        CmdWrite(0, "sourceIdx", data, tc, false, {}, false, "", -1, 0);
        activeZone = true;
        PQGetPartInfo(so, 1, tc);
        PQTabletRestart(tc);
        PQGetPartInfo(so, 1, tc);
    });
}

Y_UNIT_TEST(TestOnDiskStoredSourceIds) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        tc.Runtime->SetScheduledLimit(200);

        // No important client, lifetimeseconds=0 - delete right now
        PQTabletPrepare({.sidMaxCount=3}, {}, tc);

        TVector<TString> writtenSourceIds;

        TVector<std::pair<ui64, TString>> data;
        activeZone = true;

        TString ss{32, '_'};
        ui32 NUM_SOURCEIDS = 100;
        data.push_back({1, ss});
        CmdWrite(0, "sourceid0", data, tc, false, {}, false, "", -1, 100);
        ui32 offset = 1200;
        for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
            try {
                TString cookie = CmdSetOwner(0, tc).first;
                THolder<TEvPersQueue::TEvRequest> request;
                tc.Runtime->ResetScheduledCount();
                request.Reset(new TEvPersQueue::TEvRequest);
                auto req = request->Record.MutablePartitionRequest();
                req->SetPartition(0);
                req->SetOwnerCookie(cookie);
                req->SetMessageNo(0);
                req->SetCmdWriteOffset(offset);
                for (ui32 i = 0; i < NUM_SOURCEIDS; ++i) {
                    auto write = req->AddCmdWrite();
                    write->SetSourceId(TStringBuilder() << "sourceid_" << i);
                    write->SetSeqNo(0);
                    write->SetData(ss);
                }
                tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
                TAutoPtr<IEventHandle> handle;
                TEvPersQueue::TEvResponse *result;
                result = tc.Runtime->GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle, [](const TEvPersQueue::TEvResponse& ev){
                    if (!ev.Record.HasPartitionResponse() ||
                        !ev.Record.GetPartitionResponse().HasCmdReadResult()) {
                        return true;
                    }
                    return false;
                }); //there could be outgoing reads in TestReadSubscription test

                UNIT_ASSERT(result);
                UNIT_ASSERT(result->Record.HasStatus());
                if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                    tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                    retriesLeft = 2;
                    continue;
                }

                if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRITE_ERROR_BAD_OFFSET) {
                    ++offset;
                    continue;
                }

                Cout << "Error code is " << static_cast<int>(result->Record.GetErrorCode()) << Endl;
                UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);

                UNIT_ASSERT(result->Record.GetPartitionResponse().CmdWriteResultSize() == NUM_SOURCEIDS);
                for (ui32 i = 0; i < NUM_SOURCEIDS; ++i) {
                    UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasAlreadyWritten());
                    UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasOffset());
                }

                retriesLeft = 0;
            } catch (NActors::TSchedulingLimitReachedException) {
                UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
                retriesLeft = 3;
            }
        }
        for (ui64 i = 100; i < 104; ++i) {
            CmdWrite(0, TStringBuilder() << "sourceid_" << i, data, tc, false, {}, false, "", -1, offset + i);
            Cout << TInstant::Now() << " written sourceid_" << i << Endl;
        }
        for (ui64 i = 100; i < 104; ++i) {
            writtenSourceIds.push_back(TStringBuilder() << "sourceid_" << i);
        }
        Cout << TInstant::Now() << " now check list of sourceIds" << Endl;
        auto sourceIds = CmdSourceIdRead(tc);
        UNIT_ASSERT(sourceIds.size() > 0);
        for (auto& s: sourceIds) {
            auto findIt = std::find(writtenSourceIds.begin(), writtenSourceIds.end(), s);
            UNIT_ASSERT_C(findIt != writtenSourceIds.end(), "try to find sourceId " << s);
        }
        Cout << TInstant::Now() << "All Ok" << Endl;
    });
}

} // Y_UNIT_TEST_SUITE(TPQTestSlow)
} // namespace NKikimr::NPQ
