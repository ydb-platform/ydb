#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    enum class ERangeResponse {
        ValidSnapshot,
        MalformedSnapshot,
        MalformedAfterSnapshot,
    };

    void TestRangeResponse(ERangeResponse response) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = TTestTxConfig::TxTablet0;
        TIntrusivePtr<TTabletStorageInfo> info = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        const ui32 groupId = info->ChannelInfo(0)->History.front().GroupID;
        const auto owner = runtime.AllocateEdgeActor();

        NKikimrTabletBase::TTabletLogEntry latest;
        latest.SetSnapshot(MakeGenStepPair(1, 1));
        latest.SetConfirmed(1);
        const TString latestBody = latest.SerializeAsString();
        const TLogoBlobID latestId(tabletId, 1, 2, 0, latestBody.size(), 0);

        NKikimrTabletBase::TTabletLogEntry snapshot;
        snapshot.SetSnapshot(MakeGenStepPair(1, 1));
        snapshot.SetConfirmed(0);
        snapshot.SetIsSnapshot(true);
        snapshot.SetIsTotalSnapshot(true);
        const TString snapshotBody = snapshot.SerializeAsString();
        const TLogoBlobID snapshotId(tabletId, 1, 1, 0, snapshotBody.size(), 0);
        const TString malformedBody("\x80", 1);

        auto discoverObserver = runtime.AddObserver<TEvBlobStorage::TEvDiscover>([&](auto& ev) {
            if (ev->Get()->TabletId != tabletId) {
                return;
            }
            auto* result = new TEvBlobStorage::TEvDiscoverResult(latestId, 0, latestBody);
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, result, 0, ev->Cookie), 0, true);
            ev.Reset();
        });

        ui32 rangeRequests = 0;
        auto rangeObserver = runtime.AddObserver<TEvBlobStorage::TEvRange>([&](auto& ev) {
            const auto* msg = ev->Get();
            if (msg->TabletId != tabletId) {
                return;
            }
            ++rangeRequests;
            auto* result = new TEvBlobStorage::TEvRangeResult(NKikimrProto::OK, msg->From, msg->To, groupId);
            if (response != ERangeResponse::MalformedSnapshot) {
                result->Responses.emplace_back(snapshotId, snapshotBody);
            }
            if (response != ERangeResponse::ValidSnapshot) {
                // A malformed synthetic entry after the snapshot leaves an otherwise valid graph
                const ui32 cookie = response == ERangeResponse::MalformedAfterSnapshot ? 1 : 0;
                const TLogoBlobID malformedId(tabletId, 1, 1, 0, malformedBody.size(), cookie);
                result->Responses.emplace_back(malformedId, malformedBody);
            }
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, result, 0, ev->Cookie), 0, true);
            ev.Reset();
        });

        TVector<TEvTabletBase::TEvRebuildGraphResult::TPtr> rebuildResults;
        auto resultObserver = runtime.AddObserver<TEvTabletBase::TEvRebuildGraphResult>([&](auto& ev) {
            if (ev->Recipient == owner) {
                rebuildResults.emplace_back(ev.Release());
            }
        });

        const auto rebuild = runtime.Register(CreateTabletReqRebuildHistoryGraph(owner, info.Get(), 1, nullptr, 0));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return !rebuildResults.empty();
        };
        runtime.DispatchEvents(options);
        UNIT_ASSERT(!rebuildResults.empty());
        const auto* result = rebuildResults.front()->Get();
        if (response == ERangeResponse::ValidSnapshot) {
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            UNIT_ASSERT(result->DependencyGraph);
            UNIT_ASSERT(result->DependencyGraph->IsValid());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(result->ErrorReason, "Log entry parse failed");
            UNIT_ASSERT(!result->DependencyGraph);
        }

        // Confirm the actor is gone and drain any extra rebuild result before checking the count
        runtime.Send(new IEventHandle(rebuild, owner, new TEvents::TEvPing, IEventHandle::FlagTrackDelivery), 0, true);
        auto undelivered = runtime.GrabEdgeEventRethrow<TEvents::TEvUndelivered>(owner);
        UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->SourceType, TEvents::TEvPing::EventType);
        UNIT_ASSERT_VALUES_EQUAL(rangeRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(rebuildResults.size(), 1);
    }

} // namespace

Y_UNIT_TEST_SUITE(TTabletRebuildHistoryGraph) {
    Y_UNIT_TEST(RejectsMalformedSnapshot) {
        TestRangeResponse(ERangeResponse::MalformedSnapshot);
    }

    Y_UNIT_TEST(RejectsMalformedEntryAfterSnapshot) {
        TestRangeResponse(ERangeResponse::MalformedAfterSnapshot);
    }

    Y_UNIT_TEST(RebuildsValidSnapshot) {
        TestRangeResponse(ERangeResponse::ValidSnapshot);
    }
}

} // namespace NKikimr
