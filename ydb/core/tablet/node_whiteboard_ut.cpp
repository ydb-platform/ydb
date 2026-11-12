#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <optional>
#include <set>

namespace NKikimr::NNodeWhiteboard {
namespace {

struct TIoProgressFixture {
    TTestBasicRuntime Runtime;
    TActorId Board;
    TActorId Client;
    ui64 Cookie = 0;
    std::set<std::pair<TActorId, ui64>> IoRequests;

    TIoProgressFixture() {
        SetupTabletServices(Runtime);
        Runtime.SetRegistrationObserverFunc([](TTestActorRuntimeBase& runtime,
                const TActorId& parentId, const TActorId& actorId) {
            TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
            runtime.EnableScheduleForActor(actorId);
        });
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPDisk::TEvIoProgressRequest::EventType) {
                // The simulated runtime may observe a deferred event more than once.
                IoRequests.emplace(ev->Sender, ev->Cookie);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Board = Runtime.Register(CreateNodeWhiteboardService());
        Client = Runtime.AllocateEdgeActor();
    }

    TActorId AddDisk(ui32 id, ui64 guid) {
        const auto actor = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeBlobStoragePDiskID(Runtime.GetNodeId(0), id), actor);
        Publish(id, guid);
        return actor;
    }

    void Publish(ui32 id, std::optional<ui64> guid) {
        auto* update = new TEvWhiteboard::TEvPDiskStateUpdate;
        update->Record.SetPDiskId(id);
        if (guid) {
            update->Record.SetGuid(*guid);
        }
        update->Record.SetState(NKikimrBlobStorage::TPDiskState::Normal);
        Runtime.Send(new IEventHandle(Board, Client, update), 0, true);
    }

    ui64 Query(bool include = true, ui64 changedSince = 0, TVector<int> fields = {}, TActorId client = {}) {
        auto* request = new TEvWhiteboard::TEvPDiskStateRequest;
        request->Record.SetIncludeIoProgress(include);
        request->Record.SetChangedSince(changedSince);
        for (int field : fields) {
            request->Record.AddFieldsRequired(field);
        }
        const auto cookie = ++Cookie;
        Runtime.Send(new IEventHandle(Board, client ? client : Client, request, 0, cookie), 0, true);
        return cookie;
    }

    NPDisk::TEvIoProgressRequest::TPtr TakeQuery(TActorId disk) {
        return Runtime.GrabEdgeEventRethrow<NPDisk::TEvIoProgressRequest>(disk);
    }

    void Reply(const NPDisk::TEvIoProgressRequest::TPtr& request, TActorId actor,
            ui32 id, ui64 guid, ui64 pending, ui64 duration, bool ready = true,
            NKikimrBlobStorage::TPDiskState::E state = NKikimrBlobStorage::TPDiskState::Normal) {
        auto* result = new NPDisk::TEvIoProgressResult;
        result->PDiskId = id;
        result->Guid = guid;
        result->Ready = ready;
        result->PendingOperations = pending;
        result->NoCompletionMs = duration;
        result->Incarnation = actor;
        result->CurrentState = state;
        Runtime.Send(new IEventHandle(request->Sender, actor, result, 0, request->Cookie), 0, true);
    }

    NKikimrWhiteboard::TEvPDiskStateResponse TakeResponse(ui64 cookie, TActorId client = {}) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvWhiteboard::TEvPDiskStateResponse>(client ? client : Client);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Record.HasResponseTime());
        return response->Get()->Record;
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TNodeWhiteboardIoProgress) {
    Y_UNIT_TEST(DefaultRequestHasNoLiveQueriesAndOptInDoesNotCacheSamples) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto initial = f.TakeResponse(f.Query(false));
        UNIT_ASSERT_VALUES_EQUAL(initial.PDiskStateInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(initial.PDiskIoProgressInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.IoRequests.size(), 0);

        const auto cookie = f.Query();
        const auto request = f.TakeQuery(disk);
        f.Reply(request, disk, 100, 1000, 7, 12345);
        const auto live = f.TakeResponse(cookie);
        UNIT_ASSERT_VALUES_EQUAL(live.PDiskIoProgressInfoSize(), 1);
        const auto& sample = live.GetPDiskIoProgressInfo(0);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetPDiskId(), 100);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetGuid(), 1000);
        UNIT_ASSERT(sample.GetReady());
        UNIT_ASSERT_VALUES_EQUAL(sample.GetPendingOperations(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetNoCompletionMs(), 12345);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetState(), NKikimrBlobStorage::TPDiskState::Normal);
        UNIT_ASSERT_VALUES_EQUAL(f.TakeResponse(f.Query(false)).PDiskIoProgressInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.IoRequests.size(), 1);

        const auto nextCookie = f.Query();
        const auto nextRequest = f.TakeQuery(disk);
        UNIT_ASSERT_UNEQUAL(request->Sender, nextRequest->Sender);
        f.Reply(nextRequest, disk, 100, 1000, 0, 0);
        const auto next = f.TakeResponse(nextCookie);
        UNIT_ASSERT_VALUES_EQUAL(next.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(next.GetPDiskIoProgressInfo(0).GetPendingOperations(), 0);
        UNIT_ASSERT_VALUES_EQUAL(next.GetPDiskIoProgressInfo(0).GetNoCompletionMs(), 0);
    }

    Y_UNIT_TEST(FilteringUsesSelectedCacheIdentityAndPreservesChangedSince) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto cookie = f.Query(true, 0, {NKikimrWhiteboard::TPDiskStateInfo::kStateFieldNumber});
        f.Reply(f.TakeQuery(disk), disk, 100, 1000, 5, 20000);
        const auto response = f.TakeResponse(cookie);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskStateInfoSize(), 1);
        UNIT_ASSERT(!response.GetPDiskStateInfo(0).HasPDiskId());
        UNIT_ASSERT(!response.GetPDiskStateInfo(0).HasGuid());
        UNIT_ASSERT(response.GetPDiskStateInfo(0).HasState());
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetPDiskIoProgressInfo(0).GetPDiskId(), 100);
        const auto filtered = f.TakeResponse(f.Query(true, Max<ui64>()));
        UNIT_ASSERT_VALUES_EQUAL(filtered.PDiskStateInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(filtered.PDiskIoProgressInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.IoRequests.size(), 1);
    }

    Y_UNIT_TEST(MismatchedCookieDiskGuidSenderAndIncarnationAreIgnored) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto stranger = f.Runtime.AllocateEdgeActor();
        const auto cookie = f.Query();
        const auto request = f.TakeQuery(disk);
        auto badReply = [&](ui64 requestCookie, ui32 id, ui64 guid, TActorId sender, TActorId incarnation) {
            auto* result = new NPDisk::TEvIoProgressResult;
            result->PDiskId = id;
            result->Guid = guid;
            result->Ready = true;
            result->PendingOperations = 999;
            result->NoCompletionMs = 999999;
            result->Incarnation = incarnation;
            result->CurrentState = NKikimrBlobStorage::TPDiskState::Normal;
            f.Runtime.Send(new IEventHandle(request->Sender, sender, result, 0, requestCookie), 0, true);
        };
        badReply(request->Cookie + 1, 100, 1000, disk, disk);
        badReply(request->Cookie, 101, 1000, disk, disk);
        badReply(request->Cookie, 100, 1001, disk, disk);
        badReply(request->Cookie, 100, 1000, stranger, stranger);
        badReply(request->Cookie, 100, 1000, disk, stranger);
        f.Reply(request, disk, 100, 1000, 7, 12345);
        const auto response = f.TakeResponse(cookie);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetPDiskIoProgressInfo(0).GetPendingOperations(), 7);
        UNIT_ASSERT_VALUES_EQUAL(response.GetPDiskIoProgressInfo(0).GetNoCompletionMs(), 12345);
    }

    Y_UNIT_TEST(MissingActorAndMissingGuidHaveNoSample) {
        TIoProgressFixture f;
        f.Publish(100, 1000);
        f.Publish(101, {});
        const auto response = f.TakeResponse(f.Query());
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskStateInfoSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.IoRequests.size(), 0);
    }

    Y_UNIT_TEST(UndeliveredAndTimeoutHaveNoSampleAndLateReplyIsNotReused) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto failedCookie = f.Query();
        const auto failedRequest = f.TakeQuery(disk);
        f.Runtime.Send(new IEventHandle(failedRequest->Sender,
            MakeBlobStoragePDiskID(f.Runtime.GetNodeId(0), 100),
            new TEvents::TEvUndelivered(NPDisk::TEvIoProgressRequest::EventType,
                TEvents::TEvUndelivered::ReasonActorUnknown), 0, failedRequest->Cookie), 0, true);
        UNIT_ASSERT_VALUES_EQUAL(f.TakeResponse(failedCookie).PDiskIoProgressInfoSize(), 0);

        const auto timeoutCookie = f.Query();
        const auto lateRequest = f.TakeQuery(disk);
        UNIT_ASSERT_VALUES_EQUAL(f.TakeResponse(timeoutCookie).PDiskIoProgressInfoSize(), 0);
        const auto nextCookie = f.Query();
        const auto nextRequest = f.TakeQuery(disk);
        UNIT_ASSERT_UNEQUAL(lateRequest->Sender, nextRequest->Sender);
        f.Reply(lateRequest, disk, 100, 1000, 999, 999999);
        f.Reply(nextRequest, disk, 100, 1000, 2, 20000);
        const auto response = f.TakeResponse(nextCookie);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetPDiskIoProgressInfo(0).GetPendingOperations(), 2);
    }

    Y_UNIT_TEST(OverlappingCallersCanCompleteOutOfOrder) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto otherClient = f.Runtime.AllocateEdgeActor();
        const auto firstCookie = f.Query();
        const auto firstRequest = f.TakeQuery(disk);
        const auto secondCookie = f.Query(true, 0, {}, otherClient);
        const auto secondRequest = f.TakeQuery(disk);
        UNIT_ASSERT_UNEQUAL(firstRequest->Sender, secondRequest->Sender);
        f.Reply(secondRequest, disk, 100, 1000, 2, 20000);
        const auto second = f.TakeResponse(secondCookie, otherClient);
        UNIT_ASSERT_VALUES_EQUAL(second.GetPDiskIoProgressInfo(0).GetPendingOperations(), 2);
        f.Reply(firstRequest, disk, 100, 1000, 1, 10000);
        const auto first = f.TakeResponse(firstCookie);
        UNIT_ASSERT_VALUES_EQUAL(first.GetPDiskIoProgressInfo(0).GetPendingOperations(), 1);
    }

    Y_UNIT_TEST(ServiceReplacementInvalidatesReceivedAndPendingSamples) {
        TIoProgressFixture f;
        const auto firstDisk = f.AddDisk(100, 1000);
        const auto secondDisk = f.AddDisk(101, 1001);
        const auto cookie = f.Query();
        const auto firstRequest = f.TakeQuery(firstDisk);
        const auto secondRequest = f.TakeQuery(secondDisk);
        f.Reply(firstRequest, firstDisk, 100, 1000, 9, 20000);
        // Dispatch the early sample before changing the service mapping.
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NPDisk::TEvIoProgressResult::EventType, 1);
        f.Runtime.DispatchEvents(options);
        const auto replacement = f.Runtime.AllocateEdgeActor();
        f.Runtime.RegisterService(MakeBlobStoragePDiskID(f.Runtime.GetNodeId(0), 100), replacement);
        f.Reply(secondRequest, secondDisk, 101, 1001, 1, 10000);
        const auto response = f.TakeResponse(cookie);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetPDiskIoProgressInfo(0).GetPDiskId(), 101);

        const auto nextCookie = f.Query();
        const auto oldRequest = f.TakeQuery(replacement);
        const auto nextSecondRequest = f.TakeQuery(secondDisk);
        const auto newest = f.Runtime.AllocateEdgeActor();
        f.Runtime.RegisterService(MakeBlobStoragePDiskID(f.Runtime.GetNodeId(0), 100), newest);
        f.Reply(oldRequest, replacement, 100, 1000, 9, 20000);
        f.Reply(nextSecondRequest, secondDisk, 101, 1001, 1, 10000);
        const auto next = f.TakeResponse(nextCookie);
        UNIT_ASSERT_VALUES_EQUAL(next.PDiskIoProgressInfoSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(next.GetPDiskIoProgressInfo(0).GetPDiskId(), 101);
    }

    Y_UNIT_TEST(NonReadyAndTerminalStateAreTransportedWithoutPolicy) {
        TIoProgressFixture f;
        const auto disk = f.AddDisk(100, 1000);
        const auto cookie = f.Query();
        f.Reply(f.TakeQuery(disk), disk, 100, 1000, 3, 15000, false,
            NKikimrBlobStorage::TPDiskState::DeviceIoError);
        const auto response = f.TakeResponse(cookie);
        UNIT_ASSERT_VALUES_EQUAL(response.PDiskIoProgressInfoSize(), 1);
        const auto& sample = response.GetPDiskIoProgressInfo(0);
        UNIT_ASSERT(!sample.GetReady());
        UNIT_ASSERT_VALUES_EQUAL(sample.GetState(), NKikimrBlobStorage::TPDiskState::DeviceIoError);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetPendingOperations(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sample.GetNoCompletionMs(), 15000);
    }
}

} // namespace NKikimr::NNodeWhiteboard
