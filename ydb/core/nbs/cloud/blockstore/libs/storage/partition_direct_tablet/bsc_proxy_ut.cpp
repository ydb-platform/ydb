#include "bsc_proxy.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto NoEventTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

// Captures the pipe client spawned by TBscProxy and drops its traffic so
// missing BSC cannot emit connect-fail while tests inject pipe events.
class TBscProxyTestEnv final
{
public:
    TTestBasicRuntime Runtime;
    TLogTitle LogTitle;
    TActorId Edge;
    TActorId Proxy;
    TActorId PipeClient;
    ui32 SendCount = 0;
    ui64 LastBscRequestCookie = 0;

    TBscProxyTestEnv()
        : LogTitle(
              0,
              TLogTitle::TPartitionDirect{
                  .DiskId = "test",
                  .TabletId = 1,
                  .Generation = 1})
    {
        SetupTabletServices(Runtime);
        Runtime.SetLogPriority(NKikimrServices::NBS_PARTITION, NLog::PRI_DEBUG);

        Edge = Runtime.AllocateEdgeActor();
        Proxy = Runtime.Register(new TBscProxy(Edge, LogTitle));
        Runtime.EnableScheduleForActor(Proxy);

        Runtime.SetRegistrationObserverFunc(
            [this](
                TTestActorRuntimeBase& runtime,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                TTestActorRuntimeBase::DefaultRegistrationObserver(
                    runtime,
                    parentId,
                    actorId);
                if (parentId == Proxy) {
                    PipeClient = actorId;
                }
            });

        Runtime.SetObserverFunc(
            [this](TAutoPtr<IEventHandle>& ev)
            {
                const ui32 type = ev->GetTypeRewrite();
                // The pipe client is TActorBootstrapped: Bootstrap and Poison
                // must be delivered or CloseClient aborts in StateBootstrap.
                if (type == TEvents::TSystem::Bootstrap ||
                    type == TEvents::TSystem::Poison)
                {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                const bool toOrFromPipe =
                    PipeClient &&
                    (ev->Recipient == PipeClient || ev->Sender == PipeClient);
                const bool isPipeSend = type == TEvTabletPipe::EvSend;
                if (toOrFromPipe || isPipeSend) {
                    if (isPipeSend) {
                        LastBscRequestCookie = ev->Cookie;
                        ++SendCount;
                        if (!PipeClient) {
                            PipeClient = ev->Recipient;
                        }
                    }
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
    }

    void SendAllocate(ui64 cookie)
    {
        const ui32 before = SendCount;
        Runtime.Send(new IEventHandle(
            Proxy,
            Edge,
            new TBscProxy::TEvSend(THolder<IEventBase>(
                new TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup())),
            0,
            cookie));
        TDispatchOptions options;
        options.CustomFinalCondition = [this, before]
        {
            return SendCount > before;
        };
        Runtime.DispatchEvents(options);
        UNIT_ASSERT_C(SendCount > before, "BSC EvSend was not observed");
        UNIT_ASSERT(PipeClient);
        UNIT_ASSERT(LastBscRequestCookie);
    }

    // A second TEvSend while one request is inflight must not hit BSC.
    void SendAllocateWhileInFlight(ui64 cookie)
    {
        const ui32 before = SendCount;
        Runtime.Send(new IEventHandle(
            Proxy,
            Edge,
            new TBscProxy::TEvSend(THolder<IEventBase>(
                new TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup())),
            0,
            cookie));

        TAutoPtr<IEventHandle> handle;
        auto* result = GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            TBscProxy::PipeFailureStatus,
            result->Record.GetStatus());
        UNIT_ASSERT(
            result->Record.GetErrorReason().Contains("already in flight"));
        UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(before, SendCount);
    }

    void StopProxy()
    {
        Runtime.Send(
            new IEventHandle(Proxy, Edge, new TEvents::TEvPoisonPill()));
        Runtime.DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(100));
    }

    void InjectConnect(TActorId clientId, NKikimrProto::EReplyStatus status)
    {
        Runtime.Send(new IEventHandle(
            Proxy,
            Edge,
            new TEvTabletPipe::TEvClientConnected(
                MakeBSControllerID(),
                status,
                clientId,
                TActorId(),
                true,
                false,
                1)));
    }

    void InjectDestroy(TActorId clientId)
    {
        Runtime.Send(new IEventHandle(
            Proxy,
            Edge,
            new TEvTabletPipe::TEvClientDestroyed(
                MakeBSControllerID(),
                clientId,
                TActorId())));
    }

    void InjectAllocateResult(ui64 cookie, NKikimrProto::EReplyStatus status)
    {
        auto result = std::make_unique<
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
        result->Record.SetStatus(status);
        Runtime.Send(
            new IEventHandle(Proxy, Edge, result.release(), 0, cookie));
    }

    void InjectInFlightAllocateResult(NKikimrProto::EReplyStatus status)
    {
        InjectAllocateResult(LastBscRequestCookie, status);
    }

    [[nodiscard]] TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult*
    GrabAllocateResult(TAutoPtr<IEventHandle>& handle, TDuration timeout)
    {
        return Runtime.GrabEdgeEvent<
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(
            handle,
            timeout);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBscProxyTest)
{
    Y_UNIT_TEST(ShouldOpenNewPipeOnSecondSend)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        const TActorId firstPipe = env.PipeClient;
        env.InjectInFlightAllocateResult(NKikimrProto::OK);

        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(env.GrabAllocateResult(handle, TDuration::Max()));

        env.SendAllocate(2);
        UNIT_ASSERT(env.PipeClient != firstPipe);
        UNIT_ASSERT_VALUES_EQUAL(2u, env.SendCount);
    }

    Y_UNIT_TEST(ShouldNotForwardResultOnOwnPipeConnectOk)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        env.InjectConnect(env.PipeClient, NKikimrProto::OK);

        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!env.GrabAllocateResult(handle, NoEventTimeout));
    }

    Y_UNIT_TEST(ShouldIgnoreForeignPipeEvents)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        const TActorId foreign = env.Runtime.AllocateEdgeActor();

        env.InjectConnect(foreign, NKikimrProto::OK);
        env.InjectDestroy(foreign);

        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!env.GrabAllocateResult(handle, NoEventTimeout));

        env.InjectDestroy(env.PipeClient);
        auto* result = env.GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            TBscProxy::PipeFailureStatus,
            result->Record.GetStatus());
    }

    Y_UNIT_TEST(ShouldForwardAllocateResultAndDropInFlight)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(7);
        env.InjectInFlightAllocateResult(NKikimrProto::OK);

        TAutoPtr<IEventHandle> handle;
        auto* result = env.GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::OK, result->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(7u, handle->Cookie);
    }

    Y_UNIT_TEST(ShouldSynthesizeErrorOnPipeDestroyWhileInFlight)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(3);
        env.InjectDestroy(env.PipeClient);

        TAutoPtr<IEventHandle> handle;
        auto* result = env.GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            TBscProxy::PipeFailureStatus,
            result->Record.GetStatus());
        UNIT_ASSERT(result->Record.GetErrorReason().Contains("pipe destroyed"));
        UNIT_ASSERT_VALUES_EQUAL(3u, handle->Cookie);
    }

    Y_UNIT_TEST(ShouldSynthesizeErrorOnPipeConnectFailureWhileInFlight)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(4);
        env.InjectConnect(env.PipeClient, NKikimrProto::ERROR);

        TAutoPtr<IEventHandle> handle;
        auto* result = env.GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            TBscProxy::PipeFailureStatus,
            result->Record.GetStatus());
        UNIT_ASSERT(
            result->Record.GetErrorReason().Contains("pipe connect failed"));
        UNIT_ASSERT_VALUES_EQUAL(4u, handle->Cookie);
    }

    Y_UNIT_TEST(ShouldRejectConcurrentSendWithTryLater)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        env.SendAllocateWhileInFlight(2);
        env.SendAllocateWhileInFlight(1);
        env.InjectInFlightAllocateResult(NKikimrProto::OK);

        TAutoPtr<IEventHandle> handle;
        auto* result = env.GrabAllocateResult(handle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::OK, result->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1u, handle->Cookie);
    }

    Y_UNIT_TEST(ShouldIgnoreStaleResultAfterPipeFailure)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(3);
        const ui64 staleBscRequestCookie = env.LastBscRequestCookie;
        env.InjectDestroy(env.PipeClient);

        TAutoPtr<IEventHandle> failureHandle;
        auto* failure = env.GrabAllocateResult(failureHandle, TDuration::Max());
        UNIT_ASSERT(failure);
        UNIT_ASSERT_VALUES_EQUAL(
            TBscProxy::PipeFailureStatus,
            failure->Record.GetStatus());

        env.InjectAllocateResult(staleBscRequestCookie, NKikimrProto::OK);
        TAutoPtr<IEventHandle> staleHandle;
        UNIT_ASSERT(!env.GrabAllocateResult(staleHandle, NoEventTimeout));
    }

    Y_UNIT_TEST(ShouldIgnoreStaleResultAfterNewSend)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        const ui64 firstBscRequestCookie = env.LastBscRequestCookie;
        env.InjectDestroy(env.PipeClient);

        TAutoPtr<IEventHandle> failureHandle;
        UNIT_ASSERT(env.GrabAllocateResult(failureHandle, TDuration::Max()));

        env.SendAllocate(2);
        env.InjectAllocateResult(firstBscRequestCookie, NKikimrProto::OK);

        TAutoPtr<IEventHandle> staleHandle;
        UNIT_ASSERT(!env.GrabAllocateResult(staleHandle, NoEventTimeout));

        env.InjectInFlightAllocateResult(NKikimrProto::OK);
        TAutoPtr<IEventHandle> okHandle;
        auto* result = env.GrabAllocateResult(okHandle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::OK, result->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(2u, okHandle->Cookie);
    }

    Y_UNIT_TEST(ShouldIgnoreStaleResultAfterRetryWithSameClientRequestCookie)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        const ui64 firstBscRequestCookie = env.LastBscRequestCookie;
        env.InjectDestroy(env.PipeClient);

        TAutoPtr<IEventHandle> failureHandle;
        UNIT_ASSERT(env.GrabAllocateResult(failureHandle, TDuration::Max()));

        env.SendAllocate(1);
        UNIT_ASSERT(env.LastBscRequestCookie != firstBscRequestCookie);

        env.InjectAllocateResult(firstBscRequestCookie, NKikimrProto::OK);
        TAutoPtr<IEventHandle> staleHandle;
        UNIT_ASSERT(!env.GrabAllocateResult(staleHandle, NoEventTimeout));

        env.InjectInFlightAllocateResult(NKikimrProto::OK);
        TAutoPtr<IEventHandle> okHandle;
        auto* result = env.GrabAllocateResult(okHandle, TDuration::Max());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::OK, result->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1u, okHandle->Cookie);
    }

    Y_UNIT_TEST(ShouldOpenNewPipeAfterFailure)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        const TActorId firstPipe = env.PipeClient;
        env.InjectDestroy(firstPipe);

        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(env.GrabAllocateResult(handle, TDuration::Max()));

        env.SendAllocate(2);
        UNIT_ASSERT(env.PipeClient != firstPipe);
    }

    Y_UNIT_TEST(ShouldIgnoreDestroyAfterSuccessWithoutSynthesizingResult)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(1);
        env.InjectInFlightAllocateResult(NKikimrProto::OK);

        TAutoPtr<IEventHandle> okHandle;
        UNIT_ASSERT(env.GrabAllocateResult(okHandle, TDuration::Max()));

        env.InjectDestroy(env.PipeClient);

        TAutoPtr<IEventHandle> errorHandle;
        UNIT_ASSERT(!env.GrabAllocateResult(errorHandle, NoEventTimeout));
    }

    Y_UNIT_TEST(ShouldPoisonWithoutSynthesizingResult)
    {
        TBscProxyTestEnv env;
        env.SendAllocate(9);
        env.StopProxy();

        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!env.GrabAllocateResult(handle, NoEventTimeout));
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
