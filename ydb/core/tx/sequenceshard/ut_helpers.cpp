#include "ut_helpers.h"

namespace NKikimr {
namespace NSequenceShard {

    TTestContext::TTestContext() {
        // nothing
    }

    TTestContext::~TTestContext() {
        // nothing
    }

    void TTestContext::Setup(ui32 nodeCount, bool useRealThreads) {
        Runtime.Reset(new TTestBasicRuntime(nodeCount, useRealThreads));

        SetupLogging();
        SetupTabletServices();

        TActorId bootstrapper = CreateTestBootstrapper(
            *Runtime,
            CreateTestTabletInfo(TabletId, TabletType, TErasureType::ErasureNone),
            &CreateSequenceShard);
        Runtime->EnableScheduleForActor(bootstrapper);

        WaitTabletBoot();
    }

    void TTestContext::SetupLogging() {
        Runtime->SetLogPriority(NKikimrServices::SEQUENCESHARD, NLog::PRI_TRACE);
    }

    void TTestContext::SetupTabletServices() {
        ::NKikimr::SetupTabletServices(*Runtime);
    }

    void TTestContext::RebootTablet() {
        ui32 nodeIndex = 0;
        ForwardToTablet(*Runtime, TabletId, TActorId(), new TEvents::TEvPoison, nodeIndex);
        WaitTabletBoot();
        InvalidateTabletResolverCache(*Runtime, TabletId, nodeIndex);
        ClientId = UnmarkedClientId = {};
    }

    void TTestContext::WaitTabletBoot() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime->DispatchEvents(options);
    }

    void TTestContext::SwitchToUnmarked() {
        ClientId = UnmarkedClientId;
    }

    void TTestContext::SwitchToMarked(ui64 schemeShardId, ui64 generation, ui64 round) {
        auto key = std::make_tuple(schemeShardId, generation, round);
        auto it = MarkedClientIds.find(key);
        if (it == MarkedClientIds.end()) {
            auto clientId = Runtime->ConnectToPipe(TabletId, TActorId(), 0, GetPipeConfigWithRetries());
            Runtime->SendToPipe(
                TabletId,
                TActorId(),
                new NEvSequenceShard::TEvMarkSchemeShardPipe(schemeShardId, generation, round),
                0,
                GetPipeConfigWithRetries(),
                clientId,
                0);
            auto res = MarkedClientIds.emplace(key, clientId);
            Y_ABORT_UNLESS(res.second);
            it = res.first;
        }
        ClientId = it->second;
    }

    void TTestContext::SendFromEdge(const TActorId& edge, IEventBase* payload, ui64 cookie) {
        ui32 nodeIndex = edge.NodeId() - Runtime->GetNodeId(0);
        if (!ClientId) {
            if (!UnmarkedClientId) {
                UnmarkedClientId = Runtime->ConnectToPipe(TabletId, edge, nodeIndex, GetPipeConfigWithRetries());
            }
            ClientId = UnmarkedClientId;
        }
        Runtime->SendToPipe(
            TabletId,
            edge,
            payload,
            nodeIndex,
            GetPipeConfigWithRetries(),
            ClientId,
            cookie);
    }

    void TTestContext::SendCreateSequence(
        ui64 cookie, const TActorId& edge,
        THolder<NEvSequenceShard::TEvCreateSequence> msg)
    {
        SendFromEdge(edge, msg.Release(), cookie);
    }

    THolder<NEvSequenceShard::TEvCreateSequenceResult> TTestContext::NextCreateSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvCreateSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvCreateSequenceResult> TTestContext::CreateSequence(
        THolder<NEvSequenceShard::TEvCreateSequence> msg)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendCreateSequence(cookie, edge, std::move(msg));
        return NextCreateSequenceResult(cookie, edge);
    }

    void TTestContext::SendAllocateSequence(
        ui64 cookie, const TActorId& edge,
        const TPathId& pathId, ui64 cache)
    {
        SendFromEdge(
            edge,
            new NEvSequenceShard::TEvAllocateSequence(pathId, cache),
            cookie);
    }

    THolder<NEvSequenceShard::TEvAllocateSequenceResult> TTestContext::NextAllocateSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvAllocateSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvAllocateSequenceResult> TTestContext::AllocateSequence(
        const TPathId& pathId, ui64 cache)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendAllocateSequence(cookie, edge, pathId, cache);
        return NextAllocateSequenceResult(cookie, edge);
    }

    void TTestContext::SendDropSequence(
        ui64 cookie, const TActorId& edge, const TPathId& pathId)
    {
        SendFromEdge(
            edge,
            new NEvSequenceShard::TEvDropSequence(pathId),
            cookie);
    }

    THolder<NEvSequenceShard::TEvDropSequenceResult> TTestContext::NextDropSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvDropSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvDropSequenceResult> TTestContext::DropSequence(
        const TPathId& pathId)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendDropSequence(cookie, edge, pathId);
        return NextDropSequenceResult(cookie, edge);
    }

    void TTestContext::SendUpdateSequence(
        ui64 cookie, const TActorId& edge,
        THolder<NEvSequenceShard::TEvUpdateSequence> msg)
    {
        SendFromEdge(edge, msg.Release(), cookie);
    }

    THolder<NEvSequenceShard::TEvUpdateSequenceResult> TTestContext::NextUpdateSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvUpdateSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvUpdateSequenceResult> TTestContext::UpdateSequence(
        THolder<NEvSequenceShard::TEvUpdateSequence> msg)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendUpdateSequence(cookie, edge, std::move(msg));
        return NextUpdateSequenceResult(cookie, edge);
    }

    void TTestContext::SendFreezeSequence(
        ui64 cookie, const TActorId& edge, const TPathId& pathId)
    {
        SendFromEdge(
            edge,
            new NEvSequenceShard::TEvFreezeSequence(pathId),
            cookie);
    }

    THolder<NEvSequenceShard::TEvFreezeSequenceResult> TTestContext::NextFreezeSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvFreezeSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvFreezeSequenceResult> TTestContext::FreezeSequence(
        const TPathId& pathId)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendFreezeSequence(cookie, edge, pathId);
        return NextFreezeSequenceResult(cookie, edge);
    }

    void TTestContext::SendRestoreSequence(
        ui64 cookie, const TActorId& edge,
        THolder<NEvSequenceShard::TEvRestoreSequence> msg)
    {
        SendFromEdge(edge, msg.Release(), cookie);
    }

    THolder<NEvSequenceShard::TEvRestoreSequenceResult> TTestContext::NextRestoreSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvRestoreSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvRestoreSequenceResult> TTestContext::RestoreSequence(
        THolder<NEvSequenceShard::TEvRestoreSequence> msg)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendRestoreSequence(cookie, edge, std::move(msg));
        return NextRestoreSequenceResult(cookie, edge);
    }

    void TTestContext::SendRedirectSequence(
        ui64 cookie, const TActorId& edge,
        const TPathId& pathId, ui64 redirectTo)
    {
        SendFromEdge(
            edge,
            new NEvSequenceShard::TEvRedirectSequence(pathId, redirectTo),
            cookie);
    }

    THolder<NEvSequenceShard::TEvRedirectSequenceResult> TTestContext::NextRedirectSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvRedirectSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvRedirectSequenceResult> TTestContext::RedirectSequence(
        const TPathId& pathId, ui64 redirectTo)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendRedirectSequence(cookie, edge, pathId, redirectTo);
        return NextRedirectSequenceResult(cookie, edge);
    }

    void TTestContext::SendGetSequence(ui64 cookie, const TActorId& edge, const TPathId& pathId)
    {
        SendFromEdge(
            edge,
            new NEvSequenceShard::TEvGetSequence(pathId),
            cookie);
    }

    THolder<NEvSequenceShard::TEvGetSequenceResult> TTestContext::NextGetSequenceResult(
        ui64 cookie, const TActorId& edge)
    {
        auto result = ExpectEdgeEvent<NEvSequenceShard::TEvGetSequenceResult>(edge, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TabletId);
        return result;
    }

    THolder<NEvSequenceShard::TEvGetSequenceResult> TTestContext::GetSequence(
        const TPathId& pathId)
    {
        ui64 cookie = RandomNumber<ui64>();
        auto edge = Runtime->AllocateEdgeActor();
        SendGetSequence(cookie, edge, pathId);
        return NextGetSequenceResult(cookie, edge);
    }

} // namespace NSequenceShard
} // namespace NKikimr
