#pragma once
#include "sequenceshard.h"

#include <ydb/core/tx/sequenceshard/public/events.h>

#include <ydb/core/erasure/erasure.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSequenceShard {

    struct TTestContext {
        TTabletTypes::EType TabletType = TTabletTypes::SequenceShard;
        ui64 TabletId = MakeTabletID(false, 1);
        THolder<TTestActorRuntime> Runtime;
        TActorId ClientId;
        TActorId UnmarkedClientId;
        TMap<std::tuple<ui64, ui64, ui64>, TActorId> MarkedClientIds;

        TTestContext();
        ~TTestContext();

        void Setup(ui32 nodeCount = 1, bool useRealThreads = false);

        void SetupLogging();
        void SetupTabletServices();

        void RebootTablet();
        void WaitTabletBoot();

        template<class TEvent>
        THolder<TEvent> ExpectEdgeEvent(const TActorId& actor) {
            return Runtime->GrabEdgeEvent<TEvent>(actor)->Release();
        }

        template<class TEvent>
        THolder<TEvent> ExpectEdgeEvent(const TActorId& actor, ui64 cookie) {
            auto ev = Runtime->GrabEdgeEvent<TEvent>(actor);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
            return ev->Release();
        }

        void SwitchToUnmarked();
        void SwitchToMarked(ui64 schemeShardId, ui64 generation, ui64 round);

        void SendFromEdge(const TActorId& edge, IEventBase* payload, ui64 cookie);

        void SendCreateSequence(
            ui64 cookie, const TActorId& edge,
            THolder<TEvSequenceShard::TEvCreateSequence> msg);
        THolder<TEvSequenceShard::TEvCreateSequenceResult> NextCreateSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvCreateSequenceResult> CreateSequence(
            THolder<TEvSequenceShard::TEvCreateSequence> msg);

        void SendAllocateSequence(
            ui64 cookie, const TActorId& edge,
            const TPathId& pathId, ui64 cache = 0);
        THolder<TEvSequenceShard::TEvAllocateSequenceResult> NextAllocateSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvAllocateSequenceResult> AllocateSequence(
            const TPathId& pathId, ui64 cache = 0);

        void SendDropSequence(
            ui64 cookie, const TActorId& edge, const TPathId& pathId);
        THolder<TEvSequenceShard::TEvDropSequenceResult> NextDropSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvDropSequenceResult> DropSequence(
            const TPathId& pathId);

        void SendUpdateSequence(
            ui64 cookie, const TActorId& edge,
            THolder<TEvSequenceShard::TEvUpdateSequence> msg);
        THolder<TEvSequenceShard::TEvUpdateSequenceResult> NextUpdateSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvUpdateSequenceResult> UpdateSequence(
            THolder<TEvSequenceShard::TEvUpdateSequence> msg);

        void SendFreezeSequence(
            ui64 cookie, const TActorId& edge, const TPathId& pathId);
        THolder<TEvSequenceShard::TEvFreezeSequenceResult> NextFreezeSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvFreezeSequenceResult> FreezeSequence(
            const TPathId& pathId);

        void SendRestoreSequence(
            ui64 cookie, const TActorId& edge,
            THolder<TEvSequenceShard::TEvRestoreSequence> msg);
        THolder<TEvSequenceShard::TEvRestoreSequenceResult> NextRestoreSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvRestoreSequenceResult> RestoreSequence(
            THolder<TEvSequenceShard::TEvRestoreSequence> msg);

        void SendRedirectSequence(
            ui64 cookie, const TActorId& edge,
            const TPathId& pathId, ui64 redirectTo);
        THolder<TEvSequenceShard::TEvRedirectSequenceResult> NextRedirectSequenceResult(
            ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvRedirectSequenceResult> RedirectSequence(
            const TPathId& pathId, ui64 redirectTo);

        void SendGetSequence(ui64 cookie, const TActorId& edge, const TPathId& pathId);
        THolder<TEvSequenceShard::TEvGetSequenceResult> NextGetSequenceResult(ui64 cookie, const TActorId& edge);
        THolder<TEvSequenceShard::TEvGetSequenceResult> GetSequence(const TPathId& pathId);
    };

} // namespace NSequenceShard
} // namespace NKikimr
