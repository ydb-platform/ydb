#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/control/immediate_control_board_impl.h>

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    class TMon;
}

namespace NKikimr {
    inline NActors::TActorId MakeMemProfMonitorID(ui32 node = 0) {
        char x[12] = {'m', 'e', 'm', 'p', 'r', 'o', 'f', 'm', 'o', 'n', 'i', 't'};
        return NActors::TActorId(node, TStringBuf(x, 12));
    }

    struct IAllocMonitor {
        virtual ~IAllocMonitor() = default;

        virtual void RegisterPages(
            NActors::TMon* mon,
            NActors::TActorSystem* actorSystem,
            NActors::TActorId actorId)
        {
            Y_UNUSED(mon);
            Y_UNUSED(actorSystem);
            Y_UNUSED(actorId);
        }

        virtual void RegisterControls(TIntrusivePtr<TControlBoard> icb) {
            Y_UNUSED(icb);
        }

        virtual void Update(TDuration interval) = 0;

        virtual void Dump(IOutputStream& out, const TString& relPath) = 0;

        virtual void DumpForLog(IOutputStream& out, size_t limit) {
            Y_UNUSED(out);
            Y_UNUSED(limit);
        }
    };

    NActors::IActor* CreateMemProfMonitor(
        TDuration interval,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TString& filePathPrefix = "");
}
