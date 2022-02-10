#pragma once
#include "options.h"
#include <ydb/core/kesus/tablet/ut_helpers.h>

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

using namespace NKikimr;
using namespace NKikimr::NKesus;

inline TString GetResourceName(size_t i) {
    return TStringBuilder() << "/Root/Res" << i;
}

struct TTestState : public TAtomicRefCount<TTestState> {
    TTestState(const TOptions& options, TTestContext& ctx)
        : Options(options)
        , ResourcesState(options.ResourcesCount)
        , EdgeActorId(ctx.Runtime->AllocateEdgeActor())
    {
        TabletId = ctx.TabletId;
        Y_ENSURE(TabletId != 0);
        CreateResources(ctx);
    }

    void CreateResources(TTestContext& ctx) {
        { // Root
            NKikimrKesus::THierarchicalDRRResourceConfig cfg;
            cfg.SetMaxUnitsPerSecond(Options.MaxUnitsPerSecond * Options.ResourcesCount);
            Cerr << "Add resource \"/Root\": " << cfg << "." << Endl;
            ctx.AddQuoterResource("/Root", cfg);
        }

        for (size_t res = 0; res < Options.ResourcesCount; ++res) {
            NKikimrKesus::THierarchicalDRRResourceConfig cfg;
            cfg.SetMaxUnitsPerSecond(Options.MaxUnitsPerSecond);
            Cerr << "Add resource \"" << GetResourceName(res) << "\": " << cfg << "." << Endl;
            ResourcesState[res].ResourceId = ctx.AddQuoterResource(GetResourceName(res), cfg);
            ResId2StateIndex[ResourcesState[res].ResourceId] = res;
        }
    }

    struct TResourceState {
        ui64 ResourceId = 0;
        double ConsumedAmount = 0;
    };

    const TOptions& Options;
    ui64 TabletId = 0;
    std::vector<TResourceState> ResourcesState;
    TActorId EdgeActorId;
    THashMap<ui64, size_t> ResId2StateIndex;
    TMutex Mutex;
};
