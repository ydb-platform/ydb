#include "probes.h"

#include "actorsystem.h"

#include <util/string/builder.h>

LWTRACE_DEFINE_PROVIDER(ACTORLIB_PROVIDER);

namespace NActors {
    TVector<NLWTrace::TDashboard> LWTraceDashboards(TActorSystemSetup* setup) {
        TVector<NLWTrace::TDashboard> result;

        NLWTrace::TDashboard slowDash;
        ui32 pools = setup->GetExecutorsCount();
        size_t top = 30;
        slowDash.SetName("ActorSystem slow events");
        slowDash.SetDescription(TStringBuilder() << "TOP" << top << " slow event executions >1M cycles for every pool (refresh page to update)");
        for (ui32 pool = 0; pool < pools; pool++) {
            auto* row = slowDash.AddRows();
            auto* cell = row->AddCells();
            cell->SetTitle(TStringBuilder() << pool << ":" << setup->GetPoolName(pool));
            cell->SetUrl(TStringBuilder() << "?mode=log&id=.ACTORLIB_PROVIDER.SlowEvent.ppoolId=" << pool << "&s=eventMs&reverse=y&head=30");
        }
        result.push_back(slowDash);

        return result;
    }
}
