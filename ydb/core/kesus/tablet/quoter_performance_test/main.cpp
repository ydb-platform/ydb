#include <ydb/core/kesus/tablet/ut_helpers.h>
#include "options.h"
#include "session.h"
#include "test_state.h"

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

using namespace NKikimr;
using namespace NKikimr::NKesus;

void Test(const TOptions& options) {
    Cerr << "Run test with " << options.ResourcesCount << " resources and " << options.SessionsCountPerResource << " sessions per each resource." << Endl;
    Cerr << "Test time: " << options.TestTime << "." << Endl;

    TTestContext ctx;
    ctx.Setup(1, true);
    ctx.Runtime->SetLogPriority(NKikimrServices::KESUS_TABLET, NLog::PRI_WARN);

    ctx.Runtime->SetDispatchTimeout(TDuration::Minutes(10));

    auto state = MakeIntrusive<TTestState>(options, ctx);
    for (size_t session = 0; session < state->Options.SessionsCountPerResource; ++session) {
        ctx.Runtime->Register(new TSessionActor(state));
    }

    for (size_t session = 0; session < state->Options.SessionsCountPerResource; ++session) {
        ctx.ExpectEdgeEvent<TEvents::TEvWakeup>(state->EdgeActorId);
    }

    for (size_t res = 0; res < options.ResourcesCount; ++res) {
        Cerr << "\"" << GetResourceName(res) << "\": " << state->ResourcesState[res].ConsumedAmount << " units." << Endl;
    }
    Cerr << "Expected: " << (options.TestTime.Seconds() * options.MaxUnitsPerSecond) << " units per resource." << Endl;
}

int main(int argc, const char** argv) {
    TOptions options(argc, argv);
    Test(options);
}
