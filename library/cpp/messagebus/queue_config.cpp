#include "queue_config.h"

using namespace NBus;

TBusQueueConfig::TBusQueueConfig() {
    // workers and listeners configuratioin
    NumWorkers = 1;
}

void TBusQueueConfig::ConfigureLastGetopt(
    NLastGetopt::TOpts& opts, const TString& prefix) {
    opts.AddLongOption(prefix + "worker-count")
        .RequiredArgument("COUNT")
        .DefaultValue(ToString(NumWorkers))
        .StoreResult(&NumWorkers);
}

TString TBusQueueConfig::PrintToString() const {
    TStringStream ss;
    ss << "NumWorkers=" << NumWorkers << "\n";
    return ss.Str();
}
