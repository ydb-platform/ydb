#include "durations.h"
#include "owner.h"

namespace NKikimr::NColumnShard {

namespace {
class TDurationCountersController: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
public:
    TDurationCountersController()
        : TBase("durations_controller")
    {

    }
};
}

TDurationController TDurationController::CreateController(const TString& name) {
    auto subGroup = Singleton<TDurationCountersController>()->CreateSubGroup("component", name);
    return TDurationController(subGroup.GetHistogram("DurationMs/Count", NMonitoring::LinearHistogram(22, 0, 10)),
        subGroup.GetHistogram("DurationMs/Sum", NMonitoring::LinearHistogram(22, 0, 10)));
}

}
