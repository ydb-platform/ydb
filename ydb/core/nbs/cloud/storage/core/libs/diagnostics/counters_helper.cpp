#include "counters_helper.h"

namespace NYdb::NBS {

using namespace NMonitoring;

TIntrusivePtr<TDynamicCounters> MakeVisibilitySubgroup(
    TDynamicCounters& counters,
    const TString& name,
    const TString& value,
    TCountableBase::EVisibility visibility)
{
    TIntrusivePtr<TDynamicCounters> subgroup;

    if (subgroup = counters.FindSubgroup(name, value)) {
        if (subgroup->Visibility() != visibility) {
            subgroup = MakeIntrusive<TDynamicCounters>(visibility);
            counters.ReplaceSubgroup(name, value, subgroup);
        }
    } else {
        subgroup = MakeIntrusive<TDynamicCounters>(visibility);
        counters.RegisterSubgroup(name, value, subgroup);
    }
    return subgroup;
}

}   // namespace NYdb::NBS
