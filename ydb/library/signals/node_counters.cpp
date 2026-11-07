#include "node_counters.h"

#include <ydb/core/base/counters.h>

#include <util/generic/cast.h>
#include <util/system/mutex.h>

namespace NKikimr::NColumnShard {

namespace {

// Encoders skip a private group, so the groups held here are not exported
class THeldGroups: public ::NMonitoring::TDynamicCounters {
public:
    // Keeps the lookup counting of the tablets group, as GetSubgroup does
    explicit THeldGroups(const TDynamicCounters* tablets)
        : TDynamicCounters(tablets)
    {
        Visibility_ = EVisibility::Private;
    }

    bool Exported = false;
};

TMutex Lock;

TIntrusivePtr<THeldGroups> GetHeldGroups(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& tablets) {
    auto group = tablets->FindSubgroup("held", "columnshard");
    if (!group) {
        group = MakeIntrusive<THeldGroups>(tablets.Get());
        tablets->RegisterSubgroup("held", "columnshard", group);
    }
    return VerifyDynamicCast<THeldGroups*>(group.Get());
}

}   // namespace

TIntrusivePtr<::NMonitoring::TDynamicCounters> GetNodeServiceCounters(
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root, const TString& type)
{
    auto tablets = GetServiceCounters(root, "tablets");
    TGuard<TMutex> guard(Lock);
    auto held = GetHeldGroups(tablets);
    if (held->Exported) {
        return tablets->GetSubgroup("type", type);
    }
    return held->GetSubgroup("type", type);
}

void ExportNodeServiceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root) {
    auto tablets = GetServiceCounters(root, "tablets");
    TGuard<TMutex> guard(Lock);
    auto held = GetHeldGroups(tablets);
    if (held->Exported) {
        return;
    }
    held->Exported = true;
    for (const auto& [id, countable] : held->ReadSnapshot()) {
        held->RemoveSubgroup(id.LabelName, id.LabelValue);
        // RegisterSubgroup aborts on a group that was created under tablets directly
        if (tablets->FindSubgroup(id.LabelName, id.LabelValue)) {
            Y_DEBUG_ABORT("counters=tablets/%s=%s exists before the ColumnShard export", id.LabelName.c_str(), id.LabelValue.c_str());
            continue;
        }
        // The same objects move, so the values and the services' pointers stay valid
        tablets->RegisterSubgroup(id.LabelName, id.LabelValue, VerifyDynamicCast<::NMonitoring::TDynamicCounters*>(countable.Get()));
    }
}

}   // namespace NKikimr::NColumnShard
