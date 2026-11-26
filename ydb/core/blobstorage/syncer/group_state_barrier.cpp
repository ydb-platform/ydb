#include "group_state_barrier.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/control/lib/dynamic_control_board_impl.h>

#include <util/string/cast.h>
#include <util/system/mutex.h>

#include <limits>

namespace NKikimr::NGroupSyncer {

namespace {

static_assert(NKikimrBlobStorage::TGroupState::EState_MAX < 63);
constexpr i64 MaxHoldStateMask = (i64{1} << (NKikimrBlobStorage::TGroupState::EState_MAX + 1)) - 1;
constexpr ui32 MaxUi32 = std::numeric_limits<ui32>::max();

// Keep subtree creation and pruning atomic with respect to other coordinators.
TMutex BarrierCountersMutex;

} // namespace

TGroupStateBarrierControls::TGroupStateBarrierControls()
    : GroupId(0, 0, MaxUi32)
    , HoldStateMask(0, 0, MaxHoldStateMask)
    , AllowGeneration(0, 0, MaxUi32)
{}

void TGroupStateBarrierControls::Register() {
    if (const auto& dcb = AppData()->Dcb) {
        dcb->RegisterSharedControl(GroupId, "Mirror3dcTransitionBarrierGroupId");
        dcb->RegisterSharedControl(HoldStateMask, "Mirror3dcTransitionBarrierHoldStateMask");
        dcb->RegisterSharedControl(AllowGeneration, "Mirror3dcTransitionBarrierAllowGeneration");
    }
}

bool TGroupStateBarrierControls::IsSelected(ui32 groupId) const {
    return HoldStateMask != 0 && static_cast<ui32>(GroupId) == groupId;
}

bool TGroupStateBarrierControls::IsEnabled(ui32 groupId, ui32 generation, EMirror3dcGroupState from) const {
    const auto state = static_cast<int>(from);
    return IsSelected(groupId) && state >= 0 && state <= NKikimrBlobStorage::TGroupState::EState_MAX
        && (static_cast<ui64>(HoldStateMask) & (ui64{1} << state))
        && static_cast<ui32>(AllowGeneration) != generation;
}

void RegisterGroupStateBarrierControls() {
    TGroupStateBarrierControls controls;
    controls.Register();
}

TGroupStateBarrier::TGroupStateBarrier(ui32 groupId, const TActorId& coordinatorId,
        const TGroupStateBarrierControls& controls)
    : GroupId(groupId)
    , CoordinatorId(coordinatorId.ToString())
    , Controls(controls)
{}

TGroupStateBarrier::~TGroupStateBarrier() {
    Reset();
    if (BarrierCounters) {
        const auto guard = Guard(BarrierCountersMutex);
        BarrierCounters->RemoveSubgroupChain({{"group", ToString(GroupId)}, {"coordinator", CoordinatorId}});
    }
}

void TGroupStateBarrier::EnsureCounters() {
    if (Held || !AppData()->Counters) {
        return;
    }
    const auto guard = Guard(BarrierCountersMutex);
    BarrierCounters = GetServiceCounters(AppData()->Counters, "bscontroller")
        ->GetSubgroup("component", "mirror3dc_transition_barrier");
    const auto counters = BarrierCounters->GetSubgroup("group", ToString(GroupId))
        ->GetSubgroup("coordinator", CoordinatorId);
    Held = counters->GetCounter("Held");
    Generation = counters->GetCounter("Generation");
    FromState = counters->GetCounter("FromState");
    ToState = counters->GetCounter("ToState");
}

bool TGroupStateBarrier::IsEnabled(ui32 generation, EMirror3dcGroupState from) const {
    return Controls.IsEnabled(GroupId, generation, from);
}

bool TGroupStateBarrier::ShouldHold(ui32 generation, EMirror3dcGroupState from, EMirror3dcGroupState to) {
    if (!IsEnabled(generation, from)) {
        Reset();
        return false;
    }

    EnsureCounters();
    if (Held) {
        Held->Set(0);
        Generation->Set(generation);
        FromState->Set(static_cast<ui32>(from));
        ToState->Set(static_cast<ui32>(to));
        Held->Set(1);
    }
    return true;
}

void TGroupStateBarrier::Reset() {
    if (Held) {
        Held->Set(0);
        Generation->Set(0);
        FromState->Set(0);
        ToState->Set(0);
    }
}

} // namespace NKikimr::NGroupSyncer
