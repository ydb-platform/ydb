#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NGroupSyncer {

// Three node-local shared controls, independent of the number of groups.
// Register at startup so a test can arm its target group while it is still NORMAL.
void RegisterGroupStateBarrierControls();

class TGroupStateBarrierControls {
    TControlWrapper GroupId;
    TControlWrapper HoldStateMask;
    TControlWrapper AllowGeneration;

public:
    TGroupStateBarrierControls();
    void Register();

    // A zero mask disables the barrier; group ID zero remains a valid target.
    bool IsSelected(ui32 groupId) const;
    bool IsEnabled(ui32 groupId, ui32 generation, EMirror3dcGroupState from) const;
};

class TGroupStateBarrier {
    const ui32 GroupId;
    const TString CoordinatorId;
    const TGroupStateBarrierControls& Controls;

    ::NMonitoring::TDynamicCounterPtr BarrierCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Held;
    ::NMonitoring::TDynamicCounters::TCounterPtr Generation;
    ::NMonitoring::TDynamicCounters::TCounterPtr FromState;
    ::NMonitoring::TDynamicCounters::TCounterPtr ToState;

    void EnsureCounters();

public:
    TGroupStateBarrier(ui32 groupId, const TActorId& coordinatorId, const TGroupStateBarrierControls& controls);
    ~TGroupStateBarrier();

    TGroupStateBarrier(const TGroupStateBarrier&) = delete;
    TGroupStateBarrier& operator=(const TGroupStateBarrier&) = delete;

    // Mask bits select the current state; an allowance applies only to this generation.
    bool IsEnabled(ui32 generation, EMirror3dcGroupState from) const;
    // Call only after automatic transition conditions have been checked. Held records
    // that this generation reached the barrier; it does not replace a fresh health check.
    bool ShouldHold(ui32 generation, EMirror3dcGroupState from, EMirror3dcGroupState to);
    void Reset();
};

} // namespace NKikimr::NGroupSyncer
