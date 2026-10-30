#include "vdisk_mongroups.h"
#include <ydb/core/base/feature_flags.h>


namespace NKikimr {
    namespace NMonGroup {

        bool IsExtendedVDiskCounters() {
            return NActors::TlsActivationContext
                && TActivationContext::ActorSystem()
                && AppData()->FeatureFlags.GetExtendedVDiskCounters();
        }

        NMonitoring::TCountableBase::EVisibility GetVDiskCounterVisibility() {
            return IsExtendedVDiskCounters() ? NMonitoring::TCountableBase::EVisibility::Public
                : NMonitoring::TCountableBase::EVisibility::Private;
        }

    } // NMonGroup
} // NKikimr

