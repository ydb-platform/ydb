#include "detailed_metrics_counter_set.h"
#include "detailed_metrics_descriptor.h"

namespace NKikimr {

const TDetailedMetricsCounterNames* GetDetailedMetricsCounterNames(TTabletTypes::EType tabletType) {
    const TDetailedMetricsDescriptor* descriptor = GetDetailedMetricsDescriptor(tabletType);
    return descriptor ? &descriptor->RawNames : nullptr;
}

} // namespace NKikimr
