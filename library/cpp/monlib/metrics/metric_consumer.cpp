#include "metric_consumer.h"

#include <util/system/yassert.h>

namespace NMonitoring {
    void IMetricConsumer::OnLabel(ui32 name, ui32 value) {
        Y_UNUSED(name, value);
        Y_ENSURE(false, "Not implemented");
    }

    std::pair<ui32, ui32> IMetricConsumer::PrepareLabel(TStringBuf name, TStringBuf value) {
        Y_UNUSED(name, value);
        Y_ENSURE(false, "Not implemented");
    }
}
