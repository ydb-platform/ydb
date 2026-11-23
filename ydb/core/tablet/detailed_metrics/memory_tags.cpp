#include "memory_tags.h"

#include <ydb/library/actors/prof/tag.h>

namespace NKikimr::NDetailedMetrics {

    ui32 NodeMemoryTag() {
        static const ui32 tag = NProfiling::MakeTag("DETAILED_METRICS_NODE");
        return tag;
    }

    ui32 PayloadMemoryTag() {
        static const ui32 tag = NProfiling::MakeTag("DETAILED_METRICS_PAYLOAD");
        return tag;
    }

    ui32 ProcessorMemoryTag() {
        static const ui32 tag = NProfiling::MakeTag("DETAILED_METRICS_PROCESSOR");
        return tag;
    }

} // namespace NKikimr::NDetailedMetrics
