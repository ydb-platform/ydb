#pragma once

#include "snapshot.h"

#include <map>

namespace NKikimr::NOlap {

// Actor-local state. Only the boundaries are durable; marking is rebuilt on load.
enum class ETruncateState {
    Created,
    MarkedPortions,
    // The cleanup transaction has removed the durable boundary. Keep it visible
    // to background tasks until Complete, but exclude it from every persist.
    RemovedFromDB,
};

using TTruncateSnapshots = std::map<TSnapshot, ETruncateState>;

}   // namespace NKikimr::NOlap
