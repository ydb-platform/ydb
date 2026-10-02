#pragma once

namespace NKikimr::NConveyorComposite {

enum class ESpecialTaskCategory {
    Insert = 0 /* "insert" */,
    Compaction = 1 /* "compaction" */,
    Normalizer = 2 /* "normalizer" */,
    Scan = 3 /* "scan" */,
    Deduplication = 4 /* "deduplication" */
};

}   // namespace NKikimr::NConveyorComposite
