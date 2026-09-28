#pragma once

#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NSysView {

enum class EProcessorMode {
    MINUTE, // aggregate stats every minute
    FAST // fast mode for tests
};

constexpr size_t TOP_PARTITIONS_COUNT = 10;

} // NSysView
} // NKikimr
