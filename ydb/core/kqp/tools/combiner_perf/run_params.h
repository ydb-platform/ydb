#pragma once

#include <util/system/defaults.h>
#include <optional>

namespace NKikimr {
namespace NMiniKQL {

enum class ETestMode {
    Full = 0,
    GeneratorOnly,
    RefOnly,
    GraphOnly,
};

enum class ESamplerType {
    StringKeysUI64Values,
    UI64KeysUI64Values,
};

enum class EHashMapImpl {
    UnorderedMap,
    Absl,
    YqlRobinHood,
};

struct TRunParams {
    ETestMode TestMode = ETestMode::Full;
    ESamplerType SamplerType = ESamplerType::StringKeysUI64Values;
    EHashMapImpl ReferenceHashType = EHashMapImpl::UnorderedMap;
    std::optional<ui64> RandomSeed;
    int NumAttempts = 0;
    size_t RowsPerRun = 0;
    size_t NumRuns = 0;
    size_t NumKeys = 0; // for numeric keys, the range is [0..NumKeys-1]
    size_t BlockSize = 0;
    size_t WideCombinerMemLimit = 0;
    size_t NumAggregations = 1;
    bool LongStringKeys = false;
    bool MeasureReferenceMemory = false;
    bool AlwaysSubprocess = false;
    bool EnableVerification = true;
};

}
}
