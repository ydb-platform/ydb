#pragma once

#include "run_params.h"

#include <util/datetime/base.h>
#include <optional>

class TTestResultCollector {
public:
    virtual void SubmitTestNameAndParams(const NKikimr::NMiniKQL::TRunParams& runParams, const char* testName, const std::optional<bool> llvm = {}, const std::optional<bool> spilling = {}) = 0;

    virtual void SubmitTimings(const TDuration& graphTime, const TDuration& referenceTime, const std::optional<TDuration> streamTime = {}) = 0;

    virtual ~TTestResultCollector() {};
};
