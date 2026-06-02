#pragma once

#include "batch_processor.h"

#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ::NBatching {

class IBatchCutter {
public:
    virtual ~IBatchCutter() = default;

    virtual TVector<TReadResult> Cut(const TReadResult& readResult) const = 0;
};

class TKafkaBatchCutter : public IBatchCutter {
public:
    TKafkaBatchCutter() = default;
    ~TKafkaBatchCutter() = default;

    TVector<TReadResult> Cut(const TReadResult& readResult) const override final;
};

class TNoOpBatchCutter : public IBatchCutter {
public:
    TNoOpBatchCutter() = default;
    ~TNoOpBatchCutter() = default;

    TVector<TReadResult> Cut(const TReadResult& readResult) const override final {
        return {readResult};
    }
};

} // namespace NKikimr::NPQ::NBatching
