#pragma once

#include "batch_processor.h"

#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ::NBatching {

class IBatchCutter {
public:
    virtual ~IBatchCutter() = default;

    virtual TVector<TReadResult> Cut(const TReadResult& readResult, ui64 readStartOffset) const = 0;
};

class TKafkaBatchCutter : public IBatchCutter {
public:
    TKafkaBatchCutter() = default;
    ~TKafkaBatchCutter() = default;

    TVector<TReadResult> Cut(const TReadResult& readResult, ui64 readStartOffset) const override final;
};

} // namespace NKikimr::NPQ::NBatching
