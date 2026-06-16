#pragma once

#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ::NBatching {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;

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
