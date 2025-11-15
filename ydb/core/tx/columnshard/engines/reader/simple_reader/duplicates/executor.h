#pragma once

#include "context.h"
#include "splitter.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TBuildFilterTaskExecutor: public std::enable_shared_from_this<TBuildFilterTaskExecutor>, TNonCopyable {
private:
    inline static const ui64 BATCH_PORTIONS_COUNT_SOFT_LIMIT = 10;

    TIntervalsIterator Portions;

public:
    TBuildFilterTaskExecutor(TIntervalsIterator&& portions)
        : Portions(std::move(portions))
    {
    }

    bool ScheduleNext(TBuildFilterContext&& context);

    ~TBuildFilterTaskExecutor() {
        AFL_VERIFY(Portions.IsDone());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
