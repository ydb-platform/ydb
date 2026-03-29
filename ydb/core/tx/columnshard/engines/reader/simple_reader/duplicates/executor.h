#pragma once

#include "common.h"
#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TBuildFilterTaskExecutor: public std::enable_shared_from_this<TBuildFilterTaskExecutor>, TNonCopyable {
private:
    TBordersIterator BordersIterator;

public:
    TBuildFilterTaskExecutor(TBordersIterator&& bordersIterator)
        : BordersIterator(std::move(bordersIterator))
    {
    }

    bool ScheduleNext(TBuildFilterContext&& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
