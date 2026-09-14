#pragma once

#include "common.h"
#include "context.h"
#include "private_events.h"

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

class TBuildFilterTaskExecutor: public std::enable_shared_from_this<TBuildFilterTaskExecutor>, TNonCopyable {
private:
    TBordersIterator BordersIterator;

public:
    TBuildFilterTaskExecutor(TBordersIterator&& bordersIterator);

    bool ScheduleNext(TBuildFilterContext&& context);
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
