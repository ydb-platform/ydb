#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TInternalFilterConstructor::TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, TColumnDataSplitter&& splitter)
    : OriginalRequest(request)
    , Intervals(std::move(splitter)) {
    AFL_VERIFY(!!OriginalRequest);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
