#include "blob_refcounter.h"
#include <util/system/yassert.h>

namespace NKikimr::NPQ {

TBlobRefCounter::TBlobRefCounter(size_t count) :
    Counter(std::make_shared<size_t>(count))
{
    Y_ABORT_UNLESS(count > 0);
}

TBlobRefCounter::TBlobRefCounter(const TBlobRefCounter& rhs) :
    Counter(rhs.Counter)
{
    if (Counter) {
        Inc();
    }
}

TBlobRefCounter::~TBlobRefCounter()
{
    if (Counter && *Counter > 0) {
        Dec();
    }
}

void TBlobRefCounter::Inc() const
{
    Y_ABORT_UNLESS(Counter);
    ++*Counter;
}

void TBlobRefCounter::Dec() const
{
    Y_ABORT_UNLESS(Counter && (*Counter > 0));
    --*Counter;
}

size_t TBlobRefCounter::GetUseCount() const
{
    return Counter ? *Counter : 0;
}

void TBlobRefCounters::Append(const TBlobRefCounter& counter)
{
    Counters.push_back(counter);
}

}
