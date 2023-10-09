#include "use_count_checker.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

TUseCountChecker::TUseCountChecker() {
}

TUseCountChecker::~TUseCountChecker() {
    auto count = Counter.Val();
    Y_ABORT_UNLESS(count == 0, "must not release when count is not zero: %ld", (long)count);
}

void TUseCountChecker::Inc() {
    Counter.Inc();
}

void TUseCountChecker::Dec() {
    Counter.Dec();
}

TUseCountHolder::TUseCountHolder()
    : CurrentChecker(nullptr)
{
}

TUseCountHolder::TUseCountHolder(TUseCountChecker* currentChecker)
    : CurrentChecker(currentChecker)
{
    if (!!CurrentChecker) {
        CurrentChecker->Inc();
    }
}

TUseCountHolder::~TUseCountHolder() {
    if (!!CurrentChecker) {
        CurrentChecker->Dec();
    }
}

TUseCountHolder& TUseCountHolder::operator=(TUseCountHolder that) {
    Swap(that);
    return *this;
}

void TUseCountHolder::Swap(TUseCountHolder& that) {
    DoSwap(CurrentChecker, that.CurrentChecker);
}

void TUseCountHolder::Reset() {
    TUseCountHolder tmp;
    Swap(tmp);
}
