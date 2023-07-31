#include "merge.h"

namespace NIPREG {

void MergeIPREGS(TReader &a, TReader& b, std::function<void(const TAddress& first, const TAddress& last, const TString *a, const TString *b)>&& proc) {
    bool hasA = a.Next();
    bool hasB = b.Next();

    TAddress top = TAddress::Lowest();
    TAddress bottom;

    do {
        // tweak ranges we've passed
        if (hasA && top > a.Get().Last)
            hasA = a.Next();
        if (hasB && top > b.Get().Last)
            hasB = b.Next();

        if (!hasA && !hasB) {
            // both rangesets have ended
            bottom = TAddress::Highest();
            proc(top, bottom, nullptr, nullptr);
            break;
        }

        const bool inA = hasA && a.Get().First <= top;
        const bool inB = hasB && b.Get().First <= top;

        if (!hasA) {
            // rangeset a has ended
            if (inB) {
                bottom = b.Get().Last;
                proc(top, bottom, nullptr, &b.Get().Data);
            } else {
                bottom = b.Get().First.Prev();
                proc(top, bottom, nullptr, nullptr);
            }
        } else if (!hasB) {
            // rangeset b has ended
            if (inA) {
                bottom = a.Get().Last;
                proc(top, bottom, &a.Get().Data, nullptr);
            } else {
                bottom = a.Get().First.Prev();
                proc(top, bottom, nullptr, nullptr);
            }
        } else if (inA && inB) {
            // inside both ranges
            bottom = Min(a.Get().Last, b.Get().Last);
            proc(top, bottom, &a.Get().Data, &b.Get().Data);
        } else if (inA) {
            // only in range a
            bottom = Min(a.Get().Last, b.Get().First.Prev());
            proc(top, bottom, &a.Get().Data, nullptr);
        } else if (inB) {
            // only in range b
            bottom = Min(b.Get().Last, a.Get().First.Prev());
            proc(top, bottom, nullptr, &b.Get().Data);
        } else {
            // outside both ranges
            bottom = Min(a.Get().First.Prev(), a.Get().First.Prev());
            proc(top, bottom, nullptr, nullptr);
        }

        top = bottom.Next();
    } while (bottom != TAddress::Highest());
}

}
