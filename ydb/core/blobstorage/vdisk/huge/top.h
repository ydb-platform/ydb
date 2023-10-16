#pragma once

#include "defs.h"
#include <util/generic/queue.h>

template <
    class TType,
    size_t NElements,
    class TContainer = std::vector<TType>,
    class TCompare = TLess<typename TContainer::value_type>
> class TTop {
    TContainer Cont;
    TCompare Compare;

public:
    TTop() {
        Y_DEBUG_ABORT_UNLESS(NElements > 0);
        Cont.reserve(NElements);
    }

    void Push(const TType &t) {
        auto cmp = [&] (const TType &t1, const TType &t2) {
            return Compare(t2, t1);
        };

        if (Cont.size() < NElements) {
            Cont.push_back(t);
            if (Cont.size() == NElements) {
                std::make_heap(Cont.begin(), Cont.end(), cmp);
            }
        } else if (Compare(Cont[0], t)) {
            std::pop_heap(Cont.begin(), Cont.end(), cmp);
            Cont.pop_back();
            Cont.push_back(t);
            std::push_heap(Cont.begin(), Cont.end(), cmp);
        }
    }

    const TContainer &GetContainer() const {
        return Cont;
    }

    void Output(IOutputStream &str) {
        bool first = true;
        for (const auto &x : Cont) {
            if (!first) {
                str << " ";
            } else {
                first = false;
            }
            str << x;
        }
    }
};
