#pragma once
#include <util/system/types.h>

#include <vector>

namespace NKikimr::NArrow::NSplitter {

template <class TContainer>
class TArrayView {
private:
    typename TContainer::iterator Begin;
    typename TContainer::iterator End;
public:
    TArrayView(typename TContainer::iterator itBegin, typename TContainer::iterator itEnd)
        : Begin(itBegin)
        , End(itEnd) {

    }

    typename TContainer::iterator begin() {
        return Begin;
    }

    typename TContainer::iterator end() {
        return End;
    }

    typename TContainer::value_type& front() {
        return *Begin;
    }

    typename TContainer::value_type& operator[](const size_t index) {
        return *(Begin + index);
    }

    size_t size() {
        return End - Begin;
    }
};

template <class TObject>
using TVectorView = TArrayView<std::vector<TObject>>;

class TSimilarPacker {
private:
    const ui64 BottomLimitNecessary = 0;
public:
    TSimilarPacker(const ui64 bottomLimitNecessary)
        : BottomLimitNecessary(bottomLimitNecessary)
    {

    }

    template <class TObject>
    std::vector<TVectorView<TObject>> Split(std::vector<TObject>& objects) {
        ui64 fullSize = 0;
        for (auto&& i : objects) {
            fullSize += i.GetSize();
        }
        if (fullSize <= BottomLimitNecessary) {
            return {TVectorView<TObject>(objects.begin(), objects.end())};
        }
        ui64 currentSize = 0;
        ui64 currentStart = 0;
        std::vector<TVectorView<TObject>> result;
        for (ui32 i = 0; i < objects.size(); ++i) {
            const ui64 nextSize = currentSize + objects[i].GetSize();
            const ui64 nextOtherSize = fullSize - nextSize;
            if ((nextSize >= BottomLimitNecessary && nextOtherSize >= BottomLimitNecessary) || (i + 1 == objects.size())) {
                result.emplace_back(TVectorView<TObject>(objects.begin() + currentStart, objects.begin() + i + 1));
                currentSize = 0;
                currentStart = i + 1;
            } else {
                currentSize = nextSize;
            }
        }
        return result;
    }
};

}
