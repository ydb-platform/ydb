#pragma once
#include <optional>

namespace NKikimr::NColumnShard {

template <class TContainer>
class TContainerAccessorWithDirection {
private:
    const TContainer& Container = nullptr;
    const bool Reverse = false;
public:
    TContainerAccessorWithDirection(const TContainer& c, const bool reverse)
        : Container(c)
        , Reverse(reverse) {

    }

    class TIterator {
    private:
        std::optional<typename TContainer::const_iterator> ForwardIterator;
        std::optional<typename TContainer::const_reverse_iterator> ReverseIterator;
    public:
        TIterator(typename TContainer::const_iterator it)
            : ForwardIterator(it) {

        }

        TIterator(typename TContainer::const_reverse_iterator it)
            : ReverseIterator(it) {

        }

        TIterator operator++() {
            if (ForwardIterator) {
                return ++(*ForwardIterator);
            } else {
                Y_ABORT_UNLESS(ReverseIterator);
                return ++(*ReverseIterator);
            }
        }

        bool operator==(const TIterator& item) const {
            if (ForwardIterator) {
                Y_ABORT_UNLESS(item.ForwardIterator);
                return *ForwardIterator == *item.ForwardIterator;
            } else {
                Y_ABORT_UNLESS(ReverseIterator);
                Y_ABORT_UNLESS(item.ReverseIterator);
                return *ReverseIterator == *item.ReverseIterator;
            }
        }

        auto& operator*() {
            if (ForwardIterator) {
                return **ForwardIterator;
            } else {
                Y_ABORT_UNLESS(ReverseIterator);
                return **ReverseIterator;
            }
        }
    };

    TIterator begin() {
        if (!Reverse) {
            return Container.begin();
        } else {
            return Container.rbegin();
        }
    }

    TIterator end() {
        if (!Reverse) {
            return Container.end();
        } else {
            return Container.rend();
        }
    }

    TIterator begin() const {
        if (!Reverse) {
            return Container.cbegin();
        } else {
            return Container.crbegin();
        }
    }

    TIterator end() const {
        if (!Reverse) {
            return Container.cend();
        } else {
            return Container.crend();
        }
    }
};

}
