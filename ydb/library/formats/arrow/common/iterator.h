#pragma once

namespace NKikimr::NArrow::NUtil {

template <typename TBase, typename TDerived>
class TRandomAccessIteratorClone {
private:
    TBase Base;

public:
    using iterator_category = TBase::iterator_category;
    using difference_type = TBase::difference_type;
    using value_type = TBase::value_type;
    using pointer = TBase::pointer;
    using reference = TBase::reference;

    TRandomAccessIteratorClone() = default;
    TRandomAccessIteratorClone(const TBase& base)
        : Base(base) {
    }

    bool operator==(const TDerived& other) const {
        return Base == other.Base;
    }
    bool operator!=(const TDerived& other) const {
        return Base != other.Base;
    }

    TDerived& operator+=(const difference_type& diff) {
        Base += diff;
        return *static_cast<TDerived*>(this);
    }
    TDerived& operator-=(const difference_type& diff) {
        Base -= diff;
        return *static_cast<TDerived*>(this);
    }
    TDerived& operator++() {
        ++Base;
        return *static_cast<TDerived*>(this);
    }
    TDerived& operator--() {
        --Base;
        return *static_cast<TDerived*>(this);
    }
    TDerived operator++(int) {
        auto ret = *static_cast<TDerived*>(this);
        ++Base;
        return ret;
    }
    TDerived operator--(int) {
        auto ret = *static_cast<TDerived*>(this);
        --Base;
        return ret;
    }
    TDerived operator+(const difference_type& diff) {
        return Base + diff;
    }
    TDerived operator-(const difference_type& diff) {
        return Base - diff;
    }

    difference_type operator-(const TDerived& other) {
        return Base - other.Base;
    }

    reference operator*() {
        return *Base;
    }
    const reference operator*() const {
        return *Base;
    }
    pointer operator->() {
        return &*Base;
    }

    pointer getPtr() const {
        return Base.getPtr();
    }
    const pointer getConstPtr() const {
        return Base.getConstPtr();
    }
};

}   // namespace NKikimr::NArrow::NUtil
