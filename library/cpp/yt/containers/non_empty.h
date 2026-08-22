#pragma once

#include <library/cpp/yt/string/string_builder.h>

#include <util/system/compiler.h>

#include <initializer_list>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
class TNonEmpty
{
public:
    using TUnderlying = TContainer;

    using size_type = typename TUnderlying::size_type;
    using difference_type = typename TUnderlying::difference_type;

    using value_type = typename TUnderlying::value_type;

    using iterator = typename TUnderlying::iterator;
    using const_iterator = typename TUnderlying::const_iterator;

    using const_reverse_iterator = typename TUnderlying::const_reverse_iterator;
    using reverse_iterator = typename TUnderlying::reverse_iterator;

    using reference = typename TUnderlying::reference;
    using const_reference = typename TUnderlying::const_reference;

    using pointer = typename TUnderlying::pointer;
    using const_pointer = typename TUnderlying::const_pointer;

    explicit TNonEmpty(TContainer underlying);
    template <class TIterator>
    TNonEmpty(TIterator first, TIterator last);
    TNonEmpty(std::initializer_list<value_type> values);

    size_type size() const;

    void resize(size_type size);
    void resize(size_type size, const value_type& value);

    iterator begin();
    const_iterator begin() const;
    iterator end();
    const_iterator end() const;

    reverse_iterator rbegin();
    const_reverse_iterator rbegin() const;
    reverse_iterator rend();
    const_reverse_iterator rend() const;

    pointer data();
    const_pointer data() const;

    reference operator[](size_type index);
    const_reference operator[](size_type index) const;

    reference front();
    const_reference front() const;
    reference back();
    const_reference back() const;

    void push_back(const value_type& value);
    void push_back(value_type&& value);

    template <class... TArgs>
    reference emplace_back(TArgs&&... args);

    void pop_back();

    const TContainer& Get() const Y_LIFETIME_BOUND;
    operator const TContainer&() const Y_LIFETIME_BOUND;

private:
    TContainer Underlying_;
};

template <class TContainer>
void FormatValue(TStringBuilderBase* builder, const TNonEmpty<TContainer>& nonempty, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define NON_EMPTY_INL_H_
#include "non_empty-inl.h"
#undef NON_EMPTY_INL_H_
